from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import expr, lit, concat_ws, sha2, col, lead, row_number, monotonically_increasing_id, when
import os
from logger import loggerF
from delta.tables import DeltaTable

logger = loggerF(__name__)

spark = SparkSession.builder \
    .appName("Star Schema SCD Generator") \
    .config("spark.hadoop.hadoop.security.access.control.only.use.native", "false") \
    .getOrCreate()

def createDimTable(vaultPath, starPath, colList, tableName):
    try:
        logger.info(f"Creating Dim_{tableName}")

        inputHub = os.path.join(vaultPath, f"hub_{tableName}")
        inputSat = os.path.join(vaultPath, f"sat_{tableName}")
        outPath = os.path.join(starPath, f"dim_{tableName}")

        hubDF = spark.read.format("delta").load(inputHub)
        satDF = spark.read.format("delta").load(inputSat)

        joinedDF = hubDF.alias("h").join(satDF.alias("s"), col(f"h.{tableName}_HK") == col(f"s.{tableName}_HK"), "inner") \
                        .select(col(f"h.{tableName}_HK"), *[col(f"s.{c}").alias(c) for c in colList], col("s.load_date"))

        joinedDF = joinedDF.withColumn("record_HK", sha2(concat_ws("||", *[col(c).cast("string") for c in colList]), 256))

        winSpec = Window.partitionBy(f"{tableName}_HK").orderBy(col("load_date").desc())
        joinedDF = joinedDF.withColumn("row_no", row_number().over(winSpec)) \
                           .withColumn("next_record_HK", lead("record_HK").over(winSpec)) \
                           .withColumn("is_new", when((col("record_HK") != col("next_record_HK")) | col("next_record_HK").isNull(), lit(True)).otherwise(lit(False)))

        newDFToAddUpdate = joinedDF.filter("is_new = True") \
                                   .withColumn(f"surrogate_key_{tableName}", monotonically_increasing_id()) \
                                   .withColumn("currentVer", when(col("row_no") == 1, lit(1)).otherwise(lit(0))) \
                                   .withColumn("activeSince", col("load_date")) \
                                   .withColumn("expiryDate", lit("9999-12-31").cast("date")) \
                                   .dropDuplicates([f"{tableName}_HK", "record_HK", "load_date"])

        dedupWin = Window.partitionBy(f"{tableName}_HK").orderBy(col("load_date").desc())
        newDFToAddUpdate = newDFToAddUpdate.withColumn("rn", row_number().over(dedupWin)) \
                                           .filter("rn = 1") \
                                           .drop("rn")

        if newDFToAddUpdate.rdd.isEmpty():
            logger.info(f"No new records to update for Dim_{tableName}")
            return

        condition = f"target.{tableName}_HK = source.{tableName}_HK AND target.currentVer = 1"

        if DeltaTable.isDeltaTable(spark, outPath):
            delta_table = DeltaTable.forPath(spark, outPath)
            logger.info(f"Merging into existing Delta table at {outPath}")
            delta_table.alias("target").merge(
                newDFToAddUpdate.alias("source"),
                expr(condition)
            ).whenMatchedUpdate(
                condition="target.record_HK <> source.record_HK",
                set={
                    "currentVer": lit(0),
                    "expiryDate": expr("current_date()")
                }
            ).whenNotMatchedInsertAll().execute()
            logger.debug(f"Dim_{tableName} successfully updated/merged.")
        else:
            logger.info(f"Creating new Delta dimension table at {outPath}")
            newDFToAddUpdate.write.format("delta").save(outPath)
            logger.debug(f"Dim_{tableName} created successfully.")
    except Exception as e:
        logger.error(f"Dim_{tableName} creation failed.", exc_info=True)
        raise


def createFactTable(vaultPath, starPath, factName, linkTableName, sat_joins: dict, joinKeys: list, factColumns: list):
    try:
        logger.info(f"Creating Fact_{factName}")

        linkPath = os.path.join(vaultPath, linkTableName)
        factDF = spark.read.format("delta").load(linkPath).withColumnRenamed("load_date", "load_date_link")

        for sat_name, join_col in sat_joins.items():
            satPath = os.path.join(vaultPath, f"sat_{sat_name}")
            satDF = spark.read.format("delta").load(satPath)

            for key in joinKeys:
                if key in satDF.columns:
                    satDF = satDF.withColumnRenamed(key, f"{key}_sat_{sat_name}")

            factDF = factDF.join(
                satDF,
                factDF[join_col] == satDF[f"{join_col}_sat_{sat_name}"],
                "inner"
            )

        allCols = list(set(joinKeys + factColumns + ["load_date_link"]))
        factDF = factDF.select(*[col(c) for c in allCols]) \
                       .withColumn("record_HK", sha2(concat_ws("||", *[col(c).cast("string") for c in allCols]), 256))

        winSpec = Window.partitionBy(*joinKeys).orderBy(col("load_date_link").desc())
        factDF = factDF.withColumn("row_no", row_number().over(winSpec)) \
                       .withColumn("next_record_HK", lead("record_HK").over(winSpec)) \
                       .withColumn("is_new", when((col("record_HK") != col("next_record_HK")) | col("next_record_HK").isNull(), lit(True)).otherwise(lit(False))) \
                       .dropDuplicates(joinKeys + ["record_HK", "load_date_link"])

        if factDF.rdd.isEmpty():
            logger.info(f"No records found after deduplication for Fact_{factName}")
            return

        newDFToAddUpdate = factDF.filter("is_new = True") \
                                 .withColumn(f"surrogate_key_{factName}", monotonically_increasing_id()) \
                                 .withColumn("currentVer", when(col("row_no") == 1, lit(1)).otherwise(lit(0))) \
                                 .withColumn("activeSince", col("load_date_link")) \
                                 .withColumn("expiryDate", lit("9999-12-31").cast("date"))

        dedup_window = Window.partitionBy(*joinKeys).orderBy(col("load_date_link").desc())
        newDFToAddUpdate = newDFToAddUpdate.withColumn("row_num", row_number().over(dedup_window)) \
                                           .filter(col("row_num") == 1).drop("row_num")

        dupes = newDFToAddUpdate.groupBy(*joinKeys).count().filter("count > 1")
        if not dupes.rdd.isEmpty():
            logger.error(f"Duplicate join keys found in source data for Fact_{factName}")
            dupes.show(truncate=False)
            raise Exception(f"Multiple source rows match the same target row for Fact_{factName}.")

        outPath = os.path.join(starPath, f"fact_{factName}")
        condition = " AND ".join([f"target.{k} = source.{k}" for k in joinKeys]) + " AND target.currentVer = 1"

        if DeltaTable.isDeltaTable(spark, outPath):
            delta_table = DeltaTable.forPath(spark, outPath)
            logger.info(f"Merging into existing Delta table at {outPath}")
            delta_table.alias("target").merge(
                newDFToAddUpdate.alias("source"),
                expr(condition)
            ).whenMatchedUpdate(
                condition="target.record_HK <> source.record_HK",
                set={
                    "currentVer": lit(0),
                    "expiryDate": expr("current_date()")
                }
            ).whenNotMatchedInsertAll().execute()
            logger.debug(f"Fact_{factName} merged successfully.")
        else:
            logger.info(f"Creating new Delta fact table at {outPath}")
            newDFToAddUpdate.write.format("delta").mode("overwrite").save(outPath)
            logger.debug(f"Fact_{factName} created successfully.")
    except Exception as e:
        logger.error(f"Fact_{factName} creation failed.", exc_info=True)
        raise
