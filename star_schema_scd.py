from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import expr, lit, concat_ws, sha2, col, lead, row_number, monotonically_increasing_id, when
import os
from logger import loggerF
from delta.tables import DeltaTable

# logger = loggerF(name="scdLogger", log_file="scd.log")
logger = loggerF(__name__)

spark = SparkSession.builder \
    .appName("Hubs-Links-Satellites Generator") \
    .config("spark.hadoop.hadoop.security.access.control.only.use.native", "false") \
    .getOrCreate()

def createDimTable(sourcePath, colList, dimName):
    try:
        logger.info(f"Creating Dim_{dimName}")

        inputHub = os.path.join(sourcePath, "delta", "vault", f"hub_{dimName}")
        inputSat = os.path.join(sourcePath, "delta", "vault", f"sat_{dimName}")
        outPath = os.path.join(sourcePath, "delta", "starSCD", f"dim_{dimName}")
        
        hubDF = spark.read.format("delta").load(inputHub)
        satDF = spark.read.format("delta").load(inputSat)

        joinedDF = hubDF.alias("h").join(satDF.alias("s"), col(f"h.{dimName}_HK") == col(f"s.{dimName}_HK"), "inner") \
                        .select(col(f"h.{dimName}_HK"), *[col(f"s.{c}").alias(c) for c in colList], col(f"s.load_date"))
        
        joinedDF = joinedDF.withColumn("record_HK", sha2(concat_ws("||", *[col(c).cast("string") for c in colList]), 256))

        winSpec = Window.partitionBy(f"{dimName}_HK").orderBy(col("load_date").desc())
        joinedDF = joinedDF.withColumn("row_no", row_number().over(winSpec)) \
                           .withColumn("next_record_HK", lead("record_HK").over(winSpec))
        
        joinedDF = joinedDF.withColumn("is_new", when((col("record_HK") != col("next_record_HK")) | col("next_record_HK").isNull(), lit(True)).otherwise(lit(False)))

        newDFToAddUpdate = joinedDF.filter("is_new = True")
        
        logger.info(f"Total records after join: {joinedDF.count()}, new flagged records: {newDFToAddUpdate.count()}")

        newDFToAddUpdate = newDFToAddUpdate.withColumn(f"surrogate_key_{dimName}", monotonically_increasing_id())

        newDFToAddUpdate = newDFToAddUpdate.withColumn("currentVer", when(col("row_no") == 1, lit(1)).otherwise(lit(0))) \
                       .withColumn("activeSince", col("load_date")) \
                       .withColumn("expiryDate", lit("9999-12-31").cast("date"))
        
        condition = f"target.{dimName}_HK = source.{dimName}_HK AND target.currentVer = 1"

        key_col = f"{dimName}_HK"
        newDFToAddUpdate = newDFToAddUpdate.dropDuplicates([key_col, "record_HK", "load_date"])

        key_col = f"{dimName}_HK"
        dedupWin = Window.partitionBy(key_col).orderBy(col("load_date").desc())

        newDFToAddUpdate = newDFToAddUpdate.withColumn("rn", row_number().over(dedupWin)) \
                                           .filter("rn = 1") \
                                           .drop("rn")

        newDFToAddUpdate.groupBy(f"{dimName}_HK").count().filter("count > 1").show()
        # exit()
        if newDFToAddUpdate.rdd.isEmpty():
            logger.info(f"No new records to update for Dim_{dimName}")
            return

        if DeltaTable.isDeltaTable(spark, outPath):
            delta_table = DeltaTable.forPath(spark, outPath)
            target_df = delta_table.toDF()
            target_df.filter("currentVer = 1") \
                        .groupBy(key_col).count() \
                        .filter("count > 1") \
                        .show()
            # exit()
            same_count = newDFToAddUpdate.count() == target_df.count()
            same_keys = newDFToAddUpdate.select(f"{dimName}_HK").subtract(target_df.select(f"{dimName}_HK")).isEmpty()

            if same_count and same_keys:
                logger.info(f"No changes detected for dim_{dimName}. Skipping merge.")
                return
            logger.info(f"Merging into existing Delta table at {outPath}")
            try:
                delta_table.alias("target").merge(
                    newDFToAddUpdate.alias("source"),
                    condition=expr(condition)
                ).whenMatchedUpdate(
                    condition="target.record_HK <> source.record_HK",
                    set={
                        "currentVer": lit(0),
                        "expiryDate": expr("current_date()")
                    }
                ).whenNotMatchedInsertAll().execute()
                logger.debug(f"Dim_{dimName} successfully updated/merged!")
            except Exception as e:
                logger.error("Error during merge operation", exc_info=True)
                raise
        else:
            logger.info(f"Creating new Delta dimension table at {outPath}")
            newDFToAddUpdate.write.format("delta").save(outPath)
            logger.debug(f"Dim_{dimName} created successfully!")
    except Exception as e:
        logger.error(f"Dim_{dimName} creating failed...", exc_info=True)
        raise

def createFactTable(sourcePath, factName, linkTableName, sat_joins: dict, joinKeys: list, factColumns: list):
    try:
        logger.info(f"Creating Fact_{factName} by joining link and satellite tables")

        linkPath = os.path.join(sourcePath, "delta", "vault", f"{linkTableName}")
        factDF = spark.read.format("delta").load(linkPath)
        factDF = factDF.withColumnRenamed("load_date", "load_date_link")

        for sat_name, join_col in sat_joins.items():
            satPath = os.path.join(sourcePath, "delta", "vault", f"sat_{sat_name}")
            satDF = spark.read.format("delta").load(satPath)

            for key in joinKeys:
                if key in satDF.columns:
                    satDF = satDF.withColumnRenamed(key, f"{key}_sat_{sat_name}")

            factDF = factDF.join(
                satDF,
                factDF[join_col] == satDF[f"{join_col}_sat_{sat_name}"],
                "inner"
            )

        allCols = list(set(joinKeys + factColumns + ['load_date_link']))
        factDF = factDF.select(*[col(c) for c in allCols])

        factDF = factDF.withColumn("record_HK", sha2(concat_ws("||", *[col(c).cast("string") for c in allCols]), 256))

        winSpec = Window.partitionBy(*joinKeys).orderBy(col("load_date_link").desc())

        factDF = factDF.withColumn("row_no", row_number().over(winSpec)) \
                       .withColumn("next_record_HK", lead("record_HK").over(winSpec)) \
                       .withColumn("is_new", when(
                            (col("record_HK") != col("next_record_HK")) | col("next_record_HK").isNull(),
                            lit(True)
                       ).otherwise(lit(False)))

        factDF = factDF.dropDuplicates(joinKeys + ["record_HK", "load_date_link"])

        if factDF.rdd.isEmpty():
            logger.info(f"No records found after deduplication for Fact_{factName}")
            return

        newDFToAddUpdate = factDF.filter("is_new = True") \
                                 .withColumn(f"surrogate_key_{factName}", monotonically_increasing_id()) \
                                 .withColumn("currentVer", when(col("row_no") == 1, lit(1)).otherwise(lit(0))) \
                                 .withColumn("activeSince", col("load_date_link")) \
                                 .withColumn("expiryDate", lit("9999-12-31").cast("date"))
        

        dedup_window = Window.partitionBy(*joinKeys).orderBy(col("load_date_link").desc())

        newDFToAddUpdate = newDFToAddUpdate.withColumn("row_num", row_number().over(dedup_window))

        newDFToAddUpdate = newDFToAddUpdate.filter(col("row_num") == 1).drop("row_num")

        dupes = newDFToAddUpdate.groupBy(*joinKeys).count().filter("count > 1")

        if not dupes.rdd.isEmpty():
            logger.error(f"Duplicate join keys found in source data for Fact_{factName} before merge:")
            dupes.show(truncate=False)
            raise Exception(f"Multiple source rows match the same target row for Fact_{factName}. Fix source data.")

        outPath = os.path.join(sourcePath, "delta", "starSCD", f"fact_{factName}")
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

            logger.info(f"Fact_{factName} merged successfully!")
        else:
            logger.info(f"Creating new Delta fact table at {outPath}")
            newDFToAddUpdate.write.format("delta").mode("overwrite").save(outPath)
            logger.info(f"Fact_{factName} created successfully!")

    except Exception as e:
        logger.error(f"Fact_{factName} creation failed: {e}", exc_info=True)
        raise