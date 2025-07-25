from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import expr, lit, concat_ws, sha2, col, lead, row_number, monotonically_increasing_id, when
import os
from logger import loggerF
from delta.tables import DeltaTable

logger = loggerF(name="scdLogger", log_file="scd.log")

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

        if newDFToAddUpdate.rdd.isEmpty():
            logger.info(f"No new records to update for Dim_{dimName}")
            return

        if DeltaTable.isDeltaTable(spark, outPath):
            delta_table = DeltaTable.forPath(spark, outPath)
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

def createFactTable():
    pass