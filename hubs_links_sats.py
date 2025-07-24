from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, concat_ws, sha2, col
import os
from logger import loggerF

logger = loggerF(__name__)

spark = SparkSession.builder \
    .appName("Hubs-Links-Satellites Generator") \
    .config("spark.hadoop.hadoop.security.access.control.only.use.native", "false") \
    .getOrCreate()

def hubCreator(sourcePath, tableName, iD):
    try:
        logger.info(f"Creating Hub_{tableName}...")

        input_path = os.path.join(sourcePath, "delta", "raw", tableName)
        print(input_path)
        output_path = os.path.join(sourcePath, "delta", "vault", f"hub_{tableName}")

        raw_df = spark.read.format("delta").load(input_path)

        hub_df = raw_df \
                  .select(iD) \
                  .dropDuplicates([iD]) \
                  .withColumn(f"{tableName}_HK", sha2(concat_ws("||", col(iD).cast("string")), 256)) \
                  .withColumn("load_time", current_timestamp())

        hub_df.write.format("delta").mode("overwrite").save(output_path)
        logger.debug(f"Hub_{tableName} successfully created!")
    except Exception as e:
        logger.error(f"Hub {tableName} creating failed...", exc_info=True)
        raise


def linkCreator(sourcePath, tableName, linkName, idsList):
    try:
        logger.info(f"Creating Link-{tableName}...")

        input_path = os.path.join(sourcePath, "delta", "raw", tableName)
        output_path = os.path.join(sourcePath, "delta", "vault", linkName)

        raw_df = spark.read.format("delta").load(input_path)

        link_df = raw_df.select(*idsList).dropDuplicates() \
                        .withColumn(linkName+"_HK", sha2(concat_ws("||", *[col(c).cast("string") for c in idsList]), 256)) \
                        .withColumn("load_date", current_timestamp()) \
                        .withColumn("source", lit(tableName + ".csv"))  
                
        link_df.write.format("delta").mode("overwrite").save(output_path)
        logger.debug(f"Link {linkName} successfully created!")
    except Exception as e: 
        logger.error(f"Link {linkName} creating failed...", exc_info=True)
        raise

def satCreator(sourcePath, tableName, colsList):
    try:
        satName = "sat_" + tableName
        logger.info(f"Creating Satellite {tableName}...")

        input_path = os.path.join(sourcePath, "delta", "raw", tableName)
        output_path = os.path.join(sourcePath, "delta", "vault", satName)

        raw_df = spark.read.format("delta").load(input_path)

        business_key = colsList[0]
        desc_cols = colsList[1:]

        sat_df = raw_df.select(*colsList) \
            .withColumn(f"{tableName}_HK", sha2(concat_ws("||", col(business_key).cast("string")), 256)) \
            .withColumn("hash_diff", sha2(concat_ws("||", *[col(c).cast("string") for c in desc_cols]), 256)) \
            .withColumn("load_date", current_timestamp())
        
        sat_df = sat_df.dropDuplicates([f"{tableName}_HK", "hash_diff"])
        
        sat_df.write.format("delta").mode("overwrite").save(output_path)
        logger.debug(f"Satellite {satName} successfully created!")
    except Exception as e:
        logger.error(f"Satellite {satName} creating failed...", exc_info=True)
        raise