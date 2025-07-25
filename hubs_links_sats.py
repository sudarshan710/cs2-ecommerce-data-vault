from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, concat_ws, sha2, col, expr
import os
from logger import loggerF
from delta.tables import DeltaTable

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
        
        condition = f"target.{tableName}_HK = source.{tableName}_HK"

        if DeltaTable.isDeltaTable(spark, output_path):
            delta_table = DeltaTable.forPath(spark, output_path)
            target_df = delta_table.toDF()
            same_count = hub_df.count() == target_df.count()
            same_keys = hub_df.select(f"{tableName}_HK").subtract(target_df.select(f"{tableName}_HK")).isEmpty()

            if same_count and same_keys:
                logger.info(f"No changes detected for Hub_{tableName}. Skipping merge.")
                return
            delta_table.alias("target").merge(
                source=hub_df.alias("source"),
                condition=expr(condition)
            ).whenNotMatchedInsertAll().execute()
            logger.debug(f"Hub_{tableName} successfully updated/merged!")
        else:
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
                        .withColumn("source", lit(f"{tableName}"))
        
        condition = f"target.`{linkName}_HK` = sourceLink.`{linkName}_HK`"

        if DeltaTable.isDeltaTable(spark, output_path):
            delta_table = DeltaTable.forPath(spark, output_path)
            target_df = delta_table.toDF()
            same_count = link_df.count() == target_df.count()
            same_keys = link_df.select(f"{linkName}_HK").subtract(target_df.select(f"{linkName}_HK")).isEmpty()

            if same_count and same_keys:
                logger.info(f"No changes detected for link_{linkName}. Skipping merge.")
                return
            delta_table.alias("target").merge(
                source=link_df.alias("sourceLink"),
                condition=expr(condition)
            ).whenNotMatchedInsertAll().execute()
            logger.debug(f"Link {linkName} successfully updated/merged!")
        else:
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

        condition = f"target.{tableName}_HK = sourceSat.{tableName}_HK AND target.hash_diff = sourceSat.hash_diff"

        
        if DeltaTable.isDeltaTable(spark, output_path):
            delta_table = DeltaTable.forPath(spark, output_path)
            target_df = delta_table.toDF()
            same_count = sat_df.count() == target_df.count()
            same_keys = sat_df.select(f"{tableName}_HK").subtract(target_df.select(f"{tableName}_HK")).isEmpty()

            if same_count and same_keys:
                logger.info(f"No changes detected for Sat_{satName}. Skipping merge.")
                return
            delta_table.alias("target").merge(
                source=sat_df.alias("sourceSat"),
                condition=expr(condition)
            ).whenNotMatchedInsertAll().execute()
            logger.debug(f"Satellite {satName} successfully updated/merged!")
        else:        
            sat_df.write.format("delta").mode("overwrite").save(output_path)    
            logger.debug(f"Satellite {satName} successfully created!")
    except Exception as e:
        logger.error(f"Satellite {satName} creating failed...", exc_info=True)
        raise