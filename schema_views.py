from pyspark.sql import SparkSession
import os
from logger import loggerF

logger = loggerF(__name__)

spark = SparkSession.builder \
    .appName("Star Schema Views") \
    .enableHiveSupport() \
    .getOrCreate()

def createSchemaView(starPath, databaseName, dimTables, factTables):
    try:
        logger.info("Create Schema Tables for Database...")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName}")

        for dimT in dimTables:
            dimTablePath = os.path.join(starPath, f"dim_{dimT}")
            tableName = f"{databaseName}.dim_{dimT}"
            logger.info(f"Registering dimension table: {tableName} from path: {dimTablePath}")

            spark.read.format("delta").load(dimTablePath) \
                      .write.format("delta") \
                      .mode("overwrite") \
                      .saveAsTable(tableName)

            logger.debug(f"Successfully registered dimension table: {tableName}")

        for factT in factTables:
            factTablePath = os.path.join(starPath, f"fact_{factT}")
            tableName = f"{databaseName}.fact_{factT}"
            logger.info(f"Registering fact table: {tableName} from path: {factTablePath}")

            spark.read.format("delta").load(factTablePath) \
                      .write.format("delta") \
                      .mode("overwrite") \
                      .saveAsTable(tableName)
            
            logger.debug(f"Successfully registered fact table: {tableName}")
        
        logger.info(f"Schema registration successful for database: {databaseName}")

    except Exception as e:
        logger.error(f"Schema view creation failed!", exc_info=True)
        raise