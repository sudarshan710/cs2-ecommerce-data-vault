from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, to_timestamp, desc, row_number
from logger import loggerF

logger = loggerF(__name__)

spark = SparkSession.builder \
        .appName("PIT Table") \
        .getOrCreate()

def customerPITTable(vaultPath, refDates) -> DataFrame:
    try:
        logger.info(f"Creating PIT-Customer Table...")
        hubCustomer = spark.read.format("delta").load(f"{vaultPath}/hub_customer").alias("hub")
        satCustomer = spark.read.format("delta").load(f"{vaultPath}/sat_customer").alias("sat")

        hubCrossDates = hubCustomer.crossJoin(refDates).alias("hubCross")

        filtered_df = hubCrossDates.join(
            satCustomer,
            (col("hubCross.customer_HK") == col("sat.customer_HK")) &
            (col("sat.load_date") <= col("hubCross.refDate")),
            how="left"
            )

        winSpec = Window.partitionBy(col("hubCross.customer_HK"), col("hubCross.refDate")).orderBy(desc("sat.load_date"))
        latestCust = filtered_df.withColumn("row_num", row_number().over(winSpec))

        pitCustomer = latestCust.filter(col("row_num") == 1).select(
            col("hubCross.customer_HK").alias("CustomerID"),
            col("sat.CustomerName"),
            col("sat.Email"),
            col("sat.JoinDate"),
            col("sat.load_date"),
            col("hubCross.refDate")
        )
        logger.debug("PIT-Customer Table successfully created!")
        return pitCustomer
    except Exception as e:
        logger.error("PIT-Customer Table creation failed.", exc_info=True)
        return hubCustomer    