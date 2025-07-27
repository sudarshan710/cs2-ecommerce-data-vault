from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, to_timestamp, desc, row_number

spark = SparkSession.builder \
        .appName("PIT Table") \
        .getOrCreate()

df = spark.read.format("delta").load("delta/vault/sat_customer")
df.show()