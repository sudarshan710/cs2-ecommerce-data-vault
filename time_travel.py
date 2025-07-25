from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os

spark = SparkSession.builder \
    .appName("Hubs-Links-Satellites Generator") \
    .config("spark.hadoop.hadoop.security.access.control.only.use.native", "false") \
    .getOrCreate()

def read_delta_time_travel(path, version=None, timestamp=None):
    reader = spark.read.format("delta")

    if version is not None:
        reader = reader.option("versionAsOf", version)
    elif timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)

    df = reader.load(path)
    return df

hub_customer_path = os.path.join("C:\Program Files\sudarshan\cs_ecommerce_data_vault", "delta", "vault", "sat_customer")
hub_product_path = os.path.join("C:\Program Files\sudarshan\cs_ecommerce_data_vault", "delta", "vault", "sat_productCatalog")
delta_table = DeltaTable.forPath(spark, hub_customer_path)
history_df = delta_table.history()  
history_df.show(truncate=True)

v_n = 3

historical_hub_df = read_delta_time_travel(hub_customer_path, version=v_n)
historical_hub_df.show()
historical_hub_df = read_delta_time_travel(hub_product_path, version=v_n)
historical_hub_df.show()