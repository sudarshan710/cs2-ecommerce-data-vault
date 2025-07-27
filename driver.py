from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp
import argparse
import os
from gdpr import mask_pii
from logger import loggerF
from hubs_links_sats import hubCreator, linkCreator, satCreator
from star_schema_scd import createDimTable, createFactTable
from pit_table import customerPITTable

logger = loggerF(__name__)

spark = SparkSession.builder \
    .appName("Driver") \
    .getOrCreate()


def ingestion(iPath, iID, tableName, rawPath):
    try:
        logger.info(f"Ingesting table: {tableName} from source...")

        df = spark.read.csv(iPath, header=True, inferSchema=True)
        df_with_audit = df.withColumn("ingestion_time", current_timestamp()) \
                          .withColumn("source", lit(tableName))

        df_with_no_null = df_with_audit.filter(col(iID).isNotNull())
        df_with_null = df_with_audit.filter(col(iID).isNull())

        df_with_no_null.write.format("delta").mode("overwrite").save(f"{rawPath}/{tableName}")
        df_with_null.write.format("delta").mode("overwrite").save(f"{rawPath}/errors/{tableName}")

        logger.info(f"Ingestion complete for {tableName}")
        df_check = spark.read.format("delta").load(f"{rawPath}/{tableName}")
        df_check.show(5, truncate=False)

    except Exception as e:
        logger.error("Ingestion failed", exc_info=True)
        raise


def main(sourceFolderPath, spark: SparkSession):
    rawPath = os.path.join(sourceFolderPath, "delta", "raw")
    vaultPath = os.path.join(sourceFolderPath, "delta", "vault")
    starPath = os.path.join(sourceFolderPath, "delta", "starSCD")


    def optimize_table(path, zorder_cols: list):
        try:
            logger.info(f"Optimizing {path} on {zorder_cols} ...")
            cols = ", ".join(zorder_cols)
            query = f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})"
            spark.sql(query)
            logger.debug(f"Optimization successful for: {path}")
        except Exception as e:
            logger.error(f"Optimization failed for: {path}", exc_info=True)
            raise

    customersPath = os.path.join(sourceFolderPath, "data_source_folder", "customer.csv")
    productCatalogPath = os.path.join(sourceFolderPath, "data_source_folder", "productCatalog.csv")
    returnsRefundsPath = os.path.join(sourceFolderPath, "data_source_folder", "returnsRefunds.csv")
    salesOrderPath = os.path.join(sourceFolderPath, "data_source_folder", "salesOrder.csv")

    ingestion(customersPath, 'CustomerID', "customer", rawPath)
    ingestion(productCatalogPath, 'ProductID', "productCatalog", rawPath)
    ingestion(returnsRefundsPath, 'ReturnID', "returnsRefunds", rawPath)
    ingestion(salesOrderPath, 'OrderID', "salesOrder", rawPath)

    pii_cols = {
        "CustomerName": 'redact',
        "Email": 'hash',
    }

    customer_df = spark.read.format("delta").load(f"{rawPath}/customer")
    masked_customer_df = mask_pii(customer_df, pii_cols)
    masked_customer_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{rawPath}/customer")

    optimize_table(f"{rawPath}/customer", ["CustomerID"])
    optimize_table(f"{rawPath}/productCatalog", ["ProductID"])
    optimize_table(f"{rawPath}/returnsRefunds", ["ReturnID"])
    optimize_table(f"{rawPath}/salesOrder", ["OrderID"])

    hubCreator(rawPath, vaultPath, "customer", "CustomerID")
    hubCreator(rawPath, vaultPath, "productCatalog", "ProductID")   
    hubCreator(rawPath, vaultPath, "salesOrder", "OrderID")

    linkCreator(rawPath, vaultPath, "salesOrder", "link-order-customer-product", ["CustomerID", "OrderID", "ProductID"])
    linkCreator(rawPath, vaultPath, "returnsRefunds", "link-return-order-product-customer", ["ReturnID", "CustomerID", "OrderID", "ProductID"])

    satCreator(rawPath, vaultPath, "customer", ["CustomerID", "CustomerName", "Email", "JoinDate"])
    satCreator(rawPath, vaultPath, "salesOrder", ["OrderID", "CustomerID", "ProductID", "OrderDate", "Quantity"])
    satCreator(rawPath, vaultPath, "productCatalog", ["ProductID", "ProductName", "Category", "Price"])
    satCreator(rawPath, vaultPath, "returnsRefunds", ["ReturnID", "OrderID", "ProductID", "CustomerID", "ReturnDate", "RefundAmount", "Reason"])

    sat_df = spark.read.format("delta").load(f"{vaultPath}/sat_customer")
    sat_df.show(5)

    createDimTable(vaultPath, starPath, ["CustomerID", "CustomerName", "Email", "JoinDate"], "customer")
    createDimTable(vaultPath, starPath, ["ProductID", "ProductName", "Category", "Price"], "productCatalog")

    createFactTable(
        vaultPath, starPath,
        "customerOrders",
        "link-order-customer-product",
        sat_joins={"salesOrder": "OrderID", "customer": "CustomerID", "productCatalog": "ProductID"},
        joinKeys=["OrderID", "CustomerID", "ProductID"],
        factColumns=["OrderDate", "Quantity", "CustomerName", "Email", "Category", "Price"]
    )

    createFactTable(
        vaultPath, starPath,
        "returnRefunds",
        "link-return-order-product-customer",
        sat_joins={"returnsRefunds": "ReturnID", "salesOrder": "OrderID", "customer": "CustomerID", "productCatalog": "ProductID"},
        joinKeys=["ReturnID", "OrderID", "CustomerID", "ProductID"],
        factColumns=["ReturnDate", "RefundAmount", "Reason", "OrderDate", "Quantity", "CustomerName", "Email", "Category", "Price"]
    )

    dates = [("2025-07-29",)]
    refDates = spark.createDataFrame(dates, ["refDate"]).withColumn("refDate", to_timestamp("refDate"))

    pitCustomer = customerPITTable(vaultPath, refDates)
    pitCustomer.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_source", type=str, help="Path to your data source folder")

    args = parser.parse_args()

    main(args.data_source, spark)