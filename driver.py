from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
import argparse
from gdpr import mask_pii, nullify_customer_pii_df
from logger import loggerF
from hubs_links_sats import hubCreator, linkCreator, satCreator
from star_schema_scd import createDimTable, createFactTable

logger = loggerF(__name__)

spark = SparkSession.builder \
    .appName("Driver") \
    .getOrCreate()

def ingestion(iPath, iID, tableName):
    try:
        logger.info(f"Initiating ingestion from source ({iID})...")
        table_name = iPath.split('/')[-1]

        df = spark.read.csv(iPath, header=True, inferSchema=True)
        df_with_audit = df.withColumn("ingestion_time", current_timestamp()) \
                          .withColumn("source", lit(table_name))

        df_with_no_null = df_with_audit.filter(col(iID).isNotNull())
        df_with_null = df_with_audit.filter(col(iID).isNull())

        df_with_no_null.write.format("delta").mode("overwrite").save(f"delta/raw/{tableName}")
        df_with_null.write.format("delta").mode("overwrite").save(f"delta/raw/errors/{tableName}")

        logger.debug("Data successfully ingested from source...")
    except Exception as e:
        logger.error("Ingestion failed...", exc_info=True)
        raise

def main(sourceFolderPath, spark: SparkSession):
    def optimize_table(path, zorder_cols: list):
        cols = ", ".join(zorder_cols)
        query = f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})"
        spark.sql(query)

    customersPath = sourceFolderPath +  "/data_source_folder/customer.csv"
    productCatalogPath = sourceFolderPath + "/data_source_folder/productCatalog.csv"
    returnsRefundsPath = sourceFolderPath + "/data_source_folder/returnsRefunds.csv"
    salesOrderPath = sourceFolderPath + "/data_source_folder/salesOrder.csv"

    ingestion(customersPath, 'CustomerID', "customer")
    ingestion(productCatalogPath, 'ProductID', "productCatalog")
    ingestion(returnsRefundsPath, 'ReturnID', "returnsRefunds")
    ingestion(salesOrderPath, 'OrderID', "salesOrder")
    
    pii_cols = {
        "CustomerName": 'redact',
        "Email": 'hash',
    }
    df = spark.read.format("delta").load("delta/raw/customer")
    masked_df = mask_pii(df, pii_cols)
    masked_df.write.format("delta").mode("overwrite").save("delta/raw/customer")

    # optimize_table("/delta/raw/customer", ["CustomerID"])
    # optimize_table("/delta/raw/productCatalog", ["ProductID"])
    # optimize_table("/delta/raw/returnsRefunds", ["ReturnID"])
    # optimize_table("/delta/raw/salesOrder", ["OrderID"])

    hubCreator(sourceFolderPath, "customer", "CustomerID")
    hubCreator(sourceFolderPath, "productCatalog", "ProductID")
    hubCreator(sourceFolderPath, "salesOrder", "OrderID")

    # # optimize_table("/delta/vault/hub_customer", ["CustomerID"])
    # # optimize_table("/delta/vault/hub_productCatalog", ["ProductID"])
    # # optimize_table("/delta/vault/hub_salesOrder", ["OrderID"])

    link1Name = "link-order-customer-product"
    link1TableIDs = ["CustomerID", "OrderID", "ProductID"]

    link2Name = "link-return-order-product-customer"
    link2TableIDs = ["ReturnID", "CustomerID", "OrderID", "ProductID"]

    linkCreator(sourceFolderPath, "salesOrder", link1Name, link1TableIDs)
    linkCreator(sourceFolderPath, "returnsRefunds", link2Name, link2TableIDs)

    # # optimize_table(f"/delta/vault/{link1Name}", link1TableIDs)
    # # optimize_table(f"/delta/vault/{link2Name}", link2TableIDs)

    customerColumns = ["CustomerID","CustomerName","Email","JoinDate"]
    salesOrderColumns = ["OrderID","CustomerID","ProductID","OrderDate","Quantity"]
    productCatalogColumns = ["ProductID","ProductName","Category","Price"]
    returnsRefundsColumns = ["ReturnID","OrderID","ProductID","CustomerID","ReturnDate","RefundAmount","Reason"]

    satCreator(sourceFolderPath, "customer", customerColumns)
    satCreator(sourceFolderPath, "salesOrder", salesOrderColumns)
    satCreator(sourceFolderPath, "productCatalog", productCatalogColumns)
    satCreator(sourceFolderPath, "returnsRefunds", returnsRefundsColumns)

    # # optimize_table("/delta/vault/sat_customer", ["CustomerID", "JoinDate"])
    # # optimize_table("/delta/vault/sat_salesOrder", ["OrderID", "OrderDate"])
    # # optimize_table("/delta/vault/sat_productCatalog", ["ProductID"])
    # # optimize_table("/delta/vault/sat_returnsRefunds", ["ReturnID", "ReturnDate"])

    createDimTable(sourceFolderPath, ["CustomerID","CustomerName","Email","JoinDate"], "customer")
    createDimTable(sourceFolderPath, ["ProductID","ProductName","Category","Price"], "productCatalog")

    # optimize_table("/delta/starSCD/dim_customer", ["CustomerID"])
    # optimize_table("/delta/starSCD/dim_productCatalog", ["ProductID", "Category"])

    createFactTable(sourceFolderPath, "customerOrders", "link-order-customer-product", sat_joins={"salesOrder": "OrderID", "customer": "CustomerID", "productCatalog": "ProductID"}, joinKeys=["OrderID", "CustomerID", "ProductID"], factColumns=["OrderDate", "Quantity", "CustomerName", "Email", "Category", "Price"])
    createFactTable(sourceFolderPath, "returnRefunds", "link-return-order-product-customer", sat_joins={"returnsRefunds": "ReturnID", "salesOrder": "OrderID", "customer": "CustomerID", "productCatalog": "ProductID"}, joinKeys=["ReturnID", "OrderID", "CustomerID", "ProductID"], factColumns=["ReturnDate", "RefundAmount", "Reason", "OrderDate", "Quantity", "CustomerName", "Email", "Category", "Price"])

    pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_source", type=str, help="Path to your data source folder")

    args = parser.parse_args()

    main(args.data_source, spark)