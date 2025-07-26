from pyspark.sql.functions import sha2, col, lit, when
from pyspark.sql import DataFrame
from logger import loggerF

logger = loggerF(__name__)

def mask_pii(df: DataFrame, pii_columns: dict) -> DataFrame:
    logger.info("Initiating PII Masking of Data...")
    try:
        for col_name, mask_type in pii_columns.items():
            if col_name in df.columns:
                logger.info(f"Masking column '{col_name}' using method '{mask_type}'")
                try:
                    if mask_type == "hash":
                        df = df.withColumn(col_name, sha2(col(col_name).cast("string"), 256))
                    elif mask_type == "redact":
                        df = df.withColumn(col_name, lit("REDACTED"))
                    else:
                        logger.error(f"Unknown mask type '{mask_type}' for column '{col_name}'")
                        raise ValueError(f"Unknown mask type: {mask_type}")
                    logger.info(f"Successfully masked column '{col_name}'")
                except Exception as col_err:
                    logger.error(f"Failed to mask column '{col_name}': {col_err}", exc_info=True)
                    raise
            else:
                logger.warning(f"Column '{col_name}' not found in DataFrame; skipping masking for this column.")

        df.show()
        logger.info("Completed PII masking for all specified columns.")
        return df

    except Exception as e:
        logger.error(f"Error during PII masking: {e}", exc_info=True)
        raise


def nullify_customer_pii_df(df: DataFrame, customer_id, pii_columns):
    logger.info("Soft deleting User with nullification...")
    try:
        for col_name in pii_columns:
            df = df.withColumn(
                col_name,
                when(col("CustomerID") == customer_id, None).otherwise(col(col_name))
            )
            logger.debug("Soft deletion successful!")
            return df
    except Exception as e:
        logger.error("Soft deletion failed...", exc_info=True)
        raise
    