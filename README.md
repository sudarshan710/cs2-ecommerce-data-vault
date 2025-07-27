# Ecommerce Data Vault & Delta Lake Architecture

This project implements a scalable and robust data architecture for ecommerce analytics using Delta Lake and Data Vault 2.0. It transforms raw operational data into clean, historized, and query-ready star schemas—designed for real-time reporting and seamless BI integration.


## 🚀 Features

- **Raw to Curated Pipeline** with schema evolution, surrogate keys, and hash generation
- **Data Vault Modeling** (Hub, Link, Satellite tables)
- **Delta Lake Capabilities**: Time Travel, GDPR delete compliance, and Vacuum retention
- **Zero-Copy Star Schema Views**
- Generate **Schema Views** for **Business Intelligence (BI) Integration** with Tableau, Power BI, etc.

## 📁 Architecture Overview

```text
Data Sources
  └─ Customer Master, Product Catalog, Sales Orders, Returns & Refunds

⬇️ Pipeline automation (hash generation, incremental loads)

⬇️ Raw Delta Lake Zone
  └─ Schema evolution, merges, surrogate keys

⬇️ Raw Delta Lake Tables
  └─ Hub, Link, Satellite tables with audit columns and hash keys

⬇️ Delta Lake Feature Layer
  └─ Time Travel, GDPR Delete, Vacuum retention

⬇️ Business Vault Zone
  └─ Zero-copy star schema views

⬇️ Publishing Layer
  └─ Star schema views

⬇️ Business Reporting / Analytics Zone
  └─ Fact & Dimension tables ready for BI tools (Tableau, Power BI)
```

## 📁 Project Structure

```text
├── __pycache__/                                # Python cache files
├── artifacts/                                  # Output or artifact files (checkpoints, logs, etc.)
├── data_source_folder/                         # Raw source data files
├── delta/                                      # Delta Lake tables storage folder
├── metastore_db/                               # Metadata storage for Spark SQL (local Derby metastore)
├── app.log                                     # Application log file
├── derby.log                                   # Derby metastore log file
├── driver.py                                   # Main driver script to run the pipeline
├── gdpr.py                                     # Script handling GDPR data deletion and compliance
├── hubs_links_sats.py                          # Script for creating Data Vault Hub, Link, and Satellite tables
├── log_after_clean_ingestion.log               # Sample Log file after clean data ingestion
├── log_after_new_customer_order.log            # Sample Log file for new customer orders processing
├── logger.py                                   # Logger configuration and utility
├── pit_table.py                                # Script managing Point-In-Time (PIT) tables for Data Vault
├── scd.log                                     # Slowly Changing Dimension log file                  
├── schema_views.py                             # Script creating star schema views from business vault
├── star_schema_scd.py                          # Script managing Slowly Changing Dimensions in star schema
├── testing.py                                  # Testing scripts and validation
├── time_travel.py                              # Script utilizing Delta Lake Time Travel features
```

## ⚙️ Technology Stack
- PySpark: 4.0.0
- Delta Lake: 4.0.0
- Hadoop: 3.0.0
- Java: 17

## ▶️ Running the Pipeline

Make sure you have the Delta Lake jars and Spark configured properly. The driver.py script accepts the path to your data source folder as a required positional argument, 
and an optional --step argument to specify which pipeline steps to execute.

### Arguments
- `data_source` (str): Path to your raw data source folder
- `--step` (str list): Steps to run. Options are:
-- `ingest` — Data ingestion
-- `vault` — Data Vault modeling (hub, link, satellite)
-- `star` — Star schema view creation
  
> By default, all steps `["ingest", "vault", "star"]` run.

```
spark-submit \
  --driver-class-path "<path-to-delta-spark-jar>;<path-to-delta-storage-jar>" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  driver.py <path-to-data-source-folder> --step <step1> <step2> ...
```

Example: Run only the Star Schema step on Windows
```
spark-submit --driver-class-path "C:\spark\jars\delta-spark_2.13-4.0.0.jar;C:\spark\jars\delta-storage-4.0.0.jar" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  driver.py "C:\Program Files\sudarshan\cs_ecommerce_data_vault" --step star
```

## 🛠️ Script Overview


| Script               | Purpose                                                                  |
| -------------------- | ------------------------------------------------------------------------ |
| `driver.py`          | Main entry point for orchestrating all steps (`ingest`, `vault`, `star`) |
| `hubs_links_sats.py` | Creates Hub, Link, and Satellite tables based on Data Vault modeling     |
| `gdpr.py`            | Handles GDPR-compliant deletion using Delta Lake APIs                    |
| `pit_table.py`       | Builds Point-In-Time (PIT) tables for efficient querying                 |
| `schema_views.py`    | Creates business-facing star schema views from the vault                 |
| `star_schema_scd.py` | Manages Slowly Changing Dimensions in star schemas                       |
| `time_travel.py`     | Demonstrates Delta Lake's Time Travel feature                            |
| `logger.py`          | Central logging configuration used across all modules                    |
| `testing.py`         | Custom scripts for testing and validation of entities


## Author

Sudarshan Zunja 

[github.com/sudarshan710](https://github.com/sudarshan710)  

## 📬 Contact
For questions, collaboration, or support, contact:

Sudarshan Zunja

📧 [sudarshanhope710@gmail.com](sudarshanhope710@gmail.com)

🔗 [github.com/sudarshan710](https://github.com/sudarshan710)  
