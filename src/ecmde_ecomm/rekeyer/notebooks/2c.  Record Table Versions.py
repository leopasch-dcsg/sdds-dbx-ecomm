# Databricks notebook source
# MAGIC %md
# MAGIC # Run in serverless

# COMMAND ----------

from datetime import datetime
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql.utils import AnalysisException

from rekeyer_table_definitions import TABLE_DEFINITIONS

# COMMAND ----------

##############
# Setup
##############
dbutils.widgets.text("ecom_catalog", "dev_ecmde_db")
dbutils.widgets.text("test_schema", "test_schema")
dbutils.widgets.text("run_id", "test_run_id")

## do not change
ECOM_CATALOG = dbutils.widgets.get("ecom_catalog")
TEST_SCHEMA = dbutils.widgets.get("test_schema")
RUN_ID = int(dbutils.widgets.get("run_id"))
REKEYER_SCHEMA = "rekeyer"
PROD_CATALOG = "prod_ecmde_db"
START_TIME = datetime.now()
VERSION_HISTORY_TABLE = f"{ECOM_CATALOG}.{REKEYER_SCHEMA}.version_source_history"

# Create schemas if not present
spark.sql(f"use catalog {ECOM_CATALOG}")
spark.sql(f"create schema if not exists {REKEYER_SCHEMA}")
if (TEST_SCHEMA is not None) and (TEST_SCHEMA.strip() != ""):
    spark.sql(f"create schema if not exists {TEST_SCHEMA}")

# COMMAND ----------

spark.sql(f"""
create table if not exists {VERSION_HISTORY_TABLE} (
    run_id BIGINT,
    run_timestamp TIMESTAMP,
    table_name STRING,
    table_version BIGINT
)
""")

# COMMAND ----------

def get_current_table_version(table_name: str) -> Tuple[str, int]:
    table_version = 0
    try:
        table_version = (
            spark.sql(f"DESCRIBE HISTORY {table_name}")
            .orderBy("version", ascending=False)
            .limit(1)
            .collect()[0]
            .version
        )
    except AnalysisException as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in e.getMessage():
            # Table not created yet, just return 0
            pass
        else:
            raise e

    return (table_name, table_version)

# COMMAND ----------

# Unique set of targets, will include all identities and downstream
tables_to_backup = {entry["target_table_full_name"] for entry in TABLE_DEFINITIONS}

# If we aren't in production, update the catalog
if ECOM_CATALOG != PROD_CATALOG:
    tables_to_backup = {
        table.replace(PROD_CATALOG, ECOM_CATALOG) for table in tables_to_backup
    }

# If we are using a test_schema, update the schema
if (TEST_SCHEMA is not None) and (TEST_SCHEMA.strip() != ""):
    tables_to_backup = {
        ".".join(
            [
                table.split(".")[0],
                TEST_SCHEMA,
                table.split(".")[1] + "__" + table.split(".")[2],
            ]
        )
        for table in tables_to_backup
    }

for table in tables_to_backup:
    print(table)

# COMMAND ----------

## Get versions
total = len(tables_to_backup)
complete = 0

table_version_rows = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(get_current_table_version, table) 
        for table
        in tables_to_backup
    ]

    for future in as_completed(futures):
        complete += 1
        table_name, table_version = future.result()
        print(f"[{datetime.now()}]: [{complete}/{total}] - {table_name}: version {table_version}")

        table_version_rows.append(
            {
                'run_id':RUN_ID,
                'run_timestamp':START_TIME,
                'table_name':table_name,
                'table_version':table_version,
            }
        )

# COMMAND ----------

(
    spark.createDataFrame(table_version_rows)
    .write.mode("append")
    .saveAsTable(VERSION_HISTORY_TABLE)
)

# COMMAND ----------

spark.sql(
    f"""
        select *
        from {VERSION_HISTORY_TABLE}
        where run_id = {RUN_ID}     
    """
).display()