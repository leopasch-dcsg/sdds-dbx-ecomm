# Databricks notebook source
# MAGIC %md
# MAGIC # Run in serverless

# COMMAND ----------

from datetime import datetime
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from rekeyer_table_definitions import TABLE_DEFINITIONS

# COMMAND ----------

##############
# Setup
##############
dbutils.widgets.text("ecom_catalog", "dev_ecmde_db")
dbutils.widgets.text("run_id", "0")

## do not change
ECOM_CATALOG = dbutils.widgets.get("ecom_catalog")
RUN_ID = int(dbutils.widgets.get("run_id"))
REKEYER_SCHEMA = "rekeyer"
PROD_CATALOG = "prod_ecmde_db"
START_TIME = datetime.now()
VERSION_HISTORY_TABLE = f"{ECOM_CATALOG}.{REKEYER_SCHEMA}.version_source_history"

# COMMAND ----------

def get_current_table_version(table_name: str) -> Tuple[str, int]:
    table_version = (
        spark.sql(f"DESCRIBE HISTORY {table_name}")
        .orderBy("version", ascending=False)
        .limit(1)
        .collect()[0]
        .version
    )

    return (table_name, table_version)

# COMMAND ----------

def rollback_table_to_version(table_name:str, table_version:int) -> str:

    # Don't change any tables that were unaffected
    _, current_version = get_current_table_version(table_name)

    if current_version == table_version:
        return f"Table {table_name} is already at version {table_version}"

    # Restore
    spark.sql(f"""
        RESTORE TABLE {table_name} TO VERSION AS OF {table_version}
    """)

    return f"Restored {table_name} to version {table_version}"

# COMMAND ----------

rollback_records = spark.sql(f"""
    select *
    from {VERSION_HISTORY_TABLE}
    where run_id = {RUN_ID}
""").collect()

for r in rollback_records:
    print(r)

# COMMAND ----------

## Rollback tables
total = len(rollback_records)
complete = 0

table_version_rows = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(
            rollback_table_to_version, 
            row.table_name,
            row.table_version
        ) 
        for row
        in rollback_records
    ]

    for future in as_completed(futures):
        complete += 1
        print(f"[{datetime.now()}]: [{complete}/{total}] - {future.result()}")


# COMMAND ----------

