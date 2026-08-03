# Databricks notebook source
# MAGIC %md
# MAGIC # Run in serverless

# COMMAND ----------

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from rekeyer_table_definitions import TABLE_DEFINITIONS

# COMMAND ----------

##############
# Setup
##############
dbutils.widgets.text("ecom_catalog", "dev_ecmde_db")
dbutils.widgets.text("test_schema", "test_schema")

## do not change
ECOM_CATALOG = dbutils.widgets.get("ecom_catalog")
TEST_SCHEMA = dbutils.widgets.get("test_schema")
REKEYER_SCHEMA = "rekeyer"
PROD_CATALOG = "prod_ecmde_db"

# Create schemas if not present
spark.sql(f"use catalog {ECOM_CATALOG}")
spark.sql(f"create schema if not exists {REKEYER_SCHEMA}")

if (TEST_SCHEMA is not None) and (TEST_SCHEMA.strip() != ""):
    spark.sql(f"create schema if not exists {TEST_SCHEMA}")

# COMMAND ----------

# Stop the run if we are copying prod to prod without test schema
if (ECOM_CATALOG == PROD_CATALOG):
    if (TEST_SCHEMA is None) or (TEST_SCHEMA.strip() == ""):
        dbutils.notebook.exit("don't need to copy prod to prod catalog")

# COMMAND ----------

tables_to_clone = set()

for entry in TABLE_DEFINITIONS:
    tables_to_clone.update([entry['source_table_full_name'], entry['target_table_full_name']])

source_target_list = []
for table in tables_to_clone:
    _,source_schema,source_table = table.split(".")

    # Check and handle test schema being set
    if (TEST_SCHEMA is not None) and (TEST_SCHEMA.strip() != ""):
        target_table = f"{ECOM_CATALOG}.{TEST_SCHEMA}.{source_schema}__{source_table}" 
    else:
        target_table = f"{ECOM_CATALOG}.{source_schema}.{source_table}"

    source_target_list.append((table,target_table))

for s_t in source_target_list:
    print(s_t)

# COMMAND ----------

def clone_table(source_table:str, target_table:str) -> str:
    if ('prod_ecmde_db' in target_table):
        if (TEST_SCHEMA is None) or (TEST_SCHEMA.strip() == ""):
            return (f"DO NOT OVERWRITE LIVE PROD {source_table} -> {target_table}")

    spark.sql(f"""
        create or replace table {target_table} clone {source_table}
    """)

    return f"Cloned {source_table} -> {target_table}"

# COMMAND ----------

## Clone tables
total = len(source_target_list)
complete = 0

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(clone_table, source_table, target_table) 
        for source_table, target_table 
        in source_target_list
    ]

    for future in as_completed(futures):
        complete += 1
        print(f"[{datetime.now()}]: [{complete}/{total}] - {future.result()}")

# COMMAND ----------

