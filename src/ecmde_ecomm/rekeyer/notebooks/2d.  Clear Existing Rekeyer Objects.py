# Databricks notebook source
# MAGIC %md
# MAGIC # Run in serverless

# COMMAND ----------

##############
# Setup
##############
dbutils.widgets.text("ecom_catalog", "dev_ecmde_db")

## do not change
ECOM_CATALOG = dbutils.widgets.get("ecom_catalog")
REKEYER_SCHEMA = "rekeyer"

# COMMAND ----------

table_name_list = spark.sql(
    f"""
    select table_name 
    from {ECOM_CATALOG}.information_schema.tables 
    where table_schema = '{REKEYER_SCHEMA}'
    """).collect()

# COMMAND ----------

for row in table_name_list:
    table_name = row.table_name

    if table_name.endswith('_rekeyed') or table_name.endswith('_mapping'):
        print(f"Dropping table: {ECOM_CATALOG}.{REKEYER_SCHEMA}.{table_name}.")
        try:
            spark.sql(f"drop table {ECOM_CATALOG}.{REKEYER_SCHEMA}.{table_name}")
        except Exception as e:
            print(f"error dropping table: {ECOM_CATALOG}.{REKEYER_SCHEMA}.{table_name}: {e}")

# COMMAND ----------

