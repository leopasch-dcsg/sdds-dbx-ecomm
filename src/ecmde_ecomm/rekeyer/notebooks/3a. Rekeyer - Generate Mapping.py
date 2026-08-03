# Databricks notebook source
# MAGIC %md
# MAGIC # Must be run in traditional cluster
# MAGIC ## Maven package dependency com.oracle.ojdbc:ojdbc8:19.3.0.0

# COMMAND ----------

from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    row_number
)

from rekeyer_table_definitions import TABLE_DEFINITIONS
from oracle_helpers import oracle_query

# COMMAND ----------

def create_mapping_table(
    table:str,
    natural_keys:list[str],
    autogen_key:str,
    target_catalog:str,
    oracle_pw:str
    )->str:
    """
    Create a mapping table between Databricks and Oracle auto-generated primary keys for a given table,
    based on natural keys and a date cutoff.

    Args:
        table_definition (list[str]): Dictionary definition of the table, must include "natural_keys"
        target_catalog (str): Target catalog name.

    Returns:
        str: Confirmation message with the name of the written mapping table.

    Summary:
        - Reads records from both Oracle and Databricks tables after the date_cutoff.
        - Casts Oracle columns to match Databricks types.
        - Joins on natural keys to align auto-generated primary keys.
        - Writes the mapping table to the {REKEYER_SCHEMA} catalog.
    """
    try:
        start_time = datetime.now()
        
        oracle_table_name = table.replace("prod_ecmde_db.","")
        oracle_columns = ",".join([autogen_key]+natural_keys)

        # Read Oracle table
        ora_query = f"""
        select 
            {oracle_columns},
            DATE_ADDED as ORA_DATE_ADDED,
            DATE_LAST_MODIFIED as ORA_DATE_LAST_MODIFIED
        from {oracle_table_name}
        where date_added >= TO_DATE('{DEFAULT_DATE}', 'YYYY-MM-DD')
        """

        ora_df = oracle_query(
            spark=spark,
            query=ora_query,
            oracle_pw=oracle_pw
        )

        # Read Databricks table
        dbx_query = f"""
        select 
            {oracle_columns},
            DATE_ADDED as DBX_DATE_ADDED,
            DATE_LAST_MODIFIED as DBX_DATE_LAST_MODIFIED
        from {table}
        where date_added >= '{DEFAULT_DATE}'
        """

        dbx_df = spark.sql(dbx_query)

        # Enforce Oracle to match Databricks Typing
        for col_name, col_type in dbx_df.dtypes:
            if col_name in ora_df.columns:
                ora_df = ora_df.withColumn(col_name, col(col_name).cast(col_type))

        # Handle the fact that some natural key values can be NULL
        # Manually verified that this still produces unique results - (TJS 2025-11-14)
        natural_key_join_condition = (ora_df[natural_keys[0]].eqNullSafe(dbx_df[natural_keys[0]]))
        for natural_key in natural_keys[1:]:
            natural_key_join_condition = natural_key_join_condition & \
                                        (ora_df[natural_key].eqNullSafe(dbx_df[natural_key]))

        # Create initial mapping
        mapping_df = (
            ora_df.withColumnRenamed(autogen_key, "ORA_"+autogen_key)
            .join(
                dbx_df.withColumnRenamed(autogen_key, "DBX_"+autogen_key), 
                on=natural_key_join_condition, 
                how="full_outer"
            )
            .filter(
                col("DBX_"+autogen_key).isNotNull() #ignore records in oracle that are not in databricks yet
            )
            .filter(
                coalesce(col("DBX_"+autogen_key),lit("None")) != 
                coalesce(col("ORA_"+autogen_key),lit("None")) # filter out records that don't need rekeying
            )
            .select(
                ["DBX_"+autogen_key, "ORA_"+autogen_key] +
                [f"{table}.{key}" for key in natural_keys] +
                ["DBX_DATE_ADDED", "ORA_DATE_ADDED", "DBX_DATE_LAST_MODIFIED", "ORA_DATE_LAST_MODIFIED"]
            )
        )

        if mapping_df.isEmpty():
            run_time = (datetime.now() - start_time).seconds
            return f"Table skipped: {target_catalog}.{REKEYER_SCHEMA}.{autogen_key}_MAPPING, no records to map"

        mapping_df.cache() #hurts small tables, helps large tables, net benefit here

        # Remap potential collisions: find the maximum key in any system, increment and count up
        # Handle case where potentially a higher key exists that matches between systems by splitting
        max_dbx_id = (
            dbx_df
            .select(autogen_key)
            .agg({autogen_key:"max"})
        ).collect()[0][0]

        max_ora_id = (
            mapping_df
            .select(["ORA_"+autogen_key])
            .agg({"ORA_"+autogen_key:"max"})
        ).collect()[0][0]

        # Handle case where only Databricks has new records
        max_ora_id = max_ora_id if max_ora_id is not None else 0
        buffer_start = max(max_dbx_id, max_ora_id) + 10 # 10 is arbitrary

        good_records = mapping_df.filter(
            col("ORA_"+autogen_key).isNotNull()
        ).withColumn(
            "MAPPED_KEY",
            col("ORA_"+autogen_key)
        )

        collision_records = mapping_df.filter(
            col("ORA_"+autogen_key).isNull()
        ).withColumn(
            "MAPPED_KEY",
            buffer_start + row_number().over(Window.orderBy(col("DBX_"+autogen_key)))
        )

        fixed_mapping_df = good_records.unionAll(collision_records)

        spark.sql(f"drop table if exists {target_catalog}.{REKEYER_SCHEMA}.{autogen_key}_MAPPING")

        (
            fixed_mapping_df.write
            .mode("overwrite")
            .option("mergeSchema", "true")
            .option("delta.feature.allowColumnDefaults","enabled")
            .saveAsTable(f"{target_catalog}.{REKEYER_SCHEMA}.{autogen_key}_MAPPING")
        )

        mapping_df.unpersist()

        run_time = (datetime.now() - start_time).seconds

        return f"Table written: {target_catalog}.{REKEYER_SCHEMA}.{autogen_key}_MAPPING in {run_time}s"
    except Exception as e:
        return f"Error writing {target_catalog}.{REKEYER_SCHEMA}.{autogen_key}_MAPPING: {e}"

# COMMAND ----------

##############
# Setup
##############
dbutils.widgets.text("ecom_catalog", "dev_ecmde_db")

## do not change
ECOM_CATALOG = dbutils.widgets.get("ecom_catalog")
REKEYER_SCHEMA = "rekeyer"
DEFAULT_DATE = "2025-03-01"
ORACLE_PW = dbutils.secrets.get("kv-dsg-ecmde-dbx", "az-ecomp-ronly")

# limit to identity columns and non-bridge tables
identity_column_table_list = [
    table_definition
    for table_definition in TABLE_DEFINITIONS
    if table_definition["is_identity"] and not table_definition["is_bridge"]
]

# Create schemas if not present
spark.sql(f"use catalog {ECOM_CATALOG}")
spark.sql(f"create schema if not exists {REKEYER_SCHEMA}")

# COMMAND ----------

## Generate all mapping tables
total = len(identity_column_table_list)
complete = 0

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(
            create_mapping_table,
            table_definition["source_table_full_name"],
            table_definition["natural_keys"],
            table_definition["source_column_name"],
            ECOM_CATALOG,
            ORACLE_PW
        )
        for table_definition 
        in identity_column_table_list
    ]

    for future in as_completed(futures):
        complete += 1
        return_msg = future.result()
        print(f"[{datetime.now()}]: [{complete}/{total}] - {return_msg}")
        if "Error" in return_msg:
            raise Exception(return_msg)