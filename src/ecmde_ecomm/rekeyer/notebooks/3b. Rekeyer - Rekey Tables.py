# Databricks notebook source
# MAGIC %md
# MAGIC # Run in serverless

# COMMAND ----------

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql.functions import lit

from rekeyer_table_definitions import TABLE_DEFINITIONS

# COMMAND ----------

def rekey_identity_column(
    target_table_name: str,
    identity_column_name: str,
    natural_keys: list[str],
) -> str:
    """
    Rekey an identity column in a target table by replacing the target column's values with
    corresponding values from a mapping table. Use this method only when the target table 
    has an explicit identity column definition.

    Args:
        target_table_name (str): Name of the target table to update.
        identity_column_name (str): Name of the identity column used for mapping.
        natural_keys (list[str]): List of natural keys to join on.

    Summary:
        - Selects records needing rekeying based on the mapping table.
        - Writes updated records to a temporary table.
        - Deletes the affected records from the source table.
        - Inserts the rekeyed records back into the source table.
        - Resynchronizes the identity column.
    """

    # Skip if no mapping table
    if not spark.catalog.tableExists(f"{ECOM_CATALOG}.{REKEYER_SCHEMA}.{identity_column_name}_mapping"):
        return f"Skipping rekey of {target_table_name}:{identity_column_name}, mapping table does not exist"

    try:
        natural_key_join_condition = " and ".join(
            [f"t1.{nat_key} <=> t2.{nat_key}" for nat_key in natural_keys]
        )

        updated_records = spark.sql(
            f"""
            select 
                t1.{identity_column_name} as OLD_{identity_column_name},
                t2.MAPPED_KEY as NEW_{identity_column_name},
                t1.* except ({identity_column_name})
            from {target_table_name} t1
            inner join {ECOM_CATALOG}.{REKEYER_SCHEMA}.{identity_column_name}_mapping t2
                    on {natural_key_join_condition}
            where t1.date_added >= '{DEFAULT_DATE}'
              and t1.{identity_column_name} != t2.MAPPED_KEY
            """
        )

        updated_records = updated_records.withColumn(
            "date_last_modified",
            lit(datetime.now())
        )

        # store the rekeyed records in a temporary location
        table_suffix = target_table_name.split(".")[-1]
        (
            updated_records.write.mode("overwrite")
            .option(
                "delta.feature.allowColumnDefaults", "supported" 
            ) # must be set because of source table constraints
            .saveAsTable(f"{ECOM_CATALOG}.{REKEYER_SCHEMA}.{table_suffix}_rekeyed")
        )

        # clear the subsection of the source table
        spark.sql(
            f"""
            delete from {target_table_name}
            where {identity_column_name} in (
                select OLD_{identity_column_name}
                from {ECOM_CATALOG}.{REKEYER_SCHEMA}.{table_suffix}_rekeyed
            )
            """
        )

        # insert all of the rekeyed records
        spark.sql(
            f"""
            insert into {target_table_name}
            select 
                NEW_{identity_column_name} as {identity_column_name},
                t1.* except(
                    OLD_{identity_column_name}, 
                    NEW_{identity_column_name}
                )
            from {ECOM_CATALOG}.{REKEYER_SCHEMA}.{table_suffix}_rekeyed t1
            """
        )

        # Resync identity column
        spark.sql(
            f"""
            alter table {target_table_name} alter column {identity_column_name} sync identity
            """
        )
        return f"Rekeyed {target_table_name}:{identity_column_name}"
    except Exception as e:
        return f"Error in rekey of {target_table_name}:{identity_column_name}: {e}"

# COMMAND ----------

def rekey_nonidentity_column(
    source_table_name: str,
    identity_column_name: str,
    target_table_name: str,
    target_column_name: str,
) -> str:
    """
    Rekey a non-identity column in a target table by updating the target column's values
    using a mapping table. This method should only be used on tables without an explicit
    identity column definition, as updating identity columns is not supported. Seeing an
    error `[DELTA_IDENTITY_COLUMNS_UPDATE_NOT_SUPPORTED]` indicates that the target table
    has an identity column.

    Args:
        source_table_name: (str): Name of the source table containing the identity_column.
        identity_column_name (str): Name of the identity column used for mapping.
        target_table_name (str): Name of the target table to update.
        target_column_name (str): Name of the column to be rekeyed.

    Summary:
        - Merges the target table with the mapping table on the specified column.
        - Updates the target column with the corresponding value from the mapping table.
    """

    # Skip if no mapping table
    if not spark.catalog.tableExists(f"{ECOM_CATALOG}.{REKEYER_SCHEMA}.{identity_column_name}_mapping"):
        return f"Skipping rekey of {target_table_name}:{identity_column_name}, mapping table does not exist"

    try:
        # update all keys
        spark.sql(
            f"""
            MERGE INTO {target_table_name} AS t1
            USING {ECOM_CATALOG}.{REKEYER_SCHEMA}.{identity_column_name}_mapping AS t2
            ON t1.{target_column_name} = t2.DBX_{identity_column_name}
            WHEN MATCHED THEN
            UPDATE SET 
                t1.{target_column_name} = t2.MAPPED_KEY,
                t1.date_last_modified = current_timestamp()
            """
        )
        return f"Rekeyed {target_table_name}:{target_column_name} from {source_table_name}:{identity_column_name}"
    except Exception as e:
        return f"Error in rekey of {target_table_name}:{target_column_name}: {e}"


# COMMAND ----------

##############
# Setup
##############
dbutils.widgets.text("ecom_catalog", "dev_ecmde_db")
dbutils.widgets.text("test_schema", "rekeyer_test")

## do not change
ECOM_CATALOG = dbutils.widgets.get("ecom_catalog")
TEST_SCHEMA = dbutils.widgets.get("test_schema")
REKEYER_SCHEMA = "rekeyer"
PROD_CATALOG = "prod_ecmde_db"
DEFAULT_DATE = "2025-03-01"

# Create schemas if not present
spark.sql(f"use catalog {ECOM_CATALOG}")
spark.sql(f"create schema if not exists {REKEYER_SCHEMA}")
if (TEST_SCHEMA is not None) and (TEST_SCHEMA.strip() != ""):
    spark.sql(f"create schema if not exists {TEST_SCHEMA}")

# COMMAND ----------

## Update table definitions for current environment
for table_definition in TABLE_DEFINITIONS:
    source_table_catalog, source_table_schema, source_table_name = table_definition['source_table_full_name'].split(".")
    target_table_catalog, target_table_schema, target_table_name = table_definition['target_table_full_name'].split(".")
    
    if TEST_SCHEMA is not None and TEST_SCHEMA.strip() != "":
        table_definition["source_table_full_name"] = f"{ECOM_CATALOG}.{TEST_SCHEMA}.{source_table_schema}__{source_table_name}"
        table_definition["target_table_full_name"] = f"{ECOM_CATALOG}.{TEST_SCHEMA}.{target_table_schema}__{target_table_name}"
    else:
        table_definition["source_table_full_name"] = f"{ECOM_CATALOG}.{source_table_schema}.{source_table_name}"
        table_definition["target_table_full_name"] = f"{ECOM_CATALOG}.{target_table_schema}.{target_table_name}"

# COMMAND ----------

## Rekey identity tables
identity_table_definitions = [
    table_definition
    for table_definition in TABLE_DEFINITIONS
    if table_definition["is_identity"] and not table_definition["is_bridge"]
]

total = len(identity_table_definitions)
complete = 0

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = []

    for table_definition in identity_table_definitions:
        futures.append(
            executor.submit(
                rekey_identity_column,
                table_definition['target_table_full_name'],
                table_definition['source_column_name'],
                table_definition['natural_keys'],
            )
        )

    for future in as_completed(futures):
        complete += 1
        return_msg = future.result()
        print(f"[{datetime.now()}]: [{complete}/{total}] - {return_msg}")
        if "Error" in return_msg:
            raise Exception(return_msg)

# COMMAND ----------

## Rekey downstream tables
downstream_table_definitions = [
    table_definition
    for table_definition in TABLE_DEFINITIONS
    if not table_definition["is_identity"] and not table_definition["is_bridge"]
]

total = len(downstream_table_definitions)
complete = 0

# in series for now - must avoid concurrent updates
# can do with asyncio and blocking, or a queue + ordering logic (preliminary est. ~3min in parallel)
for table_definition in downstream_table_definitions:

    return_msg = rekey_nonidentity_column(
        table_definition["source_table_full_name"],
        table_definition["source_column_name"],
        table_definition["target_table_full_name"],
        table_definition["target_column_name"],
    )

    complete += 1
    print(f"[{datetime.now()}]: [{complete}/{total}] - {return_msg}")

    if "Error" in return_msg:
        raise Exception(return_msg)

# COMMAND ----------

