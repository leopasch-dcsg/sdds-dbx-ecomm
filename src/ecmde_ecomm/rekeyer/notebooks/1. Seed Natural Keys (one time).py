# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from datetime import datetime
cutoff_time = datetime.strptime('2025-03-01',"%Y-%m-%d")

# COMMAND ----------

# DBTITLE 1,Identify auto-generated columns
def get_auto_generated_columns(table_name):

    table_ddl = spark.sql(f"show create table {table_name}").collect()[0][0]

    # exclude tables without auto-generated columns
    if "AS IDENTITY" not in table_ddl:
        return None

    # exclude empty tables
    records = spark.sql(f"select count(*) from {table_name}").collect()[0][0]
    if records == 0:
        return None

    # exclude tables not updated since last sync
    last_edit = spark.sql(
        f"select timestamp from (DESCRIBE HISTORY {table_name}) order by timestamp desc limit 1"
    ).collect()[0][0]
    if last_edit < cutoff_time:
        return None

    auto_generated_column = [
        line.strip().split(" ")[0]
        for line in table_ddl.split("\n")
        if "AS IDENTITY" in line
    ][0]

    # parse and return table_name, and auto-generated column name
    return (table_name, auto_generated_column, records, last_edit)

# COMMAND ----------

# DBTITLE 1,All auto-generated keys by table
from pyspark.sql.functions import col
from concurrent.futures import ThreadPoolExecutor, as_completed

table_list = spark.sql(
  """
  select concat_ws(".", table_catalog, table_schema, table_name) as table_name 
  from prod_ecmde_db.information_schema.tables
  where table_schema not in ('test','bodi_test') --test schemas, exclude/drop
    and table_name not like '%_fix%' --leftover from schema fixes, exclude/drop
    and table_name not like '%_history' --leftover from schema fixes, exclude/drop
  """
).collect()

# go wide, queries are blocking, would take forever in sequence
table_definitions = []
with ThreadPoolExecutor(max_workers=24) as executor:
  futures = [executor.submit(get_auto_generated_columns, row.table_name) for row in table_list]
  for future in as_completed(futures):
      if future.result():
          table_definitions.append(future.result())

base_tables = spark.createDataFrame(table_definitions, ["table_name", "auto_generated_column", "records", "last_edit"])
base_tables.display()

# COMMAND ----------

# DBTITLE 1,Get best-guess natural keys (one-time)
### Only to generate the base file, do not run again, edits have been made

# import json

# bundle_path = "/Workspace/Users/2b2a244a-a0f2-40c9-afaf-7b64b9a39705/.bundle/de-ecomp-dbx/production/files"
# base_path = f"{bundle_path}/src/utils/remorph/all_remorph_configs/table_wise"

# base_table_names = base_tables.select("table_name").collect()

# natural_key_mapping = {}

# for row in base_table_names:
#     table_name = row.table_name.lower()
#     table_name = table_name.replace("prod_ecmde_db","ecomp")
#     table_name = table_name.replace(".","__")
#     file_path = f"{base_path}/{table_name}.json"
#     try:
#         with open(file_path, "r") as f:
#             table_def = json.loads(f.read())
#             join_columns = table_def["tables"][0]["join_columns"]
#             join_columns = [column.upper() for column in join_columns]
#     except:
#         join_columns = []

#     natural_key_mapping[row.table_name] = join_columns

# with open("natural_key_mapping.json","w") as f:
#     f.write(json.dumps(natural_key_mapping, indent=4))

# COMMAND ----------

# DBTITLE 1,Verify natural key mapping
from natural_key_mapping import NATURAL_KEY_MAPPING

for table, columns in NATURAL_KEY_MAPPING.items():
    if columns[0] == "IGNORE":
        continue

    column_list = ",\n\t    ".join(columns)
    sql = f"""
        SELECT 
            {column_list},
            COUNT(*)
        FROM {table}
        GROUP BY ALL
        HAVING COUNT(*) > 1
    """
    try:
        if not spark.sql(sql).isEmpty():
            print("NON-UNIQUE: ", table, columns)
    except Exception as e:
        print(f"error in read:\n{sql}")

# COMMAND ----------

