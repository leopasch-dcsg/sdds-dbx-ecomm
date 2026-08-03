# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from ecmde_ecomm.ecomp.lineup import LineupDBXEgress,LineupDBXEgressConfig
from ecmde_ecomm.common import CredentialUtil, NotebookUtil
from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.common.spark import spark_session

# Define notebook widgets for dynamic parameters
NotebookUtil.text_widget("azure_kv_scope", "", "Azure Key-Vault Scope")
NotebookUtil.text_widget("lineup_mysql_jdbc_url", "", "Lineup MYSQL JDBC URL")
NotebookUtil.text_widget("ecomp_catalog", "", "ECOMP Catalog")

# Fetch parameters from notebook widgets
azure_kv_scope = NotebookUtil.notebook_param("azure_kv_scope")
jdbc_url = NotebookUtil.notebook_param("lineup_mysql_jdbc_url")
ecomp_catalog = NotebookUtil.notebook_param("ecomp_catalog")

# Fetch secrets from Azure Key Vault
user = CredentialUtil.secret(azure_kv_scope, "lineup-mysql-user")
password = CredentialUtil.secret(azure_kv_scope, "lineup-mysql-password")

config = LineupDBXEgressConfig(
    jdbc_url=jdbc_url,
    connection_properties={
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "rewriteBatchedStatements": "true",
        "isolationLevel": "READ_COMMITTED",
        "useServerPrepStmts": "true",
        "useCursorFetch": "true",
        "defaultFetchSize": "50000",
    }
)

spark = SparkSession.builder.getOrCreate()
dbx_loader = LineupDBXEgress(spark, config)

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.content_worklist_sku",
    partition_column="DKS_SKU",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.content_worklist_sku")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.content_worklist_sku")

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.cwl_pmms_style_task",
    partition_column="cwl_pmms_style_task_id",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.cwl_pmms_style_task")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.cwl_pmms_style_task")

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.hr_associate",
    partition_column="hr_associate_id",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.hr_associate")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.hr_associate")