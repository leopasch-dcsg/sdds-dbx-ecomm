# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql import SparkSession
from ecmde_ecomm.ecomp.lineup import LineupDBXEgress, LineupDBXEgressConfig
from ecmde_ecomm.common.util import CredentialUtil, NotebookUtil
from databricks.sdk.runtime import dbutils
import time

# Define notebook widgets for dynamic parameters
NotebookUtil.text_widget("azure_kv_scope", "", "Azure Key-Vault Scope")
NotebookUtil.text_widget("lineup_mysql_jdbc_url", "", "Lineup MYSQL JDBC URL")
NotebookUtil.text_widget("lineup_mysql_jdbc_host", "", "Lineup MYSQL JDBC HOST")
NotebookUtil.text_widget("ecomp_catalog", "", "ECOMP Catalog")

# Fetch parameters from notebook widgets
azure_kv_scope = NotebookUtil.notebook_param("azure_kv_scope")
jdbc_url = NotebookUtil.notebook_param("lineup_mysql_jdbc_url")
jdbc_host = NotebookUtil.notebook_param("lineup_mysql_jdbc_host")
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
        # SSL & Timeout Settings for Azure
        "useSSL": "true",
        "requireSSL": "true",
        "sslMode": "REQUIRED",
        "verifyServerCertificate": "false",
        "enabledTLSProtocols": "TLSv1.2",
        "connectTimeout": "30000",
        "socketTimeout": "600000",
        "tcpKeepAlive": "true",
        "serverTimezone": "UTC" # Equivalent to your sessionVariables time_zone
    }
)

spark = SparkSession.builder.getOrCreate()

spark = SparkSession.builder.getOrCreate()

dbx_loader = LineupDBXEgress(spark, config)

start_time = time.time()
dbx_loader.execute_mysql_sp_pymysql("ecom.sp_upsert_cwl_pmms_style_task_filter_all")
end_time = time.time()

print(f"Total execution time: {end_time - start_time} seconds")


