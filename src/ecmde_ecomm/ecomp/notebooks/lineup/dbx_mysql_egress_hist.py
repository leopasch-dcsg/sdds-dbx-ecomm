# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from ecmde_ecomm.ecomp.lineup import LineupEgress, LineupEgressConfig
from ecmde_ecomm.common import CredentialUtil, NotebookUtil
from ecmde_ecomm.common.spark import spark_session

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

# Create config using fetched values
config = LineupEgressConfig(
    jdbc_url=jdbc_url,
    jdbc_host=jdbc_host,
    connection_properties={
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "rewriteBatchedStatements": "true",
        "isolationLevel": "READ_COMMITTED",
    },
)

egress = LineupEgress(spark_session(), config)

# Export tables daily Line up Dim Hist

egress.write_table_trunc_hist_batch(f"{ecomp_catalog}.ecom_dim.web_product", "ecom_dim.web_product","PRODUCT_KEY","overwrite",8)
egress.write_table_trunc_hist_batch(f"{ecomp_catalog}.ecom_dim.style", "ecom_dim.style","STYLE_KEY","overwrite",8)
egress.write_table_trunc_hist_batch(f"{ecomp_catalog}.ecom_dim.dks_sku", "ecom_dim.dks_sku","DKS_SKU_KEY","overwrite",8)
egress.write_table_trunc_hist_batch(f"{ecomp_catalog}.ecom_dim.dks_sku_pim", "ecom_dim.dks_sku_pim","DKS_SKU_KEY","overwrite",8)
egress.write_table_trunc_hist_batch(f"{ecomp_catalog}.ecom_dim.bridge_web_sku_product", "ecom_dim.bridge_web_sku_product","BRIDGE_WEB_SKU_PRODUCT_KEY","overwrite",8)
