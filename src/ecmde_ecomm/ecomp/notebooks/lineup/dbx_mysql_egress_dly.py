# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------
from ecmde_ecomm.ecomp.lineup import LineupEgress, LineupEgressConfig
from ecmde_ecomm.common import CredentialUtil, NotebookUtil
from ecmde_ecomm.common.spark import spark_session
import datetime

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

# Export tables daily Line up STG Full load

egress.write_table_trunc_hist_batch(f"{ecomp_catalog}.ecom.stg_ddw_inv_po_metrics", "ecom_stg.stg_ddw_inv_po_metrics","PRODUCT_ID","overwrite",8)
egress.write_table_trunc_hist_batch(f"{ecomp_catalog}.ecom.stg_ddw_purchase_orders", "ecom_stg.stg_ddw_purchase_orders","PO_HEADER_ID","overwrite",8)

# Export tables daily Line up DIM Full load

egress.write_table_trunc(f"{ecomp_catalog}.ecom_dim.color_code", "color_code")
egress.write_table_trunc(f"{ecomp_catalog}.ecom_dim.brand", "brand")
egress.write_table_trunc(f"{ecomp_catalog}.ecom_dim.size_code", "size_code")
egress.write_table_trunc(
    f"{ecomp_catalog}.ecom_dim.product_hierarchy", "product_hierarchy"
)

# Export tables daily Line up PIM DIM Full load

egress.write_table_trunc_pim(
    f"{ecomp_catalog}.ecom_dim.pim_product_emast_color", "pim_product_emast_color"
)
egress.write_table_trunc_pim(
    f"{ecomp_catalog}.ecom_dim.pim_product_emast", "pim_product_emast"
)
egress.write_table_trunc_pim(f"{ecomp_catalog}.ecom_dim.pim_product", "pim_product")

# Export tables daily Line up DIM Incremental

egress.write_table_trunc_hist_batch_2iy(f"{ecomp_catalog}.ecom_dim.web_product", "web_product")
egress.write_table_trunc_hist_batch_2iy(f"{ecomp_catalog}.ecom_dim.style", "style")
egress.write_table_trunc_hist_batch_2iy(f"{ecomp_catalog}.ecom_dim.dks_sku", "dks_sku")
egress.write_table_trunc_hist_batch_2iy(
    f"{ecomp_catalog}.ecom_dim.dks_sku_pim", "dks_sku_pim"
)
egress.write_table_trunc_hist_batch_2iy(
    f"{ecomp_catalog}.ecom_dim.bridge_web_sku_product", "bridge_web_sku_product"
)

# Export tables daily Line up ECOM Tables Full Load

egress.write_table_trunc(f"{ecomp_catalog}.ecom.cwl_hierarchy", "ecom.cwl_hierarchy")
egress.write_table_trunc(f"{ecomp_catalog}.ecom.cwl_vendor", "ecom.cwl_vendor")
