# Databricks notebook source

# COMMAND ----------
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
from pyspark.sql.functions import col
from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.util import NotebookUtil, CredentialUtil
from ecmde_ecomm.common.logger import Logger

__logger = Logger.logger("SKU inventory data from Inventory Index")

# COMMAND ----------
try:
    es_host = NotebookUtil.notebook_param("es_host")
    if not es_host:
        raise ElementNotFoundError("'es_host' is required.")

    es_port = NotebookUtil.notebook_param("es_port", "443")

    es_user = NotebookUtil.notebook_param("es_user")
    if not es_user:
        raise ElementNotFoundError("'es_user' is required.")

    es_password_key = NotebookUtil.notebook_param("es_password_key")
    if not es_password_key:
        raise ElementNotFoundError("'es_password_key' is required.")

    es_kv_scope = NotebookUtil.notebook_param("es_kv_scope")
    if not es_kv_scope:
        raise ElementNotFoundError("'es_kv_scope' is required.")

    es_index = NotebookUtil.notebook_param("es_index")
    if not es_index:
        raise ElementNotFoundError("'es_index' is required.")

    destination_catalog = NotebookUtil.notebook_param("destination_catalog")
    if not destination_catalog:
        raise ElementNotFoundError("'destination_catalog' is required.")

    destination_schema = NotebookUtil.notebook_param("destination_schema")
    if not destination_schema:
        raise ElementNotFoundError("'destination_schema' is required.")

    destination_table = NotebookUtil.notebook_param("destination_table")
    if not destination_table:
        raise ElementNotFoundError("'destination_table' is required.")

    es_password = CredentialUtil.secret(es_kv_scope, es_password_key)
    destination_fqn = f"{destination_catalog}.{destination_schema}.{destination_table}"

    __logger.info(f"Reading from Elasticsearch index '{es_index}' at '{es_host}'.")

    df = (
        spark.read
        .format("org.elasticsearch.spark.sql")
        .option("es.net.http.auth.user", es_user)
        .option("es.net.http.auth.pass", es_password)
        .option("es.nodes", es_host)
        .option("es.port", es_port)
        .option("es.net.ssl", "true")
        .option("es.nodes.wan.only", "true")
        .option("es.nodes.discovery", "false")
        .option("es.resource", f"{es_index}/_doc")
        .load()
        .select(
            col("ecodeId").cast("string"),
            col("createdBy").cast("string"),
            col("createdOn").cast("string"),
            col("doPublish").cast("string"),
            col("messageSource").cast("string"),
            col("modifiedBy").cast("string"),
            col("modifiedOn").cast("string"),
            col("recordStatus").cast("string"),
        )
    )

    __logger.info(f"Writing to Delta table '{destination_fqn}'.")

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "false").saveAsTable(destination_fqn)

    __logger.info(f"Ingest complete. ES index '{es_index}' -> Delta table '{destination_fqn}'.")

    print(
        f"""
        Processing complete.
        Elasticsearch Index: {es_index}
        Destination Table: {destination_fqn}
        """
    )

except Exception as e:
    __logger.error("The job has failed. See output for error details.", exc_info=e)
    raise e
