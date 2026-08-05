# Databricks notebook source

# COMMAND ----------
import sys
from databricks.sdk.runtime import dbutils
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
# Make ecmde_ecomm / sdds_ecomm importable when running interactively from a
# Databricks Git folder. The bundle job attaches the built wheel as a library,
# so this is a no-op there.
import os

try:
    import ecmde_ecomm  # noqa: F401
except ModuleNotFoundError:
    _nb_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    _idx = _nb_path.find("/src/")
    if _idx == -1:
        raise ModuleNotFoundError(
            "ecmde_ecomm is not installed and the notebook path does not contain '/src/'. "
            "Install the wheel on the cluster or run this notebook via `databricks bundle run`."
        )
    _src_dir = "/Workspace" + _nb_path[: _idx + len("/src")]
    if _src_dir not in sys.path:
        sys.path.insert(0, _src_dir)

# COMMAND ----------
from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.mysql.mysql_ingestion import MySqlIngestConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from sdds_ecomm.del_unpub_cats import DelUnpubCatsIngestionProvider

# COMMAND ----------
# Declare notebook widgets for every parameter the bronze notebook consumes.
# Bundle job runs override these via `base_parameters`; interactive runs use
# whatever is typed into the widget UI (or the default below).
NotebookUtil.text_widget("azure_kv_scope", "kv-dsg-ecmde-dbx", "Azure Key-Vault Scope")
NotebookUtil.text_widget("dbx_env", "DEV", "Databricks Env")
NotebookUtil.text_widget("dbx_user_id", "", "Databricks User Id")
NotebookUtil.text_widget(
    "mysql_jdbc_url",
    "jdbc:mysql://mysql-catalog-preview-ue-p-1.mysql.database.azure.com:3306/service_instance_db?serverTimezone=UTC&useSSL=true&requireSSL=false&allowMultiQueries=true",
    "MySQL JDBC URL",
)
NotebookUtil.text_widget("mysql_username_key", "lineup-mysql-user", "MySQL Username KV Key")
NotebookUtil.text_widget("mysql_password_key", "lineup-mysql-password", "MySQL Password KV Key")
NotebookUtil.text_widget("mysql_watermark_column", "updated_at", "MySQL Watermark Column")
NotebookUtil.text_widget("dbx_source_table", "unpublished_categories", "Source (MySQL) Table")
NotebookUtil.text_widget("dbx_destination_catalog", "dev_sdsc_db", "Destination Catalog")
NotebookUtil.text_widget("dbx_destination_schema", "sdds", "Destination Schema")
NotebookUtil.text_widget("dbx_destination_table", "unpublished_categories", "Destination Table")
NotebookUtil.text_widget(
    "dbx_merge_watermark_table_name",
    "dev_sdsc_db.sdds.watermarks",
    "Watermark Delta Table",
)

# COMMAND ----------
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


def show_mysql_schemas_and_tables(config: MySqlIngestConfig, spark, limit: int = 30) -> None:
    """
    Discovery step: show the first `limit` (schema, table) pairs reachable on
    the configured MySQL connection, excluding MySQL internal system schemas.
    Purely diagnostic — no data is written by this step.
    """
    subquery = (
        "(SELECT TABLE_SCHEMA, TABLE_NAME "
        "FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA NOT IN ('mysql','information_schema','performance_schema','sys') "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME "
        f"LIMIT {int(limit)}) t"
    )

    df = (
        spark.read.format("jdbc")
        .option("url", config.jdbc_url)
        .option("dbtable", subquery)
        .option("user", config.username)
        .option("password", config.password)
        .option("driver", MYSQL_DRIVER)
        .load()
    )

    print(f"First {limit} schemas/tables reachable on the MySQL connection:")
    df.show(limit, truncate=False)


watermark_table_name = NotebookUtil.notebook_param("dbx_merge_watermark_table_name")
if watermark_table_name is None:
    raise ElementNotFoundError(
        "The dbx_merge_watermark_table_name parameter is missing and is required for this notebook to execute."
    )

try:
    config = MySqlIngestConfig.from_notebook_params()

    # Discovery: list the first 30 schemas/tables on the MySQL connection so the
    # job log always captures what is reachable before the ingest runs.
    show_mysql_schemas_and_tables(config, spark, limit=30)

    watermark = Watermark(
        watermark_table_name,
        config.destination_catalog,
        config.destination_schema,
        config.destination_table,
        spark,
    )

    result = (
        DelUnpubCatsIngestionProvider.provider(config, watermark, spark)
        .operation()
        .execute()
    )

    print(
        f"""
        Processing complete.
        Source Table:       {result.source_table}
        Destination Table:  {result.destination_table_qualified}
        Records Processed:  {result.records_processed}
        Initial Watermark:  {result.initial_watermark_timestamp_utc.isoformat()}
        Next Watermark:     {result.next_watermark_timestamp_utc.isoformat()}
        """
    )
except Exception as e:
    print("The job has failed. See output for error details.", e)
    raise e
