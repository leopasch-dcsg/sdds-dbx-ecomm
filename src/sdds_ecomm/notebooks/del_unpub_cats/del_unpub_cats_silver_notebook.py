# Databricks notebook source

# COMMAND ----------
from databricks.sdk.runtime import dbutils
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
# Make ecmde_ecomm / sdds_ecomm importable when running interactively from a
# Databricks Git folder. The bundle job attaches the built wheel as a library,
# so this is a no-op there.
import sys

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
from ecmde_ecomm.common.util import NotebookUtil
from sdds_ecomm.del_unpub_cats.silver.del_unpub_cats_silver_operations import (
    SilverUnpublishedCategoriesTransform,
)

# COMMAND ----------
# Declare widgets so interactive runs work without job base_parameters.
NotebookUtil.text_widget("dbx_user_id", "", "Databricks User Id")
NotebookUtil.text_widget(
    "dbx_bronze_table_qualified",
    "dev_sdsc_db.sdds.unpublished_categories",
    "Bronze Delta Table (catalog.schema.table)",
)
NotebookUtil.text_widget(
    "dbx_silver_table_qualified",
    "dev_sdsc_db.sdds.unpublished_categories_latest",
    "Silver Delta Table (catalog.schema.table)",
)

# COMMAND ----------
bronze_table = NotebookUtil.notebook_param("dbx_bronze_table_qualified")
silver_table = NotebookUtil.notebook_param("dbx_silver_table_qualified")
dbx_user_id = NotebookUtil.notebook_param("dbx_user_id")

try:
    transform = SilverUnpublishedCategoriesTransform(
        spark=spark,
        bronze_table=bronze_table,
        silver_table=silver_table,
        dbx_user_id=dbx_user_id,
    )

    records = transform.execute()

    print(
        f"""
        Silver transform complete.
        Bronze Table:      {bronze_table}
        Silver Table:      {silver_table}
        Records Written:   {records}
        """
    )
except Exception as e:
    print("The job has failed. See output for error details.", e)
    raise e
