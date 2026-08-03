# Databricks notebook source


from ecmde_ecomm.common.spark import spark_session
from ecmde_ecomm.common import Logger

spark = spark_session()
logger = Logger.logger("Order Service Line Notebook")

# COMMAND ----------
from datetime import datetime, timezone

from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.dbx.env import DbxDataQuality
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark


from ecmde_ecomm.fulfillment.silver.order_service_line_merge_operations import (
    WarrantyLineLoader,
)



watermark_table_name = NotebookUtil.notebook_param("dbx_merge_watermark_table_name")
if watermark_table_name is None:
    raise ElementNotFoundError(
        "The dbx_merge_watermark_table_name parameter is missing and is required "
        "for this notebook to execute."
    )



dbx_env         = NotebookUtil.notebook_param("dbx_env")
dbx_user_id     = NotebookUtil.notebook_param("dbx_user_id")
dbx_data_quality= NotebookUtil.notebook_param("dbx_data_quality")

destination_catalog = NotebookUtil.notebook_param("destination_catalog")
destination_schema  = NotebookUtil.notebook_param("destination_schema")
destination_table   = NotebookUtil.notebook_param("destination_table")

ecmde_silver_catalog = NotebookUtil.notebook_param("ecmde_silver_catalog")
ecmde_silver_schema  = NotebookUtil.notebook_param("ecmde_silver_schema")


source_catalog = ecmde_silver_catalog
source_schema  = ecmde_silver_schema
source_table   = "co_stage_order_message_placed"

config = MergeConfig(
    dbx_env=dbx_env,
    dbx_user_id=dbx_user_id,
    source_catalog=source_catalog,
    source_schema=source_schema,
    source_table=source_table,
    destination_catalog=destination_catalog,
    destination_schema=destination_schema,
    destination_table=destination_table,
)



data_quality = DbxDataQuality.from_notebook_params()


watermark = Watermark(
    watermark_table_name,
    config.destination_catalog,
    config.destination_schema,
    config.destination_table,
    spark,
)


loader = WarrantyLineLoader(
    ecmde_silver_catalog=ecmde_silver_catalog,
    ecmde_silver_schema=ecmde_silver_schema,
    destination_catalog=destination_catalog,
    destination_schema=destination_schema,
    config=config,
    watermark=watermark,
    spark=spark,
    batch_date_utc=datetime.now(timezone.utc),
)


try:

    result = loader.execute()



    logger.info(
        f"""
        Processing complete.
        Source Catalog.Schema.Table: {config.source_catalog}.{config.source_schema}.{config.source_table}
        Destination Catalog.Schema.Table: {config.destination_catalog}.{config.destination_schema}.{config.destination_table}
        Data Quality Layer: {data_quality}
        Merge watermark table: {watermark_table_name}
        """
    )
except Exception as e:
    logger.error("The job has failed. See output for error details.", {e})
    raise e
