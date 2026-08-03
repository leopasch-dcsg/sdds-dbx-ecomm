# Databricks notebook source

# COMMAND ----------
import sys
from databricks.sdk.runtime import dbutils
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.dbx.env import DbxDataQuality
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.ecomp.co_stage.orders_merge_operations_provider import (
    OrdersMergeOperationProvider,
)

watermark_table_name = NotebookUtil.notebook_param("dbx_merge_watermark_table_name")
if watermark_table_name is None:
    raise ElementNotFoundError(
        "The dbx_merge_watermark_table_name parameter is missing and is required for this notebook to execute."
    )

try:
    config = MergeConfig.from_notebook_params()
    watermark = Watermark(
        watermark_table_name,
        config.destination_catalog,
        config.destination_schema,
        config.destination_table,
        spark,
    )

    data_quality = DbxDataQuality.from_notebook_params()
    result = (
        OrdersMergeOperationProvider.provider(config, watermark, spark)
        .operation(data_quality)
        .execute()
    )

    print(
        f"""
        Processing complete.
        Source Table: {result.source_table_qualified}
        Destination Table: {result.destination_table_qualified}
        Records Processed: {result.records_processed}
        Initial Watermark: {result.initial_watermark_timestamp_utc.isoformat()}
        Next Watermark: {result.next_watermark_timestamp_utc.isoformat()}
        """
    )
except Exception as e:
    print("The job has failed. See output for error details.", e)
    raise e
