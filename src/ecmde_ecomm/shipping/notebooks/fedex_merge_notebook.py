# Databricks notebook source

# COMMAND ----------
import sys
from databricks.sdk.runtime import dbutils
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()


path_prefix = dbutils.widgets.get("workspace_path_prefix")
sys.path.append(f"{path_prefix}/src")

# COMMAND ----------
from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.dbx.env import DbxDataQuality
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.shipping.oracle_merge import OracleMergeConfig
from ecmde_ecomm.shipping.fedex_merge_operations_provider import (
    FedExMergeOperationProvider,
)


watermark_table_name = NotebookUtil.notebook_param("dbx_merge_watermark_table_name")
if watermark_table_name is None:
    raise ElementNotFoundError(
        "The dbx_merge_watermark_table_name parameter is missing and is required for this notebook to execute."
    )

try:

    config = OracleMergeConfig.from_notebook_params()

    watermark = Watermark(
        watermark_table_name,
        config.destination_catalog,
        config.destination_schema,
        config.destination_table,
        spark,
    )

    data_quality = DbxDataQuality.from_notebook_params()

    result = (
        FedExMergeOperationProvider.provider(config, watermark, spark)
        .operation()
        .execute()
    )

    print(
        f"""
        Processing Complete
        Destination Table: {result.destination_table_qualified}
        Records Processed: {result.records_processed}
        Initial Watermark: {result.initial_watermark_timestamp_utc.isoformat()}
        Next Watermark: {result.next_watermark_timestamp_utc.isoformat()}
        """
    )

except Exception as e:
    print("The job has failed. See output for error details.", e)
    raise e
