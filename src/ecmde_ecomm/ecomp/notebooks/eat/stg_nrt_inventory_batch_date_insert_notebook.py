# Databricks notebook source

# COMMAND ----------
import sys
from databricks.sdk.runtime import dbutils
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
from datetime import datetime, timezone
from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.dbx.etl.batch_insert import BatchInsertConfig
from ecmde_ecomm.ecomp.eat.ecomp.eat_availability_batch_insert_operations import (
    StageNrtInventoryBatchInsert,
)

watermark_table_name = NotebookUtil.notebook_param("dbx_watermark_table_name")
if watermark_table_name is None:
    raise ElementNotFoundError(
        "The dbx_watermark_table_name parameter is missing and is required for this notebook to execute."
    )

batch_date_utc_override = NotebookUtil.notebook_param("dbx_batch_date_utc_override")
if batch_date_utc_override is not None:
    batch_date_utc = datetime.strptime(batch_date_utc_override, "%Y-%m-%dT%H:%M:%S%z")
else:
    batch_date_utc = datetime.now(timezone.utc)

print(f"Using Batch Date: {batch_date_utc.isoformat()}")

try:
    config = BatchInsertConfig.from_notebook_params()
    watermark = Watermark(
        watermark_table_name,
        config.destination_catalog,
        config.destination_schema,
        config.destination_table,
        spark,
    )

    operation = StageNrtInventoryBatchInsert(config, watermark, spark, batch_date_utc)
    result = operation.execute()

    print(
        f"""
        Processing complete.
        Source Table: {result.source_table_qualified}
        Destination Table: {result.destination_table_qualified}
        Records Processed: {result.records_processed}
        Initial Batch Timestamp: {result.initial_batch_timestamp_utc.isoformat()}
        Next Batch Timestamp: {result.next_batch_timestamp_utc.isoformat()}
        """
    )

except Exception as e:
    print("The job has failed. See output for error details.")
    raise e
