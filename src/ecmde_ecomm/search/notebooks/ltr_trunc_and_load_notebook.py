# Databricks notebook source

# COMMAND ----------
import sys
from databricks.connect import DatabricksSession
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

spark = DatabricksSession.builder.getOrCreate()

path_prefix = spark.conf.get("workspace_path_prefix")
sys.path.append(f"{path_prefix}/src")

# COMMAND ----------
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadConfig
from ecmde_ecomm.search.ltr_load_operations_provider import LTRLoadOperationProvider

# COMMAND ----------

try:
    config = LoadConfig.from_notebook_params()
    if config.lookback_days is not None:
        start_date_est = (
            datetime.now(ZoneInfo("America/New_York"))
            - timedelta(days=(1 + int(config.lookback_days)))
        ).date()
        end_date_est = (
            datetime.now(ZoneInfo("America/New_York")) - timedelta(days=1)
        ).date()
        result = (
            LTRLoadOperationProvider.provider(
                config, spark, start_date_est, end_date_est
            )
            .operation()
            .execute()
        )
    else:
        result = (
            LTRLoadOperationProvider.provider(config, spark, None, None)
            .operation()
            .execute()
        )

    print(
        f"""
        Processing complete.
        Source Table: {result.source_table_qualified}
        Secondary Source Table: {result.secondary_source_table_qualified}
        Destination Table: {result.destination_table_qualified}
        Using Start Date: {result.start_date_est}
        Using End Date: {result.end_date_est}
        """
    )

except Exception as e:
    print("The job has failed. See output for error details.")
    raise e
