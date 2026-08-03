# Databricks notebook source

# COMMAND ----------
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import display
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

spark = DatabricksSession.builder.getOrCreate()

path_prefix = spark.conf.get("workspace_path_prefix")
sys.path.append(f"{path_prefix}/src")

# COMMAND ----------
from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.dbx.elastic.divergence_calculation import DivergenceConfig
from ecmde_ecomm.search.ltr.abb_divergence_calculation import ABBDivergenceCalculation

# COMMAND ----------

config = DivergenceConfig.from_notebook_params()

try:
    result = ABBDivergenceCalculation(config, spark).execute()
    print(
        f"""
        Processing complete.
        Source Table: {result.destination_table_qualified}
        """
    )

except Exception as e:
    print("The job has failed. See output for error details.")
    raise e
