# Databricks notebook source

# COMMAND ----------
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")

path_prefix = spark.conf.get("workspace_path_prefix")
sys.path.append(f"{path_prefix}/src")

# COMMAND ----------
from ecmde_ecomm.common.bq.egress import (
    BigQueryEgressOperation,
    BigQueryCredentials,
    BigQueryDestination,
    DatabricksSource,
)

try:
    (
        BigQueryEgressOperation.builder(spark)
        .with_bigquery_credentials(BigQueryCredentials.from_notebook_params())
        .with_databricks_source(DatabricksSource.from_notebook_params())
        .with_bigquery_destination(BigQueryDestination.from_notebook_params())
        .operation()
        .execute()
    )
    print("BigQuery egress complete (mode=overwrite).")
except Exception as ex:
    print("The job failed, see job output for error details.", ex)
    raise
