# Databricks notebook source

# COMMAND ----------
import sys
import dlt
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")

path_prefix = spark.conf.get("workspace_path_prefix")
sys.path.append(f"{path_prefix}/src")

# COMMAND ----------
from ecmde_ecomm.common.dbx.kafka.dlt import KafkaDLT
from ecmde_ecomm.common.dbx.kafka.conf import KafkaDLTConfig

dlt_config = KafkaDLTConfig.from_spark_conf(spark)


@dlt.table(
    comment=dlt_config.dbx_table_comment,
    name=dlt_config.dbx_table,
)
def dataframe():
    kafka_dlt = KafkaDLT(spark, dlt_config)
    return kafka_dlt.kafka_dataframe()
