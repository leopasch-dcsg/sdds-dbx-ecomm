# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# COMMAND ----------

from ecmde_ecomm.common.spark import spark_session
spark = spark_session()

# COMMAND ----------
from ecmde_ecomm.common.dbx.kafka.conf import KafkaProducerConfig
from ecmde_ecomm.search.intelligent_filtering.if_dbx_to_kafka import IFKafkaProducer
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.dbx.etl.watermark import Watermark

watermark_table_name = NotebookUtil.notebook_param("dbx_watermark_table_name")
watermark_catalog_name = NotebookUtil.notebook_param("dbx_watermark_catalog_name")
watermark_schema_name = NotebookUtil.notebook_param("dbx_watermark_schema_name")

config = KafkaProducerConfig.from_notebook_params()
watermark = Watermark(
    watermark_table_name,
    watermark_catalog_name,
    watermark_schema_name,
    config.kafka_config.publish_topic,
    spark,
)

source_table_name = NotebookUtil.notebook_param("kafka_publish_source_table_name")
end_date = NotebookUtil.notebook_param("end_date_est")
producer = IFKafkaProducer(spark, config, watermark, source_table_name, end_date)


result = producer.produce(["webstore", "event_type", "event_value", "facet"])

print(
    f"""
        Processing Complete.
        Kafka Topic: {result.topic}
        Last Batch Date: {result.last_batch_date_utc.isoformat()}
        Next Batch Date: {result.next_batch_date_utc.isoformat()}
    """
)