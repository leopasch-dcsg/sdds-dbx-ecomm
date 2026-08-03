import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StringType
from ecmde_ecomm.common.dbx.kafka.conf import KafkaDLTConfig, KeyType


class KafkaSchemaError(Exception):
    """Raised when a Registry Schema error condition is encountered."""

    pass


class KafkaDLT:
    __logger = logging.getLogger(__name__)

    def __init__(self, spark: SparkSession, config: KafkaDLTConfig):
        self.spark = spark
        self.config = config

    def kafka_dataframe(self) -> DataFrame:
        """
        Create a data from one or more Kafka topics that will be ingested by a DLT flow.

        Raises:
            KafkaIngestError: if there's an error ingesting data from the kafka topic.
            KafkaSchemaError: if there's an error fetching the schema from the Kafka Schema Registry.

        Returns:
            a data frame containing the data from one or more kafka topics that will be ingested by a DLT flow.
        """
        reader = self.__get_spark_reader()

        df = (
            reader.load()
            .withColumn("ingested_on_utc", current_timestamp())
            .withColumn("ingested_by", lit(self.config.dbx_user_id))
            .withColumn("message_timestamp", col("timestamp"))
            .withColumn(
                "value",
                from_avro(
                    col("value"),
                    subject=self.config.registry_config.schema_registry_value_subject,
                    schemaRegistryAddress=self.config.registry_config.schema_registry_url,
                    options=self.config.registry_config.schema_registry_conf(),
                ),
            )
        )

        if self.config.registry_config.schema_registry_key_type == KeyType.STRING:
            df = df.withColumn("key", col("key").cast(StringType()))
        else:
            df = df.withColumn(
                "key",
                from_avro(
                    col("key"),
                    subject=self.config.registry_config.schema_registry_key_subject,
                    schemaRegistryAddress=self.config.registry_config.schema_registry_url,
                    options=self.config.registry_config.schema_registry_conf(),
                ),
            )

        return df.select(
            "key",
            "value.*",
            "topic",
            "partition",
            "offset",
            "message_timestamp",
            "ingested_on_utc",
            "ingested_by",
        )

    def __get_spark_reader(self) -> DataStreamReader:
        reader = (
            self.spark.readStream.format("kafka")
            .option(
                "kafka.bootstrap.servers", self.config.kafka_config.bootstrap_servers
            )
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("startingOffsets", "earliest")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config", self.config.kafka_config.jaas_config)
            .option("kafka.sasl.mechanism", "PLAIN")
            .option(
                "kafka.session.timeout.ms",
                f"{self.config.kafka_config.session_timeout_ms}",
            )
            .option(
                "failOnDataLoss",
                f"{str(self.config.kafka_config.fail_on_data_loss).lower()}",
            )
            .option("subscribe", self.config.kafka_config.subscribed_topics())
        )

        return reader
