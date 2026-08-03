from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.functions import struct, lit, col, when
from ecmde_ecomm.common.logger import Logger
from pyspark.sql import DataFrame, SparkSession
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.dbx.kafka.conf import KafkaProducerConfig
from ecmde_ecomm.common.errors import ExpectationNotMetError


@dataclass(frozen=True)
class KafkaProducerResult:
    topic: str
    source_dataframe: DataFrame
    last_batch_date_utc: datetime
    next_batch_date_utc: datetime
    key_schema: Schema
    value_schema: Schema


class KafkaProducer(ABC):
    def __init__(
        self, spark: SparkSession, config: KafkaProducerConfig, watermark: Watermark
    ):
        if (
            config.kafka_config.publish_topic is None
            or len(config.kafka_config.publish_topic.strip()) == 0
        ):
            raise ExpectationNotMetError(
                "Unable to create an instance of producer without a valid topic name. Publishing topic name cannot be null or empty."
            )

        self.spark = spark
        self.config = config
        self.watermark = watermark
        self.batch_date_utc = datetime.now(timezone.utc)
        self._logger = Logger.logger(__class__.__name__)

    @abstractmethod
    def source_dataframe(self, last_batch_date_utc: datetime) -> DataFrame:
        """
        Return a dataframe with the data columns that you want to publish to Kafka. Note that the data in your dataframe
        should match the structure of your Avro schema
        """
        pass

    def produce(
        self,
        key_columns: list[str],
        tombstone_columns: list[str] | None = None,
    ) -> KafkaProducerResult:
        topic = self.config.kafka_config.publish_topic
        key_schema = self.__key_schema()
        value_schema = self.__value_schema()

        self._logger.info("Fetching last watermark.")
        last_batch_date_utc = self.watermark.last_watermark_utc()

        self._logger.info("Fetching batch data.")
        source_dataframe = self.source_dataframe(last_batch_date_utc)

        self._logger.info(f"Begin Writing to Kafka Topic: {topic}")
        (
            self.avro_dataframe(
                source_dataframe,
                key_columns,
                key_schema.schema_str,
                value_schema.schema_str,
                tombstone_columns,
            )
            .write.format("kafka")
            .option(
                "kafka.bootstrap.servers", self.config.kafka_config.bootstrap_servers
            )
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config", self.config.kafka_config.jaas_config)
            .option("kafka.sasl.mechanism", "PLAIN")
            .option(
                "kafka.session.timeout.ms",
                f"{self.config.kafka_config.session_timeout_ms}",
            )
            .option("topic", topic)
            .save()
        )
        self._logger.info(f"Done Writing to Kafka Topic: {topic}")

        self._logger.info(f"Updating watermark.")
        self.watermark.update_watermark_timestamp(self.batch_date_utc)

        return KafkaProducerResult(
            topic,
            source_dataframe,
            last_batch_date_utc,
            self.batch_date_utc,
            key_schema,
            value_schema,
        )

    def avro_dataframe(
        self,
        source_dataframe: DataFrame,
        key_columns: list[str],
        key_schema: str,
        value_schema: str,
        tombstone_columns: list[str] | None = None,
    ) -> DataFrame:

        key_data = struct(*[source_dataframe[column] for column in key_columns])
        if tombstone_columns is None:
            value_data = struct(
                *[source_dataframe[column] for column in source_dataframe.columns]
            )
        else:
            any_null = reduce(lambda x, y: x | y, [col(c).isNull() for c in tombstone_columns])
            value_data = when(any_null, lit(None)).otherwise(struct(
                *[source_dataframe[column] for column in source_dataframe.columns]
            ))

        return source_dataframe.select(
            lit(self.config.kafka_config.publish_topic).alias("topic"),
            to_avro(
                data=key_data,
                subject=lit(self.config.registry_config.schema_registry_key_subject),
                schemaRegistryAddress=self.config.registry_config.schema_registry_url,
                options=self.config.registry_config.schema_registry_conf(),
                jsonFormatSchema=key_schema,
            ).alias("key"),
            to_avro(
                data=value_data,
                subject=lit(self.config.registry_config.schema_registry_value_subject),
                schemaRegistryAddress=self.config.registry_config.schema_registry_url,
                options=self.config.registry_config.schema_registry_conf(),
                jsonFormatSchema=value_schema,
            ).alias("value"),
        )

    def __value_schema(self):
        return self.__schema(self.config.registry_config.schema_registry_value_subject)

    def __key_schema(self):
        return self.__schema(self.config.registry_config.schema_registry_key_subject)

    def __schema(self, subject: str):
        client = SchemaRegistryClient(
            self.config.registry_config.schema_registry_client_conf()
        )
        return client.get_latest_version(subject).schema
