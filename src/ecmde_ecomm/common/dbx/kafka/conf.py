from enum import Enum
from databricks.sdk.runtime import dbutils
from dataclasses import dataclass
from typing import List
from ecmde_ecomm.common.errors import ElementNotFoundError, IllegalArgumentError
from ecmde_ecomm.common.util import NotebookUtil
from pyspark.sql import SparkSession


DEFAULT_KAFKA_SESSION_TIMEOUT_MS = "10000"
DEFAULT_KAFKA_FAIL_ON_DATA_LOSS = "true"


class KeyType(Enum):
    AVRO = "avro"
    STRING = "string"

    def __init__(self, key_type):
        self.key_type = key_type

    @staticmethod
    def from_value(value: str):
        for entry in KeyType:
            if entry.key_type.lower() == value.lower():
                return entry

        raise ElementNotFoundError(
            f"The key type {value} does not exist or is not a supported value."
        )


@dataclass(frozen=True)
class KafkaSchemaRegistryConfig:
    schema_registry_url: str
    schema_registry_user: str
    schema_registry_password: str
    schema_registry_key_subject: str | None
    schema_registry_value_subject: str
    schema_registry_key_type: KeyType = KeyType.AVRO

    def __post_init__(self):
        if len(self.schema_registry_url) == 0:
            raise IllegalArgumentError("schema_registry_url cannot be an empty string.")

        if len(self.schema_registry_user) == 0:
            raise IllegalArgumentError(
                "schema_registry_user cannot be an empty string."
            )

        if len(self.schema_registry_password) == 0:
            raise IllegalArgumentError(
                "schema_registry_password cannot be an empty string."
            )

        if (
            self.schema_registry_key_type == KeyType.AVRO
            and len(self.schema_registry_key_subject.strip()) == 0
        ):
            raise IllegalArgumentError(
                "schema_registry_key_subject cannot be an empty string."
            )

        if len(self.schema_registry_value_subject.strip()) == 0:
            raise IllegalArgumentError(
                "schema_registry_value_subject cannot be an empty string."
            )

    def schema_registry_conf(self) -> dict[str, str]:
        """
        This configuration dictionary should be used primarily when consuming from Kafka and you need
        to inline fetch the schema that a message was published with.
        """
        return {
            "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
            "confluent.schema.registry.basic.auth.user.info": f"{self.schema_registry_user}:{self.schema_registry_password}",
        }

    def schema_registry_client_conf(self) -> dict[str, str]:
        """
        This configuration dictionary should be used when bootstrapping an instance of SchemRegistryClient in order to
        fetch a schema at a specific version. This would primarily be used when producing messages out to Kafka.
        """
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": f"{self.schema_registry_user}:{self.schema_registry_password}",
        }

    @staticmethod
    def from_spark_conf(spark: SparkSession):
        registry_url = NotebookUtil.spark_param(spark, "kafka_schema_registry_url")

        registry_key_subject = NotebookUtil.spark_param(
            spark, "kafka_schema_registry_key_subject"
        )

        registry_value_subject = NotebookUtil.spark_param(
            spark, "kafka_schema_registry_value_subject"
        )

        key_type = KeyType.from_value(
            NotebookUtil.spark_param(spark, "kafka_schema_registry_key_type", "avro")
        )

        azure_secret_scope = NotebookUtil.spark_param(spark, "azure_kv_scope")
        registry_user = dbutils.secrets.get(
            azure_secret_scope,
            NotebookUtil.spark_param(spark, "kafka_schema_registry_user_key"),
        )
        registry_password = dbutils.secrets.get(
            azure_secret_scope,
            NotebookUtil.spark_param(spark, "kafka_schema_registry_password_key"),
        )

        return KafkaSchemaRegistryConfig(
            registry_url,
            registry_user,
            registry_password,
            registry_key_subject,
            registry_value_subject,
            key_type,
        )

    @staticmethod
    def from_notebook_params():
        registry_url = NotebookUtil.notebook_param("kafka_schema_registry_url")

        registry_key_subject = NotebookUtil.notebook_param(
            "kafka_schema_registry_key_subject"
        )

        registry_value_subject = NotebookUtil.notebook_param(
            "kafka_schema_registry_value_subject"
        )

        key_type = KeyType.from_value(
            NotebookUtil.notebook_param("kafka_schema_registry_key_type", "avro")
        )

        azure_secret_scope = NotebookUtil.notebook_param("azure_kv_scope")
        registry_user = dbutils.secrets.get(
            azure_secret_scope,
            NotebookUtil.notebook_param("kafka_schema_registry_user_key"),
        )

        registry_password = dbutils.secrets.get(
            azure_secret_scope,
            NotebookUtil.notebook_param("kafka_schema_registry_password_key"),
        )

        return KafkaSchemaRegistryConfig(
            registry_url,
            registry_user,
            registry_password,
            registry_key_subject,
            registry_value_subject,
            key_type,
        )


@dataclass(frozen=True)
class KafkaConfig:
    topics: List[str]
    jaas_config: str
    bootstrap_servers: str
    session_timeout_ms: int = 10000
    fail_on_data_loss: bool = False
    publish_topic: str | None = None

    def __post_init__(self):
        if len(self.topics) > 0:
            for topic in self.topics:
                if len(topic) == 0:
                    raise IllegalArgumentError("Topic names cannot be empty string.")

        if len(self.jaas_config) == 0:
            raise IllegalArgumentError("jaas_config cannot be empty.")

        if len(self.bootstrap_servers) == 0:
            raise IllegalArgumentError("bootstrap_servers cannot be empty.")

        if self.session_timeout_ms <= 0:
            raise IllegalArgumentError("session_timeout_ms must be greater than 0.")

    def subscribed_topics(self, separator: str = ",") -> str:
        return separator.join(self.topics)

    @staticmethod
    def from_spark_conf(spark: SparkSession):
        topics_param = NotebookUtil.spark_param(spark, "kafka_topics")
        topics: list[str] = [] if topics_param is None else topics_param.split(",")

        bootstrap_servers = NotebookUtil.spark_param(spark, "kafka_bootstrap_servers")
        fail_on_data_loss = (
            NotebookUtil.spark_param(
                spark, "kafka_fail_on_data_loss", DEFAULT_KAFKA_FAIL_ON_DATA_LOSS
            ).lower()
            == "true"
        )
        session_timeout_ms = int(
            NotebookUtil.spark_param(
                spark, "kafka_session_timeout_ms", DEFAULT_KAFKA_SESSION_TIMEOUT_MS
            )
        )

        azure_secret_scope = NotebookUtil.spark_param(spark, "azure_kv_scope")
        jaas_config = dbutils.secrets.get(
            azure_secret_scope,
            NotebookUtil.spark_param(spark, "kafka_jaas_config_secret_key"),
        )

        publish_topic = NotebookUtil.spark_param(spark, "kafka_publish_topic")

        return KafkaConfig(
            topics,
            jaas_config,
            bootstrap_servers,
            session_timeout_ms,
            fail_on_data_loss,
            publish_topic,
        )

    @staticmethod
    def from_notebook_params():
        topics_param = NotebookUtil.notebook_param("kafka_topics")
        topics: list[str] = [] if topics_param is None else topics_param.split(",")

        bootstrap_servers = NotebookUtil.notebook_param("kafka_bootstrap_servers")
        fail_on_data_loss = (
            NotebookUtil.notebook_param(
                "kafka_fail_on_data_loss", DEFAULT_KAFKA_FAIL_ON_DATA_LOSS
            ).lower()
            == "true"
        )
        session_timeout_ms = int(
            NotebookUtil.notebook_param(
                "kafka_session_timeout_ms", DEFAULT_KAFKA_SESSION_TIMEOUT_MS
            )
        )

        azure_secret_scope = NotebookUtil.notebook_param("azure_kv_scope")
        jaas_config = dbutils.secrets.get(
            azure_secret_scope,
            NotebookUtil.notebook_param("kafka_jaas_config_secret_key"),
        )

        publish_topic = NotebookUtil.notebook_param("kafka_publish_topic")

        return KafkaConfig(
            topics,
            jaas_config,
            bootstrap_servers,
            session_timeout_ms,
            fail_on_data_loss,
            publish_topic,
        )


@dataclass(frozen=True)
class KafkaProducerConfig:
    registry_config: KafkaSchemaRegistryConfig
    kafka_config: KafkaConfig

    @staticmethod
    def from_spark_conf(spark: SparkSession):
        return KafkaProducerConfig(
            KafkaSchemaRegistryConfig.from_spark_conf(spark),
            KafkaConfig.from_spark_conf(spark),
        )

    @staticmethod
    def from_notebook_params():
        return KafkaProducerConfig(
            KafkaSchemaRegistryConfig.from_notebook_params(),
            KafkaConfig.from_notebook_params(),
        )


@dataclass(frozen=True)
class KafkaDLTConfig:
    dbx_catalog: str
    dbx_schema: str
    dbx_table: str
    dbx_table_comment: str
    dbx_user_id: str
    registry_config: KafkaSchemaRegistryConfig
    kafka_config: KafkaConfig

    def __post_init__(self):
        if len(self.dbx_catalog.strip()) == 0:
            raise IllegalArgumentError("dbx_catalog cannot be empty or whitespace.")

        if len(self.dbx_schema.strip()) == 0:
            raise IllegalArgumentError("dbx_schema cannot be empty or whitespace.")

        if len(self.dbx_table.strip()) == 0:
            raise IllegalArgumentError("dbx_table cannot be empty or whitespace.")

        if len(self.dbx_table_comment.strip()) == 0:
            raise IllegalArgumentError(
                "dbx_table_comment cannot be empty or whitespace."
            )

        if len(self.dbx_user_id.strip()) == 0:
            raise IllegalArgumentError("dbx_user_id cannot be empty or whitespace.")

    def fully_qualified_table(self) -> str:
        """
        Returns: the fully-qualified Databricks table name.

        Format: {catalog}.{schema}.{table}.
        """
        return f"{self.dbx_catalog}.{self.dbx_schema}.{self.dbx_table}"

    @staticmethod
    def from_spark_conf(spark: SparkSession):
        dbx_catalog = NotebookUtil.spark_param(spark, "dbx_destination_catalog")
        dbx_schema = NotebookUtil.spark_param(spark, "dbx_destination_schema")
        dbx_table = NotebookUtil.spark_param(spark, "dbx_destination_table")
        dbx_table_comment = NotebookUtil.spark_param(
            spark, "dbx_destination_table_comment"
        )
        dbx_user_id = NotebookUtil.spark_param(spark, "dbx_user_id")
        return KafkaDLTConfig(
            dbx_catalog,
            dbx_schema,
            dbx_table,
            dbx_table_comment,
            dbx_user_id,
            KafkaSchemaRegistryConfig.from_spark_conf(spark),
            KafkaConfig.from_spark_conf(spark),
        )

    @staticmethod
    def from_notebook_params():
        dbx_catalog = NotebookUtil.notebook_param("dbx_destination_catalog")
        dbx_schema = NotebookUtil.notebook_param("dbx_destination_schema")
        dbx_table = NotebookUtil.notebook_param("dbx_destination_table")
        dbx_table_comment = NotebookUtil.notebook_param("dbx_destination_table_comment")
        dbx_user_id = NotebookUtil.notebook_param("dbx_user_id")
        return KafkaDLTConfig(
            dbx_catalog,
            dbx_schema,
            dbx_table,
            dbx_table_comment,
            dbx_user_id,
            KafkaSchemaRegistryConfig.from_notebook_params(),
            KafkaConfig.from_notebook_params(),
        )
