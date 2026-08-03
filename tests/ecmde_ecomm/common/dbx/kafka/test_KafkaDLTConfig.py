from databricks.sdk.runtime import dbutils
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.dbx.kafka.conf import (
    KafkaDLTConfig,
    KafkaSchemaRegistryConfig,
    KafkaConfig,
)
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def kafka_schema_registry_config_from_spark_conf_test(
    spark
) -> KafkaSchemaRegistryConfig:
    registry_url = spark.conf.get("kafka_schema_registry_url")
    registry_user = spark.conf.get("kafka_schema_registry_user_key")
    registry_password = spark.conf.get("kafka_schema_registry_password_key")
    registry_key_subject = spark.conf.get("kafka_schema_registry_key_subject")
    registry_value_subject = spark.conf.get(
        "kafka_schema_registry_value_subject"
    )

    return KafkaSchemaRegistryConfig(
        registry_url,
        registry_user,
        registry_password,
        registry_key_subject,
        registry_value_subject,
    )


@pytest.fixture
def kafka_config_from_spark_conf_test(
    spark
) -> KafkaConfig:
    topics = spark.conf.get("kafka_topics").split(",")
    jaas_config = spark.conf.get("kafka_jaas_config_secret_key")
    bootstrap_server = spark.conf.get("kafka_bootstrap_servers")
    session_timeout_ms = int(spark.conf.get("kafka_session_timeout_ms"))
    fail_on_data_loss = (spark.conf.get("kafka_fail_on_data_loss")) == "true"

    return KafkaConfig(
        topics, jaas_config, bootstrap_server, session_timeout_ms, fail_on_data_loss
    )


@pytest.fixture
def dbx_catalog_test_double() -> str:
    return "dev_ent_bronze"


@pytest.fixture
def dbx_schema_test_double() -> str:
    return "ecmde"


@pytest.fixture
def dbx_table_name_test_double() -> str:
    return "a_table_name"


@pytest.fixture
def dbx_table_comment_test_double() -> str:
    return "a table comment."


@pytest.fixture
def dbx_user_id_test_double() -> str:
    return "user1"


@pytest.fixture
def kafka_schema_registry_test_double() -> KafkaSchemaRegistryConfig:
    return KafkaSchemaRegistryConfig(
        "https://schem/registry/url.com",
        "schema_user",
        "schema_password",
        "schema_key_subject",
        "schema_value_subject",
    )


@pytest.fixture
def kafka_config_test_double() -> KafkaConfig:
    return KafkaConfig(
        ["my-topic"], "jaasconfig;", "https://bootstrap/server.com", 45, False
    )


def get_notebook_params_from_args(*args) -> str | int:
    variable_value = args[0]

    if "dbx_destination_catalog" == variable_value:
        return "test_catalog"
    elif "dbx_destination_schema" == variable_value:
        return "test_ecmde"
    elif "dbx_destination_table" == variable_value:
        return "a_table_name"
    elif "dbx_destination_table_comment" == variable_value:
        return "a table comment."
    elif "dbx_user_id" == variable_value:
        return "user1"
    elif "kafka_schema_registry_url" == variable_value:
        return "https://schem/registry/url.com"
    elif "kafka_schema_registry_user_key" == variable_value:
        return "user_1"
    elif "kafka_schema_registry_password_key" == variable_value:
        return "password_1"
    elif "kafka_schema_registry_key_subject" == variable_value:
        return "schema_key_subject"
    elif "kafka_schema_registry_value_subject" == variable_value:
        return "schema_value_subject"
    elif "kafka_topics" == variable_value:
        return "red,blue,green"
    elif "kafka_jaas_config_secret_key" == variable_value:
        return "jaas"
    elif "kafka_bootstrap_servers" == variable_value:
        return "server"
    elif "kafka_session_timeout_ms" == variable_value:
        return "45"
    elif "kafka_fail_on_data_loss" == variable_value:
        return "False"
    elif "azure_kv_scope" == variable_value:
        return "scope"


@pytest.fixture
def kafka_schema_registry_config_from_notebook_params_test():
    return KafkaSchemaRegistryConfig(
        "https://schem/registry/url.com",
        "user_1",
        "password_1",
        "schema_key_subject",
        "schema_value_subject",
    )


@pytest.fixture
def kafka_config_from_notebook_params_test():
    return KafkaConfig(["red", "blue", "green"], "jaas", "server", 45, False)


@pytest.mark.unit
class TestKafkaDLTConfig:
    def test_from_notebook_params(
        self,
        kafka_schema_registry_config_from_notebook_params_test,
        kafka_config_from_notebook_params_test,
        monkeypatch,
    ):
        def mock_get(*args):
            return get_notebook_params_from_args(*args)

        def secret_get(*args):

            value = args[1]
            return value

        monkeypatch.setattr(dbutils.widgets, "get", mock_get)

        monkeypatch.setattr(dbutils.secrets, "get", secret_get)
        expected = KafkaDLTConfig(
            "test_catalog",
            "test_ecmde",
            "a_table_name",
            "a table comment.",
            "user1",
            kafka_schema_registry_config_from_notebook_params_test,
            kafka_config_from_notebook_params_test,
        )
        actual = KafkaDLTConfig.from_notebook_params()
        assert expected == actual

    def test_from_spark_conf(
        self,
        spark,
        monkeypatch,
        kafka_schema_registry_config_from_spark_conf_test,
        kafka_config_from_spark_conf_test,
    ):
        def secret_get(*args):

            value = args[1]
            return value

        monkeypatch.setattr(dbutils.secrets, "get", secret_get)

        dbx_catalog_test = spark.conf.get("dbx_destination_catalog")
        dbx_schema_test = spark.conf.get("dbx_destination_schema")
        dbx_table_test = spark.conf.get("dbx_destination_table")
        dbx_table_comment_test = spark.conf.get("dbx_destination_table_comment")
        dbx_user_id = spark.conf.get("dbx_user_id")
        conf = KafkaDLTConfig.from_spark_conf(spark)

        expected = KafkaDLTConfig(
            dbx_catalog_test,
            dbx_schema_test,
            dbx_table_test,
            dbx_table_comment_test,
            dbx_user_id,
            kafka_schema_registry_config_from_spark_conf_test,
            kafka_config_from_spark_conf_test,
        )
        assert expected == conf

    def test_ctor_with_illegal_dbx_catalog(
        self,
        dbx_schema_test_double,
        dbx_table_name_test_double,
        dbx_table_comment_test_double,
        dbx_user_id_test_double,
        kafka_schema_registry_test_double,
        kafka_config_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaDLTConfig(
                "",
                dbx_schema_test_double,
                dbx_table_name_test_double,
                dbx_table_comment_test_double,
                dbx_user_id_test_double,
                kafka_schema_registry_test_double,
                kafka_config_test_double,
            )

    def test_ctor_with_illegal_dbx_schema(
        self,
        dbx_catalog_test_double,
        dbx_table_name_test_double,
        dbx_table_comment_test_double,
        dbx_user_id_test_double,
        kafka_schema_registry_test_double,
        kafka_config_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaDLTConfig(
                dbx_catalog_test_double,
                "",
                dbx_table_name_test_double,
                dbx_table_comment_test_double,
                dbx_user_id_test_double,
                kafka_schema_registry_test_double,
                kafka_config_test_double,
            )

    def test_ctor_with_illegal_dbx_table(
        self,
        dbx_catalog_test_double,
        dbx_schema_test_double,
        dbx_table_comment_test_double,
        dbx_user_id_test_double,
        kafka_schema_registry_test_double,
        kafka_config_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaDLTConfig(
                dbx_catalog_test_double,
                dbx_schema_test_double,
                "",
                dbx_table_comment_test_double,
                dbx_user_id_test_double,
                kafka_schema_registry_test_double,
                kafka_config_test_double,
            )

    def test_ctor_with_illegal_dbx_table_comment(
        self,
        dbx_catalog_test_double,
        dbx_schema_test_double,
        dbx_table_name_test_double,
        dbx_user_id_test_double,
        kafka_schema_registry_test_double,
        kafka_config_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaDLTConfig(
                dbx_catalog_test_double,
                dbx_schema_test_double,
                dbx_table_name_test_double,
                "",
                dbx_user_id_test_double,
                kafka_schema_registry_test_double,
                kafka_config_test_double,
            )

    def test_ctor_with_illegal_dbx_user_id(
        self,
        dbx_catalog_test_double,
        dbx_schema_test_double,
        dbx_table_name_test_double,
        dbx_table_comment_test_double,
        kafka_schema_registry_test_double,
        kafka_config_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaDLTConfig(
                dbx_catalog_test_double,
                dbx_schema_test_double,
                dbx_table_name_test_double,
                dbx_table_comment_test_double,
                "",
                kafka_schema_registry_test_double,
                kafka_config_test_double,
            )

    def test_fully_qualified_table(
        self,
        dbx_catalog_test_double,
        dbx_schema_test_double,
        dbx_table_name_test_double,
        dbx_table_comment_test_double,
        dbx_user_id_test_double,
        kafka_schema_registry_test_double,
        kafka_config_test_double,
    ):
        config = KafkaDLTConfig(
            dbx_catalog_test_double,
            dbx_schema_test_double,
            dbx_table_name_test_double,
            dbx_table_comment_test_double,
            dbx_user_id_test_double,
            kafka_schema_registry_test_double,
            kafka_config_test_double,
        )

        expected = f"{dbx_catalog_test_double}.{dbx_schema_test_double}.{dbx_table_name_test_double}"
        assert expected == config.fully_qualified_table()
