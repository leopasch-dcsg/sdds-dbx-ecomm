import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("ECMDE ECOMM Unit Test Session").config(
            map={
                "spark.jars.packages": "org.apache.spark:spark-avro_2.12:3.5.4",
                "spark.sql.session.timeZone": "UTC"
            }).getOrCreate()
    )

    spark.conf.set("dbx_destination_catalog", "test_catalog")
    spark.conf.set("dbx_destination_schema", "ecmde")
    spark.conf.set("dbx_destination_table", "a_table_name")
    spark.conf.set("dbx_destination_table_comment", "a table comment.")
    spark.conf.set("dbx_user_id", "user1")
    spark.conf.set("kafka_topics", "red,blue,purple")
    spark.conf.set("kafka_jaas_config_secret_key", "jaas")
    spark.conf.set("kafka_bootstrap_servers", "server")
    spark.conf.set("kafka_session_timeout_ms", "45")
    spark.conf.set("kafka_fail_on_data_loss", "false")
    spark.conf.set("kafka_schema_registry_url", "https://schem/registry/url.com")
    spark.conf.set("kafka_schema_registry_user_key", "user_1")
    spark.conf.set("kafka_schema_registry_password_key", "password_1")
    spark.conf.set("kafka_schema_registry_key_subject", "schema_key_subject")
    spark.conf.set("kafka_schema_registry_value_subject", "schema_value_subject")
    spark.conf.set("azure_kv_scope", "scope")

    yield spark
    spark.stop()