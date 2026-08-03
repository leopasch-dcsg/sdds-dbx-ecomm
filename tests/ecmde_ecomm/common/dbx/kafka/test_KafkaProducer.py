from datetime import datetime
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.kafka.conf import (
    KafkaProducerConfig,
    KafkaSchemaRegistryConfig,
    KafkaConfig,
)
from ecmde_ecomm.common.dbx.kafka.producer import KafkaProducer
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.errors import ExpectationNotMetError
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def kafka_producer_config_no_publish_topic() -> KafkaProducerConfig:
    return KafkaProducerConfig(
        KafkaSchemaRegistryConfig(
            "localhost",
            "user_name",
            "password",
            "ecmde-sku-inventory-key",
            "ecmde-sku-inventory-value",
        ),
        KafkaConfig(
            ["ecmde-sku-inventory"],
            "some_fake_jaas_config",
            "localhost",
        ),
    )


@pytest.mark.unit
class TestKafkaProducer:
    def test_with_invalid_config(self, spark, kafka_producer_config_no_publish_topic):
        with pytest.raises(ExpectationNotMetError):
            MockKafkaProducer(
                spark,
                kafka_producer_config_no_publish_topic,
                Watermark("water.mark.table.name", "silver", "silver", "table", spark),
            )


class MockKafkaProducer(KafkaProducer):
    def source_dataframe(self, last_batch_date_utc: datetime) -> DataFrame:
        pass
