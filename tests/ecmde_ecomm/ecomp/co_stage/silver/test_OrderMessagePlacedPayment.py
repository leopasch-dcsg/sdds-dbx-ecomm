import os
import time
import zoneinfo
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)
from datetime import datetime, timezone
from ecmde_ecomm.common.dbx.env import DbxEnv
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.ecomp.co_stage.silver.order_placed_payment_merge_operations import (
    SilverOrderMessagePlacedPayment,
)
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def bronze_schema() -> StructType:
    return StructType(
        [
            StructField("message_key", StringType(), False),
            StructField("message_dttm", StringType(), False),
            StructField("order_number", StringType(), False),
            StructField("order_last_update_dttm", StringType(), False),
            StructField("payment_type", StringType(), False),
            StructField("cardnumber", StringType(), False),
            StructField("authorizedamount", FloatType(), False),
            StructField("ingested_on_utc", TimestampType(), True),
        ]
    )


def bronze_data():
    return [
        (
            message_key_test_str_double(),
            timestamp_test_str_double(),
            order_number_test_str_double(),
            timestamp_test_str_double(),
            payment_type_test_str_double(),
            card_number_test_str_double(),
            authorized_amount_test_float(),
            timestamp_utc_test_double(),
        )
    ]


@pytest.fixture
def order_placed_payment_merge_operation(spark) -> SilverOrderMessagePlacedPayment:
    config = MergeConfig(
        "dev_ent_bronze_db",
        "ecmde",
        "co_stage_order_message_placed_payment_dlt",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_placed_payment",
        "sp-ddp-ecmde",
        DbxEnv.DEV,
    )

    watermark = Watermark(
        "dev_ent_silver_db.ecmde.watermarks",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_placed_payment",
        spark,
    )

    return SilverOrderMessagePlacedPayment(
        config, watermark, spark, batch_date_utc_test_double()
    )


def message_key_test_str_double() -> str:
    return "14226_PROD5_20449027190"


def batch_date_utc_test_double() -> datetime:
    return datetime(2025, 2, 4, 0, 0, 0, tzinfo=timezone.utc)


def timestamp_test_str_double() -> str:
    return "2025-01-01T00:00:00"


def timestamp_utc_test_double() -> datetime:
    expected = "2025-01-01T00:00:00.000+00:00"
    return datetime.strptime(expected, "%Y-%m-%dT%H:%M:%S.%f%z")


def timestamp_etc_test_double() -> datetime:
    etc_tzinfo = zoneinfo.ZoneInfo("America/New_York")
    return datetime(2025, 1, 1, 0, 0, 0, tzinfo=etc_tzinfo)


def order_number_test_str_double() -> str:
    return "123456789"


def payment_type_test_str_double() -> str:
    return "Visa"


def card_number_test_str_double() -> str:
    return "1234567892345674"


def authorized_amount_test_float() -> float:
    return 85.19


def authorized_amount_test_str_float() -> str:
    return "85.19"


@pytest.mark.unit
class TestOrderMessagePlacedPayment:
    def test_get_sql_statement_for_incremental_changes_is_not_none(
        self, order_placed_payment_merge_operation
    ):
        sql = order_placed_payment_merge_operation.get_sql_statement_for_incremental_changes(
            datetime.now(timezone.utc)
        )
        assert sql is not None

    def test_perform_changeset_transforms_for_utc_dates(
        self, order_placed_payment_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_placed_payment_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            expected = batch_date_utc_test_double()
            assert (
                expected.isoformat()
                == rows[0].silver_created_on_utc.astimezone(timezone.utc).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].silver_updated_on_utc.astimezone(timezone.utc).isoformat()
            )

        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_for_etc_date_conversions(
        self, order_placed_payment_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_placed_payment_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            etc_tzinfo = zoneinfo.ZoneInfo("America/New_York")
            expected = timestamp_etc_test_double()

            assert (
                expected.isoformat()
                == rows[0].message_dttm.astimezone(etc_tzinfo).isoformat()
            )

            assert (
                expected.isoformat()
                == rows[0].order_last_update_dttm.astimezone(etc_tzinfo).isoformat()
            )
        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_column_casts(
        self, order_placed_payment_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_placed_payment_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            assert rows[0].message_key == message_key_test_str_double()
            assert rows[0].order_number == order_number_test_str_double()
            assert rows[0].payment_type == payment_type_test_str_double()
            assert rows[0].card_number == card_number_test_str_double()
            assert rows[0].authorized_amount == authorized_amount_test_str_float()

        finally:
            os.environ["TZ"] = ""
            time.tzset()
