import os
import time
import zoneinfo
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
    IntegerType,
)
from datetime import datetime, timezone

from ecmde_ecomm.common.dbx.env import DbxEnv
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.ecomp.co_stage.silver.order_discount_merge_operations import (
    SilverOrderMessageDiscount,
)
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def bronze_schema() -> StructType:
    return StructType(
        [
            StructField("message_key", StringType(), False),
            StructField("message_dttm", StringType(), False),
            StructField("order_number", StringType(), True),
            StructField("external_item_id", StringType(), True),
            StructField("order_line_unit_seq", IntegerType(), True),
            StructField("discount", FloatType(), True),
            StructField("applicable_discount_amount", FloatType(), True),
            StructField("discount_level", StringType(), True),
            StructField("discount_name", StringType(), True),
            StructField("discount_desc", StringType(), True),
            StructField("cart_desc", StringType(), True),
            StructField("discount_ext_id", StringType(), True),
            StructField("discount_type", StringType(), True),
            StructField("discount_code", StringType(), True),
            StructField("discount_applied_dttm", StringType(), True),
            StructField("ingested_on_utc", TimestampType(), True),
        ]
    )


def bronze_data(
    discount_applied_dttm_override: str | None = None,
):

    if discount_applied_dttm_override is None:
        discount_applied_dttm = timestamp_test_str_double()
    else:
        discount_applied_dttm = discount_applied_dttm_override

    return [
        (
            message_key_test_double(),
            timestamp_test_str_double(),
            order_number_test_double(),
            external_item_id_test_double(),
            order_line_unit_seq_test_double(),
            discount_test_double(),
            applicable_discount_amount_test_double(),
            discount_level_test_double(),
            discount_name_test_double(),
            discount_desc_test_double(),
            cart_desc_test_double(),
            discount_ext_id_test_double(),
            discount_type_test_double(),
            discount_code_test_double(),
            discount_applied_dttm,
            timestamp_utc_test_double(),
        )
    ]


@pytest.fixture
def discount_merge_operation(spark) -> SilverOrderMessageDiscount:
    config = MergeConfig(
        "dev_ent_bronze_db",
        "ecmde",
        "co_stage_order_message_discount_dlt",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_discount_silver",
        "sp-ddp-ecmde",
        DbxEnv.DEV,
    )

    watermark = Watermark(
        "dev_ent_silver_db.ecmde.watermarks",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_discount_silver",
        spark,
    )

    return SilverOrderMessageDiscount(
        config, watermark, spark, batch_date_utc_test_double()
    )


def message_key_test_double() -> str:
    return "14226_PROD5_20449027190"


def batch_date_utc_test_double() -> datetime:
    return datetime(2025, 1, 5, 0, 0, 0, tzinfo=timezone.utc)


def future_date_str_test_double() -> str:
    return "2025-01-05T01:00:00"


def timestamp_test_str_double() -> str:
    return "2025-01-01T00:00:00"


def timestamp_utc_test_double() -> datetime:
    expected = "2025-01-01T00:00:00.000+00:00"
    return datetime.strptime(expected, "%Y-%m-%dT%H:%M:%S.%f%z")


def timestamp_etc_test_double() -> datetime:
    etc_tzinfo = zoneinfo.ZoneInfo("America/New_York")
    return datetime(2025, 1, 1, 0, 0, 0, tzinfo=etc_tzinfo)


def order_number_test_double() -> str:
    return "1234567890"


def external_item_id_test_double() -> str:
    return "16395212525"


def order_line_unit_seq_test_double() -> int:
    return 0


def discount_test_double() -> float:
    return 15.75


def applicable_discount_amount_test_double() -> float:
    return 7.5


def discount_level_test_double() -> str:
    return "ITEM"


def discount_name_test_double() -> str:
    return "SPECIAL_ITEM_DISC"


def discount_desc_test_double() -> str:
    return "Sample discount"


def cart_desc_test_double() -> str:
    return "Cart-level discount"


def discount_ext_id_test_double() -> str:
    return "EXT_DISC_ID_123"


def discount_type_test_double() -> str:
    return "TYPE-X"


def discount_code_test_double() -> str:
    return "DISC_CODE_001"


@pytest.mark.unit
class TestOrderMessageDiscount:
    def test_get_sql_statement_for_incremental_changes_is_not_none(
        self, discount_merge_operation
    ):

        sql = discount_merge_operation.get_sql_statement_for_incremental_changes(
            datetime.now(timezone.utc)
        )
        assert sql is not None

    def test_perform_changeset_transforms_for_utc_to_et_conversion(
        self, discount_merge_operation, spark, bronze_schema
    ):

        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = discount_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            etc_tzinfo = zoneinfo.ZoneInfo("America/New_York")
            expected = timestamp_etc_test_double()
            actual = rows[0].discount_applied_dttm.astimezone(etc_tzinfo)

            assert expected.isoformat() == actual.isoformat()

        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_for_utc_dates(
        self, discount_merge_operation, spark, bronze_schema
    ):

        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = discount_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            expected = batch_date_utc_test_double()
            assert (
                expected.isoformat()
                == rows[0].date_added.astimezone(timezone.utc).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].silver_created_on_utc.astimezone(timezone.utc).isoformat()
            )

        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_for_etc_date_conversions(
        self, discount_merge_operation, spark, bronze_schema
    ):

        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = discount_merge_operation.perform_changeset_transforms(
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
                == rows[0].discount_applied_dttm.astimezone(etc_tzinfo).isoformat()
            )

        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_column_casts(
        self, discount_merge_operation, spark, bronze_schema
    ):

        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = discount_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            assert int(rows[0].order_line_unit_seq) == order_line_unit_seq_test_double()
            assert float(rows[0].discount) == discount_test_double()
            assert (
                float(rows[0].applicable_discount_amount)
                == applicable_discount_amount_test_double()
            )

        finally:
            os.environ["TZ"] = ""
            time.tzset()
