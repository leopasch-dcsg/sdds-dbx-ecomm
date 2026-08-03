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
from ecmde_ecomm.common.dbx.etl.quarantine import QuarantineReason
from ecmde_ecomm.ecomp.co_stage.silver.order_cancel_merge_operations import (
    SilverOrderMessageCancel,
    SilverOrderMessageCancelQuarantine,
)
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def bronze_schema() -> StructType:
    return StructType(
        [
            StructField("message_key", StringType(), False),
            StructField("message_dttm", StringType(), False),
            StructField("order_number", StringType(), True),
            StructField("order_state", StringType(), True),
            StructField("order_source", StringType(), True),
            StructField("order_channel", StringType(), True),
            StructField("aosstorenumber", StringType(), True),
            StructField("aosassociateid", StringType(), True),
            StructField("order_type", StringType(), True),
            StructField("order_placed_dttm", StringType(), True),
            StructField("order_last_update_dttm", StringType(), True),
            StructField("identity_id", StringType(), True),
            StructField("auth_id", StringType(), True),
            StructField("loyalty_acct_id", StringType(), True),
            StructField("address1", StringType(), True),
            StructField("address2", StringType(), True),
            StructField("address3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip", StringType(), True),
            StructField("country", StringType(), True),
            StructField("reward_cert_codes", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("external_item_id", StringType(), True),
            StructField("order_line_num", StringType(), True),
            StructField("order_line_unit_seq", IntegerType(), True),
            StructField("order_line_state", StringType(), True),
            StructField("order_line_last_update_dttm", StringType(), True),
            StructField("original_price", FloatType(), True),
            StructField("discount", FloatType(), True),
            StructField("purchase_price", FloatType(), True),
            StructField("est_unit_tax", FloatType(), True),
            StructField("upc", StringType(), True),
            StructField("est_delivery_date", StringType(), True),
            StructField("guarenteedtogetthere_date", StringType(), True),
            StructField("line_item_type", StringType(), True),
            StructField("ship_sku", StringType(), True),
            StructField("ship_upc", StringType(), True),
            StructField("ship_mode", StringType(), True),
            StructField("ship_location_id", StringType(), True),
            StructField("ship_carrier", StringType(), True),
            StructField("ship_class", StringType(), True),
            StructField("original_ship_charge", FloatType(), True),
            StructField("ship_discount", FloatType(), True),
            StructField("ship_charge", FloatType(), True),
            StructField("ship_address1", StringType(), True),
            StructField("ship_address2", StringType(), True),
            StructField("ship_address3", StringType(), True),
            StructField("ship_city", StringType(), True),
            StructField("ship_state", StringType(), True),
            StructField("ship_zip", StringType(), True),
            StructField("tax_product_code", StringType(), True),
            StructField("est_ship_tax_shp_dtl", FloatType(), True),
            StructField("cancel_source", StringType(), True),
            StructField("cancel_agent", StringType(), True),
            StructField("cancel_dttm", StringType(), True),
            StructField("cancel_reason", StringType(), True),
            StructField("ingested_on_utc", TimestampType(), True),
            StructField("unit_cancel_source", StringType(), True),
            StructField("unit_cancel_dttm", StringType(), True),
            StructField("unit_cancel_reason", StringType(), True),
        ]
    )


def bronze_data(
    cancel_source: str = "CallCenter",
    order_placed_dttm_override: str | None = None,
    cancel_dttm_override: str | None = None,
    cancel_reason: str = "Invalid Shipping Address",
):

    if cancel_dttm_override is None:
        cancel_dttm = timestamp_test_str_double()
    else:
        cancel_dttm = cancel_dttm_override

    if order_placed_dttm_override is None:
        order_placed_dttm = timestamp_test_str_double()
    else:
        order_placed_dttm = order_placed_dttm_override

    return [
        (
            message_key_test_double(),
            timestamp_test_str_double(),
            order_number_test_double(),
            order_state_test_double(),
            order_source_test_double(),
            order_channel_test_double(),
            aosstorenumber_test_double(),
            aosassociateid_test_double(),
            order_type_test_double(),
            order_placed_dttm,
            timestamp_test_str_double(),
            identity_id_test_double(),
            auth_id_test_double(),
            loyalty_acct_id_test_double(),
            adress_test_double(),
            adress_test_double(),
            adress_test_double(),
            city_test_double(),
            state_test_double(),
            zip_test_double(),
            country_test_double(),
            reward_cert_codes_test_double(),
            sku_test_double(),
            external_item_id_test_double(),
            order_line_num_test_double(),
            order_line_unit_seq_test_double(),
            order_line_state_test_double(),
            timestamp_test_str_double(),
            original_price_test_double(),
            discount_test_double(),
            purchase_price_test_double(),
            est_unit_tax_test_double(),
            upc_test_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            line_item_type_test_double(),
            ship_sku_test_double(),
            ship_upc_double(),
            ship_mode_double(),
            ship_location_id_test_double(),
            ship_carrier_test_double(),
            ship_class_test_double(),
            original_ship_charge_test_double(),
            ship_discount_test_double(),
            ship_charge_test_double(),
            adress_test_double(),
            adress_test_double(),
            adress_test_double(),
            city_test_double(),
            state_test_double(),
            zip_test_double(),
            tax_product_code_test_double(),
            est_ship_tax_shp_dtl_test_double(),
            cancel_source,
            cancel_agent_test_double(),
            cancel_dttm,
            cancel_reason,
            timestamp_utc_test_double(),
            unit_cancel_source_test_double(),
            timestamp_test_str_double(),
            unit_cancel_reason_test_double(),
        )
    ]


@pytest.fixture
def quarantine_merge_operation(spark) -> SilverOrderMessageCancelQuarantine:
    config = MergeConfig(
        "dev_ent_bronze_db",
        "ecmde",
        "co_stage_order_message_cancel_dlt",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_cancel_quarantine",
        "sp-ddp-ecmde",
        DbxEnv.DEV,
    )

    watermark = Watermark(
        "dev_ent_silver_db.ecmde.watermarks",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_cancel_quarantine",
        spark,
    )

    return SilverOrderMessageCancelQuarantine(
        config, watermark, spark, batch_date_utc_test_double()
    )


@pytest.fixture
def order_cancel_merge_operation(spark) -> SilverOrderMessageCancel:
    config = MergeConfig(
        "dev_ent_bronze_db",
        "ecmde",
        "co_stage_order_message_cancel_dlt",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_cancel_quarantine",
        "sp-ddp-ecmde",
        DbxEnv.DEV,
    )

    watermark = Watermark(
        "dev_ent_silver_db.ecmde.watermarks",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_cancel_quarantine",
        spark,
    )

    return SilverOrderMessageCancel(
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


def order_state_test_double() -> str:
    return "canceled"


def order_source_test_double() -> str:
    return "DicksSportingGoods"


def order_channel_test_double() -> str:
    return "Desktop"


def aosstorenumber_test_double() -> None:
    return None


def aosassociateid_test_double() -> str:
    return "1234-associate_id"


def order_type_test_double() -> str:
    return "BOPIS"


def identity_id_test_double() -> str:
    return "932aefd2-5f9a-4e7f"


def auth_id_test_double() -> None:
    return None


def loyalty_acct_id_test_double() -> str:
    return "L10DF8F2S952"


def adress_test_double() -> str:
    return "123 Sesame Street"


def city_test_double() -> str:
    return "Stafford"


def state_test_double() -> str:
    return "VA"


def zip_test_double() -> str:
    return "22554-2031"


def country_test_double() -> str:
    return "us"


def reward_cert_codes_test_double() -> None:
    return None


def sku_test_double() -> str:
    return "254153269852"


def external_item_id_test_double() -> str:
    return "16395212525"


def order_line_num_test_double() -> str:
    return "2"


def order_line_unit_seq_test_double_str() -> str:
    return "0"


def order_line_unit_seq_test_double() -> int:
    return 0


def order_line_state_test_double() -> str:
    return "returned"


def original_price_test_double_str() -> str:
    return "179.99"


def original_price_test_double() -> float:
    return 179.99


def discount_test_double_str() -> str:
    return "8.98"


def discount_test_double() -> float:
    return 8.98


def purchase_price_test_double_str() -> str:
    return "171.01"


def purchase_price_test_double() -> float:
    return 171.01


def est_unit_tax_test_double_str() -> str:
    return "7.06"


def est_unit_tax_test_double() -> float:
    return 7.06


def upc_test_double() -> str:
    return "850032102541"


def line_item_type_test_double() -> str:
    return "Product"


def ship_sku_test_double() -> str:
    return "1241876"


def ship_upc_double() -> str:
    return "2"


def ship_mode_double() -> str:
    return "0"


def ship_location_id_test_double() -> str:
    return "39"


def ship_carrier_test_double() -> str:
    return "8.99"


def ship_class_test_double() -> str:
    return "171.00"


def original_ship_charge_test_double_str() -> str:
    return "7.97"


def original_ship_charge_test_double() -> float:
    return 7.97


def ship_discount_test_double_str() -> str:
    return "2.99"


def ship_discount_test_double() -> float:
    return 2.99


def ship_charge_test_double_str() -> str:
    return "8.56"


def ship_charge_test_double() -> float:
    return 8.56


def tax_product_code_test_double() -> str:
    return "91000"


def est_ship_tax_shp_dtl_test_double_str() -> str:
    return "3.57"


def est_ship_tax_shp_dtl_test_double() -> float:
    return 3.57


def cancel_source_test_double() -> str:
    return "CallCenter"


def cancel_agent_test_double() -> str:
    return "LPTEAM"


def unit_cancel_source_test_double() -> None:
    return None


def unit_cancel_reason_test_double() -> None:
    return None


@pytest.mark.unit
class TestOrderMessageCancel:
    def test_get_sql_statement_for_incremental_changes_is_not_none(
        self, order_cancel_merge_operation
    ):
        sql = order_cancel_merge_operation.get_sql_statement_for_incremental_changes(
            datetime.now(timezone.utc)
        )
        assert sql is not None

    def test_perform_changeset_transforms_with_call_center_cancel_source(
        self, order_cancel_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_cancel_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            expected = timestamp_utc_test_double().isoformat()
            actual: datetime = rows[0].cancel_dttm.astimezone(timezone.utc).isoformat()

            assert expected == actual
        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_with_other_cancel_source(
        self, order_cancel_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(
                bronze_data("Fraud"), bronze_schema
            )
            output = order_cancel_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            etc_tzinfo = zoneinfo.ZoneInfo("America/New_York")
            expected = timestamp_etc_test_double()
            actual = rows[0].cancel_dttm.astimezone(etc_tzinfo)

            assert expected.isoformat() == actual.isoformat()

        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_for_utc_dates(
        self, order_cancel_merge_operation, spark, bronze_schema, monkeypatch
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_cancel_merge_operation.perform_changeset_transforms(
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
        self, order_cancel_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_cancel_merge_operation.perform_changeset_transforms(
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
                == rows[0].order_placed_dttm.astimezone(etc_tzinfo).isoformat()
            )

            assert (
                expected.isoformat()
                == rows[0].order_last_update_dttm.astimezone(etc_tzinfo).isoformat()
            )

            assert (
                expected.isoformat()
                == rows[0]
                .order_line_last_update_dttm.astimezone(etc_tzinfo)
                .isoformat()
            )

            assert (
                expected.isoformat()
                == rows[0].est_delivery_date.astimezone(etc_tzinfo).isoformat()
            )

            assert (
                expected.isoformat()
                == rows[0].guarenteedtogetthere_date.astimezone(etc_tzinfo).isoformat()
            )

            assert (
                expected.isoformat()
                == rows[0].unit_cancel_dttm.astimezone(etc_tzinfo).isoformat()
            )
        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_column_casts(
        self, order_cancel_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(
                bronze_data("Fraud"), bronze_schema
            )
            output = order_cancel_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()

            assert rows[0].order_line_unit_seq == order_line_unit_seq_test_double_str()
            assert rows[0].original_price == original_price_test_double_str()
            assert rows[0].discount == discount_test_double_str()
            assert rows[0].purchase_price == purchase_price_test_double_str()
            assert rows[0].est_unit_tax == est_unit_tax_test_double_str()
            assert (
                rows[0].original_ship_charge == original_ship_charge_test_double_str()
            )
            assert rows[0].ship_discount == ship_discount_test_double_str()
            assert rows[0].ship_charge == ship_charge_test_double_str()
            assert (
                rows[0].est_ship_tax_shp_dtl == est_ship_tax_shp_dtl_test_double_str()
            )

        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_invalid_shipping_address_mapping(
        self, order_cancel_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(
                bronze_data(cancel_reason="Invalid Loyalty Shipping Address"),
                bronze_schema,
            )
            output = order_cancel_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            assert rows[0].cancel_reason == "Invalid Shipping Address"

            incremental_dataframe = spark.createDataFrame(
                bronze_data(cancel_reason="Invalid Address"),
                bronze_schema,
            )
            output = order_cancel_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            assert rows[0].cancel_reason == "Invalid Address"
        finally:
            os.environ["TZ"] = ""
            time.tzset()


@pytest.mark.unit
class TestOrderMessageCancelQuarantine:
    def test_get_sql_statement_for_incremental_changes_is_not_none(
        self, quarantine_merge_operation
    ):
        sql = quarantine_merge_operation.get_sql_statement_for_incremental_changes(
            datetime.now(timezone.utc)
        )
        assert sql is not None

    def test_quarantine_reason_code(
        self, quarantine_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()

            incremental_dataframe = spark.createDataFrame(
                bronze_data("Fraud"), bronze_schema
            )
            output = quarantine_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            quarantine_code = int(rows[0].quarantine_code)
            assert quarantine_code == QuarantineReason.UNDEFINED.value

            incremental_dataframe = spark.createDataFrame(
                bronze_data("Fraud", future_date_str_test_double()), bronze_schema
            )
            output = quarantine_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            quarantine_code = int(rows[0].quarantine_code)
            assert quarantine_code == QuarantineReason.FUTURE_ORDER_DATE.value

            incremental_dataframe = spark.createDataFrame(
                bronze_data("CallCenter", None, future_date_str_test_double()),
                bronze_schema,
            )
            output = quarantine_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            quarantine_code = int(rows[0].quarantine_code)
            assert quarantine_code == QuarantineReason.FUTURE_ORDER_CANCEL_DATE.value

        finally:
            os.environ["TZ"] = ""
            time.tzset()
