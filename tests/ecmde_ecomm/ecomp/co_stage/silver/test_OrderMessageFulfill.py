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
from ecmde_ecomm.ecomp.co_stage.silver.order_fulfill_merge_operations import (
    SilverOrderMessageFulfill,
    SilverOrderMessageFulfillQuarantine,
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
            StructField("order_type", StringType(), True),
            StructField("aosstorenumber", StringType(), True),
            StructField("aosassociateid", StringType(), True),
            StructField("order_placed_dttm", StringType(), True),
            StructField("order_last_update_dttm", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("external_item_id", StringType(), True),
            StructField("order_line_num", StringType(), True),
            StructField("order_line_unit_seq", IntegerType(), True),
            StructField("order_line_state", StringType(), True),
            StructField("order_line_last_update_dttm", StringType(), True),
            StructField("purchase_price", FloatType(), True),
            StructField("return_price", FloatType(), True),
            StructField("est_unit_tax", FloatType(), True),
            StructField("est_delivery_date", StringType(), True),
            StructField("start_est_delivery_date", StringType(), True),
            StructField("guarenteedtogetthere_date", StringType(), True),
            StructField("line_item_type", StringType(), True),
            StructField("fulfillment_type", StringType(), True),
            StructField("purchase_order", StringType(), True),
            StructField("fulfill_location_id", StringType(), True),
            StructField("fulfill_address1", StringType(), True),
            StructField("fulfill_address2", StringType(), True),
            StructField("fulfill_address3", StringType(), True),
            StructField("fulfill_city", StringType(), True),
            StructField("fulfill_state", StringType(), True),
            StructField("fulfill_zip", StringType(), True),
            StructField("fulfill_order_id", StringType(), True),
            StructField("fulfill_shipped_dttm", StringType(), True),
            StructField("ship_sku", StringType(), True),
            StructField("ship_upc", StringType(), True),
            StructField("ship_mode", StringType(), True),
            StructField("ship_location_id", StringType(), True),
            StructField("ship_carrier", StringType(), True),
            StructField("ship_class", StringType(), True),
            StructField("ship_tracking_num", StringType(), True),
            StructField("ship_charge", FloatType(), True),
            StructField("ship_address1", StringType(), True),
            StructField("ship_address2", StringType(), True),
            StructField("ship_address3", StringType(), True),
            StructField("ship_city", StringType(), True),
            StructField("ship_state", StringType(), True),
            StructField("ship_zip", StringType(), True),
            StructField("tax_product_code", StringType(), True),
            StructField("est_ship_tax_shp_dtl", FloatType(), True),
            StructField("return_tracking_num", StringType(), True),
            StructField("return_reason", StringType(), True),
            StructField("return_location", StringType(), True),
            StructField("return_dttm", StringType(), True),
            StructField("return_label_creation_date", StringType(), True),
            StructField("return_unreceipted", FloatType(), True),
            StructField("return_fraud_check_id", StringType(), True),
            StructField("return_pickup_date", StringType(), True),
            StructField("return_delivery_date", StringType(), True),
            StructField("return_process_date", StringType(), True),
            StructField("return_source", StringType(), True),
            StructField("ingested_on_utc", TimestampType(), True),
            StructField("unit_cancel_source", StringType(), True),
            StructField("unit_cancel_dttm", StringType(), True),
            StructField("unit_cancel_reason", StringType(), True),
        ]
    )


def bronze_data(order_placed_dttm_override: str | None = None):
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
            order_type_test_double(),
            aosstorenumber_test_double(),
            aosassociateid_test_double(),
            order_placed_dttm,
            timestamp_test_str_double(),
            sku_test_double(),
            external_item_id_test_double(),
            order_line_num_test_double(),
            order_line_unit_seq_test_double(),
            order_line_state_test_double(),
            timestamp_test_str_double(),
            purchase_price_test_double(),
            return_price_test_double(),
            est_unit_tax_test_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            line_item_type_test_double(),
            fulfillment_type_test_double(),
            purchase_order_test_double(),
            fulfill_location_id_test_double(),
            fulfill_address1_test_double(),
            fulfill_address2_test_double(),
            fulfill_address3_test_double(),
            city_test_double(),
            state_test_double(),
            zip_test_double(),
            fulfill_order_id_test_double(),
            timestamp_test_str_double(),
            ship_sku_test_double(),
            ship_upc_test_double(),
            ship_mode_test_double(),
            ship_location_id_test_double(),
            ship_carrier_test_double(),
            ship_class_test_double(),
            ship_tracking_num_test_double(),
            ship_charge_test_double(),
            adress_test_double(),
            adress_test_double(),
            adress_test_double(),
            city_test_double(),
            state_test_double(),
            zip_test_double(),
            tax_product_code_test_double(),
            est_ship_tax_shp_dtl_test_double(),
            return_tracking_num_test_double(),
            return_reason_test_double(),
            adress_test_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            return_unreceipted_test_double(),
            return_fraud_check_id_test_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            order_source_test_double(),
            timestamp_utc_test_double(),
            unit_cancel_source_test_double(),
            timestamp_test_str_double(),
            unit_cancel_reason_test_double(),
        ),
        (
            message_key_test_double(),
            timestamp_test_str_double(),
            order_number_with_fr_test_double(),
            order_state_test_double(),
            order_source_test_double(),
            order_type_test_double(),
            aosstorenumber_test_double(),
            aosassociateid_test_double(),
            order_placed_dttm,
            timestamp_test_str_double(),
            sku_test_double(),
            external_item_id_test_double(),
            order_line_num_test_double(),
            order_line_unit_seq_test_double(),
            order_line_state_test_double(),
            timestamp_test_str_double(),
            purchase_price_test_double(),
            return_price_test_double(),
            est_unit_tax_test_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            line_item_type_test_double(),
            fulfillment_type_test_double(),
            purchase_order_test_double(),
            fulfill_location_id_test_double(),
            fulfill_address1_test_double(),
            fulfill_address2_test_double(),
            fulfill_address3_test_double(),
            city_test_double(),
            state_test_double(),
            zip_test_double(),
            fulfill_order_id_with_fr_test_double(),
            timestamp_test_str_double(),
            ship_sku_test_double(),
            ship_upc_test_double(),
            ship_mode_test_double(),
            ship_location_id_test_double(),
            ship_carrier_test_double(),
            ship_class_test_double(),
            ship_tracking_num_test_double(),
            ship_charge_test_double(),
            adress_test_double(),
            adress_test_double(),
            adress_test_double(),
            city_test_double(),
            state_test_double(),
            zip_test_double(),
            tax_product_code_test_double(),
            est_ship_tax_shp_dtl_test_double(),
            return_tracking_num_test_double(),
            return_reason_test_double(),
            adress_test_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            return_unreceipted_test_double(),
            return_fraud_check_id_test_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            timestamp_test_str_double(),
            order_source_test_double(),
            timestamp_utc_test_double(),
            unit_cancel_source_test_double(),
            timestamp_test_str_double(),
            unit_cancel_reason_test_double(),
        ),
    ]


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


def timestamp_etc_test_str_double() -> str:
    return "2025-01-01T00:00:00"


def timestamp_etc_test_double() -> datetime:
    etc_tzinfo = zoneinfo.ZoneInfo("America/New_York")
    return datetime(2025, 1, 1, 0, 0, 0, tzinfo=etc_tzinfo)


def order_number_test_double() -> str:
    return "1234567890"


def order_number_with_fr_test_double() -> str:
    return "10528801190"


def order_state_test_double() -> str:
    return "fulfilled"


def order_source_test_double() -> str:
    return "DicksSportingGoods"


def order_type_test_double() -> str:
    return "BOPIS"

def aosstorenumber_test_double() -> None:
    return None

def aosassociateid_test_double() -> str:
    return "1234-associate_id"

def sku_test_double() -> str:
    return "254153269852"


def external_item_id_test_double() -> str:
    return "16395212525"


def order_line_num_test_double() -> str:
    return "2"


def order_line_unit_seq_test_double() -> int:
    return 0


def order_line_state_test_double() -> str:
    return "shipped"


def purchase_price_test_double() -> float:
    return 171.01


def return_price_test_double() -> float:
    return 10.01


def est_unit_tax_test_double() -> float:
    return 7.06


def line_item_type_test_double() -> str:
    return "Product"


def fulfillment_type_test_double() -> str:
    return "Standard"


def purchase_order_test_double() -> str:
    return "PO123456"


def fulfill_location_id_test_double() -> str:
    return "39"


def fulfill_address1_test_double() -> str:
    return "123 Fulfill St"


def fulfill_address2_test_double() -> str:
    return "Suite 100"


def fulfill_address3_test_double() -> str:
    return "District"


def fulfill_order_id_test_double() -> str:
    return "FO12345"


def fulfill_order_id_with_fr_test_double() -> str:
    return "10528801190.002"


def fulfill_order_id_with_fr_no_dot_test_double() -> str:
    return "105288011900002"


def ship_sku_test_double() -> str:
    return "SHIPSKU123"


def ship_upc_test_double() -> str:
    return "SHIPUPC123"


def ship_mode_test_double() -> str:
    return "0"


def ship_location_id_test_double() -> str:
    return "39"


def ship_carrier_test_double() -> str:
    return "FedEx"


def ship_class_test_double() -> str:
    return "Standard"


def ship_tracking_num_test_double() -> str:
    return "TRACK123"


def ship_charge_test_double() -> float:
    return 8.56


def tax_product_code_test_double() -> str:
    return "91000"


def est_ship_tax_shp_dtl_test_double() -> float:
    return 3.57


def return_tracking_num_test_double() -> str:
    return "RETTRACK123"


def return_reason_test_double() -> str:
    return "Damaged"


def return_fraud_check_id_test_double() -> str:
    return "FCID123"


def adress_test_double() -> str:
    return "123 Sesame Street"


def city_test_double() -> str:
    return "Stafford"


def state_test_double() -> str:
    return "VA"


def zip_test_double() -> str:
    return "22554-2031"


def return_unreceipted_test_double() -> float:
    return 0.0


def unit_cancel_source_test_double():
    return None


def unit_cancel_reason_test_double():
    return None


@pytest.fixture
def fulfill_quarantine_merge_operation(spark) -> SilverOrderMessageFulfillQuarantine:
    config = MergeConfig(
        "dev_ent_bronze_db",
        "ecmde",
        "co_stage_order_message_fulfill_dlt",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_fulfill_quarantine",
        "sp-ddp-ecmde",
        DbxEnv.DEV,
    )
    watermark = Watermark(
        "dev_ent_silver_db.ecmde.watermarks",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_fulfill_quarantine",
        spark,
    )
    return SilverOrderMessageFulfillQuarantine(
        config, watermark, spark, batch_date_utc_test_double()
    )


@pytest.fixture
def order_fulfill_merge_operation(spark) -> SilverOrderMessageFulfill:
    config = MergeConfig(
        "dev_ent_bronze_db",
        "ecmde",
        "co_stage_order_message_fulfill_dlt",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_fulfill_quarantine",
        "sp-ddp-ecmde",
        DbxEnv.DEV,
    )
    watermark = Watermark(
        "dev_ent_silver_db.ecmde.watermarks",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_fulfill_quarantine",
        spark,
    )
    return SilverOrderMessageFulfill(
        config, watermark, spark, batch_date_utc_test_double()
    )


@pytest.mark.unit
class TestOrderMessageFulfill:
    def test_get_sql_statement_for_incremental_changes_is_not_none(
        self, order_fulfill_merge_operation
    ):
        sql = order_fulfill_merge_operation.get_sql_statement_for_incremental_changes(
            datetime.now(timezone.utc)
        )
        assert sql is not None

    def test_perform_changeset_transforms_for_utc_dates(
        self, order_fulfill_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()
            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_fulfill_merge_operation.perform_changeset_transforms(
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
        self, order_fulfill_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()
            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_fulfill_merge_operation.perform_changeset_transforms(
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
                == rows[0].start_est_delivery_date.astimezone(etc_tzinfo).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].guarenteedtogetthere_date.astimezone(etc_tzinfo).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].fulfill_shipped_dttm.astimezone(etc_tzinfo).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].return_dttm.astimezone(etc_tzinfo).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].return_label_creation_date.astimezone(etc_tzinfo).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].return_pickup_date.astimezone(etc_tzinfo).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].return_delivery_date.astimezone(etc_tzinfo).isoformat()
            )
            assert (
                expected.isoformat()
                == rows[0].return_process_date.astimezone(etc_tzinfo).isoformat()
            )
        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_column_casts(
        self, order_fulfill_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()
            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = order_fulfill_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            assert rows[0].order_line_unit_seq == str(order_line_unit_seq_test_double())
            assert rows[0].purchase_price == str(purchase_price_test_double())
            assert rows[0].return_price == str(return_price_test_double())
            assert rows[0].est_unit_tax == str(est_unit_tax_test_double())
            assert rows[0].ship_charge == str(ship_charge_test_double())
            assert rows[0].est_ship_tax_shp_dtl == str(
                est_ship_tax_shp_dtl_test_double()
            )
        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_with_fr_order(
        self, order_fulfill_merge_operation, spark, bronze_schema
    ):
        incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
        output = order_fulfill_merge_operation.perform_changeset_transforms(
            incremental_dataframe
        )

        regular_row = output.filter(
            f"order_number = {order_number_test_double()}"
        ).collect()

        fr_row = output.filter(
            f"order_number = {order_number_with_fr_test_double()}"
        ).collect()

        assert regular_row[0].fulfill_order_id == fulfill_order_id_test_double()
        assert (
            fr_row[0].fulfill_order_id == fulfill_order_id_with_fr_no_dot_test_double()
        )


@pytest.mark.unit
class TestOrderMessageFulfillQuarantine:
    def test_get_sql_statement_for_incremental_changes_is_not_none(
        self, fulfill_quarantine_merge_operation
    ):
        sql = fulfill_quarantine_merge_operation.get_sql_statement_for_incremental_changes(
            datetime.now(timezone.utc)
        )
        assert sql is not None

    def test_quarantine_reason_code(
        self, fulfill_quarantine_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()
            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = fulfill_quarantine_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            quarantine_code = int(rows[0].quarantine_code)
            assert quarantine_code == QuarantineReason.UNDEFINED.value
            incremental_dataframe = spark.createDataFrame(
                bronze_data(future_date_str_test_double()), bronze_schema
            )
            output = fulfill_quarantine_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            quarantine_code = int(rows[0].quarantine_code)
            assert quarantine_code == QuarantineReason.FUTURE_ORDER_DATE.value
        finally:
            os.environ["TZ"] = ""
            time.tzset()
