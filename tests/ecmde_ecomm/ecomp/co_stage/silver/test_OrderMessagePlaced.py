import os
import time
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
from ecmde_ecomm.ecomp.co_stage.silver.order_placed_merge_operations import (
    SilverOrderMessagePlaced,
    SilverOrderMessagePlacedQuarantine,
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
            StructField("associations", StringType(), True),
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
            StructField("ingested_on_utc", TimestampType(), True),
        ]
    )


def bronze_data(order_placed_dttm_override=None):
    return [
        (
            "14226_PROD5_20449027190",
            "2025-01-01T00:00:00",
            "1234567890",
            "placed",
            "DicksSportingGoods",
            "Desktop",
            None,
            "1234-associate_id",
            '{"storeId":"Store39","device":"web"}',
            "BOPIS",
            order_placed_dttm_override or "2025-01-01T00:00:00",
            "2025-01-01T01:00:00",
            "932aefd2-5f9a-4e7f",
            None,
            "L10DF8F2S952",
            "123 Sesame Street",
            "Suite 101",
            None,
            "Stafford",
            "VA",
            "22554-2031",
            "US",
            None,
            "254153269852",
            "16395212525",
            "2",
            0,
            "fulfilled",
            "2025-01-01T02:00:00",
            179.99,
            8.98,
            171.01,
            7.06,
            "850032102541",
            "2025-01-05T12:00:00",
            "2025-01-05T15:00:00",
            "Product",
            "1241876",
            "2",
            "Standard",
            "Store39",
            "FedEx",
            "Standard",
            7.97,
            2.99,
            8.56,
            "123 Sesame Street",
            "Suite 101",
            None,
            "Stafford",
            "VA",
            "22554",
            "91000",
            3.57,
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        )
    ]


@pytest.fixture
def silver_merge_operation(spark) -> SilverOrderMessagePlaced:
    config = MergeConfig(
        "dev_ent_bronze_db",
        "ecmde",
        "co_stage_order_message_placed_dlt",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_placed",
        "sp-ddp-ecmde",
        DbxEnv.DEV,
    )

    watermark = Watermark(
        "dev_ent_silver_db.ecmde.watermarks",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_placed",
        spark,
    )

    return SilverOrderMessagePlaced(
        config, watermark, spark, datetime(2025, 1, 5, tzinfo=timezone.utc)
    )


@pytest.fixture
def quarantine_merge_operation(spark) -> SilverOrderMessagePlacedQuarantine:
    config = MergeConfig(
        "dev_ent_bronze_db",
        "ecmde",
        "co_stage_order_message_placed_dlt",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_placed_quarantine",
        "sp-ddp-ecmde",
        DbxEnv.DEV,
    )

    watermark = Watermark(
        "dev_ent_silver_db.ecmde.watermarks",
        "dev_ent_silver_db",
        "ecmde",
        "co_stage_order_message_placed_quarantine",
        spark,
    )

    return SilverOrderMessagePlacedQuarantine(
        config, watermark, spark, datetime(2025, 1, 5, tzinfo=timezone.utc)
    )


@pytest.mark.unit
class TestOrderMessagePlaced:
    def test_get_sql_statement_for_incremental_changes_is_not_none(
        self, silver_merge_operation
    ):
        sql = silver_merge_operation.get_sql_statement_for_incremental_changes(
            datetime.now(timezone.utc)
        )
        assert sql is not None

    def test_perform_changeset_transforms(
        self, silver_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()
            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = silver_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            assert rows[0].order_state == "placed"
        finally:
            os.environ["TZ"] = ""
            time.tzset()

    def test_perform_changeset_transforms_column_casts(
        self, silver_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()
            incremental_dataframe = spark.createDataFrame(bronze_data(), bronze_schema)
            output = silver_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            assert rows[0].original_price == "179.99"
            assert rows[0].discount == "8.98"
            assert rows[0].purchase_price == "171.01"
        finally:
            os.environ["TZ"] = ""
            time.tzset()


@pytest.mark.unit
class TestOrderMessagePlacedQuarantine:
    def test_quarantine_reason_code(
        self, quarantine_merge_operation, spark, bronze_schema
    ):
        try:
            os.environ["TZ"] = "UTC"
            time.tzset()
            incremental_dataframe = spark.createDataFrame(
                bronze_data(order_placed_dttm_override="2025-02-01T00:00:00"),
                bronze_schema,
            )
            output = quarantine_merge_operation.perform_changeset_transforms(
                incremental_dataframe
            )
            rows = output.collect()
            quarantine_code = int(rows[0].quarantine_code)
            assert quarantine_code == QuarantineReason.FUTURE_ORDER_DATE.value
        finally:
            os.environ["TZ"] = ""
            time.tzset()
