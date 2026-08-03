from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    col,
    to_timestamp,
    when,
    upper,
)
from pyspark.sql.types import StringType
from ecmde_ecomm.common.util import DateUtil
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation
from ecmde_ecomm.common.dbx.etl.quarantine import QuarantineReason

ORDER_MESSAGE_PLACED_DEFAULT_ZONE = "America/New_York"


def perform_order_message_placed_transforms(
    incremental_changeset: DataFrame, dbx_user_id: str, batch_date_utc: datetime
) -> DataFrame:
    return (
        incremental_changeset.withColumn(
            "order_line_unit_seq", col("order_line_unit_seq").cast(StringType())
        )
        .withColumn("original_price", col("original_price").cast(StringType()))
        .withColumn("discount", col("discount").cast(StringType()))
        .withColumn("purchase_price", col("purchase_price").cast(StringType()))
        .withColumn("est_unit_tax", col("est_unit_tax").cast(StringType()))
        .withColumn(
            "original_ship_charge", col("original_ship_charge").cast(StringType())
        )
        .withColumn("ship_discount", col("ship_discount").cast(StringType()))
        .withColumn("ship_charge", col("ship_charge").cast(StringType()))
        .withColumn(
            "est_ship_tax_shp_dtl", col("est_ship_tax_shp_dtl").cast(StringType())
        )
        .withColumn(
            "message_dttm",
            DateUtil.make_timestamp_with_zone(
                col("message_dttm"), ORDER_MESSAGE_PLACED_DEFAULT_ZONE
            ),
        )
        .withColumn(
            "order_placed_dttm",
            DateUtil.make_timestamp_with_zone(
                col("order_placed_dttm"), ORDER_MESSAGE_PLACED_DEFAULT_ZONE
            ),
        )
        .withColumn(
            "order_last_update_dttm",
            DateUtil.make_timestamp_with_zone(
                col("order_last_update_dttm"), ORDER_MESSAGE_PLACED_DEFAULT_ZONE
            ),
        )
        .withColumn(
            "order_line_last_update_dttm",
            DateUtil.make_timestamp_with_zone(
                col("order_line_last_update_dttm"), ORDER_MESSAGE_PLACED_DEFAULT_ZONE
            ),
        )
        .withColumn(
            "est_delivery_date",
            DateUtil.make_timestamp_with_zone(
                col("est_delivery_date"), ORDER_MESSAGE_PLACED_DEFAULT_ZONE
            ),
        )
        .withColumn(
            "guarenteedtogetthere_date",
            DateUtil.make_timestamp_with_zone(
                col("guarenteedtogetthere_date"), ORDER_MESSAGE_PLACED_DEFAULT_ZONE
            ),
        )
        .withColumn("date_added", lit(batch_date_utc))
        .withColumn("silver_created_by", lit(dbx_user_id))
        .withColumn("silver_created_on_utc", lit(batch_date_utc))
    )


class SilverOrderMessagePlaced(MergeOperation):
    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            select min(message_key) message_key,
                   min(message_dttm) message_dttm,
                   order_number,
                   order_state,
                   order_source,
                   order_channel,
                   aosstorenumber,
                   aosassociateid,
                   any_value(bronze.associations) as associations,
                   order_type,
                   order_placed_dttm,
                   min(order_last_update_dttm) order_last_update_dttm,
                   identity_id,
                   auth_id,
                   loyalty_acct_id,
                   address1,
                   address2,
                   address3,
                   city,
                   state,
                   zip,
                   country,
                   reward_cert_codes,
                   sku,
                   external_item_id,
                   order_line_num,
                   order_line_unit_seq,
                   order_line_state,
                   min(order_line_last_update_dttm) order_line_last_update_dttm,
                   original_price,
                   discount,
                   purchase_price,
                   est_unit_tax,
                   upc,
                   est_delivery_date,
                   guarenteedtogetthere_date,
                   line_item_type,
                   ship_sku,
                   ship_upc,
                   ship_mode,
                   ship_location_id,
                   ship_carrier,
                   ship_class,
                   original_ship_charge,
                   ship_discount,
                   ship_charge,
                   ship_address1,
                   ship_address2,
                   ship_address3,
                   ship_city,
                   ship_state,
                   ship_zip,
                   tax_product_code,
                   est_ship_tax_shp_dtl
              from {self.config.source_table_qualified()} bronze
             where bronze.ingested_on_utc >= '{last_watermark_utc}'
               and '{self.batch_date_utc}' >= make_timestamp(
                    date_part('YEAR', bronze.ORDER_PLACED_DTTM),
                    date_part('MONTH', bronze.ORDER_PLACED_DTTM),
                    date_part('DAY', bronze.ORDER_PLACED_DTTM),
                    date_part('HOUR', bronze.ORDER_PLACED_DTTM),
                    date_part('MINUTE', bronze.ORDER_PLACED_DTTM),
                    date_part('SECOND', bronze.ORDER_PLACED_DTTM),
                    'America/New_York'
                )
            group by order_number,
                     order_state,
                     order_source,
                     order_channel,
                     aosstorenumber,
                     aosassociateid,
                     order_type,
                     order_placed_dttm,
                     identity_id,
                     auth_id,
                     loyalty_acct_id,
                     address1,
                     address2,
                     address3,
                     city,
                     state,
                     zip,
                     country,
                     reward_cert_codes,
                     sku,
                     external_item_id,
                     order_line_num,
                     order_line_unit_seq,
                     order_line_state,
                     original_price,
                     discount,
                     purchase_price,
                     est_unit_tax,
                     upc,
                     est_delivery_date,
                     guarenteedtogetthere_date,
                     line_item_type,
                     ship_sku,
                     ship_upc,
                     ship_mode,
                     ship_location_id,
                     ship_carrier,
                     ship_class,
                     original_ship_charge,
                     ship_discount,
                     ship_charge,
                     ship_address1,
                     ship_address2,
                     ship_address3,
                     ship_city,
                     ship_state,
                     ship_zip,
                     tax_product_code,
                     est_ship_tax_shp_dtl
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return (
            perform_order_message_placed_transforms(
                incremental_changeset, self.config.dbx_user_id, self.batch_date_utc
            )
            .select(
                "message_key",
                "message_dttm",
                "order_number",
                "order_state",
                "order_source",
                "order_channel",
                "aosstorenumber",
                "aosassociateid",
                "associations",
                "order_type",
                "order_placed_dttm",
                "order_last_update_dttm",
                "identity_id",
                "auth_id",
                "loyalty_acct_id",
                "address1",
                "address2",
                "address3",
                "city",
                "state",
                "zip",
                "country",
                "reward_cert_codes",
                "sku",
                "external_item_id",
                "order_line_num",
                "order_line_unit_seq",
                "order_line_state",
                "order_line_last_update_dttm",
                "original_price",
                "discount",
                "purchase_price",
                "est_unit_tax",
                "upc",
                "est_delivery_date",
                "guarenteedtogetthere_date",
                "line_item_type",
                "ship_sku",
                "ship_upc",
                "ship_mode",
                "ship_location_id",
                "ship_carrier",
                "ship_class",
                "original_ship_charge",
                "ship_discount",
                "ship_charge",
                "ship_address1",
                "ship_address2",
                "ship_address3",
                "ship_city",
                "ship_state",
                "ship_zip",
                "tax_product_code",
                "est_ship_tax_shp_dtl",
                "silver_created_by",
                "silver_created_on_utc",
                "date_added",
            )
            .alias("source")
        )

    def merge_updates(self, updates: DataFrame) -> None:
        (
            DeltaTable.forName(self.spark, self.config.destination_table_qualified())
            .alias("target")
            .merge(
                updates,
                """
                target.order_number = source.order_number
                and target.order_state = source.order_state
                and target.order_line_state = source.order_line_state
                and target.order_source = source.order_source
                and target.order_channel = source.order_channel
                and target.sku = source.sku
                and target.order_line_num = source.order_line_num
                and target.order_line_unit_seq = source.order_line_unit_seq
                """,
            )
            .whenMatchedUpdate(
                set={
                    "message_key": "source.message_key",
                    "message_dttm": "source.message_dttm",
                    "aosstorenumber": "source.aosstorenumber",
                    "aosassociateid": "source.aosassociateid",
                    "associations": "source.associations",
                    "order_type": "source.order_type",
                    "order_placed_dttm": "source.order_placed_dttm",
                    "order_last_update_dttm": "source.order_last_update_dttm",
                    "identity_id": "source.identity_id",
                    "auth_id": "source.auth_id",
                    "loyalty_acct_id": "source.loyalty_acct_id",
                    "address1": "source.address1",
                    "address2": "source.address2",
                    "address3": "source.address3",
                    "city": "source.city",
                    "state": "source.state",
                    "zip": "source.zip",
                    "country": "source.country",
                    "reward_cert_codes": "source.reward_cert_codes",
                    "external_item_id": "source.external_item_id",
                    "order_line_last_update_dttm": "source.order_line_last_update_dttm",
                    "original_price": "source.original_price",
                    "discount": "source.discount",
                    "purchase_price": "source.purchase_price",
                    "est_unit_tax": "source.est_unit_tax",
                    "upc": "source.upc",
                    "est_delivery_date": "source.est_delivery_date",
                    "guarenteedtogetthere_date": "source.guarenteedtogetthere_date",
                    "line_item_type": "source.line_item_type",
                    "ship_sku": "source.ship_sku",
                    "ship_upc": "source.ship_upc",
                    "ship_mode": "source.ship_mode",
                    "ship_location_id": "source.ship_location_id",
                    "ship_carrier": "source.ship_carrier",
                    "ship_class": "source.ship_class",
                    "original_ship_charge": "source.original_ship_charge",
                    "ship_discount": "source.ship_discount",
                    "ship_charge": "source.ship_charge",
                    "ship_address1": "source.ship_address1",
                    "ship_address2": "source.ship_address2",
                    "ship_address3": "source.ship_address3",
                    "ship_city": "source.ship_city",
                    "ship_state": "source.ship_state",
                    "ship_zip": "source.ship_zip",
                    "tax_product_code": "source.tax_product_code",
                    "est_ship_tax_shp_dtl": "source.est_ship_tax_shp_dtl",
                    "date_added": "source.date_added",
                    "silver_created_by": "source.silver_created_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "message_key": "source.message_key",
                    "message_dttm": "source.message_dttm",
                    "order_number": "source.order_number",
                    "order_state": "source.order_state",
                    "order_source": "source.order_source",
                    "order_channel": "source.order_channel",
                    "aosstorenumber": "source.aosstorenumber",
                    "aosassociateid": "source.aosassociateid",
                    "associations": "source.associations",
                    "order_type": "source.order_type",
                    "order_placed_dttm": "source.order_placed_dttm",
                    "order_last_update_dttm": "source.order_last_update_dttm",
                    "identity_id": "source.identity_id",
                    "auth_id": "source.auth_id",
                    "loyalty_acct_id": "source.loyalty_acct_id",
                    "address1": "source.address1",
                    "address2": "source.address2",
                    "address3": "source.address3",
                    "city": "source.city",
                    "state": "source.state",
                    "zip": "source.zip",
                    "country": "source.country",
                    "reward_cert_codes": "source.reward_cert_codes",
                    "sku": "source.sku",
                    "external_item_id": "source.external_item_id",
                    "order_line_num": "source.order_line_num",
                    "order_line_unit_seq": "source.order_line_unit_seq",
                    "order_line_state": "source.order_line_state",
                    "order_line_last_update_dttm": "source.order_line_last_update_dttm",
                    "original_price": "source.original_price",
                    "discount": "source.discount",
                    "purchase_price": "source.purchase_price",
                    "est_unit_tax": "source.est_unit_tax",
                    "upc": "source.upc",
                    "est_delivery_date": "source.est_delivery_date",
                    "guarenteedtogetthere_date": "source.guarenteedtogetthere_date",
                    "line_item_type": "source.line_item_type",
                    "ship_sku": "source.ship_sku",
                    "ship_upc": "source.ship_upc",
                    "ship_mode": "source.ship_mode",
                    "ship_location_id": "source.ship_location_id",
                    "ship_carrier": "source.ship_carrier",
                    "ship_class": "source.ship_class",
                    "original_ship_charge": "source.original_ship_charge",
                    "ship_discount": "source.ship_discount",
                    "ship_charge": "source.ship_charge",
                    "ship_address1": "source.ship_address1",
                    "ship_address2": "source.ship_address2",
                    "ship_address3": "source.ship_address3",
                    "ship_city": "source.ship_city",
                    "ship_state": "source.ship_state",
                    "ship_zip": "source.ship_zip",
                    "tax_product_code": "source.tax_product_code",
                    "est_ship_tax_shp_dtl": "source.est_ship_tax_shp_dtl",
                    "date_added": "source.date_added",
                    "silver_created_by": "source.silver_created_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                }
            )
        ).execute()


class SilverOrderMessagePlacedQuarantine(MergeOperation):
    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            select min(message_key) message_key,
                   min(message_dttm) message_dttm,
                   order_number,
                   order_state,
                   order_source,
                   order_channel,
                   aosstorenumber,
                   aosassociateid,
                   any_value(bronze.associations) as associations,
                   order_type,
                   order_placed_dttm,
                   min(order_last_update_dttm) order_last_update_dttm,
                   identity_id,
                   auth_id,
                   loyalty_acct_id,
                   address1,
                   address2,
                   address3,
                   city,
                   state,
                   zip,
                   country,
                   reward_cert_codes,
                   sku,
                   external_item_id,
                   order_line_num,
                   order_line_unit_seq,
                   order_line_state,
                   min(order_line_last_update_dttm) order_line_last_update_dttm,
                   original_price,
                   discount,
                   purchase_price,
                   est_unit_tax,
                   upc,
                   est_delivery_date,
                   guarenteedtogetthere_date,
                   line_item_type,
                   ship_sku,
                   ship_upc,
                   ship_mode,
                   ship_location_id,
                   ship_carrier,
                   ship_class,
                   original_ship_charge,
                   ship_discount,
                   ship_charge,
                   ship_address1,
                   ship_address2,
                   ship_address3,
                   ship_city,
                   ship_state,
                   ship_zip,
                   tax_product_code,
                   est_ship_tax_shp_dtl
              from {self.config.source_table_qualified()} bronze
             where bronze.ingested_on_utc >= '{last_watermark_utc}'
               and (
                    make_timestamp(
                      date_part('YEAR', bronze.order_placed_dttm),
                      date_part('MONTH', bronze.order_placed_dttm),
                      date_part('DAY', bronze.order_placed_dttm),
                      date_part('HOUR', bronze.order_placed_dttm),
                      date_part('MINUTE', bronze.order_placed_dttm),
                      date_part('SECOND', bronze.order_placed_dttm),
                      'America/New_York'
                   ) > '{self.batch_date_utc}'
               )
            group by order_number,
                     order_state,
                     order_source,
                     order_channel,
                     aosstorenumber,
                     aosassociateid,
                     order_type,
                     order_placed_dttm,
                     identity_id,
                     auth_id,
                     loyalty_acct_id,
                     address1,
                     address2,
                     address3,
                     city,
                     state,
                     zip,
                     country,
                     reward_cert_codes,
                     sku,
                     external_item_id,
                     order_line_num,
                     order_line_unit_seq,
                     order_line_state,
                     original_price,
                     discount,
                     purchase_price,
                     est_unit_tax,
                     upc,
                     est_delivery_date,
                     guarenteedtogetthere_date,
                     line_item_type,
                     ship_sku,
                     ship_upc,
                     ship_mode,
                     ship_location_id,
                     ship_carrier,
                     ship_class,
                     original_ship_charge,
                     ship_discount,
                     ship_charge,
                     ship_address1,
                     ship_address2,
                     ship_address3,
                     ship_city,
                     ship_state,
                     ship_zip,
                     tax_product_code,
                     est_ship_tax_shp_dtl
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return (
            perform_order_message_placed_transforms(
                incremental_changeset, self.config.dbx_user_id, self.batch_date_utc
            )
            .select("*")
            .withColumn(
                "quarantine_code",
                when(
                    col("order_placed_dttm") > lit(self.batch_date_utc),
                    QuarantineReason.FUTURE_ORDER_DATE.value,
                ).otherwise(QuarantineReason.UNDEFINED.value),
            )
            .select(
                "message_key",
                "message_dttm",
                "quarantine_code",
                "order_number",
                "order_state",
                "order_source",
                "order_channel",
                "aosstorenumber",
                "aosassociateid",
                "associations",
                "order_type",
                "order_placed_dttm",
                "order_last_update_dttm",
                "identity_id",
                "auth_id",
                "loyalty_acct_id",
                "address1",
                "address2",
                "address3",
                "city",
                "state",
                "zip",
                "country",
                "reward_cert_codes",
                "sku",
                "external_item_id",
                "order_line_num",
                "order_line_unit_seq",
                "order_line_state",
                "order_line_last_update_dttm",
                "original_price",
                "discount",
                "purchase_price",
                "est_unit_tax",
                "upc",
                "est_delivery_date",
                "guarenteedtogetthere_date",
                "line_item_type",
                "ship_sku",
                "ship_upc",
                "ship_mode",
                "ship_location_id",
                "ship_carrier",
                "ship_class",
                "original_ship_charge",
                "ship_discount",
                "ship_charge",
                "ship_address1",
                "ship_address2",
                "ship_address3",
                "ship_city",
                "ship_state",
                "ship_zip",
                "tax_product_code",
                "est_ship_tax_shp_dtl",
                "silver_created_by",
                "silver_created_on_utc",
                "date_added",
            )
            .alias("source")
        )

    def merge_updates(self, updates: DataFrame) -> None:
        (
            DeltaTable.forName(self.spark, self.config.destination_table_qualified())
            .alias("target")
            .merge(
                updates,
                """
                target.order_number = source.order_number
                and target.order_state = source.order_state
                and target.order_line_state = source.order_line_state
                and target.order_source = source.order_source
                and target.order_channel = source.order_channel
                and target.sku = source.sku
                and target.order_line_num = source.order_line_num
                and target.order_line_unit_seq = source.order_line_unit_seq
                """,
            )
            .whenMatchedUpdate(
                set={
                    "message_key": "source.message_key",
                    "message_dttm": "source.message_dttm",
                    "quarantine_code": "source.quarantine_code",
                    "aosstorenumber": "source.aosstorenumber",
                    "aosassociateid": "source.aosassociateid",
                    "associations": "source.associations",
                    "order_type": "source.order_type",
                    "order_placed_dttm": "source.order_placed_dttm",
                    "order_last_update_dttm": "source.order_last_update_dttm",
                    "identity_id": "source.identity_id",
                    "auth_id": "source.auth_id",
                    "loyalty_acct_id": "source.loyalty_acct_id",
                    "address1": "source.address1",
                    "address2": "source.address2",
                    "address3": "source.address3",
                    "city": "source.city",
                    "state": "source.state",
                    "zip": "source.zip",
                    "country": "source.country",
                    "reward_cert_codes": "source.reward_cert_codes",
                    "external_item_id": "source.external_item_id",
                    "order_line_last_update_dttm": "source.order_line_last_update_dttm",
                    "original_price": "source.original_price",
                    "discount": "source.discount",
                    "purchase_price": "source.purchase_price",
                    "est_unit_tax": "source.est_unit_tax",
                    "upc": "source.upc",
                    "est_delivery_date": "source.est_delivery_date",
                    "guarenteedtogetthere_date": "source.guarenteedtogetthere_date",
                    "line_item_type": "source.line_item_type",
                    "ship_sku": "source.ship_sku",
                    "ship_upc": "source.ship_upc",
                    "ship_mode": "source.ship_mode",
                    "ship_location_id": "source.ship_location_id",
                    "ship_carrier": "source.ship_carrier",
                    "ship_class": "source.ship_class",
                    "original_ship_charge": "source.original_ship_charge",
                    "ship_discount": "source.ship_discount",
                    "ship_charge": "source.ship_charge",
                    "ship_address1": "source.ship_address1",
                    "ship_address2": "source.ship_address2",
                    "ship_address3": "source.ship_address3",
                    "ship_city": "source.ship_city",
                    "ship_state": "source.ship_state",
                    "ship_zip": "source.ship_zip",
                    "tax_product_code": "source.tax_product_code",
                    "est_ship_tax_shp_dtl": "source.est_ship_tax_shp_dtl",
                    "date_added": "source.date_added",
                    "silver_created_by": "source.silver_created_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "message_key": "source.message_key",
                    "message_dttm": "source.message_dttm",
                    "quarantine_code": "source.quarantine_code",
                    "order_number": "source.order_number",
                    "order_state": "source.order_state",
                    "order_source": "source.order_source",
                    "order_channel": "source.order_channel",
                    "aosstorenumber": "source.aosstorenumber",
                    "aosassociateid": "source.aosassociateid",
                    "associations": "source.associations",
                    "order_type": "source.order_type",
                    "order_placed_dttm": "source.order_placed_dttm",
                    "order_last_update_dttm": "source.order_last_update_dttm",
                    "identity_id": "source.identity_id",
                    "auth_id": "source.auth_id",
                    "loyalty_acct_id": "source.loyalty_acct_id",
                    "address1": "source.address1",
                    "address2": "source.address2",
                    "address3": "source.address3",
                    "city": "source.city",
                    "state": "source.state",
                    "zip": "source.zip",
                    "country": "source.country",
                    "reward_cert_codes": "source.reward_cert_codes",
                    "sku": "source.sku",
                    "external_item_id": "source.external_item_id",
                    "order_line_num": "source.order_line_num",
                    "order_line_unit_seq": "source.order_line_unit_seq",
                    "order_line_state": "source.order_line_state",
                    "order_line_last_update_dttm": "source.order_line_last_update_dttm",
                    "original_price": "source.original_price",
                    "discount": "source.discount",
                    "purchase_price": "source.purchase_price",
                    "est_unit_tax": "source.est_unit_tax",
                    "upc": "source.upc",
                    "est_delivery_date": "source.est_delivery_date",
                    "guarenteedtogetthere_date": "source.guarenteedtogetthere_date",
                    "line_item_type": "source.line_item_type",
                    "ship_sku": "source.ship_sku",
                    "ship_upc": "source.ship_upc",
                    "ship_mode": "source.ship_mode",
                    "ship_location_id": "source.ship_location_id",
                    "ship_carrier": "source.ship_carrier",
                    "ship_class": "source.ship_class",
                    "original_ship_charge": "source.original_ship_charge",
                    "ship_discount": "source.ship_discount",
                    "ship_charge": "source.ship_charge",
                    "ship_address1": "source.ship_address1",
                    "ship_address2": "source.ship_address2",
                    "ship_address3": "source.ship_address3",
                    "ship_city": "source.ship_city",
                    "ship_state": "source.ship_state",
                    "ship_zip": "source.ship_zip",
                    "tax_product_code": "source.tax_product_code",
                    "est_ship_tax_shp_dtl": "source.est_ship_tax_shp_dtl",
                    "date_added": "source.date_added",
                    "silver_created_by": "source.silver_created_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                }
            )
        ).execute()
