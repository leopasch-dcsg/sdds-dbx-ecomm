from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation


class GoldOrderMessagePlaced(MergeOperation):
    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            select *
              from {self.config.source_table_qualified()} silver
             where silver.silver_created_on_utc >= '{last_watermark_utc}'
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return (
            incremental_changeset.withColumn(
                "gold_created_by", lit(self.config.dbx_user_id)
            )
            .withColumn("gold_created_on_utc", lit(self.batch_date_utc))
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
                "date_added",
                "gold_created_by",
                "gold_created_on_utc",
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
                    "gold_created_by": "source.gold_created_by",
                    "gold_created_on_utc": "source.gold_created_on_utc",
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
                    "gold_created_by": "source.gold_created_by",
                    "gold_created_on_utc": "source.gold_created_on_utc",
                }
            )
        ).execute()
