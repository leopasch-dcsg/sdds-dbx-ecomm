from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from ecmde_ecomm.common.dbx.etl.merge import MergeOperation


class GoldOrderMessageFulfill(MergeOperation):
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
                "order_type",
                "aosstorenumber",
                "aosassociateid",
                "order_placed_dttm",
                "order_last_update_dttm",
                "sku",
                "external_item_id",
                "order_line_num",
                "order_line_unit_seq",
                "order_line_state",
                "order_line_last_update_dttm",
                "purchase_price",
                "return_price",
                "est_unit_tax",
                "est_delivery_date",
                "start_est_delivery_date",
                "guarenteedtogetthere_date",
                "line_item_type",
                "fulfillment_type",
                "purchase_order",
                "fulfill_location_id",
                "fulfill_address1",
                "fulfill_address2",
                "fulfill_address3",
                "fulfill_city",
                "fulfill_state",
                "fulfill_zip",
                "fulfill_order_id",
                "fulfill_shipped_dttm",
                "ship_sku",
                "ship_upc",
                "ship_mode",
                "ship_location_id",
                "ship_carrier",
                "ship_class",
                "ship_tracking_num",
                "ship_charge",
                "ship_address1",
                "ship_address2",
                "ship_address3",
                "ship_city",
                "ship_state",
                "ship_zip",
                "tax_product_code",
                "est_ship_tax_shp_dtl",
                "return_tracking_num",
                "return_reason",
                "return_location",
                "return_dttm",
                "return_label_creation_date",
                "return_unreceipted",
                "return_fraud_check_id",
                "return_pickup_date",
                "return_delivery_date",
                "return_process_date",
                "return_source",
                "unit_cancel_source",
                "unit_cancel_dttm",
                "unit_cancel_reason",
                "date_added",
                "gold_created_by",
                "gold_created_on_utc",
            )
        )

    def merge_updates(self, updates: DataFrame) -> None:
        (
            DeltaTable.forName(self.spark, self.config.destination_table_qualified())
            .alias("target")
            .merge(
                updates.alias("source"),
                """
                target.order_number = source.order_number
                AND target.order_state = source.order_state
                AND target.order_line_state = source.order_line_state
                AND target.order_source = source.order_source
                AND target.sku = source.sku
                AND target.order_line_num = source.order_line_num
                AND target.order_line_unit_seq = source.order_line_unit_seq
                """,
            )
            .whenMatchedUpdate(
                set={
                    "message_key": "source.message_key",
                    "message_dttm": "source.message_dttm",
                    "aosstorenumber": "source.aosstorenumber",
                    "aosassociateid": "source.aosassociateid",
                    "order_placed_dttm": "source.order_placed_dttm",
                    "order_last_update_dttm": "source.order_last_update_dttm",
                    "order_line_last_update_dttm": "source.order_line_last_update_dttm",
                    "purchase_price": "source.purchase_price",
                    "return_price": "source.return_price",
                    "est_unit_tax": "source.est_unit_tax",
                    "est_delivery_date": "source.est_delivery_date",
                    "start_est_delivery_date": "source.start_est_delivery_date",
                    "guarenteedtogetthere_date": "source.guarenteedtogetthere_date",
                    "line_item_type": "source.line_item_type",
                    "fulfillment_type": "source.fulfillment_type",
                    "purchase_order": "source.purchase_order",
                    "fulfill_location_id": "source.fulfill_location_id",
                    "fulfill_address1": "source.fulfill_address1",
                    "fulfill_address2": "source.fulfill_address2",
                    "fulfill_address3": "source.fulfill_address3",
                    "fulfill_city": "source.fulfill_city",
                    "fulfill_state": "source.fulfill_state",
                    "fulfill_zip": "source.fulfill_zip",
                    "fulfill_order_id": "source.fulfill_order_id",
                    "fulfill_shipped_dttm": "source.fulfill_shipped_dttm",
                    "ship_sku": "source.ship_sku",
                    "ship_upc": "source.ship_upc",
                    "ship_mode": "source.ship_mode",
                    "ship_location_id": "source.ship_location_id",
                    "ship_carrier": "source.ship_carrier",
                    "ship_class": "source.ship_class",
                    "ship_tracking_num": "source.ship_tracking_num",
                    "ship_charge": "source.ship_charge",
                    "ship_address1": "source.ship_address1",
                    "ship_address2": "source.ship_address2",
                    "ship_address3": "source.ship_address3",
                    "ship_city": "source.ship_city",
                    "ship_state": "source.ship_state",
                    "ship_zip": "source.ship_zip",
                    "tax_product_code": "source.tax_product_code",
                    "est_ship_tax_shp_dtl": "source.est_ship_tax_shp_dtl",
                    "return_tracking_num": "source.return_tracking_num",
                    "return_reason": "source.return_reason",
                    "return_location": "source.return_location",
                    "return_dttm": "source.return_dttm",
                    "return_label_creation_date": "source.return_label_creation_date",
                    "return_unreceipted": "source.return_unreceipted",
                    "return_fraud_check_id": "source.return_fraud_check_id",
                    "return_pickup_date": "source.return_pickup_date",
                    "return_delivery_date": "source.return_delivery_date",
                    "return_process_date": "source.return_process_date",
                    "return_source": "source.return_source",
                    "unit_cancel_source": "source.unit_cancel_source",
                    "unit_cancel_dttm": "source.unit_cancel_dttm",
                    "unit_cancel_reason": "source.unit_cancel_reason",
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
                    "order_type": "source.order_type",
                    "aosstorenumber": "source.aosstorenumber",
                    "aosassociateid": "source.aosassociateid",
                    "order_placed_dttm": "source.order_placed_dttm",
                    "order_last_update_dttm": "source.order_last_update_dttm",
                    "sku": "source.sku",
                    "external_item_id": "source.external_item_id",
                    "order_line_num": "source.order_line_num",
                    "order_line_unit_seq": "source.order_line_unit_seq",
                    "order_line_state": "source.order_line_state",
                    "order_line_last_update_dttm": "source.order_line_last_update_dttm",
                    "purchase_price": "source.purchase_price",
                    "return_price": "source.return_price",
                    "est_unit_tax": "source.est_unit_tax",
                    "est_delivery_date": "source.est_delivery_date",
                    "start_est_delivery_date": "source.start_est_delivery_date",
                    "guarenteedtogetthere_date": "source.guarenteedtogetthere_date",
                    "line_item_type": "source.line_item_type",
                    "fulfillment_type": "source.fulfillment_type",
                    "purchase_order": "source.purchase_order",
                    "fulfill_location_id": "source.fulfill_location_id",
                    "fulfill_address1": "source.fulfill_address1",
                    "fulfill_address2": "source.fulfill_address2",
                    "fulfill_address3": "source.fulfill_address3",
                    "fulfill_city": "source.fulfill_city",
                    "fulfill_state": "source.fulfill_state",
                    "fulfill_zip": "source.fulfill_zip",
                    "fulfill_order_id": "source.fulfill_order_id",
                    "fulfill_shipped_dttm": "source.fulfill_shipped_dttm",
                    "ship_sku": "source.ship_sku",
                    "ship_upc": "source.ship_upc",
                    "ship_mode": "source.ship_mode",
                    "ship_location_id": "source.ship_location_id",
                    "ship_carrier": "source.ship_carrier",
                    "ship_class": "source.ship_class",
                    "ship_tracking_num": "source.ship_tracking_num",
                    "ship_charge": "source.ship_charge",
                    "ship_address1": "source.ship_address1",
                    "ship_address2": "source.ship_address2",
                    "ship_address3": "source.ship_address3",
                    "ship_city": "source.ship_city",
                    "ship_state": "source.ship_state",
                    "ship_zip": "source.ship_zip",
                    "tax_product_code": "source.tax_product_code",
                    "est_ship_tax_shp_dtl": "source.est_ship_tax_shp_dtl",
                    "return_tracking_num": "source.return_tracking_num",
                    "return_reason": "source.return_reason",
                    "return_location": "source.return_location",
                    "return_dttm": "source.return_dttm",
                    "return_label_creation_date": "source.return_label_creation_date",
                    "return_unreceipted": "source.return_unreceipted",
                    "return_fraud_check_id": "source.return_fraud_check_id",
                    "return_pickup_date": "source.return_pickup_date",
                    "return_delivery_date": "source.return_delivery_date",
                    "return_process_date": "source.return_process_date",
                    "return_source": "source.return_source",
                    "unit_cancel_source": "source.unit_cancel_source",
                    "unit_cancel_dttm": "source.unit_cancel_dttm",
                    "unit_cancel_reason": "source.unit_cancel_reason",
                    "date_added": "source.date_added",
                    "gold_created_by": "source.gold_created_by",
                    "gold_created_on_utc": "source.gold_created_on_utc",
                }
            )
        ).execute()
