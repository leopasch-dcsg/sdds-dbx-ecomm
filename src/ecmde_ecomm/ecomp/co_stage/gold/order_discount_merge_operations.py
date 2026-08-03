from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation


class GoldOrderMessageDiscount(MergeOperation):
    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            SELECT *
              FROM {self.config.source_table_qualified()} silver
             WHERE silver.silver_created_on_utc >= '{last_watermark_utc}'
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
                "external_item_id",
                "order_line_unit_seq",
                "discount",
                "applicable_discount_amount",
                "discount_level",
                "discount_name",
                "discount_desc",
                "cart_desc",
                "discount_ext_id",
                "discount_type",
                "discount_code",
                "discount_applied_dttm",
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
                AND target.external_item_id = source.external_item_id
                AND target.order_line_unit_seq = source.order_line_unit_seq
                AND target.discount_level = source.discount_level
                AND target.discount_name = source.discount_name
                AND target.discount_ext_id = source.discount_ext_id
                AND target.discount_type = source.discount_type
                """,
            )
            .whenMatchedUpdate(
                set={
                    "message_key": "source.message_key",
                    "message_dttm": "source.message_dttm",
                    "discount": "source.discount",
                    "applicable_discount_amount": "source.applicable_discount_amount",
                    "discount_applied_dttm": "source.discount_applied_dttm",
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
                    "external_item_id": "source.external_item_id",
                    "order_line_unit_seq": "source.order_line_unit_seq",
                    "discount": "source.discount",
                    "applicable_discount_amount": "source.applicable_discount_amount",
                    "discount_level": "source.discount_level",
                    "discount_name": "source.discount_name",
                    "discount_desc": "source.discount_desc",
                    "cart_desc": "source.cart_desc",
                    "discount_ext_id": "source.discount_ext_id",
                    "discount_type": "source.discount_type",
                    "discount_code": "source.discount_code",
                    "discount_applied_dttm": "source.discount_applied_dttm",
                    "date_added": "source.date_added",
                    "gold_created_by": "source.gold_created_by",
                    "gold_created_on_utc": "source.gold_created_on_utc",
                }
            )
            .execute()
        )
