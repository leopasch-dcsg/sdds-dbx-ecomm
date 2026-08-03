from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from ecmde_ecomm.common.dbx.etl.merge import MergeOperation


class GoldOrderMessagePlacedPayment(MergeOperation):
    def get_sql_statement_for_incremental_changes(
        self, last_watermark: datetime
    ) -> str:
        return f"""
            Select *
            from {self.config.source_table_qualified()} silver
            where silver.silver_created_on_utc >= '{last_watermark}'
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return (
            incremental_changeset.withColumn("date_added", lit(self.batch_date_utc))
            .withColumn("ecomp_created_by", lit(self.config.dbx_user_id))
            .withColumn("ecomp_created_on_utc", lit(self.batch_date_utc))
            .withColumn("ecomp_updated_by", lit(self.config.dbx_user_id))
            .withColumn("ecomp_updated_on_utc", lit(self.batch_date_utc))
            .select(
                "message_key",
                "message_dttm",
                "order_number",
                "order_last_update_dttm",
                "payment_type",
                "card_number",
                "authorized_amount",
                "date_added",
                "ecomp_created_by",
                "ecomp_created_on_utc",
                "ecomp_updated_by",
                "ecomp_updated_on_utc",
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
                and target.payment_type = source.payment_type
                and target.card_number = source.card_number
                """,
            )
            .whenMatchedUpdate(
                set={
                    "message_key": "source.message_key",
                    "message_dttm": "source.message_dttm",
                    "order_last_update_dttm": "source.order_last_update_dttm",
                    "authorized_amount": "source.authorized_amount",
                    "ecomp_updated_by": "source.ecomp_updated_by",
                    "ecomp_updated_on_utc": "source.ecomp_updated_on_utc",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "message_key": "source.message_key",
                    "message_dttm": "source.message_dttm",
                    "order_number": "source.order_number",
                    "order_last_update_dttm": "source.order_last_update_dttm",
                    "payment_type": "source.payment_type",
                    "card_number": "source.card_number",
                    "authorized_amount": "source.authorized_amount",
                    "date_added": "source.date_added",
                    "ecomp_created_by": "source.ecomp_created_by",
                    "ecomp_created_on_utc": "source.ecomp_created_on_utc",
                    "ecomp_updated_by": "source.ecomp_updated_by",
                    "ecomp_updated_on_utc": "source.ecomp_updated_on_utc",
                }
            )
        ).execute()
