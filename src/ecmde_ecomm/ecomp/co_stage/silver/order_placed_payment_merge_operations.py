from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    lit,
    col,
)
from pyspark.sql.types import StringType
from ecmde_ecomm.common.util import DateUtil
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation

ORDER_MESSAGE_PLACED_PAYMENT_DEFAULT_ZONE = "America/New_York"


def perform_order_message_placed_payment_transforms(
    incremental_changeset: DataFrame,
    dbx_user_id: str,
    batch_date_utc: datetime,
) -> DataFrame:
    return (
        incremental_changeset.withColumn(
            "message_key", col("message_key").cast(StringType())
        )
        .withColumn(
            "message_dttm",
            DateUtil.make_timestamp_with_zone(
                col("message_dttm"), ORDER_MESSAGE_PLACED_PAYMENT_DEFAULT_ZONE
            ),
        )
        .withColumn("order_number", col("order_number").cast(StringType()))
        .withColumn(
            "order_last_update_dttm",
            DateUtil.make_timestamp_with_zone(
                col("order_last_update_dttm"), ORDER_MESSAGE_PLACED_PAYMENT_DEFAULT_ZONE
            ),
        )
        .withColumn("payment_type", col("payment_type").cast(StringType()))
        .withColumn("card_number", col("cardnumber").cast(StringType()))
        .withColumn("authorized_amount", col("authorizedamount").cast(StringType()))
        .withColumn("silver_created_by", lit(dbx_user_id))
        .withColumn("silver_created_on_utc", lit(batch_date_utc))
        .withColumn("silver_updated_by", lit(dbx_user_id))
        .withColumn("silver_updated_on_utc", lit(batch_date_utc))
    )


class SilverOrderMessagePlacedPayment(MergeOperation):
    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            select row_number() over (partition by order_number, payment_type, cardnumber order by order_last_update_dttm asc) as row_number,
                   payments.MESSAGE_KEY,
                   payments.MESSAGE_DTTM,
                   payments.ORDER_NUMBER,
                   payments.ORDER_LAST_UPDATE_DTTM,
                   payments.PAYMENT_TYPE,
                   payments.CARDNUMBER,
                   payments.authorizedamount
              from (
                 select bronze.MESSAGE_KEY,
                        bronze.MESSAGE_DTTM,
                        bronze.ORDER_NUMBER,
                        bronze.ORDER_LAST_UPDATE_DTTM,
                        bronze.PAYMENT_TYPE,
                        bronze.CARDNUMBER,
                        bronze.authorizedamount
                   from {self.config.source_table_qualified()} bronze
                  where bronze.ingested_on_utc >= '{last_watermark_utc}'
              ) payments
            qualify row_number = 1;
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return (
            perform_order_message_placed_payment_transforms(
                incremental_changeset,
                self.config.dbx_user_id,
                self.batch_date_utc,
            )
            .select(
                "message_key",
                "message_dttm",
                "order_number",
                "order_last_update_dttm",
                "payment_type",
                "card_number",
                "authorized_amount",
                "silver_created_by",
                "silver_created_on_utc",
                "silver_updated_by",
                "silver_updated_on_utc",
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
                    "silver_updated_by": "source.silver_updated_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
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
                    "silver_created_by": "source.silver_created_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                }
            )
        ).execute()
