from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
)
from pyspark.sql.types import StringType
from ecmde_ecomm.common.util import DateUtil
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation

DISCOUNT_DEFAULT_ZONE = "America/New_York"


def perform_order_message_discount_transforms(
    incremental_changeset: DataFrame,
    dbx_user_id: str,
    batch_date_utc: datetime,
) -> DataFrame:
    partition_keys = [
        "order_number",
        "order_line_unit_seq",
        "external_item_id",
        "discount_level",
        "discount_name",
        "discount_ext_id",
        "discount_type",
    ]

    window_spec = Window.partitionBy(*partition_keys).orderBy(
        col("discount_applied_dttm").asc()
    )

    deduplicated_changeset = (
        incremental_changeset.withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
        .withColumn("order_number", col("order_number").cast(StringType()))
        .withColumn(
            "order_line_unit_seq", col("order_line_unit_seq").cast(StringType())
        )
        .withColumn("discount", col("discount").cast(StringType()))
        .withColumn(
            "applicable_discount_amount",
            col("applicable_discount_amount").cast(StringType()),
        )
        .withColumn(
            "discount_applied_dttm",
            DateUtil.make_timestamp_with_zone(
                col("discount_applied_dttm"), DISCOUNT_DEFAULT_ZONE
            ),
        )
        .withColumn(
            "message_dttm",
            DateUtil.make_timestamp_with_zone(
                col("message_dttm"), DISCOUNT_DEFAULT_ZONE
            ),
        )
        .withColumn("date_added", lit(batch_date_utc))
        .withColumn("silver_created_by", lit(dbx_user_id))
        .withColumn("silver_created_on_utc", lit(batch_date_utc))
    )

    return deduplicated_changeset


class SilverOrderMessageDiscount(MergeOperation):
    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            SELECT
                bronze.message_key AS message_key,
                MAX(bronze.message_dttm) AS message_dttm,
                CAST(bronze.order_number AS STRING) AS order_number,
                bronze.external_item_id AS external_item_id,
                CAST(bronze.order_line_unit_seq AS STRING) AS order_line_unit_seq,
                bronze.discount AS discount,
                bronze.applicable_discount_amount AS applicable_discount_amount,
                bronze.discount_level AS discount_level,
                bronze.discount_name AS discount_name,
                bronze.discount_desc AS discount_desc,
                bronze.cart_desc AS cart_desc,
                bronze.discount_ext_id AS discount_ext_id,
                bronze.discount_type AS discount_type,
                bronze.discount_code AS discount_code,
                MAX(bronze.discount_applied_dttm) AS discount_applied_dttm
            FROM {self.config.source_table_qualified()} bronze
            WHERE bronze.ingested_on_utc >= '{last_watermark_utc}'
              AND '{self.batch_date_utc}' >= make_timestamp(
                  date_part('YEAR', bronze.discount_applied_dttm),
                  date_part('MONTH', bronze.discount_applied_dttm),
                  date_part('DAY', bronze.discount_applied_dttm),
                  date_part('HOUR', bronze.discount_applied_dttm),
                  date_part('MINUTE', bronze.discount_applied_dttm),
                  date_part('SECOND', bronze.discount_applied_dttm),
                  '{DISCOUNT_DEFAULT_ZONE}'
              )
            GROUP BY
                bronze.message_key,
                CAST(bronze.order_number AS STRING),
                bronze.external_item_id,
                CAST(bronze.order_line_unit_seq AS STRING),
                bronze.discount,
                bronze.applicable_discount_amount,
                bronze.discount_level,
                bronze.discount_name,
                bronze.discount_desc,
                bronze.cart_desc,
                bronze.discount_ext_id,
                bronze.discount_type,
                bronze.discount_code
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return perform_order_message_discount_transforms(
            incremental_changeset, self.config.dbx_user_id, self.batch_date_utc
        ).select(
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
            "silver_created_by",
            "silver_created_on_utc",
            "date_added",
        )

    def merge_updates(self, updates: DataFrame) -> None:
        (
            DeltaTable.forName(self.spark, self.config.destination_table_qualified())
            .alias("target")
            .merge(
                updates.alias("source"),
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
                    "silver_created_by": "source.silver_created_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                    "date_added": "source.date_added",
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
                    "silver_created_by": "source.silver_created_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                    "date_added": "source.date_added",
                }
            )
            .execute()
        )
