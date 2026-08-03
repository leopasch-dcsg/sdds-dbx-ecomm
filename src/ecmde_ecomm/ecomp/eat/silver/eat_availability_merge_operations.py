from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, lit

from ecmde_ecomm.common.dbx.etl.merge import MergeOperation
from ecmde_ecomm.common.util import DateUtil


class SilverEatAvailability(MergeOperation):
    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            select row_number() over (partition by sku_inv.sku, sku_inv.location, sku_inv.inventory_change_event_etc order by sku_inv.offset desc) as row_number,
                   sku_inv.partition,
                   sku_inv.offset,
                   sku_inv.sku,
                   sku_inv.location,
                   sku_inv.atsqty,
                   sku_inv.isaqty,
                   sku_inv.boplqty,
                   sku_inv.inventory_change_event_etc,
                   sku_inv.isDeleted
              from (
                    select bronze.partition,
                           bronze.offset,
                           bronze.sku,
                           bronze.location,
                           nvl(bronze.atsqty, 0) atsqty,
                           nvl(bronze.isaqty, 0) isaqty,
                           nvl(bronze.boplqty, 0) boplqty,
                           coalesce(
                                bronze.time, 
                                convert_timezone(
                                    'UTC', 
                                    'America/New_York', 
                                    bronze.message_timestamp
                                )
                           ) as inventory_change_event_etc,
                           nvl(bronze.isDeleted, false) as isDeleted
                     from {self.config.source_table_qualified()} bronze
                     where bronze.ingested_on_utc >= '{last_watermark_utc}'
                       and bronze.sku is not null
                       and bronze.location is not null
              ) sku_inv
            qualify row_number = 1;
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return (
            incremental_changeset.withColumn(
                "atp_qty",
                when(col("isDeleted") == True, lit(0)).otherwise(col("atsqty")),
            )
            .withColumn(
                "isa_qty",
                when(col("isDeleted") == True, lit(0)).otherwise(col("isaqty")),
            )
            .withColumn(
                "bopl_qty",
                when(col("isDeleted") == True, lit(0)).otherwise(col("boplqty")),
            )
            .withColumnRenamed("location", "location_id")
            .withColumn(
                "inventory_change_event_utc",
                DateUtil.make_timestamp_with_zone(
                    col("inventory_change_event_etc"), "America/New_York"
                ),
            )
            .withColumn("silver_created_by", lit(self.config.dbx_user_id))
            .withColumn("silver_created_on_utc", lit(self.batch_date_utc))
            .withColumn("silver_updated_by", lit(self.config.dbx_user_id))
            .withColumn("silver_updated_on_utc", lit(self.batch_date_utc))
            .select(
                "sku",
                "location_id",
                "atp_qty",
                "isa_qty",
                "bopl_qty",
                "inventory_change_event_utc",
                "silver_created_by",
                "silver_created_on_utc",
                "silver_updated_by",
                "silver_updated_on_utc",
            )
        ).alias("source")

    def merge_updates(self, updates: DataFrame) -> None:
        (
            DeltaTable.forName(self.spark, self.config.destination_table_qualified())
            .alias("target")
            .merge(
                updates,
                """
                target.sku = source.sku
                and target.location_id = source.location_id
                and target.inventory_change_event_utc = source.inventory_change_event_utc
                """,
            )
            .whenMatchedUpdate(
                set={
                    "atp_qty": "source.atp_qty",
                    "isa_qty": "source.isa_qty",
                    "bopl_qty": "source.bopl_qty",
                    "silver_updated_by": "source.silver_updated_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "sku": "source.sku",
                    "location_id": "source.location_id",
                    "atp_qty": "source.atp_qty",
                    "isa_qty": "source.isa_qty",
                    "bopl_qty": "source.bopl_qty",
                    "inventory_change_event_utc": "source.inventory_change_event_utc",
                    "silver_created_by": "source.silver_created_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                }
            )
        ).execute()


class EcompEatChainAtpMerge(MergeOperation):

    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            select row_number() over (partition by chain_atp.sku, chain_atp.location, chain_atp.inventory_change_event_etc order by chain_atp.offset desc) as row_number,
                   chain_atp.location,
                   chain_atp.sku,
                   chain_atp.atsqty,
                   chain_atp.isaqty,
                   chain_atp.boplqty,
                   chain_atp.inventory_change_event_etc,
                   chain_atp.isDeleted,
                   chain_atp.message_timestamp,
                   chain_atp.partition,
                   chain_atp.offset
              from (
                  select COALESCE(bronze.key.sku, bronze.sku) AS sku,
                         COALESCE(bronze.key.location, bronze.location) AS location,
                         CASE WHEN bronze.atsqty IS NULL THEN 0
                              ELSE COALESCE(bronze.atsqty, 0)
                         END AS atsqty,
                         CASE WHEN bronze.isaqty IS NULL THEN 0
                              ELSE COALESCE(bronze.isaqty, 0)
                         END AS isaqty,
                         CASE WHEN bronze.boplqty IS NULL THEN 0
                              ELSE COALESCE(bronze.boplqty, 0)
                         END AS boplqty,
                         CASE WHEN bronze.time IS NULL THEN bronze.message_timestamp
                              ELSE COALESCE(
                                    bronze.time,
                                    CONVERT_TIMEZONE('UTC','America/New_York', bronze.message_timestamp)
                              )
                         END AS inventory_change_event_etc,
                         CASE WHEN bronze.isDeleted IS NULL THEN TRUE
                              ELSE bronze.isDeleted
                         END AS isDeleted,
                         bronze.message_timestamp,
                         bronze.partition,
                         bronze.offset
                    from {self.config.source_table_qualified()} bronze
                   where bronze.ingested_on_utc >= '{last_watermark_utc}'
              ) chain_atp
            qualify row_number = 1;
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return (
            incremental_changeset.withColumnRenamed("sku", "item_id")
            .withColumnRenamed("location", "store_id")
            .withColumn(
                "bopis_atp_qty",
                when(col("isDeleted") == True, lit(0)).otherwise(col("atsqty")),
            )
            .withColumn(
                "isa_qty",
                when(col("isDeleted") == True, lit(0)).otherwise(col("isaqty")),
            )
            .withColumn(
                "bopl_qty",
                when(col("isDeleted") == True, lit(0)).otherwise(col("boplqty")),
            )
            .withColumnRenamed("isDeleted", "is_deleted")
            .withColumn(
                "date_last_modified_eom",
                DateUtil.make_timestamp_with_zone(
                    col("inventory_change_event_etc"), "America/New_York"
                ),
            )
            .withColumn("date_added", lit(self.batch_date_utc))
            .select(
                "message_timestamp",
                "partition",
                "offset",
                "item_id",
                "store_id",
                "bopis_atp_qty",
                "isa_qty",
                "bopl_qty",
                "is_deleted",
                "date_last_modified_eom",
                "date_added",
            )
        ).alias("source")

    def merge_updates(self, updates: DataFrame) -> None:
        (
            DeltaTable.forName(self.spark, self.config.destination_table_qualified())
            .alias("target")
            .merge(
                updates,
                """
                target.item_id = source.item_id
                and target.store_id = source.store_id
                and target.date_last_modified_eom = source.date_last_modified_eom
                """,
            )
            .whenMatchedUpdate(
                set={
                    "bopis_atp_qty": "source.bopis_atp_qty",
                    "isa_qty": "source.isa_qty",
                    "bopl_qty": "source.bopl_qty",
                    "is_deleted": "source.is_deleted",
                    "message_timestamp": "source.message_timestamp",
                    "partition": "source.partition",
                    "offset": "source.offset",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "item_id": "source.item_id",
                    "store_id": "source.store_id",
                    "bopis_atp_qty": "source.bopis_atp_qty",
                    "isa_qty": "source.isa_qty",
                    "bopl_qty": "source.bopl_qty",
                    "is_deleted": "source.is_deleted",
                    "date_added": "source.date_added",
                    "date_last_modified_eom": "source.date_last_modified_eom",
                    "message_timestamp": "source.message_timestamp",
                    "partition": "source.partition",
                    "offset": "source.offset",
                }
            )
        ).execute()
