from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import functions as F
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation, MergeConfig
from ecmde_ecomm.fulfillment.model.order_sku_status_code import OrderSkuStatus
from ecmde_ecomm.fulfillment.model.chain_code import ChainCode
from ecmde_ecomm.common.dbx.etl.watermark import Watermark


def perform_order_fulfillment_transforms(
    changeset_df: DataFrame,
    orders_df: DataFrame,
    dbx_user_id: str,
    batch_date_utc: datetime,
) -> DataFrame:

    partition_keys = ["order_id", "sku"]

    enriched_df = (
        changeset_df.alias("changeset")
        .join(
            orders_df.select("order_id", "order_input_source").alias("orders"),
            on="order_id",
            how="left",
        )
        .select(
            F.col("changeset.*"),
            F.col("orders.order_input_source"),
        )
    )

    min_status_window = Window.partitionBy(*partition_keys)
    enriched_with_min_status = enriched_df.withColumn(
        "min_non_999_item_status",
        F.coalesce(
            F.min(F.when(F.col("item_status") != 999, F.col("item_status"))).over(
                min_status_window
            ),
            F.lit(999),
        ),
    )

    ordering_window = Window.partitionBy(*partition_keys).orderBy("line_seq_number")
    count_window = Window.partitionBy(*partition_keys)

    initial_df = (
        enriched_with_min_status.withColumn(
            "row_number", F.row_number().over(ordering_window)
        )
        .withColumn(
            "min_do_create_dttm_utc", F.min("sourced_timestamp").over(count_window)
        )
        .withColumn("po_number", F.min("po_number").over(count_window))
        .withColumn("save_the_sale", F.max("save_the_sale").over(count_window))
        .filter("row_number = 1")
        .drop("row_number")
        .withColumn(
            "order_sku_status_key",
            F.when(
                F.col("min_non_999_item_status").isin(
                    *OrderSkuStatus.PACKED.status_ids
                ),
                F.lit(OrderSkuStatus.PACKED.key),
            )
            .when(
                F.col("min_non_999_item_status").isin(
                    *OrderSkuStatus.FULFILLED.status_ids
                ),
                F.lit(OrderSkuStatus.FULFILLED.key),
            )
            .when(
                F.col("min_non_999_item_status").isin(
                    *OrderSkuStatus.UNKNOWN.status_ids
                ),
                F.lit(OrderSkuStatus.UNKNOWN.key),
            )
            .otherwise(F.lit(0)),
        )
        .filter(F.col("order_sku_status_key") != 0)
        .withColumn(
            "chain_key",
            F.when(F.col("order_input_source") == ChainCode.G3.code, ChainCode.G3.key)
            .when(
                F.col("order_input_source") == ChainCode.GolfGalaxy.code,
                ChainCode.GolfGalaxy.key,
            )
            .when(
                F.col("order_input_source") == ChainCode.DicksSportingGoods.code,
                ChainCode.DicksSportingGoods.key,
            )
            .when(
                F.col("order_input_source") == ChainCode.Moosejaw.code,
                ChainCode.Moosejaw.key,
            )
            .when(
                F.col("order_input_source") == ChainCode.PublicLands.code,
                ChainCode.PublicLands.key,
            )
            .otherwise(ChainCode.UNK.key)
            .cast("int"),
        )
    )

    status_aggregation_df = (
        changeset_df.filter(F.col("item_status") != 999)
        .groupBy(*partition_keys)
        .agg(
            F.max("nmn_updated_timestamp").alias("order_sku_status_dttm"),
            F.max("silver_layer_update_timestamp").alias("date_last_modified_utc"),
        )
    )

    joined_status_df = (
        initial_df.join(status_aggregation_df, on=partition_keys, how="left")
        .withColumn("silver_created_on_utc", F.lit(batch_date_utc))
        .withColumn("silver_created_by", F.lit(dbx_user_id))
        .withColumn("silver_updated_on_utc", F.lit(batch_date_utc))
        .withColumn("silver_updated_by", F.lit(dbx_user_id))
        .withColumn("modified_by", F.lit("OSOSilver"))
    )

    dedupe_window = Window.partitionBy(*partition_keys).orderBy(
        F.col("date_last_modified_utc").desc()
    )
    final_df = (
        joined_status_df.withColumn("row_rank", F.row_number().over(dedupe_window))
        .filter(F.col("row_rank") == 1)
        .drop("row_rank")
    )

    return final_df.select(
        F.col("order_id").alias("web_ord_num"),
        "sku",
        "order_sku_status_key",
        "po_number",
        "save_the_sale",
        "min_do_create_dttm_utc",
        "chain_key",
        "order_sku_status_dttm",
        "date_last_modified_utc",
        "modified_by",
        "silver_created_on_utc",
        "silver_created_by",
        "silver_updated_on_utc",
        "silver_updated_by",
    )


class SilverOrderFulfillment(MergeOperation):

    def __init__(
        self,
        oso_catalog: str,
        fit_catalog: str,
        ecomp_catalog: str,
        config: MergeConfig,
        watermark: Watermark,
        spark: SparkSession,
        batch_date_utc: datetime = datetime.now(timezone.utc),
    ):
        if batch_date_utc is None:
            batch_date_utc = datetime.now(timezone.utc)

        super().__init__(config, watermark, spark, batch_date_utc)
        self.oso_catalog = oso_catalog
        self.fit_catalog = fit_catalog
        self.ecomp_catalog = ecomp_catalog

    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
            SELECT
                CAST(oso.order_id AS BIGINT)             AS order_id,
                CAST(oso.sku      AS BIGINT)             AS sku,
                oso.item_status,
                oso.po_number,
                CASE
                    WHEN oso.item_status =112 OR history.final_status = 112 THEN 'Y'
                    ELSE null
                END AS save_the_sale,
                oso.line_seq_number ,
                oso.sourced_timestamp,
                oso.nmn_updated_timestamp,
                oso.silver_layer_update_timestamp    
            FROM {self.config.source_table_qualified()} oso
            JOIN {self.oso_catalog}.oso.newman_fr_history history
            ON
                oso.order_id = history.order_id and oso.line_number = history.line_number and oso.line_seq_number = history.line_seq_number
            WHERE oso.silver_layer_update_timestamp >= '{last_watermark_utc}'
              AND lower(oso.managed_by) = 'newman'
              AND (lower(oso.routing_partner) in ('fit','vft', 'otv','wm') OR lower(history.system) ='ao-consumer')
        """

    def perform_changeset_transforms(self, changeset_df: DataFrame) -> DataFrame:
        newman_orders_df = self.spark.table(f"{self.oso_catalog}.oso.newman_order")
        return perform_order_fulfillment_transforms(
            changeset_df,
            newman_orders_df,
            self.config.dbx_user_id,
            self.batch_date_utc,
        )

    def merge_updates(self, updates_df: DataFrame) -> None:
        (
            DeltaTable.forName(self.spark, self.config.destination_table_qualified())
            .alias("target")
            .merge(
                updates_df.alias("source"),
                "target.web_ord_num = source.web_ord_num AND target.sku = source.sku",
            )
            .whenMatchedUpdate(
                set={
                    "order_sku_status_key": "source.order_sku_status_key",
                    "po_number": "source.po_number",
                    "save_the_sale": "source.save_the_sale",
                    "min_do_create_dttm_utc": "source.min_do_create_dttm_utc",
                    "chain_key": "source.chain_key",
                    "order_sku_status_dttm": "source.order_sku_status_dttm",
                    "date_last_modified_utc": "source.date_last_modified_utc",
                    "modified_by": "source.modified_by",
                    "silver_updated_by": "source.silver_updated_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "web_ord_num": "source.web_ord_num",
                    "sku": "source.sku",
                    "order_sku_status_key": "source.order_sku_status_key",
                    "po_number": "source.po_number",
                    "save_the_sale": "source.save_the_sale",
                    "min_do_create_dttm_utc": "source.min_do_create_dttm_utc",
                    "chain_key": "source.chain_key",
                    "order_sku_status_dttm": "source.order_sku_status_dttm",
                    "date_last_modified_utc": "source.date_last_modified_utc",
                    "modified_by": "source.modified_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                    "silver_created_by": "source.silver_created_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                }
            )
            .execute()
        )
