from datetime import datetime, timezone

from delta.tables import DeltaTable

from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql.functions import from_utc_timestamp

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation, MergeConfig
from ecmde_ecomm.fulfillment.model.chain_code import ChainCode
from ecmde_ecomm.fulfillment.model.trans_type import TransType
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.util import DateUtil
from enum import Enum


def add_duplicate_ranking_columns(changeset: DataFrame) -> DataFrame:

    line_dup_window = Window.partitionBy(
        "order_id",
        "sku",
        "line_seq_number",
        "fr_sequence_number",
    ).orderBy("line_number")

    sku_group_window = Window.partitionBy(
        "order_id",
        "sku",
        "fr_sequence_number",
    )

    dup_offset_window = (
        Window.partitionBy(
            "order_id",
            "sku",
            "fr_sequence_number",
        )
        .orderBy("line_seq_number", "line_number")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        changeset.withColumn("_dup_rank", F.row_number().over(line_dup_window))
        .withColumn(
            "_max_line_seq_number", F.max("line_seq_number").over(sku_group_window)
        )
        .withColumn(
            "_dup_offset",
            F.sum(F.when(F.col("_dup_rank") > 1, 1).otherwise(0)).over(
                dup_offset_window
            ),
        )
    )


def add_effective_line_seq_number(changeset: DataFrame) -> DataFrame:

    is_primary_row = F.col("_dup_rank") == 1
    bumped_line_seq_number = F.col("_max_line_seq_number") + F.col("_dup_offset")

    return (
        add_duplicate_ranking_columns(changeset)
        .withColumn(
            "effective_line_seq_number",
            F.when(is_primary_row, F.col("line_seq_number")).otherwise(
                bumped_line_seq_number
            ),
        )
        .drop("_dup_rank", "_max_line_seq_number", "_dup_offset")
    )


def perform_txn_order_sku_transforms(
    incremental_changeset: DataFrame,
    oso_catalog: str,
    ecomp_catalog: str,
    dbx_user_id: str,
    batch_date_utc: datetime,
    last_watermark_utc: datetime,
) -> DataFrame:

    spark = incremental_changeset.sparkSession

    order_df = incremental_changeset.select("order_id").distinct()

    window_spec = Window.partitionBy(
        "order_id", "line_number", "line_seq_number"
    ).orderBy(F.col("audit_seq_number"))
    rank_window_spec = Window.partitionBy(
        "order_id",
        "line_number",
        "line_seq_number",
        "item_status",
        "fr_sequence_number",
    ).orderBy(F.col("audit_seq_number").desc())

    item_audit = (
        spark.table(f"{oso_catalog}.oso.newman_item_audit")
        .join(order_df, "order_id")
        .filter(F.col("audit_timestamp").isNotNull())
        .groupBy(
            "order_id",
            "line_number",
            "line_seq_number",
            "audit_timestamp",
            "audit_seq_number",
        )
        .pivot(
            "field_name",
            [
                "itemStatus",
                "sourceFacilityNumber",
                "expectedShipDate",
                "frSeq",
                "actionCode",
            ],
        )
        .agg(F.first("new_value"))
        .withColumn(
            "item_status", F.last("itemStatus", ignorenulls=True).over(window_spec)
        )
        .withColumn(
            "source_store_cd",
            F.last("sourceFacilityNumber", ignorenulls=True).over(window_spec),
        )
        .withColumn(
            "expected_ship_date",
            F.last("expectedShipDate", ignorenulls=True).over(window_spec),
        )
        .withColumn(
            "fr_sequence_number", F.last("frSeq", ignorenulls=True).over(window_spec)
        )
        .withColumn(
            "action_code", F.last("actionCode", ignorenulls=True).over(window_spec)
        )
        .withColumn("_rank", F.row_number().over(rank_window_spec))
        .filter(F.col("_rank") == 1)
        .drop(
            "itemStatus",
            "sourceFacilityNumber",
            "expectedShipDate",
            "frSeq",
            "actionCode",
            "_rank",
        )
        .filter(F.col("fr_sequence_number") != "null")
        .filter(F.col("item_status") != "null")
        .filter(
            F.col("item_status")
            .try_cast("int")
            .isin(111, 112, 113, 114, 200, 790, 810, 999)
        )
        .alias("item_audit")
    )

    trans_type_key_expr = (
        F.when(
            F.col("combined_txn.item_status") == TransType.Allocated.item_status,
            TransType.Allocated.key,
        )
        .when(
            F.col("combined_txn.item_status") == TransType.Declined.item_status,
            TransType.Declined.key,
        )
        .when(
            F.col("combined_txn.item_status") == TransType.SaveOfferSent.item_status,
            TransType.SaveOfferSent.key,
        )
        .when(
            F.col("combined_txn.item_status")
            == TransType.SaveOfferAccepted.item_status,
            TransType.SaveOfferAccepted.key,
        )
        .when(
            F.col("combined_txn.item_status") == TransType.SaveOfferExpired.item_status,
            TransType.SaveOfferExpired.key,
        )
        .when(
            F.col("combined_txn.item_status")
            == TransType.SaveOfferRejected.item_status,
            TransType.SaveOfferRejected.key,
        )
        .otherwise(None)
        .cast(IntegerType())
    )

    order_input = (
        spark.table(f"{oso_catalog}.oso.newman_order")
        .select("order_id", "order_input_source")
        .alias("order_input")
    )

    decline_codes = (
        spark.table(f"{ecomp_catalog}.ecom_dim.reason")
        .filter(F.col("data_source_key") == 23)
        .select(F.col("reason_code").alias("code"))
        .alias("decline_codes")
    )

    cancel_codes_src = spark.table("prod_oso_db.oso_ref.cancel_code")

    cancel_codes = (
        cancel_codes_src.select(
            [F.col(col).cast("string") for col in cancel_codes_src.columns]
        )
        .withColumnRenamed("code", "cancel_code")
        .alias("cancel_codes")
    )

    source_reason = F.coalesce(
        F.col("combined_txn.legacy"),
        F.col("combined_txn.decline_cancel_code"),
    )

    decline_txn = item_audit.filter(
        F.col("item_audit.item_status")
        .try_cast("int")
        .isin(112, 113, 114, 790, 810, 999)
    ).alias("decline_txn")

    decline_txn_with_default = decline_txn.withColumn(
        "action_code",
        F.when(F.col("decline_txn.item_status") == 810, "BOPL_FAKE_DECLINE")
        .when(F.col("decline_txn.item_status") == 112, "STS_ACCEPTED")
        .when(F.col("decline_txn.item_status") == 113, "STS_REJECTED")
        .when(F.col("decline_txn.item_status") == 114, "STS_EXPIRED")
        .otherwise(F.col("action_code")),
    )

    decline_detail = (
        decline_txn_with_default.groupBy(
            "order_id", "line_number", "line_seq_number", "fr_sequence_number"
        )
        .agg(
            F.min(F.struct(F.col("audit_timestamp"), F.col("action_code"))).alias(
                "min_struct"
            )
        )
        .select(
            "*",
            F.col("min_struct.audit_timestamp").alias("decline_dttm"),
            F.col("min_struct.action_code").alias("action_code"),
        )
        .drop("min_struct")
        .alias("decline_detail")
    )

    decline_counts = (
        decline_detail.withColumn("decline_cancel_code", F.col("action_code"))
        .join(
            decline_codes,
            on=(
                (F.col("action_code").isNotNull())
                & (
                    F.concat(F.col("action_code").cast("string"), F.lit("NMN"))
                    == F.col("decline_codes.code")
                )
            ),
            how="left",
        )
        .join(
            cancel_codes,
            on=(
                (F.col("action_code").isNotNull())
                & (F.col("action_code") == F.col("cancel_codes.cancel_code"))
            ),
            how="left",
        )
        .withColumnRenamed("decline_codes.code", "decline_code")
        .withColumn("decline_unit", F.lit(1))
        .drop("cancel_code")
        .alias("decline_counts")
    )

    new_changeset = add_effective_line_seq_number(
        incremental_changeset.alias("new_changeset")
    ).alias("new_changeset")

    allocated_txn = (
        item_audit.filter(
            F.col("item_audit.item_status").try_cast("int").isin(111, 200)
        )
        .drop(F.col("action_code"))
        .alias("allocated_txn")
    )

    combined_txn = (
        allocated_txn.join(
            decline_counts,
            ["order_id", "line_number", "line_seq_number", "fr_sequence_number"],
            how="full",
        )
    ).alias("combined_txn")

    df = new_changeset.join(
        combined_txn,
        ["order_id", "line_number", "line_seq_number", "fr_sequence_number"],
        "inner",
    ).join(order_input, "order_id", "left")

    chain_key = (
        F.when(
            F.col("order_input.order_input_source") == ChainCode.G3.code,
            ChainCode.G3.key,
        )
        .when(
            F.col("order_input.order_input_source") == ChainCode.GolfGalaxy.code,
            ChainCode.GolfGalaxy.key,
        )
        .when(
            F.col("order_input.order_input_source")
            == ChainCode.DicksSportingGoods.code,
            ChainCode.DicksSportingGoods.key,
        )
        .when(
            F.col("order_input.order_input_source") == ChainCode.Moosejaw.code,
            ChainCode.Moosejaw.key,
        )
        .when(
            F.col("order_input.order_input_source") == ChainCode.PublicLands.code,
            ChainCode.PublicLands.key,
        )
        .otherwise(ChainCode.UNK.key)
        .cast(IntegerType())
    )

    fulfill_num = F.concat(
        F.col("combined_txn.order_id").cast("string"),
        F.lpad(F.col("combined_txn.fr_sequence_number").cast("string"), 4, "0"),
    ).cast("bigint")

    alloc_seq_expr = F.concat(
        F.lpad(F.col("new_changeset.sku").cast("string"), 8, "0"),
        F.lpad(F.col("new_changeset.effective_line_seq_number").cast("string"), 4, "0"),
        F.lpad(F.col("combined_txn.fr_sequence_number").cast("string"), 4, "0"),
    )

    expected_ship_date_expr = F.to_utc_timestamp(
        F.date_add(
            F.to_date(F.lit("1970-01-01")),
            F.when(
                F.col("combined_txn.expected_ship_date") != "null",
                F.col("combined_txn.expected_ship_date").cast(IntegerType()),
            ).otherwise(None),
        ),
        "America/New_York",
    )

    txn_ts = F.coalesce(
        F.col("combined_txn.audit_timestamp"), F.col("new_changeset.sourced_timestamp")
    )

    enriched = df.select(
        trans_type_key_expr.alias("trans_type_key"),
        F.date_format(from_utc_timestamp(txn_ts, "America/New_York"), "yyyyMMdd")
        .cast("bigint")
        .alias("txn_date_key"),
        F.date_format(from_utc_timestamp(txn_ts, "America/New_York"), "HH:mm:ss").alias(
            "time_code_24"
        ),
        F.coalesce(
            alloc_seq_expr,
            F.col("new_changeset.external_line_id"),
        ).alias("txn_seq_number"),
        F.col("combined_txn.order_id").alias("web_ord_num"),
        F.col("new_changeset.sku").alias("dks_sku"),
        F.lit(1).alias("units"),
        F.upper(F.trim(F.col("new_changeset.package_tracking_id"))).alias(
            "tracking_number"
        ),
        fulfill_num.alias("order_fulfill_number"),
        expected_ship_date_expr.alias("estimated_ship_date"),
        source_reason.alias("source_reason_cd"),
        F.col("combined_txn.decline_dttm"),
        F.col("combined_txn.decline_unit"),
        chain_key.alias("chain_key"),
        F.col("combined_txn.source_store_cd").alias("source_store_cd"),
        F.col("new_changeset.nmn_updated_timestamp").alias("date_last_modified"),
        F.lit(batch_date_utc).alias("silver_created_on_utc"),
        F.lit(dbx_user_id).alias("silver_created_by"),
        F.lit(batch_date_utc).alias("silver_updated_on_utc"),
        F.lit(dbx_user_id).alias("silver_updated_by"),
        F.lit("OSO-Silver").alias("modified_by"),
    )

    time_dim = (
        spark.table(f"{ecomp_catalog}.ecom_dim.time_dim")
        .select(F.col("time_code_24").alias("time_code_24_dim"), F.col("time_key"))
        .alias("tm")
    )

    with_time_key = enriched.alias("en").join(
        time_dim,
        on=F.col("en.time_code_24") == F.col("tm.time_code_24_dim"),
        how="left",
    )

    final = with_time_key.select(
        "trans_type_key",
        "txn_date_key",
        F.col("tm.time_key").alias("txn_time_key"),
        "txn_seq_number",
        "web_ord_num",
        "dks_sku",
        "units",
        "tracking_number",
        "order_fulfill_number",
        "estimated_ship_date",
        "source_reason_cd",
        "decline_dttm",
        "decline_unit",
        "chain_key",
        "source_store_cd",
        "date_last_modified",
        "silver_created_on_utc",
        "silver_created_by",
        "silver_updated_on_utc",
        "silver_updated_by",
        "modified_by",
    )

    dedupe_win = Window.partitionBy(
        "web_ord_num", "txn_date_key", "txn_time_key", "txn_seq_number", "chain_key"
    ).orderBy(F.col("date_last_modified").desc())

    return (
        final.withColumn("_rk", F.row_number().over(dedupe_win))
        .filter(F.col("_rk") == 1)
        .drop("_rk")
    )


class TxnOrderSkuMerge(MergeOperation):

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
        super().__init__(config, watermark, spark, batch_date_utc)
        self.oso_catalog = oso_catalog
        self.fit_catalog = fit_catalog
        self.ecomp_catalog = ecomp_catalog

    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
                 SELECT
                       history.order_id,
                       history.sku,
                       history.line_number,
                       history.line_seq_number,
                       history.source_facility_number,
                       history.final_status AS item_status,
                       history.action_code,
                       item.ordered_fulfillment_mode,
                       history.executed_fulfillment_mode,
                       item.po_number,
                       history.sourced_timestamp,
                       history.fr_sequence_number,
                       item.package_tracking_id,
                       item.expected_ship_date,
                       item.external_line_id,
                       history.completed_timestamp AS nmn_updated_timestamp
                    FROM {self.oso_catalog}.oso.newman_fr_history history
                    JOIN {self.config.source_table_qualified()} item ON item.order_id = history.order_id and item.line_number = history.line_number and item.line_seq_number = history.line_seq_number
                    WHERE history.silver_layer_update_timestamp >= '{last_watermark_utc}' - INTERVAL 5 MINUTES
                    AND lower(history.managed_by) = 'newman'
                    AND lower(history.system) in ('fit','vft','wm', 'otv', 'ao-consumer', 'oso')
                    UNION ALL
                    SELECT
                       order_id,
                       sku,
                       line_number,
                       line_seq_number,
                       source_facility_number,
                       item_status,
                       action_code,
                       ordered_fulfillment_mode,
                       executed_fulfillment_mode,
                       po_number,
                       sourced_timestamp,
                       fr_sequence_number,
                       package_tracking_id,
                       expected_ship_date,
                       external_line_id,
                       nmn_updated_timestamp
                    FROM {self.config.source_table_qualified()}
                    WHERE item_status NOT IN (800, 999)
                    AND silver_layer_update_timestamp >= '{last_watermark_utc}' - INTERVAL 5 MINUTES
                    AND lower(managed_by) = 'newman'
                    AND lower(routing_partner) in ('fit','vft','otv','wm')
                """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return perform_txn_order_sku_transforms(
            incremental_changeset,
            self.oso_catalog,
            self.ecomp_catalog,
            self.config.dbx_user_id,
            self.batch_date_utc,
            self.watermark.last_watermark_utc(),
        )

    def merge_updates(self, updates: DataFrame) -> None:
        target = DeltaTable.forName(
            self.spark, self.config.destination_table_qualified()
        )
        merge_cond = """
            target.txn_date_key      = source.txn_date_key
            AND target.txn_time_key  = source.txn_time_key
            AND target.web_ord_num   = source.web_ord_num
            AND target.txn_seq_number= source.txn_seq_number
            AND target.chain_key     = source.chain_key
        """
        (
            target.alias("target")
            .merge(updates.alias("source"), merge_cond)
            .whenMatchedUpdate(
                set={
                    "trans_type_key": "source.trans_type_key",
                    "tracking_number": "source.tracking_number",
                    "order_fulfill_number": "source.order_fulfill_number",
                    "estimated_ship_date": "source.estimated_ship_date",
                    "source_reason_cd": "source.source_reason_cd",
                    "decline_dttm": "source.decline_dttm",
                    "decline_unit": "source.decline_unit",
                    "date_last_modified": "source.date_last_modified",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                    "modified_by": "source.modified_by",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "trans_type_key": "source.trans_type_key",
                    "txn_date_key": "source.txn_date_key",
                    "txn_time_key": "source.txn_time_key",
                    "web_ord_num": "source.web_ord_num",
                    "txn_seq_number": "source.txn_seq_number",
                    "dks_sku": "source.dks_sku",
                    "units": "source.units",
                    "tracking_number": "source.tracking_number",
                    "order_fulfill_number": "source.order_fulfill_number",
                    "estimated_ship_date": "source.estimated_ship_date",
                    "source_reason_cd": "source.source_reason_cd",
                    "decline_dttm": "source.decline_dttm",
                    "decline_unit": "source.decline_unit",
                    "chain_key": "source.chain_key",
                    "source_store_cd": "source.source_store_cd",
                    "date_last_modified": "source.date_last_modified",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                    "silver_created_by": "source.silver_created_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                    "modified_by": "source.modified_by",
                }
            )
            .execute()
        )
