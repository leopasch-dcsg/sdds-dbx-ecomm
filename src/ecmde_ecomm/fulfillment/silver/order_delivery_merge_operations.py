from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    when,
    count as count_,
    max as max_,
    min as min_,
    lpad,
    date_format,
    coalesce,
    to_timestamp,
    concat,
    upper,
    trim,
    round as round_,
    countDistinct,
    translate,
    expr,
    sum as sum_,
    row_number,
    concat_ws,
)

from ecmde_ecomm.common.dbx.etl.merge import MergeOperation, MergeConfig
from ecmde_ecomm.fulfillment.model.fulfillmentmode_code import FulfillmentModeCode
from ecmde_ecomm.common.model.webstore_desc import WebstoreCode
from ecmde_ecomm.common.dbx.etl.watermark import Watermark


BUSINESS_COLS = [
    "order_delivery_key",
    "webstore_key",
    "package_type_id",
    "sci_lpn_id",
    "tracking_number",
    "delivery_status_cd",
    "fulfillment_date_key",
    "fulfillment_location_cd",
    "actual_shipped_dttm",
    "shipped_qty",
    "package_type_descr",
    "ship_via",
    "carrier_key",
    "fulfillment_mode_key",
    "order_fulfill_number",
    "web_ord_num",
    "lpn_create_dttm",
    "date_last_modified_utc",
    "modified_by",
    "silver_created_on_utc",
    "silver_created_by",
    "silver_updated_on_utc",
    "silver_updated_by",
]

BUSINESS_SET_DICT = {
    c: f"source.{c}" for c in BUSINESS_COLS if c != "order_delivery_key"
}


def perform_order_delivery_transforms(
    incremental_df: DataFrame,
    newman_order_df: DataFrame,
    webstore_dim_df: DataFrame,
    fit_package_events_df: DataFrame,
    carrier_dim_df: DataFrame,
    dom_package_type_dim_df: DataFrame,
    mode_dim_df: DataFrame,
    dbx_user_id: str,
    batch_date_utc: datetime,
    ecomp_catalog: str,
) -> DataFrame:

    incremental_clean_df = (
        incremental_df.withColumn(
            "tracking_clean", trim(upper(col("package_tracking_id")))
        )
        .withColumn(
            "carrier_code_lookup",
            when(
                upper(col("package_carrier")).isin(
                    "FDXEXPR", "FDXGRND", "FDXHOME", "FDE", "FDEG", "FEDG"
                ),
                lit("FEDEX"),
            )
            .when(upper(col("package_carrier")).isin("SEFL"), lit("GENERIC"))
            .otherwise(col("package_carrier")),
        )
        .alias("incremental_clean")
    )

    shipped_counts_df = (
        incremental_clean_df.select(
            col("order_id"),
            col("package_id"),
            col("line_seq_number"),
            col("line_number"),
        )
        .groupBy("order_id", "package_id")
        .agg(countDistinct("line_number", "line_seq_number").alias("shipped_qty"))
    )

    newman_order_alias = newman_order_df.alias("newman_order")
    webstore_dim_alias = webstore_dim_df.alias("webstore_dim")
    fit_package_alias = fit_package_events_df.withColumnRenamed(
        "package_id", "fit_package_id"
    ).alias("fit_package")

    carrier_map_df = carrier_dim_df.selectExpr(
        "carrier_code", "carrier_key AS carrier_key_mapped"
    ).alias("carrier_map")

    webstore_map_df = webstore_dim_alias.select(
        col("webstore_key"),
        when(
            col("WEBSTORE_DESC") == WebstoreCode.DicksSportingGoods.desc,
            WebstoreCode.DicksSportingGoods.code,
        )
        .when(col("WEBSTORE_DESC") == WebstoreCode.G3.desc, WebstoreCode.G3.code)
        .when(
            col("WEBSTORE_DESC") == WebstoreCode.GolfGalaxy.desc,
            WebstoreCode.GolfGalaxy.code,
        )
        .when(
            col("WEBSTORE_DESC") == WebstoreCode.PublicLands.desc,
            WebstoreCode.PublicLands.code,
        )
        .otherwise(col("WEBSTORE_DESC"))
        .alias("webstore_code"),
    ).alias("webstore_map")

    joined_df = (
        incremental_clean_df.join(newman_order_alias, "order_id", "left")
        .join(
            webstore_map_df,
            expr("webstore_map.webstore_code = newman_order.order_input_source"),
            "left",
        )
        .join(
            fit_package_alias,
            (
                col("fit_package.tracking_id")
                == col("incremental_clean.package_tracking_id")
            )
            & col("fit_package.carrier").isin("FDXGRND", "FDXEXPR", "DDUS"),
            "left",
        )
        .join(
            carrier_map_df,
            col("carrier_map.carrier_code")
            == col("incremental_clean.carrier_code_lookup"),
            "left",
        )
    )

    mode_dim_df = mode_dim_df.select(
        col("fulfillment_mode_code").alias("mode_code"),
        col("fulfillment_mode_key"),
    ).alias("mode_dim")

    joined_df = joined_df.withColumn(
        "carrier_mode_code",
        upper(
            concat_ws(
                "-",
                trim(coalesce(col("incremental_clean.carrier_code_lookup"), lit(""))),
                trim(
                    coalesce(
                        col("incremental_clean.executed_fulfillment_mode"),
                        col("incremental_clean.ordered_fulfillment_mode"),
                    )
                ),
            )
        ),
    ).withColumn(
        "mode_only_code",
        upper(
            trim(
                coalesce(
                    col("incremental_clean.executed_fulfillment_mode"),
                    col("incremental_clean.ordered_fulfillment_mode"),
                )
            )
        ),
    )

    joined_df = joined_df.join(
        mode_dim_df.select(
            col("mode_code").alias("fm1_code"),
            col("fulfillment_mode_key").alias("fm1_key"),
        ).alias("fm1"),
        upper(col("fm1.fm1_code")) == col("carrier_mode_code"),
        "left",
    )

    joined_df = joined_df.join(
        mode_dim_df.select(
            col("mode_code").alias("fm2_code"),
            col("fulfillment_mode_key").alias("fm2_key"),
        ).alias("fm2"),
        (upper(col("fm2.fm2_code")) == col("mode_only_code")) & col("fm1_key").isNull(),
        "left",
    )

    joined_df = joined_df.withColumn(
        "fulfillment_mode_key", coalesce(col("fm1_key"), col("fm2_key"), lit(-1))
    ).withColumn("raw_mode_code", coalesce(col("fm1_code"), col("fm2_code")))

    joined_df = joined_df.drop(
        "carrier_mode_code",
        "mode_only_code",
        "fm1_key",
        "fm1_code",
        "fm2_key",
        "fm2_code",
    )

    joined_df = joined_df.withColumn(
        "fulfillment_mode_key",
        when(
            col("raw_mode_code") == lit("SAMEDAY"), lit(FulfillmentModeCode.DDSD.key)
        ).otherwise(coalesce(col("fulfillment_mode_key"), lit(-1))),
    )

    joined_df = joined_df.drop("raw_mode_code", "mode_code")

    tracking_window = Window.partitionBy("tracking_clean")

    base_df = (
        joined_df.withColumn(
            "order_delivery_key",
            concat(
                lit(""),
                trim(col("incremental_clean.order_id")),
                trim(col("incremental_clean.package_tracking_id")),
            ),
        )
        .withColumn("webstore_key", col("webstore_map.webstore_key").cast("bigint"))
        .withColumn(
            "sci_lpn_id",
            coalesce(
                translate(col("incremental_clean.package_id").cast("string"), ".", "0"),
                concat(
                    col("incremental_clean.order_id").cast("string"),
                    lpad(
                        col("incremental_clean.fr_sequence_number").cast("string"),
                        4,
                        "0",
                    ),
                    lpad(lit(1).cast("string"), 4, "0"),
                ),
            ),
        )
        .withColumn("tracking_number", col("incremental_clean.tracking_clean"))
        .withColumn(
            "delivery_status_cd",
            when(
                col("incremental_clean.package_ship_timestamp").isNotNull(), lit("F")
            ).otherwise(lit("A")),
        )
        .withColumn(
            "fulfillment_date_key",
            date_format(
                max_(col("incremental_clean.package_ship_timestamp")).over(
                    tracking_window
                ),
                "yyyyMMdd",
            ).cast("bigint"),
        )
        .withColumn(
            "fulfillment_location_cd",
            col("incremental_clean.source_facility_number").cast("bigint"),
        )
        .withColumn(
            "actual_shipped_dttm",
            max_(col("incremental_clean.package_ship_timestamp")).over(tracking_window),
        )
        .withColumn("package_type_descr", col("fit_package.package_type"))
        .withColumn("ship_via", col("incremental_clean.carrier_code_lookup"))
        .withColumn("carrier_key", col("carrier_map.carrier_key_mapped"))
        .withColumn(
            "order_fulfill_number",
            concat(
                col("incremental_clean.order_id").cast("string"),
                lpad(
                    col("incremental_clean.fr_sequence_number").cast("string"), 4, "0"
                ),
            ).cast("bigint"),
        )
        .withColumn("web_ord_num", col("incremental_clean.order_id").cast("bigint"))
        .withColumn(
            "lpn_create_dttm",
            min_(
                when(
                    col("fit_package.event_type") == "LABEL_CREATED",
                    col("fit_package.event_timestamp"),
                )
            ).over(tracking_window),
        )
        .withColumn(
            "date_last_modified_utc",
            max_(col("incremental_clean.nmn_updated_timestamp")).over(tracking_window),
        )
        .withColumn("modified_by", lit("OSO-Silver"))
        .withColumn("silver_created_on_utc", lit(batch_date_utc))
        .withColumn("silver_created_by", lit(dbx_user_id))
        .withColumn("silver_updated_on_utc", lit(batch_date_utc))
        .withColumn("silver_updated_by", lit(dbx_user_id))
    )

    base_with_shipped = base_df.join(
        shipped_counts_df,
        on=[
            base_df["order_id"] == shipped_counts_df["order_id"],
            base_df["package_id"] == shipped_counts_df["package_id"],
        ],
        how="left",
    ).withColumn(
        "shipped_qty",
        coalesce(shipped_counts_df["shipped_qty"], lit(0)),
    )

    package_type_map_df = dom_package_type_dim_df.select(
        col("description"), col("package_type_id")
    ).alias("package_type_map")

    final_df = base_with_shipped.join(
        package_type_map_df,
        trim(col("package_type_descr")) == trim(col("package_type_map.description")),
        "left",
    ).withColumn("package_type_id", col("package_type_map.package_type_id"))

    w = Window.partitionBy("sci_lpn_id").orderBy(col("date_last_modified_utc").desc())

    deduped_window_df = (
        final_df.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    final_dedup_df = deduped_window_df.dropDuplicates(["order_delivery_key"])
    return final_dedup_df.select(*BUSINESS_COLS)


class SilverOrderDelivery(MergeOperation):
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
                order_id,
                package_tracking_id,
                executed_fulfillment_mode,
                ordered_fulfillment_mode,
                package_ship_timestamp,
                source_facility_number,
                package_id,
                line_seq_number,
               line_number,
                fr_sequence_number,
                package_carrier,
                sku,
                nmn_updated_timestamp
            FROM {self.config.source_table_qualified()} oso
            WHERE  package_carrier <> 'Athlete'
             AND managed_by = 'newman'
             AND fr_sequence_number IS NOT NULL
              AND lower(routing_partner)  in ('fit','vft','wm')
              AND (
  package_id IS NULL
  OR package_id  NOT RLIKE '[^0-9,.]'
)
          --    AND package_id not rlike '[^0-9,.]'
              AND silver_layer_update_timestamp >= '{last_watermark_utc}'
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        spark = self.spark
        return perform_order_delivery_transforms(
            incremental_df=incremental_changeset,
            newman_order_df=spark.table(f"{self.oso_catalog}.oso.newman_order"),
            webstore_dim_df=spark.table(f"{self.ecomp_catalog}.ecom_dim.webstore"),
            fit_package_events_df=spark.table(
                f"{self.fit_catalog}.fit.fit_package_events_silver"
            ),
            carrier_dim_df=spark.table(f"{self.ecomp_catalog}.ecom_dim.carrier"),
            dom_package_type_dim_df=spark.table(
                f"{self.ecomp_catalog}.domp.dom_package_type"
            ),
            mode_dim_df=spark.table(f"{self.ecomp_catalog}.ecom_dim.fulfillment_mode"),
            dbx_user_id=self.config.dbx_user_id,
            batch_date_utc=self.batch_date_utc,
            ecomp_catalog=self.ecomp_catalog,
        )

    def merge_updates(self, updates: DataFrame) -> None:
        target_table = DeltaTable.forName(
            self.spark, self.config.destination_table_qualified()
        )
        (
            target_table.alias("target")
            .merge(
                updates.alias("source"),
                "target.order_delivery_key = source.order_delivery_key",
            )
            .whenMatchedUpdate(set=BUSINESS_SET_DICT)
            .whenNotMatchedInsert(values={c: f"source.{c}" for c in BUSINESS_COLS})
            .execute()
        )
