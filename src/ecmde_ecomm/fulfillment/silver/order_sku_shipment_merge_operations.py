from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.functions import (
    col,
    lit,
    when,
    upper,
    coalesce,
    concat,
    lpad,
    date_format,
    regexp_replace,
    min as sparkMin,
    max as sparkMax,
    count as sparkCount,
    row_number,
    translate,
    convert_timezone,
)
from pyspark.sql.types import StringType, IntegerType, LongType
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation, MergeConfig
from ecmde_ecomm.fulfillment.model.channel_code import ChannelCode
from ecmde_ecomm.fulfillment.model.chain_code import ChainCode
from ecmde_ecomm.fulfillment.model.fulfillmentstatus_code import FulfillmentStatus
from ecmde_ecomm.common.model.webstore_desc import WebstoreCode
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.util import DateUtil

DEFAULT_ZONE = "America/New_York"
CANCELLED_ITEM_STATUS = 999
WEB_STORE_KEY_UNK = -999


def perform_order_sku_shipment_transforms(
    incremental_df: DataFrame,
    dbx_user_id: str,
    batch_date_utc: datetime,
    oso_catalog: str,
    ecomp_catalog: str,
) -> DataFrame:

    spark = incremental_df.sparkSession
    newman_order = spark.table(f"{oso_catalog}.oso.newman_order").select(
        col("order_id"), col("order_input_source").alias("order_input_src")
    )
    webstore_dim = spark.table(f"{ecomp_catalog}.ecom_dim.webstore").select(
        col("webstore_desc"),
        col("webstore_key"),
    )
    df = incremental_df.join(newman_order, "order_id", "left").withColumn(
        "webstore_desc",
        F.when(
            F.col("order_input_src") == WebstoreCode.G3.code,
            F.lit(WebstoreCode.G3.desc),
        )
        .when(
            F.col("order_input_src") == WebstoreCode.GolfGalaxy.code,
            F.lit(WebstoreCode.GolfGalaxy.desc),
        )
        .when(
            F.col("order_input_src") == WebstoreCode.DicksSportingGoods.code,
            F.lit(WebstoreCode.DicksSportingGoods.desc),
        )
        .when(
            F.col("order_input_src") == WebstoreCode.Moosejaw.code,
            F.lit(WebstoreCode.Moosejaw.desc),
        )
        .when(
            F.col("order_input_src") == WebstoreCode.PublicLands.code,
            F.lit(WebstoreCode.PublicLands.desc),
        )
        .otherwise(F.col("order_input_src")),
    )

    webstore_dim = spark.table(f"{ecomp_catalog}.ecom_dim.webstore").select(
        F.col("webstore_desc"), F.col("webstore_key")
    )
    df = df.join(webstore_dim, "webstore_desc", "left")

    seq4 = lpad(coalesce(col("fr_sequence_number").cast("string"), lit("0")), 4, "0")

    df = df.withColumn("order_fulfill_key", concat(col("order_id"), seq4)).withColumn(
        "order_fulfill_number", concat(col("order_id"), seq4)
    )

    df = (
        df.withColumn("chain_key", col("order_input_src").cast(IntegerType()))
        .withColumn("web_ord_num", col("order_id").cast(LongType()))
        .withColumn(
            "webstore_key",
            coalesce(col("webstore_key"), lit(WEB_STORE_KEY_UNK)).cast(IntegerType()),
        )
        .withColumn("dks_sku", col("sku").cast(IntegerType()))
    )

    df = df.withColumn(
        "sci_lpn_id",
        coalesce(
            translate(col("package_id").cast(StringType()), ".", "0"),
            concat(
                col("order_id").cast(StringType()),
                lpad(col("fr_sequence_number").cast(StringType()), 4, "0"),
                lpad(lit(1).cast(StringType()), 4, "0"),
            ),
        ),
    )

    df = df.withColumn(
        "order_sku_shipment_key",
        concat(col("order_id").cast(StringType()), col("sku").cast(StringType())).cast(
            LongType()
        ),
    )

    df = df.withColumn(
        "package_ship_timestamp_et",
        convert_timezone(
            lit("UTC"),
            lit("America/New_York"),
            col("package_ship_timestamp"),
        ),
    ).withColumn(
        "fulfillment_date_key",
        date_format(col("package_ship_timestamp_et"), "yyyyMMdd").cast(IntegerType()),
    )

    df = df.withColumn(
        "fulfillment_location_cd", col("source_facility_number").cast(IntegerType())
    )

    shipped_units_df = df.groupBy("order_id", "sku", "package_id").agg(
        sparkCount("line_seq_number").alias("shipped_units")
    )
    df = df.join(
        shipped_units_df,
        ["order_id", "sku", "package_id"],
        "left",
    )

    df = df.withColumn("tracking_number", upper(F.trim(col("package_tracking_id"))))

    window_spec = Window.partitionBy("web_ord_num", "dks_sku", "sci_lpn_id")

    df = df.withColumn(
        "eom_shipped_dttm", sparkMax(col("package_ship_timestamp")).over(window_spec)
    )

    df = df.withColumn("date_last_modified", col("nmn_updated_timestamp"))
    df = df.withColumn("modified_by", lit("OSOSilver"))

    df = (
        df.withColumn("silver_created_on_utc", lit(batch_date_utc))
        .withColumn("silver_created_by", lit(dbx_user_id))
        .withColumn("silver_updated_on_utc", lit(batch_date_utc))
        .withColumn("silver_updated_by", lit(dbx_user_id))
    )

    dedupe_w = Window.partitionBy("web_ord_num", "dks_sku", "sci_lpn_id").orderBy(
        col("date_last_modified").desc()
    )
    df = (
        df.withColumn("rn", row_number().over(dedupe_w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    return df.select(
        "order_sku_shipment_key",
        "fulfillment_date_key",
        "fulfillment_location_cd",
        "shipped_units",
        "webstore_key",
        "tracking_number",
        "dks_sku",
        "sci_lpn_id",
        "order_fulfill_number",
        "eom_shipped_dttm",
        "web_ord_num",
        "date_last_modified",
        "modified_by",
        "silver_created_on_utc",
        "silver_created_by",
        "silver_updated_on_utc",
        "silver_updated_by",
    )


class SilverOrderSkuShipment(MergeOperation):
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
          ni.order_id,
          ni.sku,
          ni.item_status,
          ni.fr_sequence_number,
          ni.package_id,
          ni.package_tracking_id,
          ni.package_ship_timestamp,
          ni.source_facility_type,
          ni.pickup_facility_type,
          ni.ordered_fulfillment_mode,
          ni.executed_fulfillment_mode,
          ni.source_facility_number,
          ni.line_seq_number,
          ni.nmn_updated_timestamp,
          ni.silver_layer_update_timestamp,
          no.order_input_source
        FROM {self.config.source_table_qualified()} ni
        JOIN {self.oso_catalog}.oso.newman_order no
          ON ni.order_id = no.order_id
        WHERE ni.silver_layer_update_timestamp>= '{last_watermark_utc}'
          AND lower(ni.managed_by) = 'newman'
          AND lower(ni.routing_partner) in ('fit','vft','wm')
          AND ni.package_carrier <> 'Athlete'
           AND (
  package_id IS NULL
  OR package_id  NOT RLIKE '[^0-9,.]'
)
          AND ni.item_status <> {CANCELLED_ITEM_STATUS}   
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return perform_order_sku_shipment_transforms(
            incremental_changeset,
            self.config.dbx_user_id,
            self.batch_date_utc,
            oso_catalog=self.oso_catalog,
            ecomp_catalog=self.ecomp_catalog,
        )

    def merge_updates(self, updates: DataFrame) -> None:
        key_expr = (
            "target.web_ord_num = source.web_ord_num "
            "AND target.dks_sku = source.dks_sku "
            "AND target.sci_lpn_id = source.sci_lpn_id"
        )
        dt = DeltaTable.forName(self.spark, self.config.destination_table_qualified())
        (
            dt.alias("target")
            .merge(updates.alias("source"), key_expr)
            .whenMatchedUpdate(
                set={
                    "order_sku_shipment_key": "source.order_sku_shipment_key",
                    "fulfillment_date_key": "source.fulfillment_date_key",
                    "fulfillment_location_cd": "source.fulfillment_location_cd",
                    "shipped_units": "source.shipped_units",
                    "webstore_key": "source.webstore_key",
                    "tracking_number": "source.tracking_number",
                    "dks_sku": "source.dks_sku",
                    "sci_lpn_id": "source.sci_lpn_id",
                    "order_fulfill_number": "source.order_fulfill_number",
                    "eom_shipped_dttm": "source.eom_shipped_dttm",
                    "web_ord_num": "source.web_ord_num",
                    "date_last_modified": "source.date_last_modified",
                    "modified_by": "source.modified_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "order_sku_shipment_key": "source.order_sku_shipment_key",
                    "fulfillment_date_key": "source.fulfillment_date_key",
                    "fulfillment_location_cd": "source.fulfillment_location_cd",
                    "shipped_units": "source.shipped_units",
                    "webstore_key": "source.webstore_key",
                    "tracking_number": "source.tracking_number",
                    "dks_sku": "source.dks_sku",
                    "sci_lpn_id": "source.sci_lpn_id",
                    "order_fulfill_number": "source.order_fulfill_number",
                    "eom_shipped_dttm": "source.eom_shipped_dttm",
                    "web_ord_num": "source.web_ord_num",
                    "date_last_modified": "source.date_last_modified",
                    "modified_by": "source.modified_by",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                    "silver_created_by": "source.silver_created_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                }
            )
            .execute()
        )
