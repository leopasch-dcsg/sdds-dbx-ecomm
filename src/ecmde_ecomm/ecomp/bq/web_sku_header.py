from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    to_timestamp,
    convert_timezone,
    lit,
)
from datetime import datetime
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from pyspark.sql.types import DecimalType, StringType
from ecmde_ecomm.common import Logger
from ecmde_ecomm.common.bq.egress import (
    BigQueryEgressOperation,
    DatabricksSource,
    BigQueryCredentials,
    BigQueryDestination,
    LoadMode,
)


class WebSkuHeaderEgressOperation(BigQueryEgressOperation):

    def __init__(
        self,
        spark: SparkSession,
        bigquery_credentials: BigQueryCredentials,
        source: DatabricksSource,
        destination: BigQueryDestination,
        watermark: Watermark | None = None,
        load_mode: LoadMode = LoadMode.TRUNCATE_LOAD,
    ):
        super().__init__(
            spark=spark,
            credentials=bigquery_credentials,
            source=source,
            destination=destination,
            watermark=watermark,
            load_mode=load_mode,
        )

        self._logger = Logger.logger(__class__.__name__)

    def get_source_dataframe(
        self, last_batch_date_utc: datetime | None = None
    ) -> DataFrame:
        self._logger.info(
            f"Getting source dataframe for {self.source.table} and casting column types to be more explicit."
        )
        df = self.spark.sql(
            f"""
                select * from {self.source.fully_qualified_table()}
                """
        )

        timestamp_format = "yyyy-MM-dd'T'HH:mm:ss"
        timezone = "America/New_York"

        return df.select(
            col("WEB_SKU_KEY").cast(DecimalType(38, 0)).alias("web_sku_key"),
            col("DKS_SKU_KEY").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("CHAIN_KEY").cast(DecimalType(38, 0)).alias("chain_key"),
            col("DKS_SKU").cast(DecimalType(38, 0)).alias("dks_sku"),
            col("WEB_SKU_CODE").cast(DecimalType(38, 0)).alias("web_sku_code"),
            col("SKU_NUMBER").cast("string").alias("sku_number"),
            col("WEB_UPC").cast("string").alias("web_upc"),
            col("WEB_PRICE").cast(DecimalType(38, 2)).alias("web_price"),
            col("CLEARANCE_TYPE_KEY")
            .cast(DecimalType(38, 0))
            .alias("clearance_type_key"),
            col("LIST_PRICE").cast(DecimalType(38, 2)).alias("list_price"),
            col("MAP_PRICE").cast(DecimalType(38, 2)).alias("map_price"),
            col("ECOMM_RETAIL").cast(DecimalType(38, 2)).alias("ecomm_retail"),
            col("BM_RETAIL").cast(DecimalType(38, 2)).alias("bm_retail"),
            col("OCE_ONLINE_RETAIL_MIN")
            .cast(DecimalType(38, 2))
            .alias("oce_online_retail_min"),
            col("OCE_ONLINE_RETAIL_MAX")
            .cast(DecimalType(38, 2))
            .alias("oce_online_retail_max"),
            col("IMAGE_CODE").cast("string").alias("image_code"),
            col("THN_IMAGE_CODE").cast("string").alias("thn_image_code"),
            col("STYLE_KEY").cast(DecimalType(38, 0)).alias("style_key"),
            col("DATA_SOURCE_KEY").cast(DecimalType(38, 0)).alias("data_source_key"),
            col("IS_CURRENT").cast("string").alias("is_current"),
            col("DATE_FROM_KEY").cast(DecimalType(38, 0)).alias("date_from_key"),
            col("DATE_TO_KEY").cast(DecimalType(38, 0)).alias("date_to_key"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_ADDED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_added"),
            col("ADDED_BY").cast("string").alias("added_by"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_LAST_MODIFIED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_last_modified"),
            col("MODIFIED_BY").cast("string").alias("modified_by"),
            col("RECORD_STATUS").cast("string").alias("record_status"),
            col("REFERENCE_ID").cast(DecimalType(38, 0)).alias("reference_id"),
            col("PIM_STYLE_CODE").cast("string").alias("pim_style_code"),
            col("FIRST_WEBSTORE_ATP_DATE_KEY")
            .cast("long")
            .alias("first_webstore_atp_date_key"),
            col("LAST_WEBSTORE_ATP_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("last_webstore_atp_date_key"),
            col("ALT_IMAGE_CNT").cast(DecimalType(38, 0)).alias("alt_image_cnt"),
            col("MAIN_IMAGE_CNT").cast(DecimalType(38, 0)).alias("main_image_cnt"),
            col("PRESALE_END_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("presale_end_date_key"),
            col("DISCONTINUED_FLG").cast("string").alias("discontinued_flg"),
            col("BUYABLE_ITEM").cast("string").alias("buyable_item"),
            col("PUBLISHED_ITEM").cast("string").alias("published_item"),
            col("CATENTRY_ID").cast("long").alias("catentry_id"),
            col("FIRST_ACTIVE_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("first_active_date_key"),
            col("LAST_ACTIVE_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("last_active_date_key"),
            col("PRICE_LAST_CHANGE_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("price_last_change_date_key"),
            col("WEB_PERM_PRICE").cast(DecimalType(38, 2)).alias("web_perm_price"),
            col("WEB_PRICE_QUALIFIER")
            .cast(DecimalType(38, 0))
            .alias("web_price_qualifier"),
            date_format(
                convert_timezone(
                    lit("UTC"), lit(timezone), col("WEB_PRICE_START_DTTM")
                ),
                timestamp_format,
            )
            .cast(StringType())
            .alias("web_price_start_dttm"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("WEB_PRICE_END_DTTM")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("web_price_end_dttm"),
            col("DKS_SKU_COST").cast(DecimalType(38, 2)).alias("dks_sku_cost"),
            col("DKS_SKU_CURR_COST")
            .cast(DecimalType(38, 2))
            .alias("dks_sku_curr_cost"),
            col("CLR_COLOR_CODE").cast("string").alias("clr_color_code"),
        )
