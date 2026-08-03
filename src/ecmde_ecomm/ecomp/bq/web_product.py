from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    to_timestamp,
    from_utc_timestamp,
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
from ecmde_ecomm.common.reporting import (
    RPT_DEFAULT_TIMEZONE,
    RPT_UTC_TIMEZONE,
    RPT_BQ_TIMESTAMP_FORMAT,
)


class WebProductEgressOperation(BigQueryEgressOperation):

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
            SELECT
              h.PRODUCT_KEY   AS PRODUCT_KEY, 
              h.PRODUCT_CODE                  AS PRODUCT_CODE,
              h.CHAIN_KEY                     AS CHAIN_KEY,
              h.PRODUCT_DESC                  AS PRODUCT_DESC,
              h.SHORT_DESCRIPTION             AS SHORT_DESCRIPTION,
              h.LONG_DESCRIPTION              AS LONG_DESCRIPTION,
              h.KEYWORDS                      AS KEYWORDS,
              h.PRODUCT_URL                   AS PRODUCT_URL,
              h.IS_A_COLLECTION               AS IS_A_COLLECTION,
              h.IS_LINKED_TO_STORE            AS IS_LINKED_TO_STORE,
              h.AVG_HEIGHT                    AS AVG_HEIGHT,
              h.AVG_DEPTH                     AS AVG_DEPTH,
              h.AVG_WIDTH                     AS AVG_WIDTH,
              h.AVG_WEIGHT                    AS AVG_WEIGHT,
              h.PRODUCT_STATUS                AS PRODUCT_STATUS,
              h.VARIANT_ENABLED               AS VARIANT_ENABLED,
              h.GLOBAL_CATEGORY_KEY           AS GLOBAL_CATEGORY_KEY,
              h.PRIMARY_PATH                  AS PRIMARY_PATH,
              h.SP_STATUS                     AS SP_STATUS,
              h.IMAGE_STATUS                  AS IMAGE_STATUS,
              h.CREATED_DATE                  AS CREATED_DATE,
              h.LAST_MODIFIED_DATE            AS LAST_MODIFIED_DATE,
              h.NEW_ARRIVAL_DATE              AS NEW_ARRIVAL_DATE,
              h.IS_CURRENT                    AS IS_CURRENT,
              h.DATE_FROM_KEY                 AS DATE_FROM_KEY,
              h.DATE_TO_KEY                   AS DATE_TO_KEY,
              h.DATA_SOURCE_KEY               AS DATA_SOURCE_KEY,
              h.DATE_ADDED                    AS DATE_ADDED,
              h.ADDED_BY                      AS ADDED_BY,
              h.DATE_LAST_MODIFIED            AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY                   AS MODIFIED_BY,
              h.RECORD_STATUS                 AS RECORD_STATUS,
              h.REFERENCE_ID                  AS REFERENCE_ID,
              h.FIRST_WEBSTORE_ATP_DATE_KEY   AS FIRST_WEBSTORE_ATP_DATE_KEY,
              h.CURR_ALT_IMAGE_CNT            AS CURR_ALT_IMAGE_CNT,
              h.LAST_WEBSTORE_ATP_DATE_KEY    AS LAST_WEBSTORE_ATP_DATE_KEY,
              h.MAX_CURRENT_PRICE             AS MAX_CURRENT_PRICE,
              h.PRESALE_END_DATE_KEY          AS PRESALE_END_DATE_KEY,
              h.RATING_TOTAL                  AS RATING_TOTAL,
              h.REVIEW_COUNT                  AS REVIEW_COUNT,
              h.PRODUCT_STATUS_GROUP          AS PRODUCT_STATUS_GROUP,
              h.CATENTRY_ID                   AS CATENTRY_ID,
              h.BUYABLE                       AS BUYABLE,
              h.PUBLISHED                     AS PUBLISHED,
              h.FIRST_ACTIVE_DATE_KEY         AS FIRST_ACTIVE_DATE_KEY,
              h.LAST_ACTIVE_DATE_KEY          AS LAST_ACTIVE_DATE_KEY,
              h.CURR_SW_IMAGE_CNT             AS CURR_SW_IMAGE_CNT,
              h.ACT_SW_IMAGE_CNT              AS ACT_SW_IMAGE_CNT,
              h.ACT_ALT_IMAGE_CNT             AS ACT_ALT_IMAGE_CNT,
              h.COLOR_CNT                     AS COLOR_CNT,
              h.MAX_LIST_PRICE                AS MAX_LIST_PRICE,
              h.PRODUCT_SEARCH_FLG            AS PRODUCT_SEARCH_FLG,
              h.LAST_DISP_WEB_ATP_DATE_KEY    AS LAST_DISP_WEB_ATP_DATE_KEY,
              h.FEATURES_OVERLAY_BADGE        AS FEATURES_OVERLAY_BADGE,
              h.HOT_MARKET_FLG                AS HOT_MARKET_FLG
            FROM {self.source.fully_qualified_table()} AS h 
            """
        )

        return df.select(
            col("PRODUCT_KEY").cast(DecimalType(38, 0)).alias("product_key"),
            col("PRODUCT_CODE").cast(StringType()).alias("product_code"),
            col("CHAIN_KEY").cast(DecimalType(38, 0)).alias("chain_key"),
            col("PRODUCT_DESC").cast(StringType()).alias("product_desc"),
            col("SHORT_DESCRIPTION").cast(StringType()).alias("short_description"),
            col("LONG_DESCRIPTION").cast(StringType()).alias("long_description"),
            col("KEYWORDS").cast(StringType()).alias("keywords"),
            col("PRODUCT_URL").cast(StringType()).alias("product_url"),
            col("IS_A_COLLECTION").cast(StringType()).alias("is_a_collection"),
            col("IS_LINKED_TO_STORE").cast(StringType()).alias("is_linked_to_store"),
            col("AVG_HEIGHT").cast("double").alias("avg_height"),
            col("AVG_DEPTH").cast("double").alias("avg_depth"),
            col("AVG_WIDTH").cast("double").alias("avg_width"),
            col("AVG_WEIGHT").cast("double").alias("avg_weight"),
            col("PRODUCT_STATUS").cast(StringType()).alias("product_status"),
            col("VARIANT_ENABLED").cast("long").alias("variant_enabled"),
            col("GLOBAL_CATEGORY_KEY")
            .cast(DecimalType(38, 0))
            .alias("global_category_key"),
            col("PRIMARY_PATH").cast(StringType()).alias("primary_path"),
            col("SP_STATUS").cast(StringType()).alias("sp_status"),
            col("IMAGE_STATUS").cast(StringType()).alias("image_status"),
            col("CREATED_DATE").cast(DecimalType(38, 0)).alias("created_date"),
            col("LAST_MODIFIED_DATE")
            .cast(DecimalType(38, 0))
            .alias("last_modified_date"),
            col("NEW_ARRIVAL_DATE").cast(DecimalType(38, 0)).alias("new_arrival_date"),
            col("IS_CURRENT").cast(StringType()).alias("is_current"),
            col("DATE_FROM_KEY").cast(DecimalType(38, 0)).alias("date_from_key"),
            col("DATE_TO_KEY").cast(DecimalType(38, 0)).alias("date_to_key"),
            col("DATA_SOURCE_KEY").cast(DecimalType(38, 0)).alias("data_source_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("DATE_ADDED")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_added"),
            col("ADDED_BY").cast(StringType()).alias("added_by"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("DATE_LAST_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_last_modified"),
            col("MODIFIED_BY").cast(StringType()).alias("modified_by"),
            col("RECORD_STATUS").cast(StringType()).alias("record_status"),
            col("REFERENCE_ID").cast(DecimalType(38, 0)).alias("reference_id"),
            col("FIRST_WEBSTORE_ATP_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("first_webstore_atp_date_key"),
            col("CURR_ALT_IMAGE_CNT")
            .cast(DecimalType(38, 0))
            .alias("curr_alt_image_cnt"),
            col("LAST_WEBSTORE_ATP_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("last_webstore_atp_date_key"),
            col("MAX_CURRENT_PRICE")
            .cast(DecimalType(38, 2))
            .alias("max_current_price"),
            col("PRESALE_END_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("presale_end_date_key"),
            col("RATING_TOTAL").cast(DecimalType(38, 6)).alias("rating_total"),
            col("REVIEW_COUNT").cast(DecimalType(38, 0)).alias("review_count"),
            col("PRODUCT_STATUS_GROUP")
            .cast(StringType())
            .alias("product_status_group"),
            col("CATENTRY_ID").cast("long").alias("catentry_id"),
            col("BUYABLE").cast(StringType()).alias("buyable"),
            col("PUBLISHED").cast(StringType()).alias("published"),
            col("FIRST_ACTIVE_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("first_active_date_key"),
            col("LAST_ACTIVE_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("last_active_date_key"),
            col("CURR_SW_IMAGE_CNT")
            .cast(DecimalType(38, 0))
            .alias("curr_sw_image_cnt"),
            col("ACT_SW_IMAGE_CNT").cast(DecimalType(38, 0)).alias("act_sw_image_cnt"),
            col("ACT_ALT_IMAGE_CNT")
            .cast(DecimalType(38, 0))
            .alias("act_alt_image_cnt"),
            col("COLOR_CNT").cast(DecimalType(38, 0)).alias("color_cnt"),
            col("MAX_LIST_PRICE").cast(DecimalType(38, 2)).alias("max_list_price"),
            col("PRODUCT_SEARCH_FLG").cast(StringType()).alias("product_search_flg"),
            col("LAST_DISP_WEB_ATP_DATE_KEY")
            .cast("long")
            .alias("last_disp_web_atp_date_key"),
            col("FEATURES_OVERLAY_BADGE")
            .cast(StringType())
            .alias("features_overlay_badge"),
            col("HOT_MARKET_FLG").cast(StringType()).alias("hot_market_flg"),
        )
