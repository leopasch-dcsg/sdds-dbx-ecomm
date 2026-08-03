from datetime import datetime
from pyspark.sql.functions import (
    col,
    date_format,
    convert_timezone,
    lit,
    current_date,
    current_timestamp,
    to_date,
    when,
    to_timestamp_ntz,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DecimalType, LongType
from ecmde_ecomm.common.bq import (
    BigQueryEgressOperation,
    BigQueryCredentials,
    DatabricksSource,
    BigQueryDestination,
    LoadMode,
)
from ecmde_ecomm.common import Logger, ExpectationNotMetError
from ecmde_ecomm.common.dbx.etl import Watermark
from ecmde_ecomm.common.reporting import (
    RPT_DEFAULT_TIMEZONE,
    RPT_UTC_TIMEZONE,
    RPT_BQ_TIMESTAMP_FORMAT,
)


class PromotionHeaderEgressOperation(BigQueryEgressOperation):
    def __init__(
        self,
        spark: SparkSession,
        bigquery_credentials: BigQueryCredentials,
        source: DatabricksSource,
        destination: BigQueryDestination,
        watermark: Watermark | None = None,
    ):
        super().__init__(
            spark,
            bigquery_credentials,
            source,
            destination,
            watermark,
            LoadMode.TRUNCATE_LOAD,
        )
        self.__logger = Logger.logger(__class__.__name__)

    def get_source_dataframe(
        self, last_batch_date_utc: datetime | None = None
    ) -> DataFrame:
        self.__logger.info(
            f"""
            Getting source dataframe for {self.source.fully_qualified_table()} 
            and casting column types to be more explicit.
            """
        )

        df = self.spark.sql(
            f"""
            SELECT
              h.PROMOTION_KEY   AS PROMOTION_KEY,
              h.PROMOTION_ID                    AS PROMOTION_ID,
              h.CHAIN_KEY                       AS CHAIN_KEY,
              h.PROMOTION_DESC                  AS PROMOTION_DESC,
              h.PROMOTION_SHORT_DESC            AS PROMOTION_SHORT_DESC,
              h.DISCOUNT_ORDER_LEVEL            AS DISCOUNT_ORDER_LEVEL,
              h.DISCOUNT_DOLLAR_TYPE            AS DISCOUNT_DOLLAR_TYPE,
              h.EFFECT_TYPE_CODE                AS EFFECT_TYPE_CODE,
              h.PROMOTION_CLASS                 AS PROMOTION_CLASS,
              h.START_DATE_KEY                  AS START_DATE_KEY,
              h.END_DATE_KEY                    AS END_DATE_KEY,
              h.DATE_ADDED                      AS DATE_ADDED,
              h.ADDED_BY                        AS ADDED_BY,
              h.DATE_LAST_MODIFIED              AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY                     AS MODIFIED_BY,
              h.RECORD_STATUS                   AS RECORD_STATUS,
              h.REFERENCE_ID                    AS REFERENCE_ID,
              h.DATA_SOURCE_KEY                 AS DATA_SOURCE_KEY,
              h.PROMO_HIER_SUB_CODE             AS PROMO_HIER_SUB_CODE,
              h.PROMO_HIER_SUB_KEY              AS PROMO_HIER_SUB_KEY,
              h.PROMO_HIER_MAIN_KEY             AS PROMO_HIER_MAIN_KEY,
              h.PPS_PROMO_ID                    AS PPS_PROMO_ID,
              h.PPS_EVENT_ID                    AS PPS_EVENT_ID,
              h.CS_IND                          AS CS_IND,
              h.EPIC_PROMO_TYPE                 AS EPIC_PROMO_TYPE,
              h.EPIC_COMP_FLAG                  AS EPIC_COMP_FLAG,
              h.EPIC_MEDIA                      AS EPIC_MEDIA,
              h.EPIC_COMP_LY_PROMOS             AS EPIC_COMP_LY_PROMOS,
              h.PR_PROMO_HEADER_ID              AS PR_PROMO_HEADER_ID,
              h.PROMOTION_DESC_UPDATED          AS PROMOTION_DESC_UPDATED,
              h.RETURN_IND                      AS RETURN_IND,
              h.EARN_IND                        AS EARN_IND,
              h.WCS_CODE                        AS WCS_CODE,
              h.EPIC_CODE_TEXT                  AS EPIC_CODE_TEXT,
              h.WCS_PURCH_COND_CHANNEL_TYPE     AS WCS_PURCH_COND_CHANNEL_TYPE,
              h.DISCOUNT_GROUP                  AS DISCOUNT_GROUP,
              h.FLASH_SALE_IND                  AS FLASH_SALE_IND,
              h.DISCOUNT_GROUP_DETAIL           AS DISCOUNT_GROUP_DETAIL,
              h.MKT_CHANNEL_ADMIN_NAME          AS MKT_CHANNEL_ADMIN_NAME,
              h.WCS_PROMOTION_TYPE              AS WCS_PROMOTION_TYPE,
              h.WCS_START_DATE                  AS WCS_START_DATE,
              h.WCS_END_DATE                    AS WCS_END_DATE,
              h.WCS_INCLUSIONS_TYPE             AS WCS_INCLUSIONS_TYPE,
              h.ADS_PROMO_CODE                  AS ADS_PROMO_CODE,
              h.EVENT_TYPE_DESC                 AS EVENT_TYPE_DESC
            FROM {self.source.fully_qualified_table()} AS h
            """
        )

        return df.select(
            col("promotion_key").cast(DecimalType(38, 0)).alias("promotion_key"),
            col("promotion_id").cast(DecimalType(38, 0)).alias("promotion_id"),
            col("chain_key").cast(DecimalType(38, 0)).alias("chain_key"),
            col("promotion_desc").cast(StringType()).alias("promotion_desc"),
            col("promotion_short_desc")
            .cast(StringType())
            .alias("promotion_short_desc"),
            col("discount_order_level")
            .cast(StringType())
            .alias("discount_order_level"),
            col("discount_dollar_type")
            .cast(StringType())
            .alias("discount_dollar_type"),
            col("effect_type_code").cast(StringType()).alias("effect_type_code"),
            col("promotion_class").cast(StringType()).alias("promotion_class"),
            col("start_date_key").cast(DecimalType(38, 0)).alias("start_date_key"),
            col("end_date_key").cast(DecimalType(38, 0)).alias("end_date_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("date_added")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_added"),
            col("added_by").cast(StringType()).alias("added_by"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("date_last_modified"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_last_modified"),
            col("modified_by").cast(StringType()).alias("modified_by"),
            col("record_status").cast(StringType()).alias("record_status"),
            col("reference_id").cast(DecimalType(38, 0)).alias("reference_id"),
            col("data_source_key").cast(DecimalType(38, 0)).alias("data_source_key"),
            col("promo_hier_sub_code").cast(StringType()).alias("promo_hier_sub_code"),
            col("promo_hier_sub_key")
            .cast(DecimalType(38, 0))
            .alias("promo_hier_sub_key"),
            col("promo_hier_main_key")
            .cast(DecimalType(38, 0))
            .alias("promo_hier_main_key"),
            col("pps_promo_id").cast(DecimalType(38, 0)).alias("pps_promo_id"),
            col("pps_event_id").cast(DecimalType(38, 0)).alias("pps_event_id"),
            col("cs_ind").cast("long").alias("cs_ind"),
            col("epic_promo_type").cast(StringType()).alias("epic_promo_type"),
            col("epic_comp_flag").cast(StringType()).alias("epic_comp_flag"),
            col("epic_media").cast(StringType()).alias("epic_media"),
            col("epic_comp_ly_promos").cast(StringType()).alias("epic_comp_ly_promos"),
            col("pr_promo_header_id")
            .cast(DecimalType(38, 0))
            .alias("pr_promo_header_id"),
            col("promotion_desc_updated")
            .cast(StringType())
            .alias("promotion_desc_updated"),
            col("return_ind").cast("long").alias("return_ind"),
            col("earn_ind").cast("long").alias("earn_ind"),
            col("wcs_code").cast(StringType()).alias("wcs_code"),
            col("epic_code_text").cast(StringType()).alias("epic_code_text"),
            col("wcs_purch_cond_channel_type")
            .cast(StringType())
            .alias("wcs_purch_cond_channel_type"),
            col("discount_group").cast(StringType()).alias("discount_group"),
            col("flash_sale_ind").cast("long").alias("flash_sale_ind"),
            col("discount_group_detail")
            .cast(StringType())
            .alias("discount_group_detail"),
            col("mkt_channel_admin_name")
            .cast(StringType())
            .alias("mkt_channel_admin_name"),
            col("wcs_promotion_type").cast(StringType()).alias("wcs_promotion_type"),
            date_format(
                when(
                    col("WCS_START_DATE") > lit("9999-12-31"),
                    to_timestamp_ntz(lit("9999-12-31T23:59:59")),
                ).otherwise(col("WCS_START_DATE")),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("wcs_start_date"),
            date_format(
                when(
                    col("WCS_END_DATE") > lit("9999-12-31"),
                    to_timestamp_ntz(lit("9999-12-31T23:59:59")),
                ).otherwise(col("WCS_END_DATE")),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("wcs_end_date"),
            col("wcs_inclusions_type").cast(StringType()).alias("wcs_inclusions_type"),
            col("ads_promo_code").cast(DecimalType(38, 0)).alias("ads_promo_code"),
            col("event_type_desc").cast(StringType()).alias("event_type_desc"),
        )
