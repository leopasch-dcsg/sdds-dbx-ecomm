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
from pyspark.sql.types import DecimalType, StringType, IntegerType
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


class PimProductEgressOperation(BigQueryEgressOperation):

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
              h.PIM_PRODUCT_KEY   AS PIM_PRODUCT_KEY,
              h.PIM_PRODUCT_CODE          AS PIM_PRODUCT_CODE,
              h.PIM_ENTITY_ID             AS PIM_ENTITY_ID,
              h.WEBSTORE_KEY              AS WEBSTORE_KEY,
              h.PIM_DATE_CREATED          AS PIM_DATE_CREATED,
              h.PIM_DATE_MODIFIED         AS PIM_DATE_MODIFIED,
              h.DATE_FROM_KEY             AS DATE_FROM_KEY,
              h.DATE_TO_KEY               AS DATE_TO_KEY,
              h.DATA_SOURCE_KEY           AS DATA_SOURCE_KEY,
              h.DATE_ADDED                AS DATE_ADDED,
              h.ADDED_BY                  AS ADDED_BY,
              h.DATE_LAST_MODIFIED        AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY               AS MODIFIED_BY,
              h.RECORD_STATUS             AS RECORD_STATUS,
              h.REFERENCE_ID              AS REFERENCE_ID,
              h.PIM_HIERARCHY_KEY         AS PIM_HIERARCHY_KEY,
              h.STYLE_READY_FLG           AS STYLE_READY_FLG,
              h.STYLE_READY_DATE_KEY      AS STYLE_READY_DATE_KEY,
              h.WEB_READY_FLG             AS WEB_READY_FLG,
              h.WEB_READY_DATE_KEY        AS WEB_READY_DATE_KEY,
              h.PRODUCT_DISPLAY_FLG       AS PRODUCT_DISPLAY_FLG,
              h.PROMO_EXCLUSION_FLG       AS PROMO_EXCLUSION_FLG,
              h.SITE_MERCH_STATUS_DESC    AS SITE_MERCH_STATUS_DESC,
              h.CONTENT_STATUS_DESC       AS CONTENT_STATUS_DESC,
              h.PRODUCT_DESC_FLG          AS PRODUCT_DESC_FLG,
              h.PRODUCT_TITLE_STORE       AS PRODUCT_TITLE_STORE,
              h.FABRIC_CONTENT_STORE      AS FABRIC_CONTENT_STORE,
              h.NONDISPLAY_REASON         AS NONDISPLAY_REASON,
              h.WEBSTORE_NOTES            AS WEBSTORE_NOTES
            FROM {self.source.fully_qualified_table()} AS h 
            """
        )

        return df.select(
            col("PIM_PRODUCT_KEY").cast(DecimalType(38, 0)).alias("pim_product_key"),
            col("PIM_PRODUCT_CODE").cast(StringType()).alias("pim_product_code"),
            col("PIM_ENTITY_ID").cast(DecimalType(38, 0)).alias("pim_entity_id"),
            col("WEBSTORE_KEY").cast(DecimalType(38, 0)).alias("webstore_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("PIM_DATE_CREATED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("pim_date_created"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("PIM_DATE_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("pim_date_modified"),
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
            col("PIM_HIERARCHY_KEY")
            .cast(DecimalType(38, 0))
            .alias("pim_hierarchy_key"),
            col("STYLE_READY_FLG").cast(StringType()).alias("style_ready_flg"),
            col("STYLE_READY_DATE_KEY")
            .cast(IntegerType())
            .alias("style_ready_date_key"),
            col("WEB_READY_FLG").cast(StringType()).alias("web_ready_flg"),
            col("WEB_READY_DATE_KEY").cast(IntegerType()).alias("web_ready_date_key"),
            col("PRODUCT_DISPLAY_FLG").cast(StringType()).alias("product_display_flg"),
            col("PROMO_EXCLUSION_FLG").cast(StringType()).alias("promo_exclusion_flg"),
            col("SITE_MERCH_STATUS_DESC")
            .cast(StringType())
            .alias("site_merch_status_desc"),
            col("CONTENT_STATUS_DESC").cast(StringType()).alias("content_status_desc"),
            col("PRODUCT_DESC_FLG").cast(StringType()).alias("product_desc_flg"),
            col("PRODUCT_TITLE_STORE").cast(StringType()).alias("product_title_store"),
            col("FABRIC_CONTENT_STORE")
            .cast(StringType())
            .alias("fabric_content_store"),
            col("NONDISPLAY_REASON").cast(StringType()).alias("nondisplay_reason"),
            col("WEBSTORE_NOTES").cast(StringType()).alias("webstore_notes"),
        )
