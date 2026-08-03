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


class PromotionEventEgressOperation(BigQueryEgressOperation):

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
                     h.PROMOTION_EVENT_KEY   AS PROMOTION_EVENT_KEY,
                      h.PPS_PROMO_ID               AS PPS_PROMO_ID,
                      h.PPS_PROMO_DESC             AS PPS_PROMO_DESC,
                      h.PPS_PROMO_SCHEME           AS PPS_PROMO_SCHEME,
                      h.PPS_PROMO_SCHEME_DESC      AS PPS_PROMO_SCHEME_DESC,
                      h.EVENT_ID                   AS EVENT_ID,
                      h.EVENT_DESC                 AS EVENT_DESC,
                      h.EVENT_START_DATE           AS EVENT_START_DATE,
                      h.EVENT_END_DATE             AS EVENT_END_DATE,
                      h.EVENT_TYPE_ID              AS EVENT_TYPE_ID,
                      h.EVENT_TYPE_DESC            AS EVENT_TYPE_DESC,
                      h.RECORD_STATUS              AS RECORD_STATUS,
                      h.DATE_ADDED                 AS DATE_ADDED,
                      h.ADDED_BY                   AS ADDED_BY,
                      h.DATE_LAST_MODIFIED         AS DATE_LAST_MODIFIED,
                      h.MODIFIED_BY                AS MODIFIED_BY,
                      h.CONTENT_FEATURE_ID         AS CONTENT_FEATURE_ID,
                      h.PAGE_DESC                  AS PAGE_DESC,
                      h.VERSION_DESCRIPTION        AS VERSION_DESCRIPTION,
                      h.BLOCK_NUMBER               AS BLOCK_NUMBER
                    FROM {self.source.fully_qualified_table()} AS h
                    """
        )

        timestamp_format = "yyyy-MM-dd'T'HH:mm:ss"
        timezone = "America/New_York"

        return df.select(
            col("PROMOTION_EVENT_KEY")
            .cast(DecimalType(38, 0))
            .alias("promotion_event_key"),
            col("PPS_PROMO_ID").cast(DecimalType(38, 0)).alias("pps_promo_id"),
            col("PPS_PROMO_DESC").cast("string").alias("pps_promo_desc"),
            col("PPS_PROMO_SCHEME").cast(DecimalType(38, 0)).alias("pps_promo_scheme"),
            col("PPS_PROMO_SCHEME_DESC").cast("string").alias("pps_promo_scheme_desc"),
            col("EVENT_ID").cast(DecimalType(38, 0)).alias("event_id"),
            col("EVENT_DESC").cast("string").alias("event_desc"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("EVENT_START_DATE")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("event_start_date"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("EVENT_END_DATE")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("event_end_date"),
            col("EVENT_TYPE_ID").cast(DecimalType(38, 0)).alias("event_type_id"),
            col("EVENT_TYPE_DESC").cast("string").alias("event_type_desc"),
            col("RECORD_STATUS").cast("string").alias("record_status"),
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
            col("CONTENT_FEATURE_ID")
            .cast(DecimalType(38, 0))
            .alias("content_feature_id"),
            col("PAGE_DESC").cast("string").alias("page_desc"),
            col("VERSION_DESCRIPTION").cast("string").alias("version_description"),
            col("BLOCK_NUMBER").cast(DecimalType(38, 0)).alias("block_number"),
        )
