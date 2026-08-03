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


class PimProductEmastColorEgressOperation(BigQueryEgressOperation):

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
              h.PIM_PRODUCT_EMAST_COLOR_KEY   AS PIM_PRODUCT_EMAST_COLOR_KEY,
              h.ECODE                         AS ECODE,
              h.COLOR                         AS COLOR,
              h.PIM_PRODUCT_EMAST_KEY         AS PIM_PRODUCT_EMAST_KEY,
              h.COUNT_SKUS                    AS COUNT_SKUS,
              h.COUNT_STYLES                  AS COUNT_STYLES,
              h.IMAGE_CNT                     AS IMAGE_CNT,
              h.SWATCH_CNT                    AS SWATCH_CNT,
              h.MODEL_CNT                     AS MODEL_CNT,
              h.MAIN_CNT                      AS MAIN_CNT,
              h.MAIN_MODEL_CNT                AS MAIN_MODEL_CNT,
              h.ALT_CNT                       AS ALT_CNT,
              h.SWATCH_FILE                   AS SWATCH_FILE,
              h.MAIN_FILE                     AS MAIN_FILE,
              h.DATE_ADDED                    AS DATE_ADDED,
              h.ADDED_BY                      AS ADDED_BY,
              h.DATE_LAST_MODIFIED            AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY                   AS MODIFIED_BY,
              h.RECORD_STATUS                 AS RECORD_STATUS,
              h.SHOP_THE_LOOK_CNT             AS SHOP_THE_LOOK_CNT,
              h.VIDEO_CNT                     AS VIDEO_CNT,
              h.LIFESTYLE_IMG_CNT             AS LIFESTYLE_IMG_CNT,
              h.SCENE7_IMG_CNT                AS SCENE7_IMG_CNT,
              h.BODY_INCLUSIVITY_CNT          AS BODY_INCLUSIVITY_CNT,
              h.FIRST_IMAGE_DATE              AS FIRST_IMAGE_DATE
            FROM {self.source.fully_qualified_table()} AS h
            """
        )

        return df.select(
            col("PIM_PRODUCT_EMAST_COLOR_KEY")
            .cast(DecimalType(38, 0))
            .alias("pim_product_emast_color_key"),
            col("ECODE").cast(StringType()).alias("ecode"),
            col("COLOR").cast(StringType()).alias("color"),
            col("PIM_PRODUCT_EMAST_KEY")
            .cast(DecimalType(38, 0))
            .alias("pim_product_emast_key"),
            col("COUNT_SKUS").cast(DecimalType(38, 0)).alias("count_skus"),
            col("COUNT_STYLES").cast(DecimalType(38, 0)).alias("count_styles"),
            col("IMAGE_CNT").cast(DecimalType(38, 0)).alias("image_cnt"),
            col("SWATCH_CNT").cast(DecimalType(38, 0)).alias("swatch_cnt"),
            col("MODEL_CNT").cast(DecimalType(38, 0)).alias("model_cnt"),
            col("MAIN_CNT").cast(DecimalType(38, 0)).alias("main_cnt"),
            col("MAIN_MODEL_CNT").cast(DecimalType(38, 0)).alias("main_model_cnt"),
            col("ALT_CNT").cast(DecimalType(38, 0)).alias("alt_cnt"),
            col("SWATCH_FILE").cast(StringType()).alias("swatch_file"),
            col("MAIN_FILE").cast(StringType()).alias("main_file"),
            col("VIDEO_CNT").cast(DecimalType(38, 0)).alias("video_cnt"),
            col("LIFESTYLE_IMG_CNT")
            .cast(DecimalType(38, 0))
            .alias("lifestyle_img_cnt"),
            col("SCENE7_IMG_CNT").cast(DecimalType(38, 0)).alias("scene7_img_cnt"),
            col("BODY_INCLUSIVITY_CNT")
            .cast(DecimalType(38, 0))
            .alias("body_inclusivity_cnt"),
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
            col("SHOP_THE_LOOK_CNT")
            .cast(DecimalType(38, 0))
            .alias("shop_the_look_cnt"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("FIRST_IMAGE_DATE"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("first_image_date"),
        )
