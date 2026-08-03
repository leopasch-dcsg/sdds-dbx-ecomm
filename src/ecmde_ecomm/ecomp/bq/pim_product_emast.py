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
from pyspark.sql.types import DecimalType, StringType, LongType
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


class PimProductEmastEgressOperation(BigQueryEgressOperation):

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
            col("PIM_PRODUCT_EMAST_KEY")
            .cast(DecimalType(38, 0))
            .alias("pim_product_emast_key"),
            col("PIM_PRODUCT_EMAST_CODE")
            .cast(StringType())
            .alias("pim_product_emast_code"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("PIM_DATE_CREATED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("pim_date_created_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("PIM_DATE_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("pim_date_modified_dttm"),
            col("DATE_FROM_KEY").cast(DecimalType(38, 0)).alias("date_from_key"),
            col("DATE_TO_KEY").cast(DecimalType(38, 0)).alias("date_to_key"),
            col("DATA_SOURCE_KEY").cast(DecimalType(38, 0)).alias("data_source_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("DATE_ADDED")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_added_dttm"),
            col("ADDED_BY").cast(StringType()).alias("added_by"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("DATE_LAST_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_last_modified_dttm"),
            col("MODIFIED_BY").cast(StringType()).alias("modified_by"),
            col("RECORD_STATUS").cast(StringType()).alias("record_status"),
            col("REFERENCE_ID").cast(DecimalType(38, 0)).alias("reference_id"),
            col("PIM_HIERARCHY_KEY")
            .cast(DecimalType(38, 0))
            .alias("pim_hierarchy_key"),
            col("CURR_IMAGE_CNT").cast(DecimalType(38, 0)).alias("curr_image_cnt"),
            col("CURR_SW_IMAGE_CNT")
            .cast(DecimalType(38, 0))
            .alias("curr_sw_image_cnt"),
            col("COLOR_CNT").cast(DecimalType(38, 0)).alias("color_cnt"),
            col("FULL_IMAGE").cast(StringType()).alias("full_image"),
            col("EM_PRODUCT_DESC_FLG").cast(StringType()).alias("em_product_desc_flg"),
            col("WSC_READY_FLG").cast(StringType()).alias("wsc_ready_flg"),
            col("PRODUCT_BUYABLE_FLG").cast(StringType()).alias("product_buyable_flg"),
            col("EM_PRODUCT_DISPLAY_FLG")
            .cast(StringType())
            .alias("em_product_display_flg"),
            col("PRODUCT_DISPLAY_DATE_KEY")
            .cast(LongType())
            .alias("product_display_date_key"),
            col("COUNTRY_OF_ORIGIN").cast(StringType()).alias("country_of_origin"),
            col("VDC_ELIGIBLE_FLG").cast(StringType()).alias("vdc_eligible_flg"),
            col("EM_PRODUCT_TITLE").cast(StringType()).alias("em_product_title"),
            col("EM_GENDER_BY_AGE").cast(StringType()).alias("em_gender_by_age"),
            col("EM_PRODUCT_BRAND").cast(StringType()).alias("em_product_brand"),
            col("DSG_STYLE").cast(StringType()).alias("dsg_style"),
            col("EM_PRODUCT_STATUS").cast(StringType()).alias("em_product_status"),
            col("DSG_FLG").cast(StringType()).alias("dsg_flg"),
            col("GG_FLG").cast(StringType()).alias("gg_flg"),
            col("FS_FLG").cast(StringType()).alias("fs_flg"),
            col("PRODUCT_SORT_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("product_sort_date_key"),
            col("WSC_READY_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("wsc_ready_date_key"),
            col("GG_READY_FLG").cast(StringType()).alias("gg_ready_flg"),
            col("DSG_READY_FLG").cast(StringType()).alias("dsg_ready_flg"),
            col("FS_READY_FLG").cast(StringType()).alias("fs_ready_flg"),
            col("EM_PRESALE_FLG").cast(StringType()).alias("em_presale_flg"),
            col("PROMO_EXCLUSION_ORDER_FLG")
            .cast(StringType())
            .alias("promo_exclusion_order_flg"),
            col("LICENSED_APPAREL_TYPE")
            .cast(StringType())
            .alias("licensed_apparel_type"),
            col("EM_ACTIVITY").cast(StringType()).alias("em_activity"),
            col("CURR_ALT_IMAGE_CNT")
            .cast(DecimalType(38, 0))
            .alias("curr_alt_image_cnt"),
            col("MAIN_IMAGE_CNT").cast(DecimalType(38, 0)).alias("main_image_cnt"),
            col("MODEL_IMAGE_CNT").cast(DecimalType(38, 0)).alias("model_image_cnt"),
            col("MODEL_MAIN_IMAGE_CNT")
            .cast(DecimalType(38, 0))
            .alias("model_main_image_cnt"),
            col("WCS_CATENTRY_ID").cast(DecimalType(38, 0)).alias("wcs_catentry_id"),
            col("CUSTOM_PRODUCT_FLG").cast(StringType()).alias("custom_product_flg"),
            col("STYLE_PRODUCT_HIERARCHY_KEY")
            .cast(DecimalType(38, 0))
            .alias("style_product_hierarchy_key"),
            col("PACK_SIZE").cast(StringType()).alias("pack_size"),
            col("STYLE_VENDOR_KEY").cast(DecimalType(38, 0)).alias("style_vendor_key"),
            col("STACKD_FLG").cast(StringType()).alias("stackd_flg"),
            col("EM_ECOM_GENDER").cast(StringType()).alias("em_ecom_gender"),
            col("EM_HAS_SPECS_FLG").cast(StringType()).alias("em_has_specs_flg"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("PRODUCT_TITLE_DATE"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("product_title_date"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("PRODUCT_DESC_DATE"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("product_desc_date"),
            col("EM_NOTES").cast(StringType()).alias("em_notes"),
            col("PL_READY_FLG").cast(StringType()).alias("pl_ready_flg"),
            col("PL_FLG").cast(StringType()).alias("pl_flg"),
            col("G3_FLG").cast(StringType()).alias("g3_flg"),
            col("EXCLUSIVE_TO").cast(StringType()).alias("exclusive_to"),
            col("MAIN_IMAGE_URL").cast(StringType()).alias("main_image_url"),
        )
