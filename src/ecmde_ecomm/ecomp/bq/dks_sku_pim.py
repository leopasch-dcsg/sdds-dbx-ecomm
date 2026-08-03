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
from pyspark.sql.types import DecimalType, StringType, IntegerType, LongType
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


class DksSkuPimEgressOperation(BigQueryEgressOperation):

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

        return df.select(
            col("DKS_SKU_KEY").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("DKS_SKU_CODE").cast(DecimalType(38, 0)).alias("dks_sku_code"),
            col("PIM_ENTITY_ID").cast(DecimalType(38, 0)).alias("pim_entity_id"),
            col("PIM_COLOR").cast(StringType()).alias("pim_color"),
            col("EM_VD_VPN").cast(StringType()).alias("em_vd_vpn"),
            col("LOC_RESTRICT_CD").cast(StringType()).alias("loc_restrict_cd"),
            col("LOC_RESTRICT_RISK").cast(StringType()).alias("loc_restrict_risk"),
            col("HAZMAT_RISK").cast(StringType()).alias("hazmat_risk"),
            col("SWATCH_FILE").cast(StringType()).alias("swatch_file"),
            col("TOT_IMAGE_CNT").cast(DecimalType(38, 0)).alias("tot_image_cnt"),
            col("SW_IMAGE_CNT").cast(DecimalType(38, 0)).alias("sw_image_cnt"),
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
            col("COLOR_FAMILY").cast(StringType()).alias("color_family"),
            col("SPORTS_TEAM").cast(StringType()).alias("sports_team"),
            col("RADIAL_SKU_NUM").cast(LongType()).alias("radial_sku_num"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("PRODUCT_DISPLAY_DTTM"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("product_display_dttm"),
            date_format(col("PRESALE_END_DATE"), RPT_BQ_TIMESTAMP_FORMAT).alias(
                "presale_end_date"
            ),
            col("ALT_IMAGE_CNT").cast(DecimalType(38, 0)).alias("alt_image_cnt"),
            col("MAIN_IMAGE_CNT").cast(DecimalType(38, 0)).alias("main_image_cnt"),
            col("MODEL_IMAGE_CNT").cast(DecimalType(38, 0)).alias("model_image_cnt"),
            col("MODEL_MAIN_IMAGE_CNT")
            .cast(DecimalType(38, 0))
            .alias("model_main_image_cnt"),
            col("PIM_PRODUCT_EMAST_COLOR_KEY")
            .cast(DecimalType(38, 0))
            .alias("pim_product_emast_color_key"),
            col("EM_SKU_PRESALE_FLG").cast(StringType()).alias("em_sku_presale_flg"),
            col("VDC_ELIG_FLG").cast(StringType()).alias("vdc_elig_flg"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("COMING_SOON_END_DTTM"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("coming_soon_end_dttm"),
            col("KIDS_SHOE_SIZE_GRP").cast(StringType()).alias("kids_shoe_size_grp"),
            col("YOUTH_AGE_RANGE").cast(StringType()).alias("youth_age_range"),
            col("EM_SKU_LEAD_TIME").cast(StringType()).alias("em_sku_lead_time"),
            col("EM_SKU_EXCLUSIVE_TO").cast(StringType()).alias("em_sku_exclusive_to"),
            col("EM_SKU_G3_ELIGIBLE_FLG")
            .cast(StringType())
            .alias("em_sku_g3_eligible_flg"),
            col("EM_SKU_G3_NOT_ELIGIBLE_FLG")
            .cast(StringType())
            .alias("em_sku_g3_not_eligible_flg"),
            col("SKU_MAIN_IMAGE_URL").cast(StringType()).alias("sku_main_image_url"),
        )
