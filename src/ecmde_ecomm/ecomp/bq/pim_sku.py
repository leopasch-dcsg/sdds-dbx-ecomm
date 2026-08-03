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


class PimSkuEgressOperation(BigQueryEgressOperation):

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
            col("PIM_SKU_KEY").cast(DecimalType(38, 0)).alias("pim_sku_key"),
            col("DKS_SKU_KEY").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("STYLE_KEY").cast(DecimalType(38, 0)).alias("style_key"),
            col("DKS_SKU").cast(DecimalType(38, 0)).alias("dks_sku"),
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
            col("SWATCH_FILE").cast(StringType()).alias("swatch_file"),
            col("ALT_IMAGE_CNT").cast(DecimalType(38, 0)).alias("alt_image_cnt"),
            col("SW_IMAGE_CNT").cast(DecimalType(38, 0)).alias("sw_image_cnt"),
            col("PRODUCT_SKU_DISPLAY_FLG")
            .cast(StringType())
            .alias("product_sku_display_flg"),
            col("SKU_NONDISPLAY_REASON")
            .cast(StringType())
            .alias("sku_nondisplay_reason"),
        )
