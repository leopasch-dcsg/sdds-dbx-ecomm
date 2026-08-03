from datetime import datetime
from pyspark.sql.functions import col, date_format, convert_timezone, lit
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DecimalType
from ecmde_ecomm.common.bq import (
    BigQueryEgressOperation,
    BigQueryCredentials,
    DatabricksSource,
    BigQueryDestination,
    LoadMode,
)
from ecmde_ecomm.common import Logger
from ecmde_ecomm.common.dbx.etl import Watermark
from ecmde_ecomm.common.reporting import (
    RPT_DEFAULT_TIMEZONE,
    RPT_UTC_TIMEZONE,
    RPT_BQ_TIMESTAMP_FORMAT,
)


class BridgePimSkuProductEgressOperation(BigQueryEgressOperation):
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
            f"Getting source dataframe for {self.source.fully_qualified_table()} and casting column types to be more explicit."
        )

        df = self.spark.sql(
            f"""
            select * from {self.source.fully_qualified_table()}
            """
        )

        return df.select(
            col("BRIDGE_PIM_SKU_PRODUCT_KEY")
            .cast(DecimalType(38, 0))
            .alias("bridge_pim_sku_product_key"),
            col("PIM_SKU_KEY").cast(DecimalType(38, 0)).alias("pim_sku_key"),
            col("PIM_PRODUCT_KEY").cast(DecimalType(38, 0)).alias("pim_product_key"),
            col("DKS_SKU_KEY").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("STYLE_KEY").cast(DecimalType(38, 0)).alias("style_key"),
            col("WEBSTORE_KEY").cast(DecimalType(38, 0)).alias("webstore_key"),
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
        )
