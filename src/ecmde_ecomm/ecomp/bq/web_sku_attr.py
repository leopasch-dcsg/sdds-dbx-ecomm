from datetime import datetime
from pyspark.sql.functions import (
    col,
    date_format,
    convert_timezone,
    lit,
    current_date,
    current_timestamp,
    to_date,
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
from ecmde_ecomm.common import Logger, ExpectationNotMetError, IllegalArgumentError
from ecmde_ecomm.common.dbx.etl import Watermark
from ecmde_ecomm.common.reporting import (
    RPT_DEFAULT_TIMEZONE,
    RPT_UTC_TIMEZONE,
    RPT_BQ_TIMESTAMP_FORMAT,
)


class WebSkuAttrEgressOperation(BigQueryEgressOperation):
    def __init__(
        self,
        spark: SparkSession,
        bigquery_credentials: BigQueryCredentials,
        source: DatabricksSource,
        destination: BigQueryDestination,
        watermark: Watermark,
    ):
        if watermark is None:
            raise IllegalArgumentError("watermark cannot be None for delta operation.")

        super().__init__(
            spark,
            bigquery_credentials,
            source,
            destination,
            watermark,
            LoadMode.DELTA,
        )
        self.__logger = Logger.logger(__class__.__name__)

    def get_source_dataframe(
        self, last_batch_date_utc: datetime | None = None
    ) -> DataFrame:
        if last_batch_date_utc is None:
            raise ExpectationNotMetError(
                "The order header egress is a delta operation and the last batch date is required."
            )

        self.__logger.info(
            f"""
            Getting source dataframe for {self.source.fully_qualified_table()} 
            and casting column types to be more explicit.
            
            Fetch Delta records:
                Last Batch Date: {last_batch_date_utc}
            """
        )

        df = self.spark.sql(
            f"""
            select *
              from {self.source.fully_qualified_table()}
             where date_added >= '{last_batch_date_utc}'
               or date_last_modified >= '{last_batch_date_utc}'
            """
        )

        return df.select(
            col("web_sku_key").cast(DecimalType(38, 0)).alias("web_sku_key"),
            col("dks_sku_key").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("chain_key").cast(DecimalType(38, 0)).alias("chain_key"),
            col("dks_sku").cast(DecimalType(38, 0)).alias("dks_sku"),
            col("country_of_origin").cast(StringType()).alias("country_of_origin"),
            col("web_sku_size_code_key")
            .cast(DecimalType(38, 0))
            .alias("web_sku_size_code_key"),
            col("size_code").cast(StringType()).alias("size_code"),
            col("size_desc").cast(StringType()).alias("size_desc"),
            col("web_sku_color_code_key")
            .cast(DecimalType(38, 0))
            .alias("web_sku_color_code_key"),
            col("color_code").cast(StringType()).alias("color_code"),
            col("color_desc").cast(StringType()).alias("color_desc"),
            col("dsp_color_code").cast(StringType()).alias("dsp_color_code"),
            col("web_sku_brand_key")
            .cast(DecimalType(38, 0))
            .alias("web_sku_brand_key"),
            col("brand_code").cast(StringType()).alias("brand_code"),
            col("brand_name").cast(StringType()).alias("brand_name"),
            col("available_for_sale").cast(StringType()).alias("available_for_sale"),
            col("available_flag").cast(StringType()).alias("available_flag"),
            col("backorder_flag").cast(StringType()).alias("backorder_flag"),
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
            col("bopis_flg").cast(StringType()).alias("bopis_flg"),
            col("avail_flg").cast(StringType()).alias("avail_flg"),
            col("atp_inv_flg").cast(StringType()).alias("atp_inv_flg"),
            col("clearance_flg").cast(StringType()).alias("clearance_flg"),
            col("color_family").cast(StringType()).alias("color_family"),
            col("presale_end_date").cast(StringType()).alias("presale_end_date"),
            col("primary_upc").cast(StringType()).alias("primary_upc"),
            col("sports_team").cast(StringType()).alias("sports_team"),
            col("variant_enabled").cast(StringType()).alias("variant_enabled"),
            col("wsc_ready").cast(StringType()).alias("wsc_ready"),
            col("promo_exclusion_group")
            .cast(StringType())
            .alias("promo_exclusion_group"),
            col("wcs_sku_has_attributes_ind")
            .cast(DecimalType(38, 0))
            .alias("wcs_sku_has_attributes_ind"),
            col("bopl_flg").cast(StringType()).alias("bopl_flg"),
        )
