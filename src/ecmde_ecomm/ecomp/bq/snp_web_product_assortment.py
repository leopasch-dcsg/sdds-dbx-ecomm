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
from ecmde_ecomm.common import Logger, ExpectationNotMetError
from ecmde_ecomm.common.dbx.etl import Watermark
from ecmde_ecomm.common.reporting import (
    RPT_DEFAULT_TIMEZONE,
    RPT_UTC_TIMEZONE,
    RPT_BQ_TIMESTAMP_FORMAT,
)


class SnpWebProductAssortmentEgressOperation(BigQueryEgressOperation):
    def __init__(
        self,
        spark: SparkSession,
        bigquery_credentials: BigQueryCredentials,
        source: DatabricksSource,
        destination: BigQueryDestination,
        watermark: Watermark,
    ):
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
            to_date(col("date_key").cast(StringType()), "yyyyMMdd").alias("date_key"),
            col("chain_key").cast(DecimalType(38, 0)).alias("chain_key"),
            col("product_key").cast(DecimalType(38, 0)).alias("product_key"),
            col("web_sku_key").cast(DecimalType(38, 0)).alias("web_sku_key"),
            col("dks_sku_key").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("style_key").cast(DecimalType(38, 0)).alias("style_key"),
            col("product_status_group")
            .cast(StringType())
            .alias("product_status_group"),
            col("product_status").cast(StringType()).alias("product_status"),
            col("new_product_status_ind")
            .cast(DecimalType(38, 0))
            .alias("new_product_status_ind"),
            col("old_product_status").cast(StringType()).alias("old_product_status"),
            col("old_product_status_group")
            .cast(StringType())
            .alias("old_product_status_group"),
            col("linked_pid_flag").cast(StringType()).alias("linked_pid_flag"),
            col("linked_pid_priority")
            .cast(DecimalType(38, 0))
            .alias("linked_pid_priority"),
            col("new_sku_ind").cast(DecimalType(38, 0)).alias("new_sku_ind"),
            col("old_sku_cnt").cast(DecimalType(38, 0)).alias("old_sku_cnt"),
            col("reference_id").cast(DecimalType(38, 0)).alias("reference_id"),
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
            col("web_atp_qty").cast(DecimalType(38, 0)).alias("web_atp_qty"),
            col("vdc_atp_qty").cast(DecimalType(38, 0)).alias("vdc_atp_qty"),
            col("sfs_atp_qty").cast(DecimalType(38, 0)).alias("sfs_atp_qty"),
            col("bopis_atp_qty").cast(DecimalType(38, 0)).alias("bopis_atp_qty"),
            col("isa_atp_qty").cast(DecimalType(38, 0)).alias("isa_atp_qty"),
            col("web_price").cast(DecimalType(38, 2)).alias("web_price"),
            col("clearance_type_key")
            .cast(DecimalType(38, 0))
            .alias("clearance_type_key"),
            col("dc_atp_qty").cast(DecimalType(38, 0)).alias("dc_atp_qty"),
            col("web_eligibility_ind").cast("long").alias("web_eligibility_ind"),
            col("dks_dc_atp_qty").cast(DecimalType(38, 0)).alias("dks_dc_atp_qty"),
            col("backstock_atp_qty")
            .cast(DecimalType(38, 0))
            .alias("backstock_atp_qty"),
            col("presale_atp_qty").cast(DecimalType(38, 0)).alias("presale_atp_qty"),
            col("list_price").cast(DecimalType(38, 2)).alias("list_price"),
            col("ddw_clr_color_code").cast(StringType()).alias("ddw_clr_color_code"),
            col("web_perm_price").cast(DecimalType(38, 2)).alias("web_perm_price"),
            col("promo_excl_grp").cast(StringType()).alias("promo_excl_grp"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
            col("webstore_bopis_atp_qty")
            .cast(DecimalType(38, 0))
            .alias("webstore_bopis_atp_qty"),
            col("webstore_isa_oh_qty")
            .cast(DecimalType(38, 0))
            .alias("webstore_isa_oh_qty"),
            col("bopl_atp_qty").cast(DecimalType(38, 0)).alias("bopl_atp_qty"),
        )
