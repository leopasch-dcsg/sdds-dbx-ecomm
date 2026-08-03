from datetime import datetime
from pyspark.sql.functions import (
    col,
    date_format,
    convert_timezone,
    lit,
    to_date,
    current_timestamp,
    current_date,
)
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

from ecmde_ecomm.common import ExpectationNotMetError


class SnpInventoryEgressOperation(BigQueryEgressOperation):
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
            to_date(col("DATE_KEY").cast(StringType()), "yyyyMMdd").alias("date_key"),
            col("CHAIN_KEY").cast("long").alias("chain_key"),
            col("DKS_SKU_KEY").cast("long").alias("dks_sku_key"),
            col("WEB_ELIGIBILITY_IND").cast("long").alias("web_eligibility_ind"),
            col("WEB_OH_QTY").cast(DecimalType(38, 0)).alias("web_oh_qty"),
            col("VDC_ELIGIBILITY_IND").cast("long").alias("vdc_eligibility_ind"),
            col("VDC_OH_QTY").cast(DecimalType(38, 0)).alias("vdc_oh_qty"),
            col("SFS_ELIGIBILITY_IND").cast("long").alias("sfs_eligibility_ind"),
            col("SFS_OH_QTY").cast(DecimalType(38, 0)).alias("sfs_oh_qty"),
            col("SFS_SS_QTY").cast(DecimalType(38, 0)).alias("sfs_ss_qty"),
            col("BOPIS_ELIGIBILITY_IND").cast("long").alias("bopis_eligibility_ind"),
            col("BOPIS_OH_QTY").cast(DecimalType(38, 0)).alias("bopis_oh_qty"),
            col("BOPIS_SS_QTY").cast(DecimalType(38, 0)).alias("bopis_ss_qty"),
            col("ISA_ELIGIBILITY_IND").cast("long").alias("isa_eligibility_ind"),
            col("ISA_OH_QTY").cast(DecimalType(38, 0)).alias("isa_oh_qty"),
            col("ISA_SS_QTY").cast(DecimalType(38, 0)).alias("isa_ss_qty"),
            col("TOTAL_OCE_OH_QTY").cast(DecimalType(38, 0)).alias("total_oce_oh_qty"),
            col("WEB_PRICE").cast(DecimalType(38, 2)).alias("web_price"),
            col("CLEARANCE_TYPE_KEY").cast("long").alias("clearance_type_key"),
            col("WEB_PRICE_DATA_IND").cast("long").alias("web_price_data_ind"),
            col("BASE_PRICE").cast(DecimalType(38, 2)).alias("base_price"),
            col("DKS_SKU").cast("long").alias("dks_sku"),
            col("WEB_SKU").cast("long").alias("web_sku"),
            col("STYLE_KEY").cast("long").alias("style_key"),
            col("REFERENCE_ID").cast("long").alias("reference_id"),
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
            col("DC_OH_QTY").cast(DecimalType(38, 0)).alias("dc_oh_qty"),
            col("TOT_DC_OH_QTY").cast(DecimalType(38, 0)).alias("tot_dc_oh_qty"),
            col("ECOM_DC_ATP_QTY").cast(DecimalType(38, 0)).alias("ecom_dc_atp_qty"),
            col("PRESALE_ATP_QTY").cast(DecimalType(38, 0)).alias("presale_atp_qty"),
            col("VDC_HUB_QTY").cast(DecimalType(38, 0)).alias("vdc_hub_qty"),
            col("PRESALE_PROJ_SFS_ATP_QTY")
            .cast(DecimalType(38, 0))
            .alias("presale_proj_sfs_atp_qty"),
            col("BACKSTOCK_ATP_QTY")
            .cast(DecimalType(38, 0))
            .alias("backstock_atp_qty"),
            col("BACKSTOCK_OH_QTY").cast(DecimalType(38, 0)).alias("backstock_oh_qty"),
            col("RDC_ELIGIBILITY_IND")
            .cast(DecimalType(38, 0))
            .alias("rdc_eligibility_ind"),
            col("DDW_CLR_COLOR_CODE").cast(StringType()).alias("ddw_clr_color_code"),
            col("WEB_PERM_PRICE").cast(DecimalType(38, 2)).alias("web_perm_price"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
            col("WEBSTORE_BOPIS_ATP_QTY")
            .cast(DecimalType(38, 0))
            .alias("webstore_bopis_atp_qty"),
            col("WEBSTORE_ISA_OH_QTY")
            .cast(DecimalType(38, 0))
            .alias("webstore_isa_oh_qty"),
            col("BOPL_ELIGIBILITY_IND")
            .cast(DecimalType(38, 0))
            .alias("bopl_eligibility_ind"),
            col("BOPL_ATP_QTY").cast(DecimalType(38, 0)).alias("bopl_atp_qty"),
        )
