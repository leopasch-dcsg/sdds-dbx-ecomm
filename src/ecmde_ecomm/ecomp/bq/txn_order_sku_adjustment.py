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


class TxnOrderSkuAdjustmentEgressOperation(BigQueryEgressOperation):
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
            col("TXN_DATE_KEY").cast(DecimalType(38, 0)).alias("txn_date_key"),
            col("TXN_TIME_KEY").cast(DecimalType(38, 0)).alias("txn_time_key"),
            col("WEB_ORD_NUM").cast(DecimalType(38, 0)).alias("web_ord_num"),
            col("TXN_SEQ_NUMBER").cast(DecimalType(38, 0)).alias("txn_seq_number"),
            col("CHAIN_KEY").cast(DecimalType(38, 0)).alias("chain_key"),
            col("TRANS_TYPE_KEY").cast(DecimalType(38, 0)).alias("trans_type_key"),
            col("DKS_SKU").cast(DecimalType(38, 0)).alias("dks_sku"),
            col("UNITS").cast(DecimalType(38, 0)).alias("units"),
            col("EXTENDED_ADJUST_AMT")
            .cast(DecimalType(38, 2))
            .alias("extended_adjust_amt"),
            col("FREIGHT_ADJUST_AMT")
            .cast(DecimalType(38, 2))
            .alias("freight_adjust_amt"),
            col("TOTAL_ADJUST_AMT").cast(DecimalType(38, 2)).alias("total_adjust_amt"),
            col("TAX_ADJUST_AMT").cast(DecimalType(38, 2)).alias("tax_adjust_amt"),
            col("PROMOTION_KEY").cast(DecimalType(38, 0)).alias("promotion_key"),
            col("PROMOTION_ID").cast(DecimalType(38, 0)).alias("promotion_id"),
            col("PROMO_CODE").cast(StringType()).alias("promo_code"),
            col("ADJUSTMENT_NOTES").cast(StringType()).alias("adjustment_notes"),
            col("DKS_SKU_KEY").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("WEB_SKU_KEY").cast(DecimalType(38, 0)).alias("web_sku_key"),
            col("STYLE_KEY").cast(DecimalType(38, 0)).alias("style_key"),
            col("PRODUCT_KEY").cast(DecimalType(38, 0)).alias("product_key"),
            col("ORDER_HEADER_KEY").cast(DecimalType(38, 0)).alias("order_header_key"),
            col("ORDER_SKU_KEY").cast(DecimalType(38, 0)).alias("order_sku_key"),
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
            col("QUAL_EXTENDED_AMT")
            .cast(DecimalType(38, 6))
            .alias("qual_extended_amt"),
            col("QUAL_FREIGHT_AMT").cast(DecimalType(38, 6)).alias("qual_freight_amt"),
            col("AVERAGE_COST").cast(DecimalType(38, 6)).alias("average_cost"),
            col("SOURCE_REASON_CD").cast(StringType()).alias("source_reason_cd"),
            col("MARGIN_DECOMP_PRIORITY_NUM")
            .cast(DecimalType(38, 0))
            .alias("margin_decomp_priority_num"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
