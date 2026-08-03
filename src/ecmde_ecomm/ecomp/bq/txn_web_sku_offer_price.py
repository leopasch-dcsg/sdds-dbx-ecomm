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


class TxnWebSkuOfferPriceEgressOperation(BigQueryEgressOperation):
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
            SELECT
              h.TXN_WEB_SKU_OFFER_PRICE_KEY   AS TXN_WEB_SKU_OFFER_PRICE_KEY,
              h.WEB_SKU_KEY                   AS WEB_SKU_KEY,
              h.DKS_SKU_KEY                   AS DKS_SKU_KEY,
              h.WEBSTORE_KEY                  AS WEBSTORE_KEY,
              h.DKS_SKU                       AS DKS_SKU,
              h.QUALIFIER                     AS QUALIFIER,
              h.PRICE                         AS PRICE,
              h.MAXIMUMQUANTITY               AS MAXIMUMQUANTITY,
              h.MINIMUMQUANTITY               AS MINIMUMQUANTITY,
              h.PRECEDENCE                    AS PRECEDENCE,
              h.IS_CURRENT                    AS IS_CURRENT,
              h.DTTM_FROM                     AS DTTM_FROM,
              h.DTTM_TO                       AS DTTM_TO,
              h.DATE_ADDED                    AS DATE_ADDED,
              h.ADDED_BY                      AS ADDED_BY,
              h.DATE_LAST_MODIFIED            AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY                   AS MODIFIED_BY,
              h.RECORD_STATUS                 AS RECORD_STATUS
            FROM {self.source.fully_qualified_table()} AS h 
            WHERE (
                 h.DATE_ADDED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 OR h.DATE_LAST_MODIFIED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 )
            """
        )

        return df.select(
            col("TXN_WEB_SKU_OFFER_PRICE_KEY")
            .cast("long")
            .alias("txn_web_sku_offer_price_key"),
            col("WEB_SKU_KEY").cast("long").alias("web_sku_key"),
            col("DKS_SKU_KEY").cast("long").alias("dks_sku_key"),
            col("WEBSTORE_KEY").cast("long").alias("webstore_key"),
            col("DKS_SKU").cast("long").alias("dks_sku"),
            col("QUALIFIER").cast(DecimalType(38, 0)).alias("qualifier"),
            col("PRICE").cast(DecimalType(38, 2)).alias("price"),
            col("MAXIMUMQUANTITY").cast(DecimalType(38, 2)).alias("maximumquantity"),
            col("MINIMUMQUANTITY").cast(DecimalType(38, 2)).alias("minimumquantity"),
            col("PRECEDENCE").cast(DecimalType(38, 0)).alias("precedence"),
            col("IS_CURRENT").cast(StringType()).alias("is_current"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("DTTM_FROM")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("dttm_from_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("DTTM_TO")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("dttm_to_dttm"),
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
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
