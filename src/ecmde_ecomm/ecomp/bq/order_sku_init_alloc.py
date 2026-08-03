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


class OrderSkuInitAllocEgressOperation(BigQueryEgressOperation):
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
              h.INIT_ALLOC_KEY   AS INIT_ALLOC_KEY,
              h.CHAIN_KEY                      AS CHAIN_KEY,
              h.WEB_ORD_NUM                    AS WEB_ORD_NUM,
              h.DKS_SKU                        AS DKS_SKU,
              h.ORDER_FULFILL_NUMBER           AS ORDER_FULFILL_NUMBER,
              h.ORDER_FULFILL_SEQ_NUMBER       AS ORDER_FULFILL_SEQ_NUMBER,
              h.WEB_SKU                        AS WEB_SKU,
              h.PID                            AS PID,
              h.UNITS                          AS UNITS,
              h.EXTENDED_AMT                   AS EXTENDED_AMT,
              h.FREIGHT_AMT                    AS FREIGHT_AMT,
              h.TAX_AMT                        AS TAX_AMT,
              h.CURRENT_COST                   AS CURRENT_COST,
              h.AVERAGE_COST                   AS AVERAGE_COST,
              h.WEB_PRICE                      AS WEB_PRICE,
              h.CLEARANCE_TYPE_KEY             AS CLEARANCE_TYPE_KEY,
              h.PROMISE_DATE                   AS PROMISE_DATE,
              h.CHANNEL_TYPE_KEY               AS CHANNEL_TYPE_KEY,
              h.FULFILLMENT_LOCATION_CD        AS FULFILLMENT_LOCATION_CD,
              h.STORE_KEY                      AS STORE_KEY,
              h.ORDER_HEADER_KEY               AS ORDER_HEADER_KEY,
              h.ORDER_SKU_KEY                  AS ORDER_SKU_KEY,
              h.ORDER_FULFILL_KEY              AS ORDER_FULFILL_KEY,
              h.ORDER_DATE_KEY                 AS ORDER_DATE_KEY,
              h.ORDER_TIME_KEY                 AS ORDER_TIME_KEY,
              h.STYLE_KEY                      AS STYLE_KEY,
              h.DKS_SKU_KEY                    AS DKS_SKU_KEY,
              h.WEB_SKU_KEY                    AS WEB_SKU_KEY,
              h.PRODUCT_KEY                    AS PRODUCT_KEY,
              h.DATE_ADDED                     AS DATE_ADDED,
              h.ADDED_BY                       AS ADDED_BY,
              h.DATE_LAST_MODIFIED             AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY                    AS MODIFIED_BY,
              h.RECORD_STATUS                  AS RECORD_STATUS,
              h.REFERENCE_ID                   AS REFERENCE_ID,
              h.FIRST_FULFILLMENT_MODE_KEY     AS FIRST_FULFILLMENT_MODE_KEY,
              h.FIRST_FULFILLMENT_MODE_DESC    AS FIRST_FULFILLMENT_MODE_DESC,
              h.ALLOC_DATE_KEY                 AS ALLOC_DATE_KEY
            FROM {self.source.fully_qualified_table()} AS h
            WHERE (
                 h.DATE_ADDED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 OR h.DATE_LAST_MODIFIED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 )
            """
        )

        return df.select(
            col("INIT_ALLOC_KEY").cast(DecimalType(38, 0)).alias("init_alloc_key"),
            col("CHAIN_KEY").cast(DecimalType(38, 0)).alias("chain_key"),
            col("WEB_ORD_NUM").cast(DecimalType(38, 0)).alias("web_ord_num"),
            col("DKS_SKU").cast(DecimalType(38, 0)).alias("dks_sku"),
            col("ORDER_FULFILL_NUMBER")
            .cast(DecimalType(38, 3))
            .alias("order_fulfill_number"),
            col("ORDER_FULFILL_SEQ_NUMBER")
            .cast(DecimalType(38, 0))
            .alias("order_fulfill_seq_number"),
            col("WEB_SKU").cast(DecimalType(38, 0)).alias("web_sku"),
            col("PID").cast(DecimalType(38, 0)).alias("pid"),
            col("UNITS").cast(DecimalType(38, 0)).alias("units"),
            col("EXTENDED_AMT").cast(DecimalType(38, 2)).alias("extended_amt"),
            col("FREIGHT_AMT").cast(DecimalType(38, 2)).alias("freight_amt"),
            col("TAX_AMT").cast(DecimalType(38, 2)).alias("tax_amt"),
            col("CURRENT_COST").cast(DecimalType(38, 6)).alias("current_cost"),
            col("AVERAGE_COST").cast(DecimalType(38, 6)).alias("average_cost"),
            col("WEB_PRICE").cast(DecimalType(38, 2)).alias("web_price"),
            col("CLEARANCE_TYPE_KEY")
            .cast(DecimalType(38, 0))
            .alias("clearance_type_key"),
            col("PROMISE_DATE").cast(DecimalType(38, 0)).alias("promise_date"),
            col("CHANNEL_TYPE_KEY").cast(DecimalType(38, 0)).alias("channel_type_key"),
            col("FULFILLMENT_LOCATION_CD")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_location_cd"),
            col("STORE_KEY").cast(DecimalType(38, 0)).alias("store_key"),
            col("ORDER_HEADER_KEY").cast(DecimalType(38, 0)).alias("order_header_key"),
            col("ORDER_SKU_KEY").cast(DecimalType(38, 0)).alias("order_sku_key"),
            col("ORDER_FULFILL_KEY")
            .cast(DecimalType(38, 0))
            .alias("order_fulfill_key"),
            col("ORDER_DATE_KEY").cast(DecimalType(38, 0)).alias("order_date_key"),
            col("ORDER_TIME_KEY").cast(DecimalType(38, 6)).alias("order_time_key"),
            col("STYLE_KEY").cast(DecimalType(38, 0)).alias("style_key"),
            col("DKS_SKU_KEY").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("WEB_SKU_KEY").cast(DecimalType(38, 0)).alias("web_sku_key"),
            col("PRODUCT_KEY").cast(DecimalType(38, 0)).alias("product_key"),
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
            col("FIRST_FULFILLMENT_MODE_KEY")
            .cast(DecimalType(38, 0))
            .alias("first_fulfillment_mode_key"),
            col("FIRST_FULFILLMENT_MODE_DESC")
            .cast(StringType())
            .alias("first_fulfillment_mode_desc"),
            col("ALLOC_DATE_KEY").cast(DecimalType(38, 0)).alias("alloc_date_key"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
