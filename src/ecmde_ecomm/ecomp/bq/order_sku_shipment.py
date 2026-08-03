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


class OrderSkuShipmentEgressOperation(BigQueryEgressOperation):
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
              h.ORDER_SKU_SHIPMENT_KEY   AS ORDER_SKU_SHIPMENT_KEY,
              h.ORDER_SKU_KEY                AS ORDER_SKU_KEY,
              h.DKS_SKU_KEY                  AS DKS_SKU_KEY,
              h.ORDER_DELIVERY_KEY           AS ORDER_DELIVERY_KEY,
              h.ORDER_FULFILL_KEY            AS ORDER_FULFILL_KEY,
              h.ORDER_HEADER_KEY             AS ORDER_HEADER_KEY,
              h.FULFILLMENT_DATE_KEY         AS FULFILLMENT_DATE_KEY,
              h.FULFILLMENT_LOCATION_CD      AS FULFILLMENT_LOCATION_CD,
              h.STORE_KEY                    AS STORE_KEY,
              h.VENDOR_KEY                   AS VENDOR_KEY,
              h.SHIPPED_UNITS                AS SHIPPED_UNITS,
              h.CHANNEL_TYPE_KEY             AS CHANNEL_TYPE_KEY,
              h.STYLE_KEY                    AS STYLE_KEY,
              h.WEBSTORE_KEY                 AS WEBSTORE_KEY,
              h.DATE_LAST_MODIFIED           AS DATE_LAST_MODIFIED,
              h.ADDED_BY                     AS ADDED_BY,
              h.RECORD_STATUS                AS RECORD_STATUS,
              h.TRACKING_NUMBER              AS TRACKING_NUMBER,
              h.DKS_SKU                      AS DKS_SKU,
              h.SCI_LPN_ID                   AS SCI_LPN_ID,
              h.ORDER_FULFILL_NUMBER         AS ORDER_FULFILL_NUMBER,
              h.EOM_SHIPPED_DTTM             AS EOM_SHIPPED_DTTM,
              h.EOM_TO_WCS_MESSAGE_DT        AS EOM_TO_WCS_MESSAGE_DT,
              h.POSTED_DATE_KEY              AS POSTED_DATE_KEY,
              h.WEB_ORD_NUM                  AS WEB_ORD_NUM,
              h.WEB_SKU_KEY                  AS WEB_SKU_KEY,
              h.PRODUCT_KEY                  AS PRODUCT_KEY,
              h.DATE_ADDED                   AS DATE_ADDED,
              h.MODIFIED_BY                  AS MODIFIED_BY,
              h.SHIP_EXTENDED_AMT            AS SHIP_EXTENDED_AMT
            FROM {self.source.fully_qualified_table()} AS h 
            WHERE (
                 h.DATE_ADDED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 OR h.DATE_LAST_MODIFIED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 )
            """
        )

        return df.select(
            col("ORDER_SKU_SHIPMENT_KEY")
            .cast(DecimalType(38, 0))
            .alias("order_sku_shipment_key"),
            col("ORDER_SKU_KEY").cast(DecimalType(38, 0)).alias("order_sku_key"),
            col("DKS_SKU_KEY").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("ORDER_DELIVERY_KEY")
            .cast(DecimalType(38, 0))
            .alias("order_delivery_key"),
            col("ORDER_FULFILL_KEY")
            .cast(DecimalType(38, 0))
            .alias("order_fulfill_key"),
            col("ORDER_HEADER_KEY").cast(DecimalType(38, 0)).alias("order_header_key"),
            col("FULFILLMENT_DATE_KEY")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_date_key"),
            col("FULFILLMENT_LOCATION_CD")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_location_cd"),
            col("STORE_KEY").cast(DecimalType(38, 0)).alias("store_key"),
            col("VENDOR_KEY").cast(DecimalType(38, 0)).alias("vendor_key"),
            col("SHIPPED_UNITS").cast(DecimalType(38, 0)).alias("shipped_units"),
            col("CHANNEL_TYPE_KEY").cast(DecimalType(38, 0)).alias("channel_type_key"),
            col("STYLE_KEY").cast(DecimalType(38, 0)).alias("style_key"),
            col("WEBSTORE_KEY").cast(DecimalType(38, 0)).alias("webstore_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("DATE_LAST_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_last_modified"),
            col("ADDED_BY").cast(StringType()).alias("added_by"),
            col("RECORD_STATUS").cast(StringType()).alias("record_status"),
            col("TRACKING_NUMBER").cast(StringType()).alias("tracking_number"),
            col("DKS_SKU").cast(DecimalType(38, 0)).alias("dks_sku"),
            col("SCI_LPN_ID").cast(DecimalType(38, 0)).alias("sci_lpn_id"),
            col("ORDER_FULFILL_NUMBER")
            .cast(DecimalType(38, 3))
            .alias("order_fulfill_number"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("EOM_SHIPPED_DTTM"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("eom_shipped_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("EOM_TO_WCS_MESSAGE_DT"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("eom_to_wcs_message_dt"),
            col("POSTED_DATE_KEY").cast("long").alias("posted_date_key"),
            col("WEB_ORD_NUM").cast(DecimalType(38, 0)).alias("web_ord_num"),
            col("WEB_SKU_KEY").cast(DecimalType(38, 0)).alias("web_sku_key"),
            col("PRODUCT_KEY").cast(DecimalType(38, 0)).alias("product_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("DATE_ADDED")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_added"),
            col("MODIFIED_BY").cast(StringType()).alias("modified_by"),
            col("SHIP_EXTENDED_AMT")
            .cast(DecimalType(38, 2))
            .alias("ship_extended_amt"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
