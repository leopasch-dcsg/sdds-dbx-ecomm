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


class OrderSkuEgressOperation(BigQueryEgressOperation):

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
              h.ORDER_SKU_KEY   AS ORDER_SKU_KEY,  
              h.CHAIN_KEY                       AS CHAIN_KEY,
              h.ORDER_DATE_KEY                  AS ORDER_DATE_KEY,
              h.WEB_ORD_NUM                     AS WEB_ORD_NUM,
              h.DKS_SKU                         AS DKS_SKU,
              h.WEB_SKU                         AS WEB_SKU,
              h.PID                             AS PID,
              h.ORDER_LINE_CNT                  AS ORDER_LINE_CNT,
              h.ORDER_SKU_STATUS_KEY            AS ORDER_SKU_STATUS_KEY,
              h.ORDER_SKU_STATUS_DTTM           AS ORDER_SKU_STATUS_DTTM,
              h.ORDER_LINE_TYPE_KEY             AS ORDER_LINE_TYPE_KEY,
              h.ORIG_TOT_UNITS                  AS ORIG_TOT_UNITS,
              h.ORIG_TOT_AMT                    AS ORIG_TOT_AMT,
              h.ORIG_TOT_EXTENDED_AMT           AS ORIG_TOT_EXTENDED_AMT,
              h.ORIG_TOT_FREIGHT_AMT            AS ORIG_TOT_FREIGHT_AMT,
              h.ORIG_TOT_TAX_AMT                AS ORIG_TOT_TAX_AMT,
              h.CURRENT_COST                    AS CURRENT_COST,
              h.AVERAGE_COST                    AS AVERAGE_COST,
              h.WEB_PRICE                       AS WEB_PRICE,
              h.CLEARANCE_TYPE_KEY              AS CLEARANCE_TYPE_KEY,
              h.PROMISE_DATE                    AS PROMISE_DATE,
              h.FINAL_TOT_UNITS                 AS FINAL_TOT_UNITS,
              h.FINAL_TOT_AMT                   AS FINAL_TOT_AMT,
              h.FINAL_TOT_EXTENDED_AMT          AS FINAL_TOT_EXTENDED_AMT,
              h.FINAL_TOT_FREIGHT_AMT           AS FINAL_TOT_FREIGHT_AMT,
              h.FINAL_TOT_TAX_AMT               AS FINAL_TOT_TAX_AMT,
              h.ORDER_HEADER_KEY                AS ORDER_HEADER_KEY,
              h.ORDER_TIME_KEY                  AS ORDER_TIME_KEY,
              h.WEBSTORE_ORDER_DATE             AS WEBSTORE_ORDER_DATE,
              h.STYLE_KEY                       AS STYLE_KEY,
              h.DKS_SKU_KEY                     AS DKS_SKU_KEY,
              h.WEB_SKU_KEY                     AS WEB_SKU_KEY,
              h.PRODUCT_KEY                     AS PRODUCT_KEY,
              h.DATA_SOURCE_KEY                 AS DATA_SOURCE_KEY,
              h.DATE_ADDED                      AS DATE_ADDED,
              h.ADDED_BY                        AS ADDED_BY,
              h.DATE_LAST_MODIFIED              AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY                     AS MODIFIED_BY,
              h.RECORD_STATUS                   AS RECORD_STATUS,
              h.REFERENCE_ID                    AS REFERENCE_ID,
              h.CHANNEL_TYPE_KEY                AS CHANNEL_TYPE_KEY,
              h.ORDER_FULFILL_NUMBER            AS ORDER_FULFILL_NUMBER,
              h.PO_NUMBER                       AS PO_NUMBER,
              h.SHIP_TO_STATE                   AS SHIP_TO_STATE,
              h.SHIP_TO_ZIP                     AS SHIP_TO_ZIP,
              h.CUST_FULFILLMENT_MODE_KEY       AS CUST_FULFILLMENT_MODE_KEY,
              h.ORIG_TOT_EXT_DISC_AMT           AS ORIG_TOT_EXT_DISC_AMT,
              h.ORIG_TOT_FREIGHT_DISC_AMT       AS ORIG_TOT_FREIGHT_DISC_AMT,
              h.PRESALE_FLG                     AS PRESALE_FLG,
              h.HOT_MARKET_FLG                  AS HOT_MARKET_FLG,
              h.PICK_DECLINE_CNT                AS PICK_DECLINE_CNT,
              h.GTGT_IND                        AS GTGT_IND,
              h.CARRIER_FREIGHT_COST            AS CARRIER_FREIGHT_COST,
              h.NUM_BOXES_FULFILLED             AS NUM_BOXES_FULFILLED,
              h.NUM_BOXES_EXPENSED              AS NUM_BOXES_EXPENSED,
              h.NUM_BOXES_DELIVERED             AS NUM_BOXES_DELIVERED,
              h.MARGIN_DECOMP_CLASSIFICATION    AS MARGIN_DECOMP_CLASSIFICATION,
              h.FINAL_AMT_UPD_KEY               AS FINAL_AMT_UPD_KEY,
              h.ACTUAL_FF_CHANNEL_KEY           AS ACTUAL_FF_CHANNEL_KEY,
              h.ACTUAL_FF_LOCATION_CNT          AS ACTUAL_FF_LOCATION_CNT,
              h.CLR_COLOR_CODE                  AS CLR_COLOR_CODE,
              h.DAY_PERM_PRICE                  AS DAY_PERM_PRICE,
              h.PO_BOX_CD                       AS PO_BOX_CD,
              h.BOPIS_SAVE_THE_SALE_FLG         AS BOPIS_SAVE_THE_SALE_FLG,
              h.BOPIS_STORE                     AS BOPIS_STORE,
              h.GTGT_DATE                       AS GTGT_DATE
            FROM {self.source.fully_qualified_table()} AS h 
            WHERE (
                 h.DATE_ADDED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 OR h.DATE_LAST_MODIFIED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 )
            """
        )

        return df.select(
            col("ORDER_SKU_KEY").cast("long").alias("order_sku_key"),
            col("CHAIN_KEY").cast("long").alias("chain_key"),
            to_date(col("ORDER_DATE_KEY").cast("string"), "yyyyMMdd").alias(
                "order_date"
            ),
            col("WEB_ORD_NUM").cast("long").alias("web_ord_num"),
            # lit(None).cast("string").alias("placementorigin"),
            col("DKS_SKU").cast(DecimalType(38, 0)).alias("dks_sku"),
            col("WEB_SKU").cast(DecimalType(38, 0)).alias("web_sku"),
            col("PID").cast(DecimalType(38, 0)).alias("pid"),
            col("ORDER_LINE_CNT").cast(DecimalType(38, 0)).alias("order_line_cnt"),
            col("ORDER_SKU_STATUS_KEY").cast("long").alias("order_sku_status_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("ORDER_SKU_STATUS_DTTM"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            )
            .cast(StringType())
            .alias("order_sku_status_dttm"),
            col("ORDER_LINE_TYPE_KEY").cast("long").alias("order_line_type_key"),
            col("ORIG_TOT_UNITS").cast(DecimalType(38, 0)).alias("orig_tot_units"),
            col("ORIG_TOT_AMT").cast(DecimalType(38, 9)).alias("orig_tot_amt"),
            col("ORIG_TOT_EXTENDED_AMT")
            .cast(DecimalType(38, 2))
            .alias("orig_tot_extended_amt"),
            col("ORIG_TOT_FREIGHT_AMT")
            .cast(DecimalType(38, 2))
            .alias("orig_tot_freight_amt"),
            col("ORIG_TOT_TAX_AMT").cast(DecimalType(38, 2)).alias("orig_tot_tax_amt"),
            col("CURRENT_COST").cast(DecimalType(38, 9)).alias("current_cost"),
            col("AVERAGE_COST").cast(DecimalType(38, 9)).alias("average_cost"),
            col("WEB_PRICE").cast(DecimalType(38, 9)).alias("web_price"),
            # lit(None).cast(DecimalType(38, 9)).alias("web_display_price"),
            col("CLEARANCE_TYPE_KEY").cast("long").alias("clearance_type_key"),
            col("PROMISE_DATE").cast(DecimalType(38, 0)).alias("promise_date"),
            col("FINAL_TOT_UNITS").cast(DecimalType(38, 0)).alias("final_tot_units"),
            col("FINAL_TOT_AMT").cast(DecimalType(38, 9)).alias("final_tot_amt"),
            col("FINAL_TOT_EXTENDED_AMT")
            .cast(DecimalType(38, 2))
            .alias("final_tot_extended_amt"),
            col("FINAL_TOT_FREIGHT_AMT")
            .cast(DecimalType(38, 2))
            .alias("final_tot_freight_amt"),
            col("FINAL_TOT_TAX_AMT")
            .cast(DecimalType(38, 2))
            .alias("final_tot_tax_amt"),
            col("ORDER_HEADER_KEY").cast("long").alias("order_header_key"),
            col("ORDER_DATE_KEY").cast("long").alias("order_date_key"),
            col("ORDER_TIME_KEY").cast("int").alias("order_time_key"),
            col("WEBSTORE_ORDER_DATE").cast("string").alias("webstore_order_date"),
            col("STYLE_KEY").cast("long").alias("style_key"),
            col("DKS_SKU_KEY").cast("long").alias("dks_sku_key"),
            col("WEB_SKU_KEY").cast("long").alias("web_sku_key"),
            col("PRODUCT_KEY").cast("long").alias("product_key"),
            col("DATA_SOURCE_KEY").cast("long").alias("data_source_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("DATE_ADDED")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            )
            .cast(StringType())
            .alias("date_added"),
            col("ADDED_BY").cast("string").alias("added_by"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("DATE_LAST_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            )
            .cast(StringType())
            .alias("date_last_modified"),
            col("MODIFIED_BY").cast("string").alias("modified_by"),
            col("RECORD_STATUS").cast("string").alias("record_status"),
            col("REFERENCE_ID").cast("long").alias("reference_id"),
            col("CHANNEL_TYPE_KEY").cast("long").alias("channel_type_key"),
            col("ORDER_FULFILL_NUMBER")
            .cast(DecimalType(38, 3))
            .alias("order_fulfill_number"),
            col("PO_NUMBER").cast("string").alias("po_number"),
            col("SHIP_TO_STATE").cast("string").alias("ship_to_state"),
            col("SHIP_TO_ZIP").cast("string").alias("ship_to_zip"),
            col("CUST_FULFILLMENT_MODE_KEY")
            .cast("long")
            .alias("cust_fulfillment_mode_key"),
            col("ORIG_TOT_EXT_DISC_AMT")
            .cast(DecimalType(38, 9))
            .alias("orig_tot_ext_disc_amt"),
            col("ORIG_TOT_FREIGHT_DISC_AMT")
            .cast(DecimalType(38, 9))
            .alias("orig_tot_freight_disc_amt"),
            col("PRESALE_FLG").cast("long").alias("presale_flg"),
            col("HOT_MARKET_FLG").cast("long").alias("hot_market_flg"),
            col("PICK_DECLINE_CNT").cast("long").alias("pick_decline_cnt"),
            col("GTGT_IND").cast("long").alias("gtgt_ind"),
            col("CARRIER_FREIGHT_COST")
            .cast(DecimalType(38, 9))
            .alias("carrier_freight_cost"),
            col("NUM_BOXES_FULFILLED")
            .cast(DecimalType(38, 0))
            .alias("num_boxes_fulfilled"),
            col("NUM_BOXES_EXPENSED")
            .cast(DecimalType(38, 0))
            .alias("num_boxes_expensed"),
            col("NUM_BOXES_DELIVERED")
            .cast(DecimalType(38, 0))
            .alias("num_boxes_delivered"),
            col("MARGIN_DECOMP_CLASSIFICATION")
            .cast(DecimalType(38, 0))
            .alias("margin_decomp_classification"),
            col("FINAL_AMT_UPD_KEY")
            .cast(DecimalType(38, 0))
            .alias("final_amt_upd_key"),
            col("ACTUAL_FF_CHANNEL_KEY")
            .cast(DecimalType(38, 0))
            .alias("actual_ff_channel_key"),
            col("ACTUAL_FF_LOCATION_CNT")
            .cast(DecimalType(38, 0))
            .alias("actual_ff_location_cnt"),
            col("CLR_COLOR_CODE").cast("string").alias("clr_color_code"),
            col("DAY_PERM_PRICE").cast(DecimalType(38, 9)).alias("day_perm_price"),
            col("PO_BOX_CD").cast("string").alias("po_box_cd"),
            col("BOPIS_SAVE_THE_SALE_FLG")
            .cast("string")
            .alias("bopis_save_the_sale_flg"),
            col("BOPIS_STORE").cast("string").alias("bopis_store"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("GTGT_DATE")
                ),
                "yyyy-MM-dd",
            )
            .cast("date")
            .alias("gtgt_date"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
