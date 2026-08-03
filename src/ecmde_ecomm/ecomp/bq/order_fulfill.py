from datetime import datetime
from pyspark.sql.functions import (
    col,
    date_format,
    convert_timezone,
    lit,
    current_date,
    current_timestamp,
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


class OrderFulfillEgressOperation(BigQueryEgressOperation):
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
              h.ORDER_FULFILL_KEY   AS ORDER_FULFILL_KEY,  
              h.ORDER_FULFILL_NUMBER           AS order_fulfill_number,
              h.ORDER_FULFILL_SEQ_NUMBER       AS order_fulfill_seq_number,
              h.CHAIN_KEY                      AS chain_key,
              h.FULFILLMENT_STATUS_CD          AS fulfillment_status_cd,
              h.FULFILLMENT_STATUS_DTTM        AS fulfillment_status_dttm,
              h.FULFILLMENT_DATE_KEY           AS fulfillment_date_key,
              h.CHANNEL_TYPE_KEY               AS channel_type_key,
              h.FULFILLMENT_LOCATION_CD        AS fulfillment_location_cd,
              h.STORE_KEY                      AS store_key,
              h.SHIPMENT_TYPE_CD               AS shipment_type_cd,
              h.SHIP_GEO_KEY                   AS ship_geo_key,
              h.TRACKING_NUMBER                AS tracking_number,
              h.CARRIER_KEY                    AS carrier_key,
              h.FULFILLMENT_MODE_KEY           AS fulfillment_mode_key,
              h.DATA_SOURCE_KEY                AS data_source_key,
              h.DATE_ADDED                     AS date_added,
              h.ADDED_BY                       AS added_by,
              h.DATE_LAST_MODIFIED             AS date_last_modified,
              h.MODIFIED_BY                    AS modified_by,
              h.RECORD_STATUS                  AS record_status,
              h.REFERENCE_ID                   AS reference_id,
              h.PROMISE_DATE_KEY               AS promise_date_key,
              h.SHIP_STATE                     AS ship_state,
              h.SHIP_ZIP                       AS ship_zip,
              h.WEB_ORD_NUM                    AS web_ord_num,
              h.FIRST_FULFILLMENT_MODE_DESC    AS first_fulfillment_mode_desc,
              h.NUM_BOXES                      AS num_boxes,
              h.DELIVERY_DATE_KEY              AS delivery_date_key,
              h.DELIVERY_EXCEPTION_FLG         AS delivery_exception_flg, 
              h.PICKUP_DATE_KEY                AS pickup_date_key,
              h.VENDOR_KEY                     AS vendor_key,
              h.SHIP_METHOD                    AS ship_method,
              h.ORDER_SUBMIT_DTTM              AS order_submit_dttm,
              h.DO_CREATE_DTTM                 AS do_create_dttm,
              h.MIN_SHIP_DTTM                  AS min_ship_dttm,
              h.DELIVERY_DTTM                  AS delivery_dttm,
              h.MIN_MANIFEST_DTTM              AS min_manifest_dttm,
              h.EDD_DTTM                       AS edd_dttm,
              h.FIRST_DO_IND                   AS first_do_ind,
              h.SPEC_ORD_FLG                   AS spec_ord_flg,
              h.PRESALE_FLG                    AS presale_flg,
              h.HOT_MARKET_FLG                 AS hot_market_flg,
              h.ORDER_HEADER_KEY               AS order_header_key,
              h.MIN_FULFILLMENT_DTTM           AS min_fulfillment_dttm,
              h.NUM_BOXES_DELIVERED            AS num_boxes_delivered,
              h.NUM_BOXES_USPS                 AS num_boxes_usps,
              h.MIN_PICKUP_DTTM                AS min_pickup_dttm,
              h.PO_NUMBER                      AS po_number
            FROM {self.source.fully_qualified_table()} AS h 
            WHERE (
                 h.DATE_ADDED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 OR h.DATE_LAST_MODIFIED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 )
            """
        )

        return df.select(
            col("order_fulfill_key")
            .cast(DecimalType(38, 0))
            .alias("order_fulfill_key"),
            col("order_fulfill_number")
            .cast(DecimalType(38, 3))
            .alias("order_fulfill_number"),
            col("order_fulfill_seq_number")
            .cast(DecimalType(38, 0))
            .alias("order_fulfill_seq_number"),
            col("chain_key").cast(DecimalType(38, 0)).alias("chain_key"),
            col("fulfillment_status_cd")
            .cast(StringType())
            .alias("fulfillment_status_cd"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("fulfillment_status_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("fulfillment_status_dttm"),
            col("fulfillment_date_key")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_date_key"),
            col("channel_type_key").cast(DecimalType(38, 0)).alias("channel_type_key"),
            col("fulfillment_location_cd")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_location_cd"),
            col("store_key").cast(DecimalType(38, 0)).alias("store_key"),
            col("shipment_type_cd").cast(StringType()).alias("shipment_type_cd"),
            col("ship_geo_key").cast(DecimalType(38, 0)).alias("ship_geo_key"),
            col("tracking_number").cast(StringType()).alias("tracking_number"),
            col("carrier_key").cast(DecimalType(38, 0)).alias("carrier_key"),
            col("fulfillment_mode_key")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_mode_key"),
            col("data_source_key").cast(DecimalType(38, 0)).alias("data_source_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("date_added")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_added_dttm"),
            col("added_by").cast(StringType()).alias("added_by"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("date_last_modified"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_last_modified_dttm"),
            col("modified_by").cast(StringType()).alias("modified_by"),
            col("record_status").cast(StringType()).alias("record_status"),
            col("reference_id").cast(DecimalType(38, 0)).alias("reference_id"),
            col("promise_date_key").cast("long").alias("promise_date_key"),
            col("ship_state").cast(StringType()).alias("ship_state"),
            col("ship_zip").cast(StringType()).alias("ship_zip"),
            col("web_ord_num").cast(DecimalType(38, 0)).alias("web_ord_num"),
            col("first_fulfillment_mode_desc")
            .cast(StringType())
            .alias("first_fulfillment_mode_desc"),
            col("num_boxes").cast(DecimalType(38, 0)).alias("num_boxes"),
            col("delivery_date_key")
            .cast(DecimalType(38, 0))
            .alias("delivery_date_key"),
            col("delivery_exception_flg")
            .cast(StringType())
            .alias("delivery_exception_flg"),
            col("pickup_date_key").cast(DecimalType(38, 0)).alias("pickup_date_key"),
            col("vendor_key").cast(DecimalType(38, 0)).alias("vendor_key"),
            col("ship_method").cast(StringType()).alias("ship_method"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("order_submit_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("order_submit_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("do_create_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("do_create_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("min_ship_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("min_ship_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("delivery_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("delivery_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("min_manifest_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("min_manifest_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("edd_dttm")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("edd_dttm"),
            col("first_do_ind").cast("long").alias("first_do_ind"),
            col("spec_ord_flg").cast(StringType()).alias("spec_ord_flg"),
            col("presale_flg").cast(StringType()).alias("presale_flg"),
            col("hot_market_flg").cast(StringType()).alias("hot_market_flg"),
            col("order_header_key").cast(DecimalType(38, 0)).alias("order_header_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("min_fulfillment_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("min_fulfillment_dttm"),
            col("num_boxes_delivered")
            .cast(DecimalType(38, 0))
            .alias("num_boxes_delivered"),
            col("num_boxes_usps").cast(DecimalType(38, 0)).alias("num_boxes_usps"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("min_pickup_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("min_pickup_dttm"),
            col("po_number").cast(StringType()).alias("po_number"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
