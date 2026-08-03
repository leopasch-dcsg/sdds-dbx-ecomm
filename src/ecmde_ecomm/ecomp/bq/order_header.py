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


class OrderHeaderEgressOperation(BigQueryEgressOperation):
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
              h.ORDER_HEADER_KEY   AS ORDER_HEADER_KEY,  
              h.WEB_ORD_NUM                AS web_ord_num,
              h.CHAIN_KEY                  AS chain_key,
              h.ORDER_DATE_KEY             AS order_date_key,
              h.ORDER_TIME_KEY             AS order_time_key,
              h.ORDER_STATUS_DTTM          AS order_status_dttm,
              h.ORDER_STATUS_KEY           AS order_status_key,
              h.CUST_SERVICE_REP_KEY       AS cust_service_rep_key,
              h.AOS_STORE_KEY              AS aos_store_key,
              h.DEVICE_TYPE_KEY            AS device_type_key,
              h.SITE_TYPE_KEY              AS site_type_key,
              h.GEO_KEY                    AS geo_key,
              h.PAYMENT_METHOD_KEY         AS payment_method_key,
              h.BILL_CUST_NUM              AS bill_cust_num,
              h.WEB_CUST_NUM               AS web_cust_num,
              h.SCORECARD_ID               AS scorecard_id,
              h.DATA_SOURCE_KEY            AS data_source_key,
              h.DATE_ADDED                 AS date_added,
              h.ADDED_BY                   AS added_by,
              h.DATE_LAST_MODIFIED         AS date_last_modified,
              h.MODIFIED_BY                AS modified_by,
              h.RECORD_STATUS              AS record_status,
              h.REFERENCE_ID               AS reference_id,
              h.BILL_STATE                 AS bill_state,
              h.BILL_ZIP                   AS bill_zip,
              h.DEMAND_IND                 AS demand_ind,
              h.QUERYSTRING_CODE           AS querystring_code,
              h.ORIGINAL_ORDER_NUM_TXT     AS original_order_num_txt,
              h.ORIGINAL_WEB_ORD_NUM       AS original_web_ord_num,
              h.ORDER_SOURCE_KEY           AS order_source_key,
              h.INITIAL_SPLIT_CNT          AS initial_split_cnt,
              h.NATURAL_SPLIT_FLG          AS natural_split_flg,
              h.FRAUD_HOLD_FLG             AS fraud_hold_flg,
              h.FRAUD_UNHOLD_DTTM          AS fraud_unhold_dttm,
              h.EOM_FLG                    AS eom_flg,
              h.TEST_ORDER_FLG             AS test_order_flg,
              h.ORDER_COMPLETE_DATE_KEY    AS order_complete_date_key,
              h.ORDER_DMD_UNITS            AS order_dmd_units,
              h.ORDER_DMD_AMT              AS order_dmd_amt,
              h.ORDER_DMD_SKU_CNT          AS order_dmd_sku_cnt,
              h.CLK_FLG                    AS clk_flg,
              h.ACTUAL_FF_CHANNEL_KEY      AS actual_ff_channel_key,
              h.ACTUAL_FF_LOCATION_CNT     AS actual_ff_location_cnt,
              h.REWARD_CERT_USED_FLG       AS reward_cert_used_flg,
              h.ACTUAL_STH_LOCATION_CNT    AS actual_sth_location_cnt
            FROM {self.source.fully_qualified_table()} AS h
            WHERE (
                 h.DATE_ADDED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 OR h.DATE_LAST_MODIFIED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 )
            """
        )

        return df.select(
            col("order_header_key").cast(LongType()).alias("order_header_key"),
            col("web_ord_num").cast(LongType()).alias("web_ord_num"),
            col("chain_key").cast(LongType()).alias("chain_key"),
            col("order_date_key").cast(LongType()).alias("order_date_key"),
            to_date(col("order_date_key").cast(StringType()), "yyyyMMdd").alias(
                "order_date"
            ),
            col("order_time_key").cast(LongType()).alias("order_time_key"),
            col("order_status_key").cast(LongType()).alias("order_status_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("order_status_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("order_status_dttm"),
            col("cust_service_rep_key").cast(LongType()).alias("cust_service_rep_key"),
            col("aos_store_key").cast(LongType()).alias("aos_store_key"),
            col("device_type_key").cast(LongType()).alias("device_type_key"),
            col("site_type_key").cast(LongType()).alias("site_type_key"),
            col("geo_key").cast(LongType()).alias("geo_key"),
            col("payment_method_key").cast(LongType()).alias("payment_method_key"),
            col("bill_cust_num").cast(StringType()).alias("bill_cust_num"),
            col("web_cust_num").cast(StringType()).alias("web_cust_num"),
            col("scorecard_id").cast(StringType()).alias("scorecard_id"),
            col("data_source_key").cast(LongType()).alias("data_source_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("date_added"),
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
            col("reference_id").cast(LongType()).alias("reference_id"),
            col("bill_state").cast(StringType()).alias("bill_state"),
            col("bill_zip").cast(StringType()).alias("bill_zip"),
            col("demand_ind").cast(LongType()).alias("demand_ind"),
            col("querystring_code").cast(StringType()).alias("querystring_code"),
            col("original_order_num_txt")
            .cast(StringType())
            .alias("original_order_num_txt"),
            col("original_web_ord_num").cast(LongType()).alias("original_web_ord_num"),
            col("order_source_key").cast(LongType()).alias("order_source_key"),
            col("initial_split_cnt").cast(LongType()).alias("initial_split_cnt"),
            col("natural_split_flg").cast(StringType()).alias("natural_split_flg"),
            col("fraud_hold_flg").cast(StringType()).alias("fraud_hold_flg"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("fraud_unhold_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("fraud_unhold_dttm"),
            col("eom_flg").cast(StringType()).alias("eom_flg"),
            col("test_order_flg").cast(StringType()).alias("test_order_flg"),
            col("order_complete_date_key")
            .cast(LongType())
            .alias("order_complete_date_key"),
            col("order_dmd_units").cast(DecimalType(38, 0)).alias("order_dmd_units"),
            col("order_dmd_amt").cast(DecimalType(38, 2)).alias("order_dmd_amt"),
            col("order_dmd_sku_cnt")
            .cast(DecimalType(38, 0))
            .alias("order_dmd_sku_cnt"),
            col("actual_ff_channel_key")
            .cast(DecimalType(38, 0))
            .alias("actual_ff_channel_key"),
            col("actual_ff_location_cnt")
            .cast(DecimalType(38, 0))
            .alias("actual_ff_location_cnt"),
            col("reward_cert_used_flg")
            .cast(StringType())
            .alias("reward_cert_used_flg"),
            col("actual_sth_location_cnt")
            .cast(DecimalType(38, 0))
            .alias("actual_sth_location_cnt"),
            current_timestamp().alias("load_ts"),
            to_date(current_date()).alias("load_date"),
        )
