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


class OrderDeliveryEgressOperation(BigQueryEgressOperation):
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
            SELECT
              h.ORDER_DELIVERY_KEY   AS ORDER_DELIVERY_KEY,  
              h.WEBSTORE_KEY                  AS webstore_key,
              h.SCI_LPN_ID                    AS sci_lpn_id,
              h.SCI_TC_LPN_ID                 AS sci_tc_lpn_id,
              h.TRACKING_NUMBER               AS tracking_number,
              h.DELIVERY_STATUS_CD            AS delivery_status_cd,
              h.DELIVERY_STATUS_DTTM          AS delivery_status_dttm,
              h.MANIFEST_DATE_KEY             AS manifest_date_key,
              h.FULFILLMENT_DATE_KEY          AS fulfillment_date_key,
              h.FULFILLMENT_LOCATION_CD       AS fulfillment_location_cd,
              h.ACTUAL_SHIPPED_DTTM           AS actual_shipped_dttm,
              h.SHIPPED_QTY                   AS shipped_qty,
              h.PACKAGE_TYPE_ID               AS package_type_id,
              h.PACKAGE_TYPE_DESCR            AS package_type_descr,
              h.SHIP_VIA                      AS ship_via,
              h.CARRIER_KEY                   AS carrier_key,
              h.FULFILLMENT_MODE_KEY          AS fulfillment_mode_key,
              h.ORDER_FULFILL_NUMBER          AS order_fulfill_number,
              h.WEB_ORD_NUM                   AS web_ord_num,
              h.DATA_SOURCE_KEY               AS data_source_key,
              h.DATE_ADDED                    AS date_added,
              h.ADDED_BY                      AS added_by,
              h.DATE_LAST_MODIFIED            AS date_last_modified,
              h.MODIFIED_BY                   AS modified_by,
              h.RECORD_STATUS                 AS record_status,
              h.REFERENCE_ID                  AS reference_id,
              h.DELIVERY_DATE_KEY             AS delivery_date_key,
              h.DELIVERY_EXCEPTION_FLG        AS delivery_exception_flg,
              h.PICKUP_DATE_KEY               AS pickup_date_key,
              h.SHIP_EXTENDED_AMT             AS ship_extended_amt,
              h.SHIP_FREIGHT_AMT              AS ship_freight_amt,
              h.SHIP_POSTED_UNITS             AS ship_posted_units,
              h.ORDER_TYPE_CD                 AS order_type_cd,
              h.RELEASE_NUMBER                AS release_number,
              h.SHIP_ESTIMATED_AMT            AS ship_estimated_amt,
              h.DELIVERY_DTTM                 AS delivery_dttm,
              h.ORIGIN_SCAN_DTTM              AS origin_scan_dttm,
              h.UPS_ZONE                      AS ups_zone,
              h.MANIFEST_DTTM                 AS manifest_dttm,
              h.ATTEMPTED_DELIVERY_DTTM       AS attempted_delivery_dttm,
              h.USPS_SCAN_DTTM                AS usps_scan_dttm,
              h.DUPLICATE_TRACKING_CD         AS duplicate_tracking_cd,
              h.CARRIER_CONFIRMED_FLG         AS carrier_confirmed_flg,
              h.CARRIER_FREIGHT_COST          AS carrier_freight_cost,
              h.DELIVERY_APPT_DTTM            AS delivery_appt_dttm,
              h.PO_NUMBER                     AS po_number,
              h.CARRIER_BILLED_WEIGHT_LBS     AS carrier_billed_weight_lbs,
              h.CARRIER_LARGE_PKG_EXP_IND     AS carrier_large_pkg_exp_ind,
              h.CARRIER_OVER_MAX_EXP_IND      AS carrier_over_max_exp_ind,
              h.SHIP_UPGRADE_CD               AS ship_upgrade_cd,
              h.ORIGIN_SCAN_STATE             AS origin_scan_state,
              h.DELIVERY_SCAN_STATE           AS delivery_scan_state,
              h.MANIFEST_SCAN_DTTM            AS manifest_scan_dttm,
              h.CARRIER_SCAN_PKG_WEIGHT_LBS   AS carrier_scan_pkg_weight_lbs,
              h.CARRIER_SCAN_PKG_DIM_WEIGHT   AS carrier_scan_pkg_dim_weight,
              h.CARRIER_SCAN_PKG_LENGTH       AS carrier_scan_pkg_length,
              h.CARRIER_SCAN_PKG_WIDTH        AS carrier_scan_pkg_width,
              h.CARRIER_SCAN_PKG_HEIGHT       AS carrier_scan_pkg_height,
              h.SCAN_LARGE_PACKAGE_TYPE       AS scan_large_package_type,
              h.LPN_CREATE_DTTM               AS lpn_create_dttm,
              h.CARRIER_ADH_CHARGE_AMT        AS carrier_adh_charge_amt,
              h.SD_TIP_AMT                    AS sd_tip_amt,
              h.COMMIT_DATE                   AS commit_date,
              h.ORIGINAL_COMMIT_DATE          AS original_commit_date
            FROM {self.source.fully_qualified_table()} AS h
            WHERE (
                 h.DATE_ADDED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 OR h.DATE_LAST_MODIFIED >= (to_timestamp('{last_batch_date_utc}') - INTERVAL 5 DAYS)
                 )
             """
        )
        return df.select(
            col("order_delivery_key")
            .cast(DecimalType(38, 0))
            .alias("order_delivery_key"),
            col("webstore_key").cast(DecimalType(38, 0)).alias("webstore_key"),
            col("sci_lpn_id").cast(DecimalType(38, 0)).alias("sci_lpn_id"),
            col("sci_tc_lpn_id").cast(StringType()).alias("sci_tc_lpn_id"),
            col("tracking_number").cast(StringType()).alias("tracking_number"),
            col("delivery_status_cd").cast(StringType()).alias("delivery_status_cd"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("delivery_status_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("delivery_status_dttm"),
            col("manifest_date_key")
            .cast(DecimalType(38, 0))
            .alias("manifest_date_key"),
            col("fulfillment_date_key")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_date_key"),
            col("fulfillment_location_cd")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_location_cd"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("actual_shipped_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("actual_shipped_dttm"),
            col("shipped_qty").cast(DecimalType(38, 0)).alias("shipped_qty"),
            col("package_type_id").cast("long").alias("package_type_id"),
            col("package_type_descr").cast(StringType()).alias("package_type_descr"),
            col("ship_via").cast(StringType()).alias("ship_via"),
            col("carrier_key").cast(DecimalType(38, 0)).alias("carrier_key"),
            col("fulfillment_mode_key")
            .cast(DecimalType(38, 0))
            .alias("fulfillment_mode_key"),
            col("order_fulfill_number")
            .cast(DecimalType(38, 0))
            .alias("order_fulfill_number"),
            col("web_ord_num").cast(DecimalType(38, 0)).alias("web_ord_num"),
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
            col("delivery_date_key")
            .cast(DecimalType(38, 0))
            .alias("delivery_date_key"),
            col("delivery_exception_flg")
            .cast(StringType())
            .alias("delivery_exception_flg"),
            col("pickup_date_key").cast(DecimalType(38, 0)).alias("pickup_date_key"),
            col("ship_extended_amt")
            .cast(DecimalType(38, 2))
            .alias("ship_extended_amt"),
            col("ship_freight_amt").cast(DecimalType(38, 2)).alias("ship_freight_amt"),
            col("ship_posted_units")
            .cast(DecimalType(38, 0))
            .alias("ship_posted_units"),
            col("order_type_cd").cast(StringType()).alias("order_type_cd"),
            col("release_number").cast(DecimalType(38, 0)).alias("release_number"),
            col("ship_estimated_amt")
            .cast(DecimalType(38, 2))
            .alias("ship_estimated_amt"),
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
                    col("origin_scan_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("origin_scan_dttm"),
            col("ups_zone").cast(StringType()).alias("ups_zone"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("manifest_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("manifest_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("attempted_delivery_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("attempted_delivery_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("usps_scan_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("usps_scan_dttm"),
            col("duplicate_tracking_cd")
            .cast(StringType())
            .alias("duplicate_tracking_cd"),
            col("carrier_confirmed_flg")
            .cast(StringType())
            .alias("carrier_confirmed_flg"),
            col("carrier_freight_cost")
            .cast(DecimalType(38, 2))
            .alias("carrier_freight_cost"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("delivery_appt_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("delivery_appt_dttm"),
            col("po_number").cast(StringType()).alias("po_number"),
            col("carrier_billed_weight_lbs")
            .cast(DecimalType(38, 2))
            .alias("carrier_billed_weight_lbs"),
            col("carrier_large_pkg_exp_ind")
            .cast("long")
            .alias("carrier_large_pkg_exp_ind"),
            col("carrier_over_max_exp_ind")
            .cast("long")
            .alias("carrier_over_max_exp_ind"),
            col("ship_upgrade_cd").cast(StringType()).alias("ship_upgrade_cd"),
            col("origin_scan_state").cast(StringType()).alias("origin_scan_state"),
            col("delivery_scan_state").cast(StringType()).alias("delivery_scan_state"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("manifest_scan_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("manifest_scan_dttm"),
            col("carrier_scan_pkg_weight_lbs")
            .cast(DecimalType(38, 2))
            .alias("carrier_scan_pkg_weight_lbs"),
            col("carrier_scan_pkg_dim_weight")
            .cast(DecimalType(38, 2))
            .alias("carrier_scan_pkg_dim_weight"),
            col("carrier_scan_pkg_length")
            .cast(DecimalType(38, 2))
            .alias("carrier_scan_pkg_length"),
            col("carrier_scan_pkg_width")
            .cast(DecimalType(38, 2))
            .alias("carrier_scan_pkg_width"),
            col("carrier_scan_pkg_height")
            .cast(DecimalType(38, 2))
            .alias("carrier_scan_pkg_height"),
            col("scan_large_package_type")
            .cast(DecimalType(38, 0))
            .alias("scan_large_package_type"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("lpn_create_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("lpn_create_dttm"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("commit_date"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("commit_date"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("original_commit_date"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("original_commit_date"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
