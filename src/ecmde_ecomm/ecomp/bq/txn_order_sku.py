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


class TxnOrderSkuEgressOperation(BigQueryEgressOperation):
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
            select *
              from {self.source.fully_qualified_table()}
             where date_added >= '{last_batch_date_utc}'
               or date_last_modified >= '{last_batch_date_utc}'
            """
        )

        return df.select(
            to_date(col("txn_date_key").cast(StringType()), "yyyyMMdd").alias(
                "txn_date"
            ),
            col("txn_time_key").cast(DecimalType(38, 0)).alias("txn_time_key"),
            col("web_ord_num").cast(DecimalType(38, 0)).alias("web_ord_num"),
            col("txn_seq_number").cast(StringType()).alias("txn_seq_number"),
            col("chain_key").cast(DecimalType(38, 0)).alias("chain_key"),
            col("trans_type_key").cast(DecimalType(38, 0)).alias("trans_type_key"),
            col("dks_sku").cast(DecimalType(38, 0)).alias("dks_sku"),
            col("order_line_type_key")
            .cast(DecimalType(38, 0))
            .alias("order_line_type_key"),
            col("units").cast(DecimalType(38, 0)).alias("units"),
            col("extended_amt").cast(DecimalType(38, 2)).alias("extended_amt"),
            col("freight_amt").cast(DecimalType(38, 2)).alias("freight_amt"),
            col("total_amt").cast(DecimalType(38, 2)).alias("total_amt"),
            col("tax_amt").cast(DecimalType(38, 2)).alias("tax_amt"),
            col("order_fulfill_key")
            .cast(DecimalType(38, 0))
            .alias("order_fulfill_key"),
            col("reason_key").cast(DecimalType(38, 0)).alias("reason_key"),
            col("tracking_number").cast(StringType()).alias("tracking_number"),
            col("order_fulfill_number")
            .cast(DecimalType(38, 3))
            .alias("order_fulfill_number"),
            col("order_fulfill_seq_number")
            .cast(DecimalType(38, 0))
            .alias("order_fulfill_seq_number"),
            col("dks_sku_key").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("web_sku_key").cast(DecimalType(38, 0)).alias("web_sku_key"),
            col("style_key").cast(DecimalType(38, 0)).alias("style_key"),
            col("product_key").cast(DecimalType(38, 0)).alias("product_key"),
            col("order_header_key").cast(DecimalType(38, 0)).alias("order_header_key"),
            col("order_sku_key").cast(DecimalType(38, 0)).alias("order_sku_key"),
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
            col("source_reason_cd").cast(StringType()).alias("source_reason_cd"),
            col("source_store_cd").cast(StringType()).alias("source_store_cd"),
            col("average_cost").cast(DecimalType(38, 2)).alias("average_cost"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("decline_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("decline_dttm"),
            col("decline_units").cast(LongType()).alias("decline_units"),
            col("order_delivery_key")
            .cast(DecimalType(38, 0))
            .alias("order_delivery_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("data_source_dttm"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("data_source_dttm"),
            col("freight_cost").cast(DecimalType(38, 2)).alias("freight_cost"),
            col("txn_cust_service_rep_key")
            .cast(DecimalType(38, 0))
            .alias("txn_cust_service_rep_key"),
            col("order_discount_amt")
            .cast(DecimalType(38, 2))
            .alias("order_discount_amt"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
            col("return_header_key")
            .cast(DecimalType(38, 0))
            .alias("return_header_key"),
            col("return_source").cast(StringType()).alias("return_source"),
            col("return_delivery_key")
            .cast(DecimalType(38, 0))
            .alias("return_delivery_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("estimated_ship_date"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("estimated_ship_date"),
        )
