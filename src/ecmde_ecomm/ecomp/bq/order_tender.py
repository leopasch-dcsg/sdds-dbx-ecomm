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


class OrderTenderEgressOperation(BigQueryEgressOperation):
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
                    header.order_header_key,
                    tender.payment_id,
                    tender.web_order_num,
                    tender.payment_method,
                    tender.card_type,
                    CASE WHEN tender.card_type = 'Gift Card' THEN tender.giftcard_num END AS giftcard_num,
                    tender.authorization_amt,
                    tender.bill_to_city,
                    tender.bill_to_state,
                    tender.bill_to_zip,
                    tender.bill_to_country,
                    tender.date_added         AS date_added,
                    tender.date_last_modified AS date_last_modified
                FROM {self.source.fully_qualified_table()} AS tender
                JOIN {self.source.catalog}.ecom_dim.order_header AS header
                  ON tender.WEB_ORDER_NUM = header.WEB_ORD_NUM
                WHERE tender.payment_method <> 'BillMeLater'
                AND header.chain_key IN (2,4,6,7,8,9)
                  AND (
                        tender.date_added         >= '{last_batch_date_utc}'
                     OR tender.date_last_modified >= '{last_batch_date_utc}'
                  )
            """
        )

        return df.select(
            col("order_header_key").cast("long").alias("order_header_key"),
            col("payment_id").cast(StringType()).alias("payment_id"),
            col("web_order_num").cast("long").alias("web_order_num"),
            col("payment_method").cast(StringType()).alias("payment_method"),
            col("card_type").cast(StringType()).alias("card_type"),
            col("giftcard_num").cast(StringType()).alias("giftcard_num"),
            col("authorization_amt")
            .cast(DecimalType(38, 5))
            .alias("authorization_amt"),
            col("bill_to_city").cast(StringType()).alias("bill_to_city"),
            col("bill_to_state").cast(StringType()).alias("bill_to_state"),
            col("bill_to_zip").cast(StringType()).alias("bill_to_zip"),
            col("bill_to_country").cast(StringType()).alias("bill_to_country"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("date_last_modified"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_last_modified"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("date_added")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_added"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
