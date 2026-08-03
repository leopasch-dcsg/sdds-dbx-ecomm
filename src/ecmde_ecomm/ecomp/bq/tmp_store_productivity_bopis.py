from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    to_timestamp,
    from_utc_timestamp,
    convert_timezone,
    lit,
)
from datetime import datetime
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from pyspark.sql.types import DecimalType, StringType
from ecmde_ecomm.common import Logger
from ecmde_ecomm.common.bq.egress import (
    BigQueryEgressOperation,
    DatabricksSource,
    BigQueryCredentials,
    BigQueryDestination,
    LoadMode,
)


class TmpStoreProductivityBopisEgressOperation(BigQueryEgressOperation):

    def __init__(
        self,
        spark: SparkSession,
        bigquery_credentials: BigQueryCredentials,
        source: DatabricksSource,
        destination: BigQueryDestination,
        watermark: Watermark | None = None,
        load_mode: LoadMode = LoadMode.TRUNCATE_LOAD,
    ):
        super().__init__(
            spark=spark,
            credentials=bigquery_credentials,
            source=source,
            destination=destination,
            watermark=watermark,
            load_mode=load_mode,
        )

        self._logger = Logger.logger(__class__.__name__)

    def get_source_dataframe(
        self, last_batch_date_utc: datetime | None = None
    ) -> DataFrame:
        self._logger.info(
            f"Getting source dataframe for {self.source.table} and casting column types to be more explicit."
        )
        df = self.spark.sql(
            f"""
                select * from {self.source.fully_qualified_table()}
                """
        )

        timestamp_format = "yyyy-MM-dd'T'HH:mm:ss"
        timezone = "America/New_York"

        return df.select(
            col("STORE_NUMBER").cast("long").alias("store_number"),
            col("CUSTOMER_ORDER_NBR").cast("string").alias("customer_order_nbr"),
            col("DISTRIBUTION_ORDER_NBR")
            .cast("string")
            .alias("distribution_order_nbr"),
            col("SKU").cast("string").alias("sku"),
            col("LINE_ITEM_ID").cast("long").alias("line_item_id"),
            date_format(
                convert_timezone(
                    lit("UTC"), lit(timezone), col("LOCAL_DO_CREATE_DATE")
                ),
                timestamp_format,
            )
            .cast(StringType())
            .alias("local_do_create_date"),
            date_format(
                convert_timezone(
                    lit("UTC"), lit(timezone), col("LOCAL_READY_FOR_PICKUP_DATE")
                ),
                timestamp_format,
            )
            .cast(StringType())
            .alias("local_ready_for_pickup_date"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("LOCAL_DECLINE_DATE")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("local_decline_date"),
            col("UNITS").cast(DecimalType(38, 4)).alias("units"),
            col("DECLINE_UNITS").cast("long").alias("decline_units"),
            col("STORE_OPEN_TIME").cast("string").alias("store_open_time"),
            col("STORE_CLOSE_TIME").cast("string").alias("store_close_time"),
            col("PREV_STORE_CLOSE_TIME").cast("string").alias("prev_store_close_time"),
            col("PREV_STORE_OPEN_DATE_KEY")
            .cast("long")
            .alias("prev_store_open_date_key"),
            col("TIME_TO_FILL").cast(DecimalType(38, 9)).alias("time_to_fill"),
            col("DECLINE_CODE").cast("string").alias("decline_code"),
            col("DATE_KEY").cast("long").alias("date_key"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("PREV_CLOSE_DATE")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("prev_close_date"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("OPEN_DTTM")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("open_dttm"),
        )
