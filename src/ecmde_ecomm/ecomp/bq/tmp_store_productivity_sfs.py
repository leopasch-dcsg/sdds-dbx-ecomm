from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    to_timestamp,
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


class TmpStoreProductivitySfsEgressOperation(BigQueryEgressOperation):

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
            col("STORE_NUMBER").cast(DecimalType(38, 0)).alias("store_number"),
            col("CUSTOMER_ORDER_NBR").cast("string").alias("customer_order_nbr"),
            col("DISTRIBUTION_ORDER_NBR")
            .cast("string")
            .alias("distribution_order_nbr"),
            col("SKU").cast("string").alias("sku"),
            col("LINE_ITEM_ID").cast(DecimalType(38, 0)).alias("line_item_id"),
            date_format(
                convert_timezone(
                    lit("UTC"), lit(timezone), col("LOCAL_ORDER_CREATE_DATE")
                ),
                timestamp_format,
            )
            .cast(StringType())
            .alias("local_order_create_date"),
            date_format(
                convert_timezone(
                    lit("UTC"), lit(timezone), col("LOCAL_DO_CREATE_DATE")
                ),
                timestamp_format,
            )
            .cast(StringType())
            .alias("local_do_create_date"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("LOCAL_MANIFEST_DATE")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("local_manifest_date"),
            col("UPS_PICKUP_TIME").cast("string").alias("ups_pickup_time"),
            col("CAPACITY").cast(DecimalType(38, 0)).alias("capacity"),
            col("AVAILABLE_FLAG").cast(DecimalType(38, 0)).alias("available_flag"),
            col("AVAILABLE_BEFORE_1")
            .cast(DecimalType(38, 0))
            .alias("available_before_1"),
            col("LATE_TODAY_FLAG").cast(DecimalType(38, 0)).alias("late_today_flag"),
            col("MANIFEST_IN_TIME").cast(DecimalType(38, 0)).alias("manifest_in_time"),
            col("DECLINE_IN_TIME").cast(DecimalType(38, 0)).alias("decline_in_time"),
            col("DECLINE_TODAY").cast(DecimalType(38, 0)).alias("decline_today"),
            col("MANIFEST_TODAY").cast(DecimalType(38, 0)).alias("manifest_today"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("LOCAL_DECLINE_DATE")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("local_decline_date"),
            col("UNITS").cast(DecimalType(38, 4)).alias("units"),
            col("DECLINE_UNITS").cast(DecimalType(38, 0)).alias("decline_units"),
            col("DATE_KEY").cast(DecimalType(38, 0)).alias("date_key"),
        )
