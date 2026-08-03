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


class TmpStoreProductivityHrsVwEgressOperation(BigQueryEgressOperation):

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
            col("DATE_KEY").cast(DecimalType(38, 0)).alias("date_key"),
            col("STORE_KEY").cast(DecimalType(38, 0)).alias("store_key"),
            col("STORE_NUMBER").cast("long").alias("store_number"),
            col("STORE_TIME_ZONE_ABBR").cast("string").alias("store_time_zone_abbr"),
            col("UPS_PICKUP_TIME").cast("string").alias("ups_pickup_time"),
            col("STORE_OPEN_TIME").cast("string").alias("store_open_time"),
            col("STORE_CLOSE_TIME").cast("string").alias("store_close_time"),
            col("STORE_OPEN_ALL_DAY_IND")
            .cast("string")
            .alias("store_open_all_day_ind"),
            col("STORE_CLOSED_ALL_DAY_IND")
            .cast("string")
            .alias("store_closed_all_day_ind"),
            col("CAPACITY").cast(DecimalType(38, 0)).alias("capacity"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_ADDED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_added"),
            col("ADDED_BY").cast("string").alias("added_by"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_LAST_MODIFIED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_last_modified"),
            col("MODIFIED_BY").cast("string").alias("modified_by"),
            col("RECORD_STATUS").cast("string").alias("record_status"),
            col("BOPIS_FLAG").cast("string").alias("bopis_flag"),
            col("SFS_FLAG").cast("string").alias("sfs_flag"),
            col("COVID19_STATUS").cast("long").alias("covid19_status"),
        )
