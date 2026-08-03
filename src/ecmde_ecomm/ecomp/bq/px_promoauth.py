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


class PxPromoauthEgressOperation(BigQueryEgressOperation):

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
            col("PX_BATCH_ID").cast("string").alias("px_batch_id"),
            col("PX_PROMOTION_ID").cast("string").alias("px_promotion_id"),
            col("COMMENTS").cast("string").alias("comments"),
            col("PROMOTIONTYPE").cast("string").alias("promotiontype"),
            col("DAILYSTARTTIME").cast("string").alias("dailystarttime"),
            col("DAILYENDTIME").cast("string").alias("dailyendtime"),
            col("WEEKDAY_SUN").cast("string").alias("weekday_sun"),
            col("WEEKDAY_MON").cast("string").alias("weekday_mon"),
            col("WEEKDAY_TUE").cast("string").alias("weekday_tue"),
            col("WEEKDAY_WED").cast("string").alias("weekday_wed"),
            col("WEEKDAY_THU").cast("string").alias("weekday_thu"),
            col("WEEKDAY_FRI").cast("string").alias("weekday_fri"),
            col("WEEKDAY_SAT").cast("string").alias("weekday_sat"),
            col("CTLPARAM").cast("string").alias("ctlparam"),
            col("OPTCOUNTER").cast("string").alias("optcounter"),
            col("ADMINSTVENAME").cast("string").alias("adminstvename"),
            col("UP_ADMINSTVENAME").cast("string").alias("up_adminstvename"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_ADDED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_added"),
            col("ADDED_BY").cast("string").alias("added_by"),
        )
