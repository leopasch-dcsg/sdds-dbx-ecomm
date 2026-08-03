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


class ClrColorLkupEgressOperation(BigQueryEgressOperation):

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
            col("CLR_COLOR_KEY").cast(DecimalType(38, 0)).alias("clr_color_key"),
            col("CLR_COLOR_CODE").cast("string").alias("clr_color_code"),
            col("CLR_COLOR_DESC").cast("string").alias("clr_color_desc"),
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
        )
