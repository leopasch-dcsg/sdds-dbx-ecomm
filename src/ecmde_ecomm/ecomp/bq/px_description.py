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
from pyspark.sql.types import DecimalType, StringType
from ecmde_ecomm.common import Logger
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.bq.egress import (
    BigQueryEgressOperation,
    DatabricksSource,
    BigQueryCredentials,
    BigQueryDestination,
    LoadMode,
)


class PxDescriptionEgressOperation(BigQueryEgressOperation):

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

        timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        timezone = "America/New_York"

        return df.select(
            col("PX_BATCH_ID").cast("string").alias("px_batch_id"),
            col("PX_PROMOTION_ID").cast("string").alias("px_promotion_id"),
            col("LANGUAGE_ID").cast("string").alias("language_id"),
            col("ADMINDESC").cast("string").alias("admindesc"),
            col("SHORTDESC").cast("string").alias("shortdesc"),
            col("LONGDESC").cast("string").alias("longdesc"),
            col("FIELD1").cast("string").alias("field1"),
            col("FIELD2").cast("string").alias("field2"),
            col("FIELD3").cast("string").alias("field3"),
            col("FIELD4").cast("string").alias("field4"),
            col("FIELD5").cast("string").alias("field5"),
            col("OPTCOUNTER").cast("string").alias("optcounter"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_ADDED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_added"),
            col("ADDED_BY").cast("string").alias("added_by"),
        )
