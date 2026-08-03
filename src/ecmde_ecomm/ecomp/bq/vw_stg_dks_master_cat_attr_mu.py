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


class VwStgDksMasterCatAttrMuEgressOperation(BigQueryEgressOperation):

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
            col("MDM_CLASS_ID").cast(DecimalType(38, 0)).alias("mdm_class_id"),
            col("MDM_ENTITY_ID").cast(DecimalType(38, 0)).alias("mdm_entity_id"),
            col("ATTRIBUTE_ID").cast(DecimalType(38, 0)).alias("attribute_id"),
            col("SORT_ID").cast(DecimalType(38, 0)).alias("sort_id"),
            col("ATTRIBUTE_NAME").cast("string").alias("attribute_name"),
            col("ATTRIBUTE_VALUE").cast("string").alias("attribute_value"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("MDM_DATE_CREATED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("mdm_date_created"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("MDM_DATE_MODIFIED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("mdm_date_modified"),
            col("MDM_RECORD_STATUS").cast("string").alias("mdm_record_status"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_ADDED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_added"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_LAST_MODIFIED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_last_modified"),
            col("STYLE_ID").cast(DecimalType(38, 0)).alias("style_id"),
            col("STYLE_NUMBER").cast("string").alias("style_number"),
        )
