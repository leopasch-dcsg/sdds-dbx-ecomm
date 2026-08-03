from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    to_timestamp,
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


class AttributeCodeEgressOperation(BigQueryEgressOperation):

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
            SELECT
              h.ATTRIBUTE_CODE_KEY   AS ATTRIBUTE_CODE_KEY,
              h.ATTRIBUTE_CODE            AS ATTRIBUTE_CODE,
              h.ATTRIBUTE_DESC            AS ATTRIBUTE_DESC,
              h.ITEM_TYPE_KEY             AS ITEM_TYPE_KEY,
              h.DATA_SOURCE_KEY           AS DATA_SOURCE_KEY,
              h.CHAIN_KEY                 AS CHAIN_KEY,
              h.IS_CURRENT                AS IS_CURRENT,
              h.DATE_FROM_KEY             AS DATE_FROM_KEY,
              h.DATE_TO_KEY               AS DATE_TO_KEY,
              h.DATE_ADDED_SOURCE         AS DATE_ADDED_SOURCE,
              h.DATE_MODIFIED_SOURCE      AS DATE_MODIFIED_SOURCE,
              h.DATE_ADDED                AS DATE_ADDED,
              h.ADDED_BY                  AS ADDED_BY,
              h.DATE_LAST_MODIFIED        AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY               AS MODIFIED_BY,
              h.RECORD_STATUS             AS RECORD_STATUS,
              h.SOURCE_ID                 AS SOURCE_ID,
              h.HASH_VALUE                AS HASH_VALUE,
              h.REFERENCE_ID              AS REFERENCE_ID
            FROM {self.source.fully_qualified_table()} AS h
            """
        )

        timestamp_format = "yyyy-MM-dd'T'HH:mm:ss"
        timezone = "America/New_York"

        return df.select(
            col("ATTRIBUTE_CODE_KEY")
            .cast(DecimalType(38, 0))
            .alias("attribute_code_key"),
            col("ATTRIBUTE_CODE").cast("string").alias("attribute_code"),
            col("ATTRIBUTE_DESC").cast("string").alias("attribute_desc"),
            col("ITEM_TYPE_KEY").cast(DecimalType(38, 0)).alias("item_type_key"),
            col("DATA_SOURCE_KEY").cast(DecimalType(38, 0)).alias("data_source_key"),
            col("CHAIN_KEY").cast(DecimalType(38, 0)).alias("chain_key"),
            col("IS_CURRENT").cast("string").alias("is_current"),
            col("DATE_FROM_KEY").cast(DecimalType(38, 0)).alias("date_from_key"),
            col("DATE_TO_KEY").cast(DecimalType(38, 0)).alias("date_to_key"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_ADDED_SOURCE")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_added_source"),
            date_format(
                convert_timezone(
                    lit("UTC"), lit(timezone), col("DATE_MODIFIED_SOURCE")
                ),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_modified_source"),
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
            col("SOURCE_ID").cast(DecimalType(38, 0)).alias("source_id"),
            col("HASH_VALUE").cast(DecimalType(38, 0)).alias("hash_value"),
            col("REFERENCE_ID").cast(DecimalType(38, 0)).alias("reference_id"),
        )
