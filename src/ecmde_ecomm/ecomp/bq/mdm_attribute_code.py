from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
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
from ecmde_ecomm.common.reporting import (
    RPT_UTC_TIMEZONE,
    RPT_DEFAULT_TIMEZONE,
    RPT_BQ_TIMESTAMP_FORMAT,
)


class MDMAttributeCodeEgressOperation(BigQueryEgressOperation):

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
              h.MDM_ATTRIBUTE_CODE_KEY AS MDM_ATTRIBUTE_CODE_KEY,  
              h.MDM_ATTRIBUTE_ID         AS MDM_ATTRIBUTE_ID,
              h.MDM_ATTRIBUTE_DESC       AS MDM_ATTRIBUTE_DESC,
              h.MDM_CLASS_ID             AS MDM_CLASS_ID,
              h.DATA_SOURCE_KEY          AS DATA_SOURCE_KEY,
              h.MDM_CATALOG_KEY          AS MDM_CATALOG_KEY,
              h.MDM_DATE_ADDED           AS MDM_DATE_ADDED,
              h.MDM_DATE_MODIFIED        AS MDM_DATE_MODIFIED,
              h.DATE_ADDED               AS DATE_ADDED,
              h.ADDED_BY                 AS ADDED_BY,
              h.DATE_LAST_MODIFIED       AS DATE_LAST_MODIFIED,
              h.MODIFIED_BY              AS MODIFIED_BY,
              h.RECORD_STATUS            AS RECORD_STATUS,
              h.REFERENCE_ID             AS REFERENCE_ID,
              h.ODS_ATTRIBUTE_COLUMN     AS ODS_ATTRIBUTE_COLUMN
            FROM {self.source.fully_qualified_table()} AS h
            """
        )

        return df.select(
            col("MDM_ATTRIBUTE_CODE_KEY")
            .cast(DecimalType(38, 0))
            .alias("mdm_attribute_code_key"),
            col("MDM_ATTRIBUTE_ID").cast(DecimalType(38, 0)).alias("mdm_attribute_id"),
            col("MDM_ATTRIBUTE_DESC").cast("string").alias("mdm_attribute_desc"),
            col("MDM_CLASS_ID").cast(DecimalType(38, 0)).alias("mdm_class_id"),
            col("DATA_SOURCE_KEY").cast(DecimalType(38, 0)).alias("data_source_key"),
            col("MDM_CATALOG_KEY").cast(DecimalType(38, 0)).alias("mdm_catalog_key"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("MDM_DATE_ADDED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            )
            .cast(StringType())
            .alias("mdm_date_added"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("MDM_DATE_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            )
            .cast(StringType())
            .alias("mdm_date_modified"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("DATE_ADDED")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            )
            .cast(StringType())
            .alias("date_added"),
            col("ADDED_BY").cast("string").alias("added_by"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("DATE_LAST_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            )
            .cast(StringType())
            .alias("date_last_modified"),
            col("MODIFIED_BY").cast("string").alias("modified_by"),
            col("RECORD_STATUS").cast("string").alias("record_status"),
            col("REFERENCE_ID").cast(DecimalType(38, 0)).alias("reference_id"),
            col("ODS_ATTRIBUTE_COLUMN").cast("string").alias("ods_attribute_column"),
        )
