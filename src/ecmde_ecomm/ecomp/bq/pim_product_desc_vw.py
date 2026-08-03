from datetime import datetime
from pyspark.sql.functions import (
    col,
    date_format,
    convert_timezone,
    lit,
    current_date,
    current_timestamp,
    to_date,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DecimalType, LongType
from ecmde_ecomm.common.bq import (
    BigQueryEgressOperation,
    BigQueryCredentials,
    DatabricksSource,
    BigQueryDestination,
    LoadMode,
)
from ecmde_ecomm.common import Logger, ExpectationNotMetError
from ecmde_ecomm.common.dbx.etl import Watermark
from ecmde_ecomm.common.reporting import (
    RPT_DEFAULT_TIMEZONE,
    RPT_UTC_TIMEZONE,
    RPT_BQ_TIMESTAMP_FORMAT,
)


class PimProductDescVwEgressOperation(BigQueryEgressOperation):
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
            select *
              from {self.source.fully_qualified_table()}
             where date_last_modified >= '{last_batch_date_utc}'
            """
        )

        return df.select(
            col("PIM_ENTITY_ID").cast(DecimalType(38, 0)).alias("pim_entity_id"),
            col("PIM_PRODUCT_CODE").cast(StringType()).alias("pim_product_code"),
            col("PRODUCT_DESCRIPTION").cast(StringType()).alias("product_description"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("DATE_LAST_MODIFIED"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_last_modified"),
            current_timestamp().alias("load_ts"),
            date_format(current_date(), "yyyy-MM-dd").cast("date").alias("load_date"),
        )
