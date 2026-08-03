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
from ecmde_ecomm.common import Logger, ExpectationNotMetError, IllegalArgumentError
from ecmde_ecomm.common.dbx.etl import Watermark
from ecmde_ecomm.common.reporting import (
    RPT_DEFAULT_TIMEZONE,
    RPT_UTC_TIMEZONE,
    RPT_BQ_TIMESTAMP_FORMAT,
)


class OrderSkuFlashEventEgressOperation(BigQueryEgressOperation):
    def __init__(
        self,
        spark: SparkSession,
        bigquery_credentials: BigQueryCredentials,
        source: DatabricksSource,
        destination: BigQueryDestination,
        watermark: Watermark,
    ):
        if watermark is None:
            raise IllegalArgumentError("watermark cannot be None for delta operation.")

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
             where date_added >= '{last_batch_date_utc}'
               or date_last_modified >= '{last_batch_date_utc}'
            """
        )

        return df.select(
            col("order_sku_key").cast(DecimalType(38, 0)).alias("order_sku_key"),
            col("deal_name").cast(StringType()).alias("deal_name"),
            col("campaign_event_desc").cast(StringType()).alias("campaign_event_desc"),
            col("cp_campaign_event_id")
            .cast(DecimalType(38, 0))
            .alias("cp_campaign_event_id"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE), lit(RPT_DEFAULT_TIMEZONE), col("date_added")
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_added"),
            col("added_by").cast(StringType()).alias("added_by"),
            date_format(
                convert_timezone(
                    lit(RPT_UTC_TIMEZONE),
                    lit(RPT_DEFAULT_TIMEZONE),
                    col("date_last_modified"),
                ),
                RPT_BQ_TIMESTAMP_FORMAT,
            ).alias("date_last_modified"),
            col("modified_by").cast(StringType()).alias("modified_by"),
            col("record_status").cast(StringType()).alias("record_status"),
            col("md_promotion_id").cast(DecimalType(38, 0)).alias("md_promotion_id"),
            col("md_event_name").cast(StringType()).alias("md_event_name"),
            col("md_promotion_desc").cast(StringType()).alias("md_promotion_desc"),
        )
