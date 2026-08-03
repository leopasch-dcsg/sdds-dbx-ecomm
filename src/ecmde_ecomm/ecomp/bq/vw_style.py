from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    to_timestamp,
    from_utc_timestamp,
)
from datetime import datetime
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from pyspark.sql.types import DecimalType
from ecmde_ecomm.common import Logger
from ecmde_ecomm.common.bq.egress import (
    BigQueryEgressOperation,
    DatabricksSource,
    BigQueryCredentials,
    BigQueryDestination,
    LoadMode,
)


class VwStyleEgressOperation(BigQueryEgressOperation):

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
            col("STYLE_ID").cast(DecimalType(38, 0)).alias("style_id"),
            col("EMAST_PIM_ENTITY_ID")
            .cast(DecimalType(38, 0))
            .alias("emast_pim_entity_id"),
            col("EMAST_PIM_ECODE").cast("string").alias("emast_pim_ecode"),
            col("DKS_PIM_ENTITY_ID")
            .cast(DecimalType(38, 0))
            .alias("dks_pim_entity_id"),
        )
