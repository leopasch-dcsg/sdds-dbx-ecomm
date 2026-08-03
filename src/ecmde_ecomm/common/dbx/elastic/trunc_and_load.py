from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.common.logger import Logger
from ecmde_ecomm.common.errors import (
    IllegalArgumentError,
)
from ecmde_ecomm.common.util import NotebookUtil


@dataclass(frozen=True)
class LoadResult:
    source_table_qualified: str | None
    secondary_source_table_qualified: str | None
    destination_table_qualified: str
    start_date_est: date | None
    end_date_est: date | None

    def __post_init__(self):
        if len(self.destination_table_qualified) == 0:
            raise IllegalArgumentError(
                "destination_table_qualified is cannot be empty string."
            )


@dataclass
class LoadConfig:
    source_catalog: str | None
    source_schema: str | None
    source_table: str | None
    source_table_2: str | None
    destination_catalog: str
    destination_schema: str
    destination_table: str
    lookback_days: int | None
    abb_rolling_window: int | None
    webstore_key: int | None
    weight: int | None
    global_avg_prior_weight: int | None
    atc_time_limit: int | None
    min_total_impressions: int | None

    def __post_init__(self):

        if len(self.destination_catalog.strip()) == 0:
            raise IllegalArgumentError("destination_catalog is missing or empty.")

        if len(self.destination_schema.strip()) == 0:
            raise IllegalArgumentError("destination_schema is missing or empty.")

        if len(self.destination_table.strip()) == 0:
            raise IllegalArgumentError("destination_table is missing or empty.")

    def destination_table_qualified(self) -> str:
        return f"{self.destination_catalog}.{self.destination_schema}.{self.destination_table}"

    def source_table_qualified(self) -> str | None:
        return f"{self.source_catalog}.{self.source_schema}.{self.source_table}"

    def source_table_qualified_2(self) -> str | None:
        return f"{self.source_catalog}.{self.source_schema}.{self.source_table_2}"

    @staticmethod
    def from_notebook_params():
        """
        Create an instance of LoadConfig from a set of standard notebook parameters.
        """
        destination_catalog = NotebookUtil.notebook_param(
            "ltr_load_destination_catalog"
        )
        destination_schema = NotebookUtil.notebook_param("ltr_load_destination_schema")
        destination_table = NotebookUtil.notebook_param("ltr_load_destination_table")
        source_catalog = NotebookUtil.notebook_param("ltr_load_source_catalog")
        source_schema = NotebookUtil.notebook_param("ltr_load_source_schema")
        source_table = NotebookUtil.notebook_param("ltr_load_source_table")
        source_table_2 = NotebookUtil.notebook_param("ltr_load_source_table_2")
        lookback_days = NotebookUtil.notebook_param("ltr_lookback_days")
        abb_rolling_window = NotebookUtil.notebook_param("ltr_rolling_window")
        webstore_key = NotebookUtil.notebook_param("ltr_atc_webstore_key")
        weight = NotebookUtil.notebook_param("ltr_abb_weight")
        global_avg_prior_weight = NotebookUtil.notebook_param(
            "ltr_abb_global_avg_prior_weight"
        )
        atc_time_limit = NotebookUtil.notebook_param("ltr_abb_atc_time_limit_minutes")
        min_total_impressions = NotebookUtil.notebook_param(
            "ltr_abb_min_total_impressions"
        )

        return LoadConfig(
            source_catalog,
            source_schema,
            source_table,
            source_table_2,
            destination_catalog,
            destination_schema,
            destination_table,
            lookback_days,
            abb_rolling_window,
            webstore_key,
            weight,
            global_avg_prior_weight,
            atc_time_limit,
            min_total_impressions,
        )


class LoadOperation(ABC):
    def __init__(
        self,
        config: LoadConfig,
        spark: SparkSession,
        start_date_est: date | None = None,
        end_date_est: date | None = None,
    ):
        self.config = config
        self.spark = spark
        self.start_date_est = start_date_est
        self.end_date_est = end_date_est
        self._logger = Logger.logger(__class__.__name__)

    def execute(self) -> LoadResult:
        """
        Overwrite the old data in the destination table with the new data.
        """
        self._logger.info("Fetching batch data.")
        dataframe = self.batch_dataframe()

        table_name = self.config.destination_table_qualified()

        self._logger.info(f"Begin Writing to Delta table {table_name}")
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(table_name)
        )
        self._logger.info(f"Done writing to Delta table {table_name}")

        return LoadResult(
            self.config.source_table_qualified(),
            self.config.source_table_qualified_2(),
            self.config.destination_table_qualified(),
            self.start_date_est,
            self.end_date_est,
        )

    @abstractmethod
    def batch_dataframe(self) -> DataFrame:
        """
        Return a new dataframe containing the base data from start_date_est till end_date_est.
        """
        pass
