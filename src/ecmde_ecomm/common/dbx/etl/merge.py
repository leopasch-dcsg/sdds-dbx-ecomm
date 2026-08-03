from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.common.errors import (
    IllegalArgumentError,
)
from ecmde_ecomm.common.dbx.env import DbxEnv
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.util import NotebookUtil


@dataclass(frozen=True)
class MergeResult:
    source_table_qualified: str
    destination_table_qualified: str
    records_processed: int
    initial_watermark_timestamp_utc: datetime
    next_watermark_timestamp_utc: datetime

    def __post_init__(self):
        if len(self.source_table_qualified) == 0:
            raise IllegalArgumentError(
                "source_table_qualified is cannot be empty string."
            )

        if len(self.destination_table_qualified) == 0:
            raise IllegalArgumentError(
                "destination_table_qualified is cannot be empty string."
            )


@dataclass
class MergeConfig:
    source_catalog: str
    source_schema: str
    source_table: str
    destination_catalog: str
    destination_schema: str
    destination_table: str
    dbx_user_id: str
    dbx_env: DbxEnv = DbxEnv.DEV

    def __post_init__(self):

        if len(self.source_catalog.strip()) == 0:
            raise IllegalArgumentError("source_catalog is missing or empty.")

        if len(self.source_schema.strip()) == 0:
            raise IllegalArgumentError("source_schema is missing or empty.")

        if len(self.source_table.strip()) == 0:
            raise IllegalArgumentError("source_table is missing or empty.")

        if len(self.destination_catalog.strip()) == 0:
            raise IllegalArgumentError("destination_catalog is missing or empty.")

        if len(self.destination_schema.strip()) == 0:
            raise IllegalArgumentError("destination_schema is missing or empty.")

        if len(self.destination_table.strip()) == 0:
            raise IllegalArgumentError("destination_table is missing or empty.")

        if len(self.dbx_user_id.strip()) == 0:
            raise IllegalArgumentError("dbx_user_id is missing or empty.")

    def source_table_qualified(self) -> str:
        return f"{self.source_catalog}.{self.source_schema}.{self.source_table}"

    def destination_table_qualified(self) -> str:
        return f"{self.destination_catalog}.{self.destination_schema}.{self.destination_table}"

    @staticmethod
    def from_notebook_params():
        """
        Create an instance of MergeConfig from a set of standard notebook parameters.
        """
        dbx_env = DbxEnv.from_notebook_params()
        dbx_user_id = NotebookUtil.notebook_param("dbx_user_id")
        source_catalog = NotebookUtil.notebook_param("dbx_merge_source_catalog")
        source_schema = NotebookUtil.notebook_param("dbx_merge_source_schema")
        source_table = NotebookUtil.notebook_param("dbx_merge_source_table")
        destination_catalog = NotebookUtil.notebook_param(
            "dbx_merge_destination_catalog"
        )
        destination_schema = NotebookUtil.notebook_param("dbx_merge_destination_schema")
        destination_table = NotebookUtil.notebook_param("dbx_merge_destination_table")

        return MergeConfig(
            source_catalog,
            source_schema,
            source_table,
            destination_catalog,
            destination_schema,
            destination_table,
            dbx_user_id,
            dbx_env,
        )


class MergeOperation(ABC):
    def __init__(
        self,
        config: MergeConfig,
        watermark: Watermark,
        spark: SparkSession,
        batch_date_utc: datetime = datetime.now(timezone.utc),
    ):
        self.__config = config
        self.__spark = spark
        self.__watermark = watermark
        self.__batch_date_utc = batch_date_utc

    @property
    def config(self) -> MergeConfig:
        return self.__config

    @property
    def spark(self) -> SparkSession:
        return self.__spark

    @property
    def watermark(self) -> Watermark:
        return self.__watermark

    @property
    def batch_date_utc(self) -> datetime:
        return self.__batch_date_utc

    @abstractmethod
    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        """
        Return the SQL statement that will be executed to fetch the incremental changeset from the source tables.
        """
        pass

    @abstractmethod
    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        """
        Allows the caller to apply any transforms against the source_dataframe. The source_dataframe contains the
        incremental dataset fetched from the source table since the last time the job executed.
        """
        pass

    @abstractmethod
    def merge_updates(self, updates: DataFrame) -> None:
        """
        Allows the caller to execute a Delta table merge into the destination table with the transforms applied to
        the incremental update from get_source_updates_from_dataframe().
        """
        pass

    def execute(self) -> MergeResult:
        """
        Execute the merge operation by incrementally fetching data from the source table and performing a delta merge
        into the destination table.
        """
        last_watermark_utc = self.watermark.last_watermark_utc()
        incremental_changeset = self.get_incremental_changeset(last_watermark_utc)
        updates = self.perform_changeset_transforms(incremental_changeset)

        self.merge_updates(updates)
        self.watermark.update_watermark_timestamp(self.__batch_date_utc)

        return MergeResult(
            self.config.source_table_qualified(),
            self.config.destination_table_qualified(),
            0,
            last_watermark_utc,
            self.__batch_date_utc,
        )

    def get_incremental_changeset(self, last_watermark_utc: datetime) -> DataFrame:
        statement = self.get_sql_statement_for_incremental_changes(last_watermark_utc)
        return self.spark.sql(statement)
