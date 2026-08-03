from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.util import NotebookUtil


@dataclass(frozen=True)
class BatchInsertResult:
    source_table_qualified: str
    destination_table_qualified: str
    records_processed: int
    initial_batch_timestamp_utc: datetime
    next_batch_timestamp_utc: datetime

    def __post_init__(self):
        if len(self.source_table_qualified) == 0:
            raise IllegalArgumentError(
                "source_table_qualified is cannot be empty string."
            )

        if len(self.destination_table_qualified) == 0:
            raise IllegalArgumentError(
                "destination_table_qualified is cannot be empty string."
            )


@dataclass(frozen=True)
class BatchInsertConfig:
    source_catalog: str
    source_schema: str
    source_table: str
    destination_catalog: str
    destination_schema: str
    destination_table: str

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

    def source_table_qualified(self) -> str:
        return f"{self.source_catalog}.{self.source_schema}.{self.source_table}"

    def destination_table_qualified(self) -> str:
        return f"{self.destination_catalog}.{self.destination_schema}.{self.destination_table}"

    @staticmethod
    def from_notebook_params():
        """
        Create an instance of BatchInsertConfig from a set of standard notebook parameters.
        """
        source_catalog = NotebookUtil.notebook_param("dbx_batch_insert_source_catalog")
        source_schema = NotebookUtil.notebook_param("dbx_batch_insert_source_schema")
        source_table = NotebookUtil.notebook_param("dbx_batch_insert_source_table")
        destination_catalog = NotebookUtil.notebook_param(
            "dbx_batch_insert_destination_catalog"
        )
        destination_schema = NotebookUtil.notebook_param(
            "dbx_batch_insert_destination_schema"
        )
        destination_table = NotebookUtil.notebook_param(
            "dbx_batch_insert_destination_table"
        )

        return BatchInsertConfig(
            source_catalog,
            source_schema,
            source_table,
            destination_catalog,
            destination_schema,
            destination_table,
        )


class BatchInsert(ABC):
    def __init__(
        self,
        config: BatchInsertConfig,
        watermark: Watermark,
        spark: SparkSession,
        batch_date_utc: datetime = datetime.now(timezone.utc),
    ):
        self.__config = config
        self.__watermark = watermark
        self.__spark = spark
        self.__batch_date_utc = batch_date_utc

    @property
    def config(self) -> BatchInsertConfig:
        return self.__config

    @property
    def spark(self) -> SparkSession:
        return self.__spark

    @property
    def batch_date_utc(self) -> datetime:
        return self.__batch_date_utc

    def execute(self) -> BatchInsertResult:
        last_batch_date_utc = self.__watermark.last_watermark_utc()
        source_dataframe = self.batch_dataframe(last_batch_date_utc)

        table_name = self.__config.destination_table_qualified()
        source_dataframe.write.format("delta").insertInto(table_name, True)
        self.__watermark.update_watermark_timestamp(self.__batch_date_utc)
        return BatchInsertResult(
            self.__config.source_table_qualified(),
            self.__config.destination_table_qualified(),
            source_dataframe.count(),
            last_batch_date_utc,
            self.batch_date_utc,
        )

    @abstractmethod
    def batch_dataframe(self, last_batch_date_utc: datetime) -> DataFrame:
        """
        Return a data frame that contains data for the given last batch date. The dataframe
        returned should match the shape of the table the data will be inserted into.
        """
        pass
