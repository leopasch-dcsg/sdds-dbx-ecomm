from abc import abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.common.logger import Logger
from ecmde_ecomm.common.errors import (
    IllegalArgumentError,
)


@dataclass(frozen=True)
class DatabricksDestination:
    catalog: str
    schema: str
    table: str

    def __post_init__(self):
        if len(self.catalog.strip()) == 0:
            raise IllegalArgumentError("catalog is missing or empty.")

        if len(self.schema.strip()) == 0:
            raise IllegalArgumentError("schema is missing or empty.")

        if len(self.table.strip()) == 0:
            raise IllegalArgumentError("table is missing or empty.")

    def fully_qualified_table(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class DatabricksSource:
    catalog: str
    schema: str

    def __post_init__(self):
        if len(self.catalog.strip()) == 0:
            raise IllegalArgumentError("catalog is missing or empty.")

        if len(self.schema.strip()) == 0:
            raise IllegalArgumentError("schema is missing or empty.")

@dataclass(frozen=True)
class DateInfo:
    end_date_est: str
    lookback_days: int

    def __post_init__(self):
        if len(self.end_date_est.strip()) == 0:
            raise IllegalArgumentError("start_date_est is missing or empty.")

        if self.lookback_days is None:
            raise IllegalArgumentError("Must have lookback days set.")

class AppendOperation:
    def __init__(
            self,
            spark: SparkSession,
            source: DatabricksSource,
            destination: DatabricksDestination,
            date_info: DateInfo,
    ):
        self.spark = spark
        self.source = source
        self.destination = destination
        self.date_info = date_info
        self.__logger = Logger.logger(__class__.__name__)



    def execute(self) -> None:
        """
        Append new data to the destination table.
        """

        self.__logger.info("Fetching batch data.")
        dataframe = self.batch_dataframe()
        table_name = self.destination.fully_qualified_table()
        self.__logger.info(f"Begin Writing to Delta table {table_name}")
        (
            dataframe.write.format("delta")
            .mode("append")
            .saveAsTable(self.destination.fully_qualified_table())
        )
        self.__logger.info(f"Done writing to Delta table {table_name}")

    @abstractmethod
    def batch_dataframe(self) -> DataFrame:
        """
        Return a new dataframe containing the base data from start_date_est till end_date_est.
        """
        pass


class AppendOperationBuilder:
    spark: SparkSession
    dbx_source: DatabricksSource
    dbx_destination: DatabricksDestination
    date_info: DateInfo

    def __init__(self ,spark: SparkSession):
        self.spark = spark

    def with_dbx_source(self, dbx_source: DatabricksSource):
        self.dbx_source = dbx_source
        return self

    def with_dbx_destination(self, dbx_destination: DatabricksDestination):
        self.dbx_destination = dbx_destination
        return self

    def with_date_info(self, date_info: DateInfo):
        self.date_info = date_info
        return self

    def operation(self) -> AppendOperation:
        if self.dbx_source is None:
            raise IllegalArgumentError(
                "Cannot create a AppendOperation without specifying the Databricks source data."
            )

        if self.dbx_destination is None:
            raise IllegalArgumentError(
                "Cannot create a AppendOperation without specifying the Databricks destination data."
            )

        if self.date_info is None:
            raise IllegalArgumentError(
                "Cannot create a AppendOperation without specifying the date_info data."
            )

        return AppendOperation(
            self.spark,
            self.dbx_source,
            self.dbx_destination,
            self.date_info,
        )
