from abc import ABC, abstractmethod
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.common.errors import (
    IllegalArgumentError,
)
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.util import NotebookUtil


@dataclass(frozen=True)
class DivergenceResult:
    destination_table_qualified: str

    def __post_init__(self):
        if len(self.destination_table_qualified) == 0:
            raise IllegalArgumentError(
                "destination_table_qualified is cannot be empty string."
            )


@dataclass
class DivergenceConfig:
    destination_catalog: str
    destination_schema: str
    destination_table: str

    def __post_init__(self):

        if len(self.destination_catalog.strip()) == 0:
            raise IllegalArgumentError("destination_catalog is missing or empty.")

        if len(self.destination_schema.strip()) == 0:
            raise IllegalArgumentError("destination_schema is missing or empty.")

        if len(self.destination_table.strip()) == 0:
            raise IllegalArgumentError("destination_table is missing or empty.")

    def destination_table_qualified(self) -> str:
        return f"{self.destination_catalog}.{self.destination_schema}.{self.destination_table}"

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

        return DivergenceConfig(
            destination_catalog,
            destination_schema,
            destination_table,
        )


class DivergenceOperation(ABC):
    def __init__(
        self,
        config: DivergenceConfig,
        spark: SparkSession,
    ):
        self.config = config
        self.spark = spark

    def execute(self) -> DivergenceResult:
        """
        Overwrite the old data in the destination table with the new data.
        """
        dataframe = self.batch_dataframe()
        table_name = self.config.destination_table_qualified()
        dataframe.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "false"
        ).saveAsTable(table_name)

        return DivergenceResult(
            self.config.destination_table_qualified(),
        )

    @abstractmethod
    def batch_dataframe(self) -> DataFrame:
        """
        Return a new dataframe containing the base data from start_date_est till end_date_est.
        """
        pass
