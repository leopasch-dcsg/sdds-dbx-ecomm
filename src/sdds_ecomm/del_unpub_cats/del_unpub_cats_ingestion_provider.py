from pyspark.sql import SparkSession

from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.errors import NotSupportedError
from ecmde_ecomm.common.mysql.mysql_ingestion import MySqlIngestConfig, MySqlIngestion
from sdds_ecomm.del_unpub_cats.bronze.del_unpub_cats_ingestion_operations import (
    BronzeUnpublishedCategoriesIngestion,
)


class DelUnpubCatsIngestionProvider:
    """
    Provider that resolves the correct bronze ingestion operation for the
    `del_unpub_cats` pipeline based on the destination table name declared
    in the notebook params.
    """

    __UNPUBLISHED_CATEGORIES = "unpublished_categories"

    def __init__(
        self,
        config: MySqlIngestConfig,
        watermark: Watermark,
        spark: SparkSession,
    ):
        self.config = config
        self.watermark = watermark
        self.spark = spark

    @staticmethod
    def provider(
        config: MySqlIngestConfig,
        watermark: Watermark,
        spark: SparkSession,
    ):
        return DelUnpubCatsIngestionProvider(config, watermark, spark)

    def operation(self) -> MySqlIngestion:
        match self.config.destination_table.lower():
            case self.__UNPUBLISHED_CATEGORIES:
                return BronzeUnpublishedCategoriesIngestion(
                    self.config, self.watermark, self.spark
                )
            case _:
                raise NotSupportedError(
                    f"The specified destination table {self.config.destination_table_qualified()} is not supported. "
                    f"Ensure a corresponding ingestion operation class is defined for this table."
                )
