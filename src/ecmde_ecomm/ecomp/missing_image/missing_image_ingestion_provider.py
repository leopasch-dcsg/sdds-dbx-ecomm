from pyspark.sql import SparkSession

from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.errors import NotSupportedError
from ecmde_ecomm.common.mysql.mysql_ingestion import MySqlIngestConfig, MySqlIngestion
from ecmde_ecomm.ecomp.missing_image.bronze.missing_image_ingestion_operations import (
    BronzeBypassColorImageCheckIngestion,
    BronzeBypassEcodeImageCheckIngestion,
)


class MissingImageIngestionProvider:
    __BYPASS_ECODE_IMAGE_CHECK = "bypass_ecode_image_check"
    __BYPASS_COLOR_IMAGE_CHECK = "bypass_color_image_check"

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
        return MissingImageIngestionProvider(config, watermark, spark)

    def operation(self) -> MySqlIngestion:
        match self.config.destination_table.lower():
            case self.__BYPASS_ECODE_IMAGE_CHECK:
                return BronzeBypassEcodeImageCheckIngestion(
                    self.config, self.watermark, self.spark
                )
            case self.__BYPASS_COLOR_IMAGE_CHECK:
                return BronzeBypassColorImageCheckIngestion(
                    self.config, self.watermark, self.spark
                )
            case _:
                raise NotSupportedError(
                    f"The specified destination table {self.config.destination_table_qualified()} is not supported. "
                    f"Ensure a corresponding ingestion operation class is defined for this table."
                )
