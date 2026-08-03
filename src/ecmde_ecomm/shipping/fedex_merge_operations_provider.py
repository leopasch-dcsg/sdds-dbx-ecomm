from pyspark.sql import SparkSession
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig, MergeOperation
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.errors import NotSupportedError
from ecmde_ecomm.shipping.fedex_tracking_merge_operations import (
    SilverFedexTracking,
)


class FedExMergeOperationProvider:

    __FEDEX_TRACKING_TABLE = "fedex_tracking"

    def __init__(self, config: MergeConfig, watermark: Watermark, spark: SparkSession):
        self.config = config
        self.watermark = watermark
        self.spark = spark

    @staticmethod
    def provider(config: MergeConfig, watermark: Watermark, spark: SparkSession):
        return FedExMergeOperationProvider(config, watermark, spark)

    def operation(self) -> MergeOperation:

        match self.config.destination_table:
            case self.__FEDEX_TRACKING_TABLE:
                return SilverFedexTracking(self.config, self.watermark, self.spark)
            case _:
                raise NotSupportedError(
                    f"The specified FedEx table {self.config.destination_table_qualified()} is not supported. "
                    f"Ensure that you have a corresponding merge operation class defined for this table "
                    f"and that it is properly configured."
                )
