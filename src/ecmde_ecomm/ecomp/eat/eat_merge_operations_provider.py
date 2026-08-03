from unittest import case

from pyspark.sql import SparkSession
from ecmde_ecomm.common.errors import NotSupportedError
from ecmde_ecomm.common.dbx.env import DbxDataQuality
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig, MergeOperation
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.ecomp.eat.silver.eat_availability_merge_operations import (
    SilverEatAvailability,
    EcompEatChainAtpMerge,
)


class EatMergeOperationsProvider:
    __SILVER_SKU_INVENTORY = "sku_inventory"

    __ECOMP_STG_EAT_CHAIN_ATP = "stg_kafka_eat_chain_atp"

    def __init__(self, config: MergeConfig, watermark: Watermark, spark: SparkSession):
        self.config = config
        self.watermark = watermark
        self.spark = spark

    @staticmethod
    def provider(config: MergeConfig, watermark: Watermark, spark: SparkSession):
        return EatMergeOperationsProvider(config, watermark, spark)

    def operation(self, data_quality: DbxDataQuality) -> MergeOperation:
        match data_quality:
            case DbxDataQuality.SILVER:
                return self.silver_operations()
            case DbxDataQuality.ECOMP:
                return self.ecomp_operations()
            case _:
                raise NotSupportedError(
                    f"There are no operations for the specified data quality {data_quality} layer."
                )

    def silver_operations(self) -> MergeOperation:
        match self.config.destination_table.lower():
            case self.__SILVER_SKU_INVENTORY:
                return SilverEatAvailability(self.config, self.watermark, self.spark)
            case _:
                raise NotSupportedError(
                    f"The specified destination table {self.config.destination_table_qualified()} is not supported. Ensure that you have a corresponding merge operation class defined for this table and that it is properly configured."
                )

    def ecomp_operations(self) -> MergeOperation:
        match self.config.destination_table.lower():
            case self.__ECOMP_STG_EAT_CHAIN_ATP:
                return EcompEatChainAtpMerge(self.config, self.watermark, self.spark)
            case _:
                raise NotSupportedError(
                    f"The specified destination table {self.config.destination_table_qualified()} is not supported. Ensure that you have a corresponding merge operation class defined for this table and that it is properly configured."
                )
