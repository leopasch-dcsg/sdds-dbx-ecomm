from unittest import case

from pyspark.sql import SparkSession

from ecmde_ecomm.common.dbx.env import DbxDataQuality
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig, MergeOperation
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.errors import NotSupportedError


from ecmde_ecomm.fulfillment.silver.order_fulfill_merge_operations import (
    SilverFulfillmentOrder,
)
from ecmde_ecomm.fulfillment.silver.order_sku_shipment_merge_operations import (
    SilverOrderSkuShipment,
)


class OrdersMergeOperationProvider:

    __SILVER_ORDER_FULFILLMENT_ORDER_FULFILL = "stg_oso_order_fulfill"
    __SILVER_ORDER_FULFILLMENT_ORDER_SKU_SHIPMENT = "stg_oso_order_sku_shipment"

    def __init__(self, config: MergeConfig, watermark: Watermark, spark: SparkSession):
        self.config = config
        self.watermark = watermark
        self.spark = spark

    @staticmethod
    def provider(config: MergeConfig, watermark: Watermark, spark: SparkSession):
        return OrdersMergeOperationProvider(config, watermark, spark)

    def operation(self, data_quality: DbxDataQuality) -> MergeOperation:
        match data_quality:
            case DbxDataQuality.SILVER:
                return self.silver_operation()

            case _:
                raise NotSupportedError(
                    f"There are no operations for the specified data quality {data_quality} layer."
                )

    def silver_operation(self) -> MergeOperation:
        match self.config.destination_table:

            case self.__SILVER_ORDER_FULFILLMENT_ORDER_FULFILL:
                return SilverFulfillmentOrder(self.config, self.watermark, self.spark)
            case self.__SILVER_ORDER_FULFILLMENT_ORDER_SKU_SHIPMENT:
                return SilverOrderSkuShipment(self.config, self.watermark, self.spark)

            case _:
                raise NotSupportedError(
                    f"The specified silver destination table {self.config.destination_table_qualified()} is not supported. Ensure that you have a corresponding merge operation class defined for this table and that it is properly configured."
                )
