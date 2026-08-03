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
from ecmde_ecomm.fulfillment.silver.order_header_merge_operations import (
    SilverOrderHeader,
)

from ecmde_ecomm.fulfillment.silver.order_sku_merge_operations import (
    SilverOrderFulfillment,
)

from ecmde_ecomm.fulfillment.silver.order_delivery_merge_operations import (
    SilverOrderDelivery,
)

from ecmde_ecomm.fulfillment.silver.txn_order_sku_merge_operations import (
    TxnOrderSkuMerge,
)


class OrdersMergeOperationProvider:

    __SILVER_ORDER_FULFILLMENT_ORDER_FULFILL = "stg_oso_order_fulfill"
    __SILVER_ORDER_FULFILLMENT_ORDER_SKU_SHIPMENT = "stg_oso_order_sku_shipment"
    __SILVER_ORDER_FULFILLMENT_ORDER_HEADER = "stg_oso_order_header"
    __SILVER_ORDER_FULFILLMENT_ORDER_SKU = "stg_oso_order_sku"
    __SILVER_ORDER_FULFILLMENT_ORDER_DELIVERY = "stg_oso_order_delivery"
    __SILVER_ORDER_FULFILLMENT_TXN_ORDER_SKU = "stg_oso_txn_order_sku"

    def __init__(
        self,
        oso_catalog: str,
        fit_catalog: str,
        ecomp_catalog: str,
        config: MergeConfig,
        watermark: Watermark,
        spark: SparkSession,
    ):
        self.oso_catalog = oso_catalog
        self.fit_catalog = fit_catalog
        self.ecomp_catalog = ecomp_catalog
        self.config = config
        self.watermark = watermark
        self.spark = spark

    @staticmethod
    def provider(
        oso_catalog: str,
        fit_catalog: str,
        ecomp_catalog: str,
        config: MergeConfig,
        watermark: Watermark,
        spark: SparkSession,
    ):
        return OrdersMergeOperationProvider(
            oso_catalog, fit_catalog, ecomp_catalog, config, watermark, spark
        )

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
                return SilverFulfillmentOrder(
                    self.oso_catalog,
                    self.fit_catalog,
                    self.ecomp_catalog,
                    self.config,
                    self.watermark,
                    self.spark,
                )

            case self.__SILVER_ORDER_FULFILLMENT_ORDER_SKU_SHIPMENT:
                return SilverOrderSkuShipment(
                    self.oso_catalog,
                    self.fit_catalog,
                    self.ecomp_catalog,
                    self.config,
                    self.watermark,
                    self.spark,
                )

            case self.__SILVER_ORDER_FULFILLMENT_ORDER_SKU:
                return SilverOrderFulfillment(
                    self.oso_catalog,
                    self.fit_catalog,
                    self.ecomp_catalog,
                    self.config,
                    self.watermark,
                    self.spark,
                )

            case self.__SILVER_ORDER_FULFILLMENT_ORDER_DELIVERY:
                return SilverOrderDelivery(
                    self.oso_catalog,
                    self.fit_catalog,
                    self.ecomp_catalog,
                    self.config,
                    self.watermark,
                    self.spark,
                )
            case self.__SILVER_ORDER_FULFILLMENT_TXN_ORDER_SKU:
                return TxnOrderSkuMerge(
                    self.oso_catalog,
                    self.fit_catalog,
                    self.ecomp_catalog,
                    self.config,
                    self.watermark,
                    self.spark,
                )

            case self.__SILVER_ORDER_FULFILLMENT_ORDER_HEADER:
                return SilverOrderHeader(
                    self.oso_catalog,
                    self.fit_catalog,
                    self.ecomp_catalog,
                    self.config,
                    self.watermark,
                    self.spark,
                )

            case _:
                raise NotSupportedError(
                    f"The specified silver destination table {self.config.destination_table_qualified()} is not supported. Ensure that you have a corresponding merge operation class defined for this table and that it is properly configured."
                )
