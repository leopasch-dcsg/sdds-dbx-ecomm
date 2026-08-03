from unittest import case

from pyspark.sql import SparkSession

from ecmde_ecomm.common.dbx.env import DbxDataQuality
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig, MergeOperation
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.errors import NotSupportedError
from ecmde_ecomm.ecomp.co_stage.gold.order_cancel_merge_operations import (
    GoldOrderMessageCancel,
)

from ecmde_ecomm.ecomp.co_stage.silver.order_cancel_merge_operations import (
    SilverOrderMessageCancel,
    SilverOrderMessageCancelQuarantine,
)
from ecmde_ecomm.ecomp.co_stage.gold.order_placed_merge_operations import (
    GoldOrderMessagePlaced,
)
from ecmde_ecomm.ecomp.co_stage.silver.order_placed_merge_operations import (
    SilverOrderMessagePlaced,
    SilverOrderMessagePlacedQuarantine,
)
from ecmde_ecomm.ecomp.co_stage.gold.order_discount_merge_operations import (
    GoldOrderMessageDiscount,
)
from ecmde_ecomm.ecomp.co_stage.silver.order_discount_merge_operations import (
    SilverOrderMessageDiscount,
)
from ecmde_ecomm.ecomp.co_stage.gold.order_placed_payment_merge_operations import (
    GoldOrderMessagePlacedPayment,
)
from ecmde_ecomm.ecomp.co_stage.silver.order_placed_payment_merge_operations import (
    SilverOrderMessagePlacedPayment,
)
from ecmde_ecomm.ecomp.co_stage.silver.order_fulfill_merge_operations import (
    SilverOrderMessageFulfill,
    SilverOrderMessageFulfillQuarantine,
)
from ecmde_ecomm.ecomp.co_stage.gold.order_fulfill_merge_operations import (
    GoldOrderMessageFulfill,
)


class OrdersMergeOperationProvider:
    __ECOMP_ORDER_MESSAGE_CANCEL = "order_message_cancel"
    __ECOMP_ORDER_MESSAGE_DISCOUNT = "order_message_discount"
    __ECOMP_ORDER_MESSAGE_FULFILL = "order_message_fulfill"
    __ECOMP_ORDER_MESSAGE_PLACED = "order_message_placed"
    __ECOMP_ORDER_MESSAGE_PLACED_PAYMENT = "order_message_placed_payment"

    __SILVER_ORDER_MESSAGE_CANCEL = "co_stage_order_message_cancel"
    __SILVER_ORDER_MESSAGE_CANCEL_QUARANTINE = (
        "co_stage_order_message_cancel_quarantine"
    )
    __SILVER_ORDER_MESSAGE_DISCOUNT = "co_stage_order_message_discount"
    __SILVER_ORDER_MESSAGE_FULFILL = "co_stage_order_message_fulfill"
    __SILVER_ORDER_MESSAGE_FULFILL_QUARANTINE = (
        "co_stage_order_message_fulfill_quarantine"
    )
    __SILVER_ORDER_MESSAGE_PLACED = "co_stage_order_message_placed"
    __SILVER_ORDER_MESSAGE_PLACED_QUARANTINE = (
        "co_stage_order_message_placed_quarantine"
    )
    __SILVER_ORDER_MESSAGE_PLACED_PAYMENT = "co_stage_order_message_placed_payment"

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
            case DbxDataQuality.ECOMP:
                return self.ecomp_operation()
            case _:
                raise NotSupportedError(
                    f"There are no operations for the specified data quality {data_quality} layer."
                )

    def ecomp_operation(self) -> MergeOperation:
        match self.config.destination_table:
            case self.__ECOMP_ORDER_MESSAGE_CANCEL:
                return GoldOrderMessageCancel(self.config, self.watermark, self.spark)
            case self.__ECOMP_ORDER_MESSAGE_PLACED:
                return GoldOrderMessagePlaced(self.config, self.watermark, self.spark)
            case self.__ECOMP_ORDER_MESSAGE_DISCOUNT:
                return GoldOrderMessageDiscount(self.config, self.watermark, self.spark)
            case self.__ECOMP_ORDER_MESSAGE_PLACED_PAYMENT:
                return GoldOrderMessagePlacedPayment(
                    self.config, self.watermark, self.spark
                )
            case self.__ECOMP_ORDER_MESSAGE_FULFILL:
                return GoldOrderMessageFulfill(self.config, self.watermark, self.spark)
            case _:
                raise NotSupportedError(
                    f"The specified ECOMP destination table {self.config.destination_table_qualified()} is not supported. Ensure that you have a corresponding merge operation class defined for this table and that it is properly configured."
                )

    def silver_operation(self) -> MergeOperation:
        match self.config.destination_table:
            case self.__SILVER_ORDER_MESSAGE_CANCEL:
                return SilverOrderMessageCancel(self.config, self.watermark, self.spark)
            case self.__SILVER_ORDER_MESSAGE_CANCEL_QUARANTINE:
                return SilverOrderMessageCancelQuarantine(
                    self.config, self.watermark, self.spark
                )
            case self.__SILVER_ORDER_MESSAGE_PLACED:
                return SilverOrderMessagePlaced(self.config, self.watermark, self.spark)
            case self.__SILVER_ORDER_MESSAGE_PLACED_QUARANTINE:
                return SilverOrderMessagePlacedQuarantine(
                    self.config, self.watermark, self.spark
                )
            case self.__SILVER_ORDER_MESSAGE_DISCOUNT:
                return SilverOrderMessageDiscount(
                    self.config, self.watermark, self.spark
                )
            case self.__SILVER_ORDER_MESSAGE_PLACED_PAYMENT:
                return SilverOrderMessagePlacedPayment(
                    self.config, self.watermark, self.spark
                )
            case self.__SILVER_ORDER_MESSAGE_FULFILL:
                return SilverOrderMessageFulfill(
                    self.config, self.watermark, self.spark
                )
            case self.__SILVER_ORDER_MESSAGE_FULFILL_QUARANTINE:
                return SilverOrderMessageFulfillQuarantine(
                    self.config, self.watermark, self.spark
                )
            case _:
                raise NotSupportedError(
                    f"The specified silver destination table {self.config.destination_table_qualified()} is not supported. Ensure that you have a corresponding merge operation class defined for this table and that it is properly configured."
                )
