from datetime import datetime, timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from ecmde_ecomm.common.errors import NotSupportedError
from ecmde_ecomm.common.dbx.etl.watermark import Watermark


class OSOStgDbxEgress:
    __ORDER_FULFILL = "stg_oso_order_fulfill"
    __ORDER_SKU_SHIPMENT = "stg_oso_order_sku_shipment"
    __ORDER_HEADER = "stg_oso_order_header"
    __ORDER_DELIVER = "stg_oso_order_delivery"
    __ORDER_SKU = "stg_oso_order_sku"
    __TXN_ORDER_SKU = "stg_oso_txn_order_sku"

    __HEADER_LOOKBACK_DAYS = 20
    __TXN_SKU_LOOKBACK_DAYS = 30
    __SKU_LOOKBACK_DAYS = 2
    __SKU_SHIPMENT_LOOKBACK_DAYS = 10
    __FULFILL_LOOKBACK_DAYS = 5

    def __init__(
        self, src_table: str, dest_table: str, watermark: Watermark, spark: SparkSession
    ):

        self.src = src_table
        self.dest = dest_table
        self.watermark = watermark
        self.spark = spark

    def run(self) -> int:
        last_wm = self.watermark.last_watermark_utc()
        table_key = self.dest.split(".")[-1].lower()

        match table_key:
            case self.__ORDER_SKU_SHIPMENT:
                lower_bound = last_wm - timedelta(
                    days=self.__SKU_SHIPMENT_LOOKBACK_DAYS
                )
            case self.__ORDER_SKU:
                lower_bound = last_wm - timedelta(days=self.__SKU_LOOKBACK_DAYS)
            case self.__ORDER_HEADER:
                lower_bound = last_wm - timedelta(days=self.__HEADER_LOOKBACK_DAYS)
            case self.__TXN_ORDER_SKU:
                lower_bound = last_wm - timedelta(days=self.__TXN_SKU_LOOKBACK_DAYS)
            case self.__ORDER_FULFILL:
                lower_bound = last_wm - timedelta(days=self.__FULFILL_LOOKBACK_DAYS)                
            case _:
                lower_bound = last_wm

        df: DataFrame = self.spark.table(self.src).filter(
            (col("silver_created_on_utc") >= lit(lower_bound))
            | (col("silver_updated_on_utc") >= lit(lower_bound))
        )

        df = df.drop(
            "silver_created_on_utc",
            "silver_created_by",
            "silver_updated_on_utc",
            "silver_updated_by",
        )

        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "false"
        ).saveAsTable(self.dest)

        return df.count()
