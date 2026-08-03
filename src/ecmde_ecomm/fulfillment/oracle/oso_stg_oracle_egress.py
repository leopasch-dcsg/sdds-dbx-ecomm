from datetime import timedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when, lower
from ecmde_ecomm.common.oracle.oracle_egress import OracleEgress, OracleConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark


class OSOStgOracleEgress(OracleEgress):
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
    __DELIVER_LOOKBACK_DAYS = 2

    def __init__(self, config: OracleConfig, watermark: Watermark, spark: SparkSession):
        super().__init__(config, spark)
        self.watermark = watermark

    def map_order_fulfill_ship_method(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "ship_method",
            when(
                lower("ship_method").isin("delivery_assembly", "delivery assembly"),
                "DLVRYASSEM",
            )
            .when(
                lower("ship_method").isin("room_of_choice", "room of choice"),
                "ROOMCHOICE",
            )
            .otherwise(col("ship_method")),
        )

    def source_dataframe(self) -> DataFrame:
        last_wm = self.watermark.last_watermark_utc()
        dest_table = self.config.destination_table.split(".")[-1].lower()

        lower_bound = {
            self.__ORDER_SKU_SHIPMENT: last_wm
            - timedelta(days=self.__SKU_SHIPMENT_LOOKBACK_DAYS),
            self.__ORDER_SKU: last_wm - timedelta(days=self.__SKU_LOOKBACK_DAYS),
            self.__ORDER_HEADER: last_wm - timedelta(days=self.__HEADER_LOOKBACK_DAYS),
            self.__TXN_ORDER_SKU: last_wm
            - timedelta(days=self.__TXN_SKU_LOOKBACK_DAYS),
            self.__ORDER_FULFILL: last_wm
            - timedelta(days=self.__FULFILL_LOOKBACK_DAYS),
            self.__ORDER_DELIVER: last_wm
            - timedelta(days=self.__DELIVER_LOOKBACK_DAYS),
        }.get(dest_table, last_wm)

        df = (
            self.spark.table(self.config.source_table)
            .filter(
                (col("silver_created_on_utc") >= lit(lower_bound))
                | (col("silver_updated_on_utc") >= lit(lower_bound))
            )
            .drop(
                "silver_created_on_utc",
                "silver_created_by",
                "silver_updated_on_utc",
                "silver_updated_by",
            )
        )

        if dest_table == self.__ORDER_FULFILL:
            df = self.map_order_fulfill_ship_method(df)

        return df
