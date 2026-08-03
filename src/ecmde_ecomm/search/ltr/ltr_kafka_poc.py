from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from ecmde_ecomm.common.dbx.kafka.producer import KafkaProducer, KafkaProducerConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark


class LtrKafkaProducer(KafkaProducer):
    def __init__(
        self,
        spark: SparkSession,
        config: KafkaProducerConfig,
        watermark: Watermark,
        source_table_qualified: str,
    ):
        super().__init__(spark, config, watermark)
        self.source_table_qualified = source_table_qualified

    def source_dataframe(self, last_batch_date_utc: datetime) -> DataFrame:
        return self.spark.sql(
            f"""
                select sku,
                       location_id,
                       atp_qty,
                       isa_qty,
                       bopl_qty,
                       (to_unix_timestamp(inventory_change_event_utc) * 1000) as inventory_change_event_utc
                  from {self.source_table_qualified} inv
                 where inv.location_id = 0
                   and (
                        inv.silver_created_on_utc >= '{last_batch_date_utc}'
                     or inv.silver_updated_on_utc >= '{last_batch_date_utc}'
                   )
            """
        )
