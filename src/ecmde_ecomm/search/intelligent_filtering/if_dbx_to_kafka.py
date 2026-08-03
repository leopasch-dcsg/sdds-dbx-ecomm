from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from ecmde_ecomm.common.dbx.kafka.producer import KafkaProducer, KafkaProducerConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark


class IFKafkaProducer(KafkaProducer):
    def __init__(
        self,
        spark: SparkSession,
        config: KafkaProducerConfig,
        watermark: Watermark,
        source_table_qualified: str,
        end_date: str,
    ):
        super().__init__(spark, config, watermark)
        self.source_table_qualified = source_table_qualified
        self.end_date = end_date


    def source_dataframe(self, last_batch_date_utc: datetime) -> DataFrame:
        return self.spark.sql(
            f"""
                select distinct
                    webstore,
                    event_type,
                    event_value,
                    facet,
                    event_count_last_x_days,
                    p_event_last_x_days,
                    facet_count_last_x_days,
                    p_facet_last_x_days
                from {self.source_table_qualified}
                where date_key = to_date('{self.end_date}');
            """
        )