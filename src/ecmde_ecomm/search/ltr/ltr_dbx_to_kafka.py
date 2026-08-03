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
                select 
                    ecode as partnumber,
                    avg_web_price_atc_z_score_shifted as avg_web_price_atc_z_score,
                    positive_profit_rate as positive_profit_rate,
                    ctr_signed_js_divergence_shifted as ctr_signed_js_divergence,
                    atc_rate_signed_js_divergence_shifted as atc_rate_signed_js_divergence,
                    order_rate_signed_js_divergence_shifted as order_rate_signed_js_divergence
                from {self.source_table_qualified}
            """
        )
