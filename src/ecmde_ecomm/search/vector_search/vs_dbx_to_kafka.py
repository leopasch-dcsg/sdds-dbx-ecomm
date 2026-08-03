from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.common.dbx.kafka.producer import KafkaProducer, KafkaProducerConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark


class VSKafkaProducer(KafkaProducer):
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
                    ecode,
                    llm_description_few_shot as description
                from {self.source_table_qualified}
            """
        )
