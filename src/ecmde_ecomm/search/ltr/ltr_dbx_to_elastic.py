from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from ecmde_ecomm.common.dbx.elastic.elastic_writer import (
    ElasticBatchInsert,
    ElasticBatchInsertConfig,
)


class ElasticDBXBatchInsert(ElasticBatchInsert):
    def __init__(
        self,
        config: ElasticBatchInsertConfig,
        spark: SparkSession,
        host: str,
        password: str,
    ):
        super().__init__(config, spark, host, password)

    def batch_dataframe(self) -> DataFrame:
        return self.spark.sql(
            f"""
                select 
                    ecode as partnumber,
                    avg_web_price_atc_z_score_shifted as avg_web_price_atc_z_score,
                    positive_profit_rate as positive_profit_rate,
                    ctr_signed_js_divergence_shifted as ctr_signed_js_divergence,
                    atc_rate_signed_js_divergence_shifted as atc_rate_signed_js_divergence,
                    order_rate_signed_js_divergence_shifted as order_rate_signed_js_divergence
                from {self.config.source_table_qualified()};
                """
        )
