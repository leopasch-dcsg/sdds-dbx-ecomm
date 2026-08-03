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
        end_date: str,
    ):
        super().__init__(config, spark, host, password)
        self.end_date = end_date

    def batch_dataframe(self) -> DataFrame:
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
                from {self.config.source_table_qualified()}
                where date_key = to_date('{self.end_date}');
                """
        )