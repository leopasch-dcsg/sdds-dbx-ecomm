from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation



class LTRATCGlobalAggLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:
        return self.spark.sql(
            f"""
                SELECT
                    date_est AS lagged_feature_date_est,
                    AVG(web_price) AS global_avg_atc_web_price,
                    STDDEV_SAMP(web_price) AS global_std_samp_atc_web_price,
                    COUNT(DISTINCT hitid) AS global_atc_event_count,
                    COUNT(DISTINCT tran_date) AS global_atc_day_count
                FROM {self.config.source_table_qualified()}
                GROUP BY date_est            
                """
        )