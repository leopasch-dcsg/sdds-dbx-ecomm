from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation


class LTRATCSearchLvlAggLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:
        return self.spark.sql(
            f"""
                /* Calculate rolling window aggregates */
                SELECT
                    date_est AS lagged_feature_date_est,
                    search_term,
                    AVG(web_price) AS avg_atc_web_price,
                    STDDEV_SAMP(web_price) AS std_samp_atc_web_price,
                    COUNT(DISTINCT hitid) AS atc_event_count,
                    COUNT(DISTINCT tran_date) AS atc_day_count
                FROM {self.config.source_table_qualified()}
                GROUP BY date_est, search_term
                HAVING atc_event_count > 1  -- we require two events to establish a variance, we drop these records and rely on a global estimate instead
                """
        )