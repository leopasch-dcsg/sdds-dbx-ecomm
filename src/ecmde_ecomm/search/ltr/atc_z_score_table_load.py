from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation


class LTRATCZScoreLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:
        return self.spark.sql(
            f"""
                    WITH 
                        agg_combo AS (
                              SELECT 
                                  ga.lagged_feature_date_est,
                                  b.tran_date,
                                  sla.search_term,
                                  sla.avg_atc_web_price,
                                  sla.atc_event_count,
                                  sla.std_samp_atc_web_price, 
                                  ga.global_avg_atc_web_price, 
                                  ga.global_std_samp_atc_web_price
                                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_atc_search_lvl_agg sla 
                                JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_atc_global_agg ga 
                                ON sla.lagged_feature_date_est = ga.lagged_feature_date_est
                                JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_atc_base b 
                                ON sla.search_term = b.search_term AND sla.lagged_feature_date_est = b.date_est
                                QUALIFY ROW_NUMBER() OVER(PARTITION BY sla.search_term, ga.lagged_feature_date_est ORDER BY b.tran_date desc) = 1
                                -- Partition used to get the most recent tran date for each search term
                                ),
                        bod_combo AS (
                                SELECT 
                                  b.bod_inv_date,
                                  b.ecode,
                                  b.avg_web_price
                                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_atc_bod_price_data b
                                  -- narrow down days to most recent day of data
                        ),
                        agg_combo_2 AS (
                                SELECT 
                                  ac.lagged_feature_date_est,
                                  ac.search_term,
                                  td.ecode,
                                  ac.avg_atc_web_price,
                                  ac.atc_event_count,
                                  ac.std_samp_atc_web_price,
                                  ac.global_avg_atc_web_price,
                                  ac.global_std_samp_atc_web_price
                                FROM agg_combo ac
                                JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_atc_term_dates_intermediary td 
                                ON ac.lagged_feature_date_est = td.date_est AND ac.search_term = td.search_term
                        ),
                        z_score_data AS (
                                SELECT 
                                  bc.bod_inv_date,
                                  ac.search_term,
                                  bc.ecode,
                                  ac.avg_atc_web_price,
                                  ac.atc_event_count,
                                  ac.global_avg_atc_web_price,
                                  ac.std_samp_atc_web_price,
                                  ac.global_std_samp_atc_web_price,
                                  bc.avg_web_price
                                FROM agg_combo_2 ac
                                JOIN bod_combo bc 
                                ON ac.ecode = bc.ecode AND ac.lagged_feature_date_est = bc.bod_inv_date
                        ),
                        intermediate_table as
                        (SELECT 
                          bod_inv_date,
                          search_term,
                          ecode,
                          avg_atc_web_price,
                          atc_event_count,
                          global_avg_atc_web_price,
                          std_samp_atc_web_price,
                          global_std_samp_atc_web_price,
                          avg_web_price,
                          (((avg_atc_web_price * atc_event_count) + global_avg_atc_web_price) / (atc_event_count + 1)) AS mu,
                          (((std_samp_atc_web_price * atc_event_count) + global_std_samp_atc_web_price) / (atc_event_count + 1)) AS sigma,
                          ((avg_web_price - mu) / sigma) AS z_score
                        FROM z_score_data)   
                        SELECT
                        *,
                        (CASE WHEN z_score < -100 THEN -100
                            WHEN z_score > 100 THEN 100
                            ELSE z_score END) + 100 as shifted_z_score
                        FROM intermediate_table
                               
                """
        )
