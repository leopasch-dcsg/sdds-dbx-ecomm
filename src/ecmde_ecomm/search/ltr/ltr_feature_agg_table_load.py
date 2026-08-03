from datetime import date,datetime
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation



class LTRFeatureAggLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:
        end_date_est = self.spark.sql(
            f"""
                SELECT end_date_est
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates
            """
        ).first()['end_date_est']


        return self.spark.sql(
            f"""
                WITH z_score_mapped_values AS(
                    SELECT
                        ecode, 
                        collect_list(map(search_term, shifted_z_score)) AS zScore_searchTerm
                    FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_atc_zscore
                    WHERE bod_inv_date = '{end_date_est}'
                    GROUP BY ecode
                        ),
                    positive_profit_rate_mapped_values AS(
                    SELECT
                        ecode, 
                        collect_list(map(search_term, time_decay_positive_profit_rate_posterior)) AS positive_profit_rate_map
                    FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_abb_positive_profit_rate
                    WHERE lagged_feature_date_est = '{end_date_est}'
                    GROUP BY ecode
                        ),
                    ctr_divergence_mapped_values AS(
                      SELECT
                        ecode, 
                        collect_list(map(search_term, signed_js_divergence_shifted)) AS ctr_js_divergence_map
                      FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_abb_ctr
                      WHERE lagged_feature_date_est = '{end_date_est}'
                      GROUP BY ecode
                        ),
                    atc_rate_divergence_mapped_values AS(
                      SELECT
                        ecode, 
                        collect_list(map(search_term, signed_js_divergence_shifted)) AS atc_rate_js_divergence_map
                      FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_abb_atc_rate
                      WHERE lagged_feature_date_est = '{end_date_est}'
                      GROUP BY ecode
                        ),
                    order_rate_divergence_mapped_values AS(
                      SELECT
                        ecode, 
                        collect_list(map(search_term, signed_js_divergence_shifted)) AS order_rate_js_divergence_map
                      FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_abb_order_rate
                      WHERE lagged_feature_date_est = '{end_date_est}'
                      GROUP BY ecode
                        ),
                    agg_mapped_values AS(
                     SELECT
                        c.ecode AS ecode,
                        COALESCE(AGGREGATE(
                        z.zScore_searchTerm,
                        CAST(MAP() AS MAP<STRING,DOUBLE>),
                        (curr, new) -> MAP_CONCAT(curr, new)
                        ), CAST(MAP() AS MAP<STRING,DOUBLE>)) AS avg_web_price_atc_z_score_shifted,
                        COALESCE(AGGREGATE(
                        p.positive_profit_rate_map,
                        CAST(MAP() AS MAP<STRING,DOUBLE>),
                        (curr, new) -> MAP_CONCAT(curr, new)
                        ), CAST(MAP() AS MAP<STRING,DOUBLE>)) AS positive_profit_rate,
                        COALESCE(AGGREGATE(
                        c.ctr_js_divergence_map,
                        CAST(MAP() AS MAP<STRING,DOUBLE>),
                        (curr, new) -> MAP_CONCAT(curr, new)
                        ), CAST(MAP() AS MAP<STRING,DOUBLE>)) AS ctr_signed_js_divergence_shifted,
                        COALESCE(AGGREGATE(
                        ar.atc_rate_js_divergence_map,
                        CAST(MAP() AS MAP<STRING,DOUBLE>),
                        (curr, new) -> MAP_CONCAT(curr, new)
                        ), CAST(MAP() AS MAP<STRING,DOUBLE>)) AS atc_rate_signed_js_divergence_shifted,
                        COALESCE(AGGREGATE(
                        o.order_rate_js_divergence_map,
                        CAST(MAP() AS MAP<STRING,DOUBLE>),
                        (curr, new) -> MAP_CONCAT(curr, new)
                        ), CAST(MAP() AS MAP<STRING,DOUBLE>)) AS order_rate_signed_js_divergence_shifted
                    FROM ctr_divergence_mapped_values AS c
                    LEFT JOIN z_score_mapped_values AS z
                    ON c.ecode = z.ecode
                    LEFT JOIN atc_rate_divergence_mapped_values AS ar
                    ON c.ecode = ar.ecode
                    LEFT JOIN order_rate_divergence_mapped_values AS o
                    ON c.ecode = o.ecode
                    LEFT JOIN positive_profit_rate_mapped_values AS p
                    ON c.ecode = p.ecode
                    )
                    SELECT 
                        ecode,
                        avg_web_price_atc_z_score_shifted,
                        positive_profit_rate,
                        ctr_signed_js_divergence_shifted,
                        atc_rate_signed_js_divergence_shifted,
                        order_rate_signed_js_divergence_shifted
                    FROM agg_mapped_values;           
                """
        )