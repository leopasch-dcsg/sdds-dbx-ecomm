from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation



class LTRABBCTRLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:

        self.spark.sql("CLEAR CACHE")
        self.spark.sql("SET spark.sql.shuffle.partitions=auto")

        start_date_est = self.spark.sql(
            f"""
                SELECT start_date_est
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates
            """
        ).first()['start_date_est']

        end_date_est = self.spark.sql(
            f"""
                        SELECT end_date_est
                        FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates
                    """
        ).first()['end_date_est']

        rolling_window_minus_one = self.spark.sql(
            f"""
                        SELECT rolling_window_minus_one
                        FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates
                    """
        ).first()['rolling_window_minus_one']

        self.create_ltr_stg_init_clicks(start_date_est, end_date_est, rolling_window_minus_one)
        self.create_ltr_stg_linked_clicks()
        self.create_ltr_stg_click_counts()
        self.create_ltr_stg_joined_impressions()
        self.create_ltr_stg_search_ecodes()
        self.create_ltr_stg_search_ecodes_with_all_dates()
        self.create_ltr_stg_agg_impressions(rolling_window_minus_one)
        self.create_ltr_stg_global_priors(rolling_window_minus_one)
        self.create_ltr_stg_query_priors(rolling_window_minus_one)
        self.create_ltr_stg_init_priors()
        self.create_ltr_stg_alphas_and_betas()
        self.create_ltr_stg_posteriors(start_date_est)

        return self.spark.sql(f"""                                                                                               
                        SELECT 
                          date_est AS lagged_feature_date_est,
                          search_term AS search_term,
                          ecode AS ecode,
                          time_decay_total_clicks_last_x_days as total_clicks_last_x_days,
                          time_decay_total_impressions_last_x_days as total_impressions_last_x_days,
                          time_decay_prior_ctr as prior_ctr,
                          time_decay_prior_alpha as prior_alpha,
                          time_decay_prior_beta as prior_beta,
                          time_decay_alpha_n as alpha_n,
                          time_decay_beta_n as beta_n,
                          time_decay_ctr_posterior as ctr_posterior,
                          time_decay_prior_ctr_std as prior_ctr_std,
                          CASE WHEN time_decay_prior_ctr_std != 0 THEN (time_decay_ctr_posterior - time_decay_prior_ctr) / time_decay_prior_ctr_std ELSE 0 END AS ctr_posterior_z_score
                        FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_posteriors
                        WHERE total_impressions_last_x_days >= {self.config.min_total_impressions}                                                                                                    
                        """)



    def create_ltr_stg_init_clicks(self, start_date_est, end_date_est, rolling_window_minus_one) -> None:
        df = self.spark.sql(f"""      
                            SELECT
                                tran_date,
                                mcvisid,
                                _visit_id,
                                hitid,
                                search_term,
                                dym_search_term,
                                ecode,
                                clk_time,
                                _evar58,
                                LEAD(clk_time) OVER (PARTITION BY tran_date, _visit_id ORDER BY clk_time) AS next_clk_time,
                                LAG(clk_time) OVER (PARTITION BY tran_date, _visit_id ORDER BY clk_time) AS prev_clk_time
                            FROM (
                                SELECT             
                                  tran_date,
                                  mcvisid,
                                  _visit_id,
                                  hitid,
                                  CAST(post_cust_hit_time_gmt AS BIGINT) * 1000 AS clk_time, -- changing from seconds to milliseconds to match ML events
                                  ecode,
                                  TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(_evar2), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS search_term,
                                  TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(SPLIT_PART(_evar63, "|", 1)), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS dym_search_term,
                                  _evar58
                                FROM entdata.clk.dks_web_only
                                WHERE tran_date BETWEEN DATE_SUB('{start_date_est}', {rolling_window_minus_one}) AND '{end_date_est}'      
                                  AND CAST(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(visit_start_time_gmt AS BIGINT)), 'America/New_York') AS DATE) >= DATE_SUB('{start_date_est}', {rolling_window_minus_one})
                                  AND ecode IS NOT NULL
                                  AND _evar2 IS NOT NULL          
                                  AND LENGTH(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(_evar2), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " "))) BETWEEN 2 AND 40
                                  AND _prop2 = 'Product Detail'
                                  AND _evar58 IN ('Internal Search', 'Search Page Refinement', 'Quick View - Search - SRLP', 'Quick View - Search - srlp')
                                  AND _evar27 LIKE '%: Shopping: Search: Results'
                                  AND post_page_event = '0'
                                  AND _report_suite = 'dsg'
                                  AND (_prop33 LIKE 'NB-%'
                                    OR _prop33 LIKE 'CB-%'
                                    OR _prop33 IS NULL)
                            )
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_clicks"
            )
        )

    def create_ltr_stg_linked_clicks(self) -> None:
        df = self.spark.sql(f"""
                            SELECT
                                tran_date,
                                mcvisid,
                                _visit_id,
                                hitid,
                                search_term,
                                ecode,
                                MAX(search_time) AS search_time,
                                /* map the click to the most recent impression. Assume search time and impression time are approximately equal */
                                MAX_BY(((1 + EXP(-0.18 * 30)) / (1 + EXP(0.18 * (signal_age-30)))), search_time) AS time_decay_click,
                                /* keep the time and age of the mapped impression */
                                MAX_BY(signal_age, search_time) AS signal_age
                            FROM (
                                SELECT
                                  c.tran_date,
                                  c.mcvisid,
                                  c._visit_id,
                                  c.hitid,
                                  c.prev_clk_time,
                                  c.clk_time,
                                  DATE_DIFF(DAY, TIMESTAMP_MILLIS(ls.time), CURRENT_TIMESTAMP()) AS signal_age,
                                  c.next_clk_time,
                                  c.ecode,
                                  COALESCE(c.dym_search_term, c.search_term) AS search_term,
                                  ls.id,
                                  ls.time AS search_time,
                                  ls.impression_items
                                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_clicks c
                                JOIN {self.config.source_table_qualified_2()} ls
                                ON 
                                    c.mcvisid = ls.mc_visitor_id
                                    AND c._visit_id = ls.visit_id
                                AND ls.search_term = COALESCE(c.dym_search_term, c.search_term)
                                WHERE
                                  ls.time <= c.clk_time
                                  AND ( 
                                    -- the first click doesn't have a previous click (if it came from an SRLP page)
                                    (_evar58 NOT IN ('Quick View - Search - SRLP', 'Quick View - Search - srlp')
                                      AND ls.time >= IFNULL(c.prev_clk_time, ls.start_time))
                                    -- OR the click came from a quick view so it's ok if there's a previous click 
                                    OR _evar58 IN ('Quick View - Search - SRLP', 'Quick View - Search - srlp')
                                  )
                                  AND ARRAY_CONTAINS(ls.impression_items, c.ecode)
                                  )
                            GROUP BY 
                                tran_date, 
                                mcvisid, 
                                _visit_id, 
                                hitid, 
                                search_term, 
                                ecode
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_linked_clicks"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_linked_clicks")

    def create_ltr_stg_click_counts(self) -> None:
        df = self.spark.sql(f"""      
                          SELECT
                            tran_date,
                            search_term,
                            ecode,
                            IFNULL(COUNT(1), 0) AS daily_clicks,
                            IFNULL(SUM(time_decay_click), 0) AS time_decay_clicks,
                            ARRAY_AGG(search_time) AS click_search_times,
                            ARRAY_AGG(signal_age) AS click_signal_age
                          FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_linked_clicks
                          GROUP BY 
                            tran_date, 
                            search_term, 
                            ecode
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_click_counts"
            )
        )

    def create_ltr_stg_joined_impressions(self) -> None:
        df = self.spark.sql(f"""           
                          SELECT
                            i.tran_date,
                            i.search_term,
                            i.ecode,
                            i.impression_count,
                            i.time_decay_impression_count,
                            LEAST(IFNULL(c.daily_clicks, 0), i.impression_count) AS click_count, -- limiting max number of clicks to number of impressions
                            LEAST(IFNULL(c.time_decay_clicks, 0), i.time_decay_impression_count) AS time_decay_click_count -- limiting max number of clicks to number of impressions with time decay counts
                          FROM {self.config.source_table_qualified()} i
                          CROSS JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates d
                          LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_click_counts c
                            ON c.tran_date = i.tran_date 
                            AND c.search_term = i.search_term
                            AND c.ecode = i.ecode
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_joined_impressions"
            )
        )

    def create_ltr_stg_search_ecodes(self) -> None:
        df = self.spark.sql(f"""    
                          SELECT DISTINCT search_term, ecode
                          FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_joined_impressions
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_search_ecodes"
            )
        )

    def create_ltr_stg_search_ecodes_with_all_dates(self) -> None:
        df = self.spark.sql(f"""  
                          SELECT
                            ad.date_est,
                            se.search_term,
                            se.ecode,
                            COALESCE(ji.impression_count, 0) AS impression_count,
                            IFNULL(ji.time_decay_impression_count, 0) AS time_decay_impression_count,
                            IFNULL(ji.click_count, 0) AS click_count,
                            IFNULL(ji.time_decay_click_count, 0) AS time_decay_click_count
                          FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_all_dates ad
                          CROSS JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_search_ecodes se
                          LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_joined_impressions ji
                            ON ad.date_est  = ji.tran_date
                            AND se.search_term = ji.search_term
                            AND se.ecode = ji.ecode
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_search_ecodes_with_all_dates"
            )
        )

    def create_ltr_stg_agg_impressions(self, rolling_window_minus_one) -> None:
        df = self.spark.sql(f"""  
                          SELECT
                            date_est,
                            search_term,
                            ecode,
                            time_decay_click_count,
                            time_decay_impression_count,
                            SUM(time_decay_click_count) OVER (
                              PARTITION BY search_term, ecode
                              ORDER BY date_est
                              ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                            ) AS time_decay_total_clicks_last_x_days,
                            SUM(time_decay_impression_count) OVER (
                              PARTITION BY search_term, ecode
                              ORDER BY date_est
                              ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                            ) AS time_decay_total_impressions_last_x_days,
                            SUM(impression_count) OVER (
                                PARTITION BY search_term, ecode
                                ORDER BY date_est
                                ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                              ) AS total_impressions_last_x_days,
                            SUM(click_count) OVER (
                                PARTITION BY search_term, ecode
                                ORDER BY date_est
                                ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                ) AS total_clicks_last_x_days
                          FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_search_ecodes_with_all_dates
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_agg_impressions"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_agg_impressions")

    def create_ltr_stg_global_priors(self, rolling_window_minus_one) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            *,
                            time_decay_total_clicks_last_x_days_global / time_decay_total_impressions_last_x_days_global AS time_decay_ctr_last_x_days_global
                          FROM (
                                SELECT
                                  date_est,
                                  SUM(time_decay_total_daily_clicks) OVER (
                                    ORDER BY date_est 
                                    ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                    ) AS time_decay_total_clicks_last_x_days_global,
                                  SUM(time_decay_total_daily_impressions) OVER (
                                    ORDER BY date_est 
                                    ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                    ) AS time_decay_total_impressions_last_x_days_global
                                FROM (
                                  SELECT
                                      date_est,
                                      SUM(time_decay_click_count) AS time_decay_total_daily_clicks,
                                      SUM(time_decay_impression_count) AS time_decay_total_daily_impressions
                                  FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_agg_impressions
                                  GROUP BY date_est
                              )
                          ) 
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_global_priors"
            )
        )

    def create_ltr_stg_query_priors(self, rolling_window_minus_one) -> None:
        df = self.spark.sql(f"""
                          SELECT 
                            date_est,
                            search_term,
                            time_decay_total_clicks_last_x_days_query,
                            time_decay_total_impressions_last_x_days_query,
                            TRY_DIVIDE(time_decay_total_clicks_last_x_days_query, time_decay_total_impressions_last_x_days_query) AS time_decay_ctr_last_x_days_query
                          FROM (
                            SELECT
                                date_est,
                                search_term,
                                SUM(time_decay_total_daily_clicks) OVER (
                                    PARTITION BY search_term 
                                    ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                    ) AS time_decay_total_clicks_last_x_days_query,
                                SUM(time_decay_total_daily_impressions) OVER (
                                    PARTITION BY search_term 
                                    ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                    ) AS time_decay_total_impressions_last_x_days_query 
                            FROM (
                              SELECT
                                date_est,
                                search_term,
                                SUM(time_decay_click_count) AS time_decay_total_daily_clicks,
                                SUM(time_decay_impression_count) AS time_decay_total_daily_impressions
                              FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_agg_impressions
                              GROUP BY date_est, search_term
                            )
                          )
                          WHERE time_decay_total_impressions_last_x_days_query > 0                                                                                                    
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_query_priors"
            )
        )

    def create_ltr_stg_init_priors(self) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            ai.date_est,
                            ai.search_term,
                            ai.ecode,
                            ai.time_decay_total_clicks_last_x_days,
                            ai.time_decay_total_impressions_last_x_days,
                            ai.total_impressions_last_x_days,
                            (IFNULL(qp.time_decay_total_impressions_last_x_days_query * qp.time_decay_ctr_last_x_days_query, 0) + {self.config.global_avg_prior_weight} * gp.time_decay_ctr_last_x_days_global) / (IFNULL(qp.time_decay_total_impressions_last_x_days_query, 0) + {self.config.global_avg_prior_weight}) AS time_decay_prior_ctr
                          FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_agg_impressions ai
                          LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_global_priors gp
                            ON gp.date_est = ai.date_est
                          LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_query_priors qp
                            ON qp.date_est = ai.date_est
                            AND qp.search_term = ai.search_term
                          WHERE
                            ai.total_clicks_last_x_days > 0                                                                                                   
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_priors"
            )
        )

    def create_ltr_stg_alphas_and_betas(self) -> None:
        df = self.spark.sql(f"""  
                          SELECT
                            *,
                            time_decay_prior_ctr * {self.config.weight} AS time_decay_prior_alpha,
                            (1 - time_decay_prior_ctr) * {self.config.weight} AS time_decay_prior_beta,
                            time_decay_prior_ctr * {self.config.weight} + time_decay_total_clicks_last_x_days AS time_decay_alpha_n,
                            (1 - time_decay_prior_ctr) * {self.config.weight} + GREATEST(time_decay_total_impressions_last_x_days - time_decay_total_clicks_last_x_days, 0) AS time_decay_beta_n
                          FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_priors
                         """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_alphas_and_betas"
            )
        )

    def create_ltr_stg_posteriors(self, start_date_est) -> None:
        df = self.spark.sql(f"""   
                          SELECT
                            a.*,
                            time_decay_alpha_n / (time_decay_alpha_n + time_decay_beta_n) AS time_decay_ctr_posterior,
                            SQRT(time_decay_prior_ctr * (1 - time_decay_prior_ctr) / 2) AS time_decay_prior_ctr_std
                          FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_alphas_and_betas a
                          WHERE a.date_est  >= '{start_date_est}'
                         """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_posteriors"
            )
        )