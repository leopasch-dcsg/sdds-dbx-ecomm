from pyspark.sql import DataFrame
from pyspark.sql.functions import length, lag, col
from pyspark.sql.window import Window
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation


class LTRABBOrderRateLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:

        self.spark.sql("CLEAR CACHE")
        self.spark.sql("SET spark.sql.shuffle.partitions=auto")

        start_date_est = self.spark.sql(
            f"""
                SELECT start_date_est
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates
            """
        ).first()["start_date_est"]

        end_date_est = self.spark.sql(
            f"""
                        SELECT end_date_est
                        FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates
                    """
        ).first()["end_date_est"]

        rolling_window_minus_one = self.spark.sql(
            f"""
                        SELECT rolling_window_minus_one
                        FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates
                    """
        ).first()["rolling_window_minus_one"]

        self.create_ltr_stg_init_order(
            start_date_est, end_date_est, rolling_window_minus_one
        )
        self.create_ltr_stg_seq_orders()
        self.create_ltr_stg_joined_seq_orders()
        self.create_ltr_stg_linked_orders()
        self.create_ltr_stg_deduplicate_linked_orders()
        self.create_ltr_stg_order_aggs_level_1()
        self.create_ltr_stg_order_aggs_level_2()
        self.create_ltr_stg_order_rate_joined_impressions()
        self.create_ltr_stg_order_rate_search_ecodes()
        self.create_ltr_stg_order_rate_search_ecodes_with_all_dates()
        self.create_ltr_stg_order_rate_agg_impressions(rolling_window_minus_one)
        self.create_ltr_stg_order_rate_global_priors(rolling_window_minus_one)
        self.create_ltr_stg_order_rate_query_priors(rolling_window_minus_one)
        self.create_ltr_stg_order_rate_init_priors()
        self.create_ltr_stg_order_rate_alphas_and_betas()
        self.create_ltr_stg_order_rate_posteriors(start_date_est)

        return self.spark.sql(
            f"""
            SELECT
              date_est AS lagged_feature_date_est,
              search_term AS search_term,
              ecode AS ecode,
              total_revenue,
              total_units,
              time_decay_total_orders_last_x_days as total_orders_last_x_days,
              time_decay_total_impressions_last_x_days as total_impressions_last_x_days,
              total_revenue_last_x_days,
              total_units_last_x_days,
              time_decay_prior_order_rate as prior_order_rate,
              time_decay_prior_alpha as prior_alpha,
              time_decay_prior_beta as prior_beta,
              time_decay_alpha_n as alpha_n,
              time_decay_beta_n as beta_n,
              time_decay_order_rate_posterior as order_rate_posterior,
              time_decay_prior_order_rate_std as prior_order_rate_std,
              CASE WHEN time_decay_prior_order_rate_std != 0 THEN (time_decay_order_rate_posterior - time_decay_prior_order_rate) / time_decay_prior_order_rate_std ELSE 0 END AS order_rate_posterior_z_score
            FROM
              {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_posteriors
            WHERE
              total_impressions_last_x_days >= {self.config.min_total_impressions}
            """
        )

    def create_ltr_stg_init_order(
        self, start_date_est, end_date_est, rolling_window_minus_one
    ) -> None:

        ltr_stg_init_orders = self.spark.sql(
            f"""
                                SELECT
                                  tran_date,
                                  mcvisid,
                                  _visit_id,
                                  _order_id,
                                  _revenue,
                                  _sku,
                                  _units,
                                  ecode,
                                  TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(_evar2), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS search_term,
                                  TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(SPLIT_PART(_evar63, "|", 1)), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS dym_search_term,
                                  CAST(post_cust_hit_time_gmt AS BIGINT) * 1000 AS order_time
                                FROM entdata.clk.dks_web_only
                                WHERE tran_date BETWEEN DATE_SUB('{start_date_est}', {rolling_window_minus_one}) AND '{end_date_est}'
                                  AND DATE(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(CAST(visit_start_time_gmt AS BIGINT)), "America/New_York")) >= DATE_SUB('{start_date_est}', {rolling_window_minus_one})
                                  AND _order_id IS NOT NULL
                                  AND _evar2 IS NOT NULL
                                  AND ecode NOT IN ('Tax', 'Shipping')
                                  AND (_evar58 IN ('Internal Search', 'Search Page Refinement') OR _evar33 IN ('Internal Search', 'Search Page Refinement'))
                                  AND duplicate_purchase = '0'
                                  AND _report_suite = 'dsg'
                                  AND LENGTH(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(_evar2), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " "))) BETWEEN 2 AND 40
            """
        ).select("*")

        (
            ltr_stg_init_orders.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_orders"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_orders")

    def create_ltr_stg_seq_orders(self) -> None:

        df = self.spark.sql(
            f"""
                SELECT
                    *,
                    LAG(order_time) OVER(PARTITION BY tran_date, mcvisid, _visit_id ORDER BY order_time) AS prev_order_time
                FROM (
                    SELECT DISTINCT
                        tran_date,
                        mcvisid,
                        _visit_id,
                        _order_id,
                        order_time
                    FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_orders
                )
                """
        )

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_seq_orders"
            )
        )

    def create_ltr_stg_joined_seq_orders(self) -> None:

        df = self.spark.sql(
            f"""
            SELECT
                    i.tran_date,
                    i.mcvisid,
                    i._visit_id,
                    i._order_id,
                    i.ecode,
                    COALESCE(i.dym_search_term, i.search_term) AS search_term,
                    i._revenue, 
                    i._sku,
                    i._units,
                    i.order_time,
                    s.prev_order_time
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_orders i
                JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_seq_orders s
                  ON i._order_id = s._order_id
            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_joined_seq_orders"
            )
        )

    def create_ltr_stg_linked_orders(self) -> None:

        df = self.spark.sql(
            f"""
                SELECT
                    o.tran_date,
                    o.mcvisid,
                    o._visit_id,
                    o.order_time,
                    o.ecode,
                    o.search_term,
                    o._order_id,
                    o._revenue, 
                    o._sku,
                    o._units,
                    ls.id,
                    ls.time AS search_time,
                    ls.impression_items
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_joined_seq_orders o
                JOIN {self.config.source_table_qualified_2()} ls
                ON
                    o.mcvisid = ls.mc_visitor_id
                    AND o._visit_id = ls.visit_id
                    AND ls.search_term = o.search_term
                WHERE
                    ls.time <= o.order_time
                    AND ls.time >= IFNULL(o.prev_order_time, ls.time)
                    AND ARRAY_CONTAINS(ls.impression_items, o.ecode)
                
            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_linked_orders"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_linked_orders")

    def create_ltr_stg_deduplicate_linked_orders(self) -> None:

        df = self.spark.sql(
            f"""  
                SELECT *
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_linked_orders o
                QUALIFY ROW_NUMBER() OVER(PARTITION BY _order_id, _sku ORDER BY order_time - search_time ) = 1
            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_deduplicate_linked_orders"
            )
        )

    def create_ltr_stg_order_aggs_level_1(self) -> None:

        df = self.spark.sql(
            f"""     
                        SELECT
                            *,
                            ((1 + EXP(-0.18 * 30)) / (1 + EXP(0.18 * (order_age-30)))) AS time_decay_order
                          FROM (
                            SELECT
                                lo.tran_date,
                                lo.search_term,
                                lo.ecode,
                                lo._order_id,
                                DATE_DIFF(DAY, TIMESTAMP_MILLIS(MAX(lo.search_time)), CURRENT_TIMESTAMP()) AS order_age,
                                SUM(lo._revenue) AS total_order_revenue,
                                SUM(lo._units) AS total_order_units
                            FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_deduplicate_linked_orders lo
                            GROUP BY lo.tran_date, lo.search_term, lo.ecode, lo._order_id
                            )
                        """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_aggs_level_1"
            )
        )

    def create_ltr_stg_order_aggs_level_2(self) -> None:

        df = self.spark.sql(
            f""" 
                            SELECT 
                                oal1.tran_date,
                                oal1.search_term,
                                oal1.ecode,
                                COUNT(DISTINCT oal1._order_id) AS order_count,
                                SUM(time_decay_order) AS time_decay_order_count,
                                SUM(oal1.total_order_revenue) AS total_revenue,
                                SUM(oal1.total_order_units) AS total_units
                            FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_aggs_level_1 oal1
                            GROUP BY oal1.tran_date, oal1.search_term, oal1.ecode
                        """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_aggs_level_2"
            )
        )

    def create_ltr_stg_order_rate_joined_impressions(self) -> None:

        df = self.spark.sql(
            f"""    
                                SELECT
                                    i.tran_date,
                                    i.search_term,
                                    i.ecode,
                                    i.impression_count,
                                    i.time_decay_impression_count,
                                    IFNULL(oa.order_count, 0) AS order_count,
                                    IFNULL(oa.time_decay_order_count, 0) AS time_decay_order_count,
                                    IFNULL(oa.total_revenue, 0) AS total_revenue,
                                    IFNULL(oa.total_units, 0) AS total_units
                                FROM {self.config.source_table_qualified()} i    
                                LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_aggs_level_2 oa
                                ON (
                                    i.tran_date = oa.tran_date
                                    AND oa.search_term = i.search_term 
                                    AND oa.ecode = i.ecode
                                )
                            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_joined_impressions"
            )
        )

    def create_ltr_stg_order_rate_search_ecodes(self) -> None:

        df = self.spark.sql(
            f"""   
                              SELECT DISTINCT
                                search_term,
                                ecode
                              FROM
                                {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_joined_impressions
                            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_search_ecodes"
            )
        )

    def create_ltr_stg_order_rate_search_ecodes_with_all_dates(self) -> None:

        df = self.spark.sql(
            f"""  
                    SELECT
                        ad.*,
                        COALESCE(ji.impression_count, 0) AS impression_count,
                        IFNULL(ji.time_decay_impression_count, 0) AS time_decay_impression_count,
                        IFNULL(ji.order_count, 0) AS order_count,
                        IFNULL(ji.time_decay_order_count, 0) AS time_decay_order_count,
                        IFNULL(ji.total_revenue, 0) AS total_revenue,
                        IFNULL(ji.total_units, 0) AS total_units
                    FROM (
                        SELECT
                            ad.date_est,
                            se.search_term,
                            se.ecode
                        FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_all_dates ad
                        CROSS JOIN
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_search_ecodes se
                    ) ad
                    LEFT JOIN
                        {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_joined_impressions ji
                    ON
                        ad.date_est = ji.tran_date
                        AND ad.search_term = ji.search_term
                        AND ad.ecode = ji.ecode
                            
                            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_search_ecodes_with_all_dates"
            )
        )

    def create_ltr_stg_order_rate_agg_impressions(
        self, rolling_window_minus_one: int
    ) -> None:

        df = self.spark.sql(
            f"""  
                  SELECT
                    * EXCEPT (impression_count , order_count),
                    SUM(time_decay_impression_count) OVER (PARTITION BY search_term, ecode ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS time_decay_total_impressions_last_x_days,
                    SUM(impression_count) OVER (PARTITION BY search_term, ecode ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS total_impressions_last_x_days,
                    SUM(time_decay_order_count) OVER (PARTITION BY search_term, ecode ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS time_decay_total_orders_last_x_days,
                    SUM(order_count) OVER (PARTITION BY search_term, ecode ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS total_orders_last_x_days,
                    SUM(total_revenue) OVER (PARTITION BY search_term, ecode ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS total_revenue_last_x_days,
                    SUM(total_units) OVER (PARTITION BY search_term, ecode ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS total_units_last_x_days
                  FROM
                    {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_search_ecodes_with_all_dates
                            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_agg_impressions"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_agg_impressions")

    def create_ltr_stg_order_rate_global_priors(
        self, rolling_window_minus_one: int
    ) -> None:

        df = self.spark.sql(
            f"""   
                      SELECT
                        *,
                        time_decay_total_orders_last_x_days_global / time_decay_total_impressions_last_x_days_global AS time_decay_order_rate_last_x_days_global
                      FROM
                      (
                        SELECT
                          date_est,
                          SUM(time_decay_total_daily_orders) OVER (ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS time_decay_total_orders_last_x_days_global,
                          SUM(time_decay_total_daily_impressions) OVER (ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS time_decay_total_impressions_last_x_days_global
                        FROM
                        (
                          SELECT
                            date_est,
                            SUM(time_decay_impression_count) AS time_decay_total_daily_impressions,
                            SUM(time_decay_order_count) AS time_decay_total_daily_orders
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_agg_impressions
                          GROUP BY
                            date_est
                        )
                      )
                            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_global_priors"
            )
        )

    def create_ltr_stg_order_rate_query_priors(
        self, rolling_window_minus_one: int
    ) -> None:

        df = self.spark.sql(
            f""" 
                  SELECT
                    *,
                    TRY_DIVIDE(time_decay_total_orders_last_x_days_query, time_decay_total_impressions_last_x_days_query) AS time_decay_order_rate_last_x_days_query
                  FROM
                  (
                    SELECT
                      date_est,
                      search_term,
                      SUM(time_decay_total_daily_impressions) OVER (PARTITION BY search_term ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS time_decay_total_impressions_last_x_days_query,
                      SUM(time_decay_total_daily_orders) OVER (PARTITION BY search_term ORDER BY date_est ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW) AS time_decay_total_orders_last_x_days_query
                    FROM
                    (
                      SELECT
                        date_est,
                        search_term,
                        SUM(time_decay_impression_count) AS time_decay_total_daily_impressions,
                        SUM(time_decay_order_count) AS time_decay_total_daily_orders
                      FROM
                        {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_agg_impressions
                      GROUP BY
                        date_est,
                        search_term
                    )
                  )
                  WHERE
                    time_decay_total_impressions_last_x_days_query > 0
                            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_query_priors"
            )
        )

    def create_ltr_stg_order_rate_init_priors(self) -> None:


        df = self.spark.sql(
            f"""   
                              SELECT
                                ai.* EXCEPT ( --impression_count,order_count, 
                                 total_orders_last_x_days, time_decay_impression_count, time_decay_order_count),
                                (IFNULL(qp.time_decay_total_impressions_last_x_days_query * qp.time_decay_order_rate_last_x_days_query, 0) + {self.config.global_avg_prior_weight} * gp.time_decay_order_rate_last_x_days_global) / (IFNULL(qp.time_decay_total_impressions_last_x_days_query, 0) + {self.config.global_avg_prior_weight}) AS time_decay_prior_order_rate
                              FROM
                                {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_agg_impressions ai
                              LEFT JOIN
                                {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_global_priors gp
                              ON
                                gp.date_est = ai.date_est
                              LEFT JOIN
                                {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_query_priors qp
                              ON
                                qp.date_est = ai.date_est
                                AND qp.search_term = ai.search_term
                              WHERE
                                ai.total_orders_last_x_days > 0
                            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_init_priors"
            )
        )

    def create_ltr_stg_order_rate_alphas_and_betas(self) -> None:

        df = self.spark.sql(
            f"""   
                  SELECT
                    *,
                    time_decay_prior_order_rate * {self.config.weight} as time_decay_prior_alpha,
                    (1 - time_decay_prior_order_rate) * {self.config.weight} as time_decay_prior_beta,
                    time_decay_prior_order_rate * {self.config.weight} + time_decay_total_orders_last_x_days AS time_decay_alpha_n,
                    (1 - time_decay_prior_order_rate) * {self.config.weight} + GREATEST(time_decay_total_impressions_last_x_days - time_decay_total_orders_last_x_days,0) AS time_decay_beta_n
                  FROM
                    {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_init_priors
                            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_alphas_and_betas"
            )
        )

    def create_ltr_stg_order_rate_posteriors(self, start_date_est: str) -> None:

        df = self.spark.sql(
            f"""    
                  SELECT
                    a.*,
                    time_decay_alpha_n / (time_decay_alpha_n + time_decay_beta_n) AS time_decay_order_rate_posterior,
                    SQRT(time_decay_prior_order_rate * (1 - time_decay_prior_order_rate) / 2) AS time_decay_prior_order_rate_std
                  FROM
                    {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_alphas_and_betas a
                  WHERE
                    date_est >= '{start_date_est}'
            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_posteriors"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_rate_posteriors")