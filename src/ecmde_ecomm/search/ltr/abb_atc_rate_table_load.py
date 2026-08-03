from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation



class LTRABBATCRateLoad(LoadOperation):
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

        self.create_ltr_stg_init_atc(start_date_est, end_date_est, rolling_window_minus_one)
        self.create_ltr_stg_init_atc_agg()
        self.create_ltr_stg_linked_atc()
        self.create_ltr_stg_agg_atc()
        self.create_ltr_stg_atc_joined_impressions()
        self.create_ltr_stg_atc_search_ecodes()
        self.create_ltr_stg_atc_search_ecodes_with_all_dates()
        self.create_ltr_stg_atc_agg_impressions(rolling_window_minus_one)
        self.create_ltr_stg_atc_global_priors(rolling_window_minus_one)
        self.create_ltr_stg_atc_query_priors(rolling_window_minus_one)
        self.create_ltr_stg_atc_init_priors()
        self.create_ltr_stg_atc_alphas_and_betas()
        self.create_ltr_stg_atc_posteriors(start_date_est)

        return self.spark.sql(f"""
                        -- finding the change in atc_posteriors from priors
                        SELECT
                          date_est AS lagged_feature_date_est,
                          search_term AS search_term,
                          ecode AS ecode,
                          time_decay_total_atc_last_x_days as total_atc_last_x_days,
                          time_decay_total_impressions_last_x_days as total_impressions_last_x_days,
                          time_decay_prior_atc_rate as prior_atc_rate,
                          time_decay_prior_alpha as prior_alpha,
                          time_decay_prior_beta as prior_beta,
                          time_decay_alpha_n as alpha_n,
                          time_decay_beta_n as beta_n,
                          time_decay_atc_rate_posterior as atc_rate_posterior,
                          time_decay_prior_atc_rate_std as prior_atc_rate_std,
                          CASE
                            WHEN time_decay_prior_atc_rate_std != 0 THEN (time_decay_atc_rate_posterior - time_decay_prior_atc_rate) / time_decay_prior_atc_rate_std
                            ELSE 0
                          END AS atc_rate_posterior_z_score
                        FROM
                          {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_posteriors
                        WHERE
                          total_impressions_last_x_days >= {self.config.min_total_impressions}
                        """)


    def create_ltr_stg_init_atc(self, start_date_est, end_date_est, rolling_window_minus_one) -> None:
        df = self.spark.sql(f""" 
                        SELECT
                          tran_date,
                          CAST(date_time_utc AS DATE) AS tran_date_utc,
                          CAST(post_cust_hit_time_gmt AS BIGINT) * 1000 AS atc_time,
                          mcvisid,
                          _visit_id,
                          ecode,
                          hitid,
                          TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(_evar2), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS search_term,
                          TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(SPLIT_PART(_evar63, "|", 1)), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS dym_search_term
                        FROM
                          entdata.clk.dks_web_only
                        WHERE
                          tran_date BETWEEN DATE_SUB('{start_date_est}', {rolling_window_minus_one}) AND '{end_date_est}'
                          AND CAST(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(visit_start_time_gmt AS BIGINT)), 'America/New_York') AS DATE) >= DATE_SUB('{start_date_est}', {rolling_window_minus_one})
                          AND _report_suite = 'dsg'
                          AND _evar58 IN (
                            'Internal Search',
                            'Search Page Refinement',
                            'Quick View - Search - SRLP',
                            'Quick View - Search - srlp'
                          )
                          AND _event_cart_add IS NOT NULL
                          AND _evar2 IS NOT NULL
                          AND LENGTH(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(_evar2), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " "))) BETWEEN 2 AND 40
                    """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_atc"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_atc")

    def create_ltr_stg_init_atc_agg(self) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            mcvisid,
                            _visit_id,
                            search_term,
                            dym_search_term,
                            ecode,
                            MAX(atc_time) AS atc_time,
                            MAX(tran_date) AS tran_date,
                            COUNT(1) AS atc_count,
                            MAX_BY(hitid, atc_time) AS hitid,
                            MAX_BY(DATE_DIFF(DAY, TIMESTAMP_MILLIS(atc_time), CURRENT_TIMESTAMP()), atc_time) AS atc_age 
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_atc
                          GROUP BY
                            mcvisid,
                            _visit_id,
                            search_term,
                            dym_search_term,
                            ecode
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_atc_agg"
            )
        )

    def create_ltr_stg_linked_atc(self) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            o.tran_date,
                            o.mcvisid,
                            o._visit_id,
                            o.atc_time,
                            ((1 + EXP(-0.18 * 30)) / (1 + EXP(0.18 * (o.atc_age-30)))) AS time_decay_atc,
                            o.atc_age,
                            o.ecode,
                            1 AS atc_event,
                            COALESCE(o.dym_search_term, o.search_term) AS search_term,
                            ls.id,
                            ls.time AS search_time,
                            ls.impression_items
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_atc_agg o
                              JOIN {self.config.source_table_qualified_2()} ls
                                ON -- we do not include a date condition since the search and ATC may fall on different dates
                                o.mcvisid = ls.mc_visitor_id
                                AND o._visit_id = ls.visit_id
                                AND ls.search_term = COALESCE(o.dym_search_term, o.search_term)
                          WHERE
                                ls.time <= o.atc_time
                                AND (o.atc_time - ls.time) <= ({self.config.atc_time_limit} * 60 * 1000)
                                AND array_contains(ls.impression_items, o.ecode)
                          QUALIFY
                            ROW_NUMBER() OVER (
                                PARTITION BY hitid
                                ORDER BY o.atc_time - ls.time ASC
                              ) = 1
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_linked_atc"
            )
        )

    def create_ltr_stg_agg_atc(self) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            tran_date,
                            search_term,
                            ecode,
                            SUM(atc_event) AS daily_atc_count,
                            SUM(time_decay_atc) AS time_decay_atc_count,
                            ARRAY_AGG(atc_age) AS atc_ages
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_linked_atc
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
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_agg_atc"
            )
        )

    def create_ltr_stg_atc_joined_impressions(self) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            i.tran_date,
                            i.search_term,
                            i.ecode,
                            i.impression_count,
                            i.time_decay_impression_count,
                            IFNULL(atc.daily_atc_count, 0) AS atc_count,
                            IFNULL(atc.time_decay_atc_count, 0) AS time_decay_atc_count,
                            atc_ages
                          FROM
                            {self.config.source_table_qualified()} i
                              LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_agg_atc AS atc
                                ON (
                                  atc.tran_date = i.tran_date
                                  AND atc.search_term = i.search_term
                                  AND atc.ecode = i.ecode
                                )
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_joined_impressions"
            )
        )

    def create_ltr_stg_atc_search_ecodes(self) -> None:
        df = self.spark.sql(f"""
                          SELECT DISTINCT
                            search_term,
                            ecode
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_joined_impressions
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_search_ecodes"
            )
        )


    def create_ltr_stg_atc_search_ecodes_with_all_dates(self) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            ad.date_est,
                            ad.search_term,
                            ad.ecode,
                            COALESCE(ji.impression_count, 0) AS impression_count,
                            IFNULL(ji.time_decay_impression_count, 0) AS time_decay_impression_count,
                            IFNULL(ji.atc_count, 0) AS atc_count,
                            IFNULL(ji.time_decay_atc_count, 0) AS time_decay_atc_count
                          FROM
                            (
                              SELECT      
                                ad.date_est,
                                se.search_term,
                                se.ecode
                              FROM
                                {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_all_dates ad
                              CROSS JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_search_ecodes se
                            ) ad
                            LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_joined_impressions ji 
                            ON ad.date_est = ji.tran_date
                            AND ad.search_term = ji.search_term
                            AND ad.ecode = ji.ecode
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_search_ecodes_with_all_dates"
            )
        )

    def create_ltr_stg_atc_agg_impressions(self, rolling_window_minus_one) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            date_est,
                            search_term,
                            ecode,
                            time_decay_impression_count,
                            time_decay_atc_count,
                            SUM(time_decay_atc_count) OVER (
                                PARTITION BY search_term, sd.ecode
                                ORDER BY date_est
                                ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                              ) AS time_decay_total_atc_last_x_days,
                            SUM(time_decay_impression_count) OVER (
                                PARTITION BY search_term, sd.ecode
                                ORDER BY date_est
                                ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                              ) AS time_decay_total_impressions_last_x_days,
                            SUM(impression_count) OVER (
                                PARTITION BY search_term, sd.ecode
                                ORDER BY date_est
                                ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                              ) AS total_impressions_last_x_days,
                              SUM(atc_count) OVER (
                                PARTITION BY search_term, sd.ecode
                                ORDER BY date_est
                                ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                              ) AS total_atc_last_x_days
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_search_ecodes_with_all_dates sd
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_agg_impressions"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_agg_impressions")

    def create_ltr_stg_atc_global_priors(self, rolling_window_minus_one) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            date_est,
                            time_decay_total_atc_last_x_days_global,
                            time_decay_total_impressions_last_x_days_global,
                            time_decay_total_atc_last_x_days_global / time_decay_total_impressions_last_x_days_global AS time_decay_atc_last_x_days_global -- something is wrong if this gives a division by zero error...
                          FROM
                            (
                              SELECT
                                  date_est,
                                  SUM(time_decay_total_daily_atc) OVER (
                                      ORDER BY date_est
                                      ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                    ) AS time_decay_total_atc_last_x_days_global,
                                  SUM(time_decay_total_daily_impressions) OVER (
                                      ORDER BY date_est
                                      ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                    ) AS time_decay_total_impressions_last_x_days_global
                              FROM
                                  (
                                    SELECT
                                      date_est,
                                      SUM(time_decay_atc_count) AS time_decay_total_daily_atc,
                                      SUM(time_decay_impression_count) AS time_decay_total_daily_impressions
                                    FROM
                                      {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_agg_impressions
                                    GROUP BY
                                      date_est
                                    )
                              )
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_global_priors"
            )
        )

    def create_ltr_stg_atc_query_priors(self, rolling_window_minus_one) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            date_est,
                            search_term,
                            time_decay_total_atc_last_x_days_query,
                            time_decay_total_impressions_last_x_days_query,
                            TRY_DIVIDE(time_decay_total_atc_last_x_days_query, time_decay_total_impressions_last_x_days_query) AS time_decay_atc_last_x_days_query
                          FROM
                            (
                              SELECT
                                date_est,
                                search_term,
                                SUM(time_decay_total_daily_atc) OVER (
                                    PARTITION BY search_term
                                    ORDER BY date_est
                                    ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                  ) AS time_decay_total_atc_last_x_days_query,
                                SUM(time_decay_total_daily_impressions) OVER (
                                    PARTITION BY search_term
                                    ORDER BY date_est
                                    ROWS BETWEEN ({rolling_window_minus_one}) PRECEDING AND CURRENT ROW
                                  ) AS time_decay_total_impressions_last_x_days_query       
                              FROM
                                (
                                  SELECT
                                    date_est,
                                    search_term,
                                    SUM(time_decay_atc_count) AS time_decay_total_daily_atc,
                                    SUM(time_decay_impression_count) AS time_decay_total_daily_impressions
                                  FROM
                                    {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_agg_impressions
                                  GROUP BY
                                    date_est,
                                    search_term
                                ) subquery
                            )
                          WHERE
                            time_decay_total_impressions_last_x_days_query > 0
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_query_priors"
            )
        )

    def create_ltr_stg_atc_init_priors(self) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            ai.date_est,
                            ai.search_term,
                            ai.ecode,
                            ai.time_decay_total_atc_last_x_days,
                            ai.time_decay_total_impressions_last_x_days,
                            ai.total_impressions_last_x_days,
                            (IFNULL(qp.time_decay_total_impressions_last_x_days_query * qp.time_decay_atc_last_x_days_query,0) + {self.config.global_avg_prior_weight} * gp.time_decay_atc_last_x_days_global) /(IFNULL(qp.time_decay_total_impressions_last_x_days_query,0) + {self.config.global_avg_prior_weight}) AS time_decay_prior_atc_rate
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_agg_impressions ai
                            LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_global_priors gp 
                              ON gp.date_est = ai.date_est
                            LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_query_priors qp 
                              ON qp.date_est = ai.date_est
                              AND qp.search_term = ai.search_term
                            WHERE
                              ai.total_atc_last_x_days > 0
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_init_priors"
            )
        )

    def create_ltr_stg_atc_alphas_and_betas(self) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            date_est,
                            search_term,
                            ecode,
                            time_decay_total_atc_last_x_days,
                            time_decay_total_impressions_last_x_days,
                            total_impressions_last_x_days,
                            time_decay_prior_atc_rate,
                            time_decay_prior_atc_rate * {self.config.weight} AS time_decay_prior_alpha,
                            (1 - time_decay_prior_atc_rate) * {self.config.weight} AS time_decay_prior_beta,
                            time_decay_prior_atc_rate * {self.config.weight} + time_decay_total_atc_last_x_days AS time_decay_alpha_n,
                            (1 - time_decay_prior_atc_rate) * {self.config.weight} + GREATEST(time_decay_total_impressions_last_x_days - time_decay_total_atc_last_x_days, 0) AS time_decay_beta_n
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_init_priors
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_alphas_and_betas"
            )
        )

    def create_ltr_stg_atc_posteriors (self, start_date_est) -> None:
        df = self.spark.sql(f"""
                          SELECT
                            a.*,
                            time_decay_alpha_n / (time_decay_alpha_n + time_decay_beta_n) AS time_decay_atc_rate_posterior,
                            SQRT(time_decay_prior_atc_rate * (1 - time_decay_prior_atc_rate) / 2) AS time_decay_prior_atc_rate_std
                          FROM
                            {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_alphas_and_betas a
                          WHERE
                            a.date_est >= '{start_date_est}'
                        """).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.source_catalog}.{self.config.source_schema}.ltr_stg_atc_posteriors"
            )
        )