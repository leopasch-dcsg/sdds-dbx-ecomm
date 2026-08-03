from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation


class LTRABBPositiveProfitRateLoad(LoadOperation):
    _TIME_DECAY_FLATNESS = 0.18
    _TIME_DECAY_MIDPOINT = 30
    _POSITIVE_PROFIT_RATE_DISCRETE_PRIOR_MEAN = 0.000001
    _POSITIVE_PROFIT_RATE_DISCRETE_PRIOR_WEIGHT = 10000.0
    _POSITIVE_PROFIT_RATE_COUNT_PRIOR_MEAN = 0.0
    _TIME_DECAY_SIGMA_0_SQ_POSITIVE_PROFIT_RATE = 1.0
    _W_MAX = 0.1
    _LOGNORMAL_AGG_TYPE = 'median'
    _UDF_CATALOG_DATASET = 'prod_datasci_db.ecm_search_ranking_abb'
    _ALPHA_POSITIVE_PROFIT_RATE = _POSITIVE_PROFIT_RATE_DISCRETE_PRIOR_MEAN * _POSITIVE_PROFIT_RATE_DISCRETE_PRIOR_WEIGHT
    _BETA_POSITIVE_PROFIT_RATE = (1 - _POSITIVE_PROFIT_RATE_DISCRETE_PRIOR_MEAN) * _POSITIVE_PROFIT_RATE_DISCRETE_PRIOR_WEIGHT

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


        self.create_ltr_stg_profit_rate_dates()
        self.create_ltr_stg_config_filtered_profit_orders()
        self.create_ltr_stg_all_orders(start_date_est, end_date_est, rolling_window_minus_one)
        self.create_ltr_stg_profit_orders_linked_searches(start_date_est, end_date_est, rolling_window_minus_one)
        self.create_ltr_stg_attributed_orders()
        self.create_ltr_stg_windowed_orders(rolling_window_minus_one)
        self.create_ltr_stg_time_decayed_orders()
        self.create_ltr_stg_feature_date_aggs()
        self.create_ltr_stg_positive_profit_arrays()
        self.create_ltr_stg_rolling_impressions(rolling_window_minus_one)
        self.create_ltr_stg_time_decayed_impressions(end_date_est)
        self.create_ltr_stg_joined_features()
        self.create_ltr_stg_posterior_updates()

        return self.spark.sql(
            f"""
                SELECT 
                    lagged_feature_date_est,
                    search_term,
                    ecode,
                    time_decay_positive_profit_rate_posterior
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_posterior_updates
            """
        )


    def create_ltr_stg_profit_rate_dates(self) -> None:

        ltr_stg_profit_rate_dates = self.spark.sql(
            f"""
                    SELECT
                        EXPLODE(
                            SEQUENCE(
                                TO_DATE(start_date_est),
                                TO_DATE(end_date_est),
                                INTERVAL 1 DAY
                            )
                        ) AS feature_date_est
                    FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dates
            """
        ).select("*")

        (
            ltr_stg_profit_rate_dates.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_profit_rate_dates"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_profit_rate_dates")

    def create_ltr_stg_config_filtered_profit_orders(self) -> None:
        ltr_stg_config_filtered_profit_orders = self.spark.sql(
            f"""
                    SELECT
                        po.*
                    FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_abb_profit_orders po
                    INNER JOIN (
                        SELECT search_term, ecode
                        FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_abb_profit_orders
                        GROUP BY search_term, ecode
                        HAVING SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END) > 0
                    ) positive_profit_pairs
                        ON  po.search_term = positive_profit_pairs.search_term
                        AND po.ecode = positive_profit_pairs.ecode
            """
        )

        (
            ltr_stg_config_filtered_profit_orders.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_config_filtered_profit_orders"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_config_filtered_profit_orders")

    def create_ltr_stg_all_orders(self, start_date_est, end_date_est, rolling_window_minus_one) -> None:

        ltr_stg_all_orders = self.spark.sql(
            f"""
                    SELECT
                        po.bod_inv_date_est,
                        po.report_suite,
                        po.webstore_key,
                        po.mcvisid,
                        po.visit_id,
                        po.order_id AS order_id,
                        po.order_time AS order_time_est,
                        po.ecode,
                        po.sku AS sku,
                        po.search_term,
                        po.profit,
                        LAG(po.order_time) OVER (
                            PARTITION BY po.mcvisid, po.visit_id, po.report_suite
                            ORDER BY po.order_time
                        ) AS prev_order_time_est
                    FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_config_filtered_profit_orders po
                        WHERE po.bod_inv_date_est >= DATE_SUB(TO_DATE('{start_date_est}'), {rolling_window_minus_one})
                        AND po.bod_inv_date_est <= TO_DATE('{end_date_est}')
            """
        )

        (
            ltr_stg_all_orders.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_all_orders"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_all_orders")

    def create_ltr_stg_profit_orders_linked_searches(self, start_date_est, end_date_est, rolling_window_minus_one) -> None:

        ltr_stg_profit_orders_linked_searches = self.spark.sql(
            f"""
                SELECT
                    FROM_UTC_TIMESTAMP(
                        CAST(FROM_UNIXTIME(ls.time / 1000) AS TIMESTAMP),
                        'America/New_York'
                    ) AS search_time_est,
                    ls.mc_visitor_id,
                    ls.visit_id,
                    ls.search_term,
                    imp.ecode_impression
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_abb_linked_searches ls
                LATERAL VIEW EXPLODE(ls.impression_items) imp AS ecode_impression
                WHERE visit_start_tran_date_est BETWEEN DATE_SUB('{start_date_est}', {rolling_window_minus_one}) AND '{end_date_est}'
            """
        )

        (
            ltr_stg_profit_orders_linked_searches.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_profit_orders_linked_searches"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_profit_orders_linked_searches")


    def create_ltr_stg_attributed_orders(self) -> None:

        ltr_stg_attributed_orders = self.spark.sql(
            f"""
                SELECT
                    o.bod_inv_date_est,
                    o.webstore_key,
                    o.order_id,
                    ls.search_time_est,
                    CAST(ls.search_time_est AS DATE) AS search_date_est,
                    o.order_time_est,
                    o.ecode,
                    o.sku,
                    o.search_term,
                    o.profit,
                    GREATEST(o.profit, 0) AS positive_profit
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_all_orders o
                INNER JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_profit_orders_linked_searches ls
                    ON  o.mcvisid    = ls.mc_visitor_id
                    AND o.visit_id  = ls.visit_id
                    AND ls.search_term = o.search_term
                    AND ls.ecode_impression = o.ecode
                    AND ls.search_time_est <= (o.order_time_est + INTERVAL 15 SECOND)
                    AND ls.search_time_est >= (
                        COALESCE(o.prev_order_time_est, ls.search_time_est)
                        - INTERVAL 15 SECOND
                    )
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY o.webstore_key, o.search_term, o.order_id, o.sku
                    ORDER BY o.order_time_est - ls.search_time_est
                ) = 1
            """
        )

        (
            ltr_stg_attributed_orders.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_attributed_orders"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_attributed_orders")



    def create_ltr_stg_windowed_orders(self, rolling_window_minus_one) -> None:
        ltr_stg_windowed_orders = self.spark.sql(
            f"""
                SELECT
                    d.feature_date_est AS lagged_feature_date_est,
                    a.*,
                    DATEDIFF(d.feature_date_est, a.search_date_est) AS signal_age
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_profit_rate_dates d
                INNER JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_attributed_orders a
                    ON  a.bod_inv_date_est >= DATE_SUB(d.feature_date_est, {rolling_window_minus_one})
                    AND a.bod_inv_date_est <= d.feature_date_est
            """
        )

        (
            ltr_stg_windowed_orders.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_windowed_orders"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_windowed_orders")

    def create_ltr_stg_time_decayed_orders(self) -> None:

        ltr_stg_time_decayed_orders = self.spark.sql(
            f"""
                SELECT
                    wo.lagged_feature_date_est,
                    wo.search_time_est,
                    wo.ecode,
                    wo.search_term,
                    wo.positive_profit,
                    wo.signal_age,
                    (1 + EXP(-{self._TIME_DECAY_FLATNESS} * {self._TIME_DECAY_MIDPOINT}))
                        / (1 + EXP({self._TIME_DECAY_FLATNESS} * (wo.signal_age - {self._TIME_DECAY_MIDPOINT})))
                        AS time_decay_weight
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_windowed_orders wo
            """
        )

        (
            ltr_stg_time_decayed_orders.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_time_decayed_orders"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_time_decayed_orders")

    def create_ltr_stg_feature_date_aggs(self) -> None:
        ltr_stg_feature_date_aggs = self.spark.sql(
            f"""
                SELECT
                    lagged_feature_date_est,
                    search_term,
                    ecode,
                    SUM(CASE WHEN positive_profit > 0
                             THEN time_decay_weight ELSE 0 END) AS time_decay_positive_profit_order_count,
            
                    -- Struct array for positive-profit posterior (filtered to positive profit only).
                    -- Sorted here once, ascending by signal_age (oldest -> newest) so the UDF
                    -- processes the most recent observation last and gives it the highest
                    -- effective weight in the sequential mu update.  Secondary sort on
                    -- search_time_est breaks same-day ties (signal_age is day-granularity);
                    -- tertiary sort on positive_profit covers any remaining duplicates.
                    ARRAY_SORT(
                        ARRAY_AGG(
                            NAMED_STRUCT(
                                'positive_profit',   positive_profit,
                                'search_time_est',   search_time_est,
                                'signal_age',        signal_age,
                                'time_decay_weight', {self._W_MAX} * time_decay_weight
                            )
                        ) FILTER (WHERE positive_profit > 0),
                        (a, b) ->
                            CASE
                                -- Primary: ascending signal_age (oldest -> newest)
                                WHEN a.signal_age < b.signal_age THEN -1
                                WHEN a.signal_age > b.signal_age THEN  1
                                -- Secondary: ascending search_time_est to break same-day ties
                                WHEN a.search_time_est < b.search_time_est THEN -1
                                WHEN a.search_time_est > b.search_time_est THEN  1
                                -- Tertiary: ascending positive_profit to break any remaining ties
                                WHEN a.positive_profit < b.positive_profit THEN -1
                                WHEN a.positive_profit > b.positive_profit THEN  1
                                ELSE 0
                            END
                    ) AS sorted_struct
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_time_decayed_orders
                GROUP BY lagged_feature_date_est, search_term, ecode
                HAVING SUM(CASE WHEN positive_profit > 0 THEN 1 ELSE 0 END) > 0
            """
        )

        (
            ltr_stg_feature_date_aggs.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_feature_date_aggs"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_feature_date_aggs")

    def create_ltr_stg_positive_profit_arrays(self) -> None:

        ltr_stg_positive_profit_arrays = self.spark.sql(
            f"""
                SELECT
                    lagged_feature_date_est,
                    search_term,
                    ecode,
                    TRANSFORM(sorted_struct, x -> x.positive_profit) AS positive_profit_values,
                    TRANSFORM(sorted_struct, x -> x.time_decay_weight) AS time_decay_positive_profit_update_weights
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_feature_date_aggs
            """
        )

        (
            ltr_stg_positive_profit_arrays.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_positive_profit_arrays"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_positive_profit_arrays")

    def create_ltr_stg_rolling_impressions(self, rolling_window_minus_one) -> None:
        ltr_stg_rolling_impressions = self.spark.sql(
            f"""
                SELECT
                    d.feature_date_est,
                    i.search_term,
                    i.ecode,
                    FLATTEN(ARRAY_AGG(i.impression_signal_age)) AS impression_signal_age
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_profit_rate_dates d
                LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_abb_impressions_agg i
                    ON  TO_DATE(i.tran_date, "America/New_York")
                        BETWEEN DATE_SUB(d.feature_date_est, {rolling_window_minus_one})
                            AND d.feature_date_est
                GROUP BY d.feature_date_est, i.search_term, i.ecode
        """
        )

        (
            ltr_stg_rolling_impressions.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_rolling_impressions"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_rolling_impressions")

    def create_ltr_stg_time_decayed_impressions(self, end_date_est) -> None:
        ltr_stg_time_decayed_impressions = self.spark.sql(
            f"""
                SELECT
                    i.feature_date_est,
                    i.search_term,
                    i.ecode,
                    SUM(
                        (1 + EXP(-{self._TIME_DECAY_FLATNESS} * {self._TIME_DECAY_MIDPOINT}))
                        / (1 + EXP({self._TIME_DECAY_FLATNESS} * (
                            sa.signal_age
                            - DATEDIFF(TO_DATE('{end_date_est}'), i.feature_date_est)
                            - {self._TIME_DECAY_MIDPOINT}
                        )))
                    ) AS time_decay_impression_count
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_rolling_impressions i
                LATERAL VIEW EXPLODE(i.impression_signal_age) sa AS signal_age
                GROUP BY i.feature_date_est, i.search_term, i.ecode
        """
        )

        (
            ltr_stg_time_decayed_impressions.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_time_decayed_impressions"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_time_decayed_impressions")

    def create_ltr_stg_joined_features(self) -> None:
        ltr_stg_joined_features = self.spark.sql(
            f"""
                SELECT
                    fa.lagged_feature_date_est,
                    fa.search_term,
                    fa.ecode,
                    COALESCE(tdi.time_decay_impression_count, 0)            AS time_decay_impression_count,
                    fa.time_decay_positive_profit_order_count,
                    pp.positive_profit_values,
                    pp.time_decay_positive_profit_update_weights
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_feature_date_aggs fa
                JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_positive_profit_arrays pp
                    ON  pp.lagged_feature_date_est = fa.lagged_feature_date_est
                    AND pp.search_term             = fa.search_term
                    AND pp.ecode                   = fa.ecode
                LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_time_decayed_impressions tdi
                    ON  tdi.feature_date_est       = fa.lagged_feature_date_est
                    AND tdi.search_term            = fa.search_term
                    AND tdi.ecode                  = fa.ecode
        """
        )

        (
            ltr_stg_joined_features.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_joined_features"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_joined_features")

    def create_ltr_stg_posterior_updates(self) -> None:
        ltr_stg_posterior_updates = self.spark.sql(
            f"""
                SELECT
                    jf.lagged_feature_date_est,
                    jf.search_term,
                    jf.ecode,
                    {self._UDF_CATALOG_DATASET}.time_decay_hurdle_lognormal_posterior_predictive(
                        jf.positive_profit_values,
                        jf.time_decay_positive_profit_update_weights,
                        {self._TIME_DECAY_SIGMA_0_SQ_POSITIVE_PROFIT_RATE},
                        {self._POSITIVE_PROFIT_RATE_COUNT_PRIOR_MEAN},
                        {self._W_MAX},
                        jf.time_decay_impression_count,
                        jf.time_decay_positive_profit_order_count,
                        {self._ALPHA_POSITIVE_PROFIT_RATE},
                        {self._BETA_POSITIVE_PROFIT_RATE},
                        '{self._LOGNORMAL_AGG_TYPE}'
                    ).posterior AS time_decay_positive_profit_rate_posterior
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_joined_features jf
        """
        )

        (
            ltr_stg_posterior_updates.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_posterior_updates"
            )
        )

        self.spark.sql(
            f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_posterior_updates")