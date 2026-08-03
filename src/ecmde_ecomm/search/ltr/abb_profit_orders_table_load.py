from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation


class LTRABBProfitOrdersLoad(LoadOperation):
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

        self.create_ltr_stg_init_clickstream(start_date_est, end_date_est, rolling_window_minus_one)
        self.create_ltr_stg_dym_mapping()
        self.create_ltr_stg_init_orders_filtered()
        self.create_ltr_stg_base_attrs()
        self.create_ltr_stg_sku_cost_rollup()
        self.create_ltr_stg_sku_costs(start_date_est, end_date_est, rolling_window_minus_one)
        self.create_ltr_stg_ecode_costs()
        self.create_ltr_stg_order_profit()

        return self.spark.sql(
            f"""
                SELECT
                    bod_inv_date_est        AS bod_inv_date_est,
                    tran_date               AS tran_date,
                    inv_date_est            AS inv_date_est,
                    search_term             AS search_term,
                    -- Product
                    ecode                   AS ecode,
                    CAST(_sku AS STRING)         AS sku,
                    -- Hit / session
                    hitid                   AS hitid,
                    _report_suite           AS report_suite,
                    CAST(_webstore_key AS STRING) AS webstore_key,
                    mcvisid                 AS mcvisid,
                    _visit_id               AS visit_id,
                    CAST(_order_id AS STRING)    AS order_id,
                    -- Order metrics
                    _units                  AS units,
                    _revenue                AS revenue,
                    -- Attribution / search metadata
                    _evar33                 AS evar33,
                    _evar58                 AS evar58,
                    -- Timestamp
                    order_time_est          AS order_time,
                    -- Pricing and profit
                    CAST(avg_web_price AS DOUBLE) AS avg_web_price,
                    avg_web_cost            AS avg_web_cost,
                    profit                  AS profit
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_order_profit
            """
        )

    def create_ltr_stg_init_clickstream(self, start_date_est, end_date_est, rolling_window_minus_one) -> None:

        ltr_stg_init_clickstream = self.spark.sql(
            f"""
                SELECT
                    tran_date,
                    -- BOD date for pricing: if order placed before 7 AM Eastern, use prior day
                    CASE
                        WHEN EXTRACT(HOUR FROM FROM_UTC_TIMESTAMP(
                                 TIMESTAMP_SECONDS(CAST(post_cust_hit_time_gmt AS BIGINT)), 'America/New_York')
                             ) < 7
                            THEN DATE_SUB(tran_date, 1)
                        ELSE tran_date
                    END AS inv_date_est,
                    -- Product identifiers
                    ecode,
                    _sku,
                    -- Report / webstore
                    _report_suite,
                    _webstore_key,
                    -- Visitor / session / hit identifiers
                    mcvisid,
                    _visit_id,
                    _order_id,
                    hitid,
                    duplicate_purchase,
                    -- Order metrics
                    _units,
                    _revenue,
                    -- Search term fields
                    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(_evar2), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS _evar2,                  -- normalized search term
                    _event_evar2_instance,-- search instance flag
                    _evar3,-- search type (e.g. DYM)
                    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(SPLIT_PART(_evar63, "|", 1)), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS dym_search_term,  -- normalized DYM suggested term
                    -- Attribution / channel
                    _evar58,-- last-touch channel
                    _evar33,-- first-touch channel
                    -- Timestamps in Eastern (America/New_York)
                    FROM_UTC_TIMESTAMP(FROM_UNIXTIME(post_cust_hit_time_gmt), 'America/New_York') AS order_time_est -- post-processed hit timestamp
                FROM entdata.clk.dks_web_only
                WHERE
                    -- Date range: start_date_est − (lookback_days − 1) → end_date_est (inclusive)
                    tran_date BETWEEN DATE_SUB('{start_date_est}', {rolling_window_minus_one}) AND '{end_date_est}'
                    -- Valid text search term (exclude nulls)
                    AND _evar2 IS NOT NULL
                    -- DSG webstore only
                    AND _report_suite = 'dsg'
                    AND _webstore_key = 6
            """
        ).select("*")

        (
            ltr_stg_init_clickstream.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_clickstream"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_clickstream")

    def create_ltr_stg_dym_mapping(self) -> None:
        ltr_stg_dym_mapping = self.spark.sql(
            f"""
                SELECT
                    raw_search_term,
                    MAX_BY(dym_search_term, order_time_est) AS dym_search_term
                FROM (
                    SELECT
                        _evar2         AS raw_search_term,
                        dym_search_term AS dym_search_term,
                        order_time_est 
                    FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_clickstream
                    WHERE
                        _evar3 LIKE '%DYM%'               -- DYM event
                        AND dym_search_term IS NOT NULL    -- has a suggested term
                        AND _event_evar2_instance IS NOT NULL  -- is a search instance hit
                )
                GROUP BY raw_search_term
            """
        ).select("*")

        (
            ltr_stg_dym_mapping.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dym_mapping"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dym_mapping")

    def create_ltr_stg_init_orders_filtered(self) -> None:
        ltr_stg_init_orders_filtered = self.spark.sql(
            f"""
                SELECT
                    ic.tran_date,
                    ic.inv_date_est,
                    -- Prefer DYM-corrected term; fall back to original normalized term
                    COALESCE(d.dym_search_term, ic._evar2) AS search_term,
                    -- Product identifiers
                    ic.ecode,
                    ic._sku,
                    -- Hit / session identifiers
                    ic.hitid,
                    ic._report_suite,
                    ic._webstore_key,
                    ic.mcvisid,
                    ic._visit_id,
                    ic._order_id,
                    -- Order metrics
                    ic._units,
                    ic._revenue,
                    -- Attribution / search metadata
                    ic._evar33,
                    ic._evar58,
                    -- Timestamp
                    ic.order_time_est
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_clickstream ic
                LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_dym_mapping d
                    ON d.raw_search_term = ic._evar2
                WHERE
                    -- Must be a purchase event with valid product data
                    ic.ecode IS NOT NULL
                    AND ic._sku IS NOT NULL
                    AND ic._order_id IS NOT NULL
                    AND ic.ecode NOT IN ('Tax', 'Shipping')
                    AND ic.duplicate_purchase = '0'
                    -- Attributed to internal search (either merchandising eVar)
                    AND (
                        ic._evar58 IN (
                            'Internal Search',
                            'Search Page Refinement',
                            'Quick View - Search - SRLP',
                            'Quick View - Search - srlp'
                        )
                        OR ic._evar33 IN (
                            'Internal Search',
                            'Search Page Refinement',
                            'Quick View - Search - SRLP',
                            'Quick View - Search - srlp'
                        )
                    )
            """
        ).select("*")

        (
            ltr_stg_init_orders_filtered.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_orders_filtered"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_orders_filtered")


    def create_ltr_stg_base_attrs(self) -> None:
        ltr_stg_base_attrs = self.spark.sql(
            """
                SELECT
                    ds.product_id,
                    ds.product_number AS sku,
                    ds.product_identifying,
                    ds.ecode,
                    ds.brand,
                    CAST(ds.sku_average_cost AS DECIMAL(38,4)) AS curr_sku_average_cost
                FROM entdata.prd.dks_sku ds
            """
        ).select("*")

        (
            ltr_stg_base_attrs.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_base_attrs"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_base_attrs")

    def create_ltr_stg_sku_cost_rollup(self) -> None:
        ltr_stg_sku_cost_rollup = self.spark.sql(
            f"""
                SELECT
                    product_id,
                    sku,
                    product_identifying,
                    ecode,
                    COALESCE(
                        curr_sku_average_cost,
                        ecode_avg_curr_sku_average_cost,
                        pia_brand_avg_curr_sku_average_cost,
                        pia_avg_curr_sku_average_cost,
                        global_avg_curr_sku_average_cost
                    ) AS curr_sku_average_cost
                FROM (
                    SELECT
                        product_id,
                        sku,
                        product_identifying,
                        ecode,
                        brand,
                        curr_sku_average_cost,
                        CASE WHEN ecode IS NOT NULL THEN AVG(curr_sku_average_cost) OVER(PARTITION BY ecode) END AS ecode_avg_curr_sku_average_cost,
                        CASE WHEN product_identifying IS NOT NULL AND brand IS NOT NULL THEN AVG(curr_sku_average_cost) OVER(PARTITION BY product_identifying, brand) END AS pia_brand_avg_curr_sku_average_cost,
                        CASE WHEN product_identifying IS NOT NULL THEN AVG(curr_sku_average_cost) OVER(PARTITION BY product_identifying) END AS pia_avg_curr_sku_average_cost,
                        global_avg_curr_sku_average_cost
                    FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_base_attrs
                    CROSS JOIN (
                        SELECT
                            AVG(curr_sku_average_cost)   AS global_avg_curr_sku_average_cost
                        FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_base_attrs
                    ) global_avg
                )
            """
        ).select("*")

        (
            ltr_stg_sku_cost_rollup.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_sku_cost_rollup"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_sku_cost_rollup")

    def create_ltr_stg_sku_costs(self, start_date_est, end_date_est, rolling_window_minus_one) -> None:
        ltr_stg_sku_costs = self.spark.sql(
            f"""
                SELECT
                    inv.bod_inv_date AS bod_inv_date_est,
                    inv.product_id,
                    ds.ecode,
                    AVG(inv.web_price) AS avg_web_price,
                    -- SKU-level cost: prefer actual order cost, fall back to rollup cost
                    COALESCE(AVG(CAST(os.sku_average_cost AS DOUBLE)), MAX(CAST(scr.curr_sku_average_cost AS DOUBLE))) AS avg_web_cost
                FROM entdata.web.inv_web_prod_bod_assortment inv
                JOIN entdata.prd.dks_sku ds
                    USING (product_id)
                JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_sku_cost_rollup scr
                    ON scr.product_id = inv.product_id
                LEFT JOIN entdata.ecm.order_sku os
                    ON os.product_id = ds.product_id
                    AND os.order_date = inv.bod_inv_date
                    AND os.webstore_key = 6
                WHERE
                    -- Match the extended clickstream window so lookback orders find pricing
                    inv.bod_inv_date BETWEEN DATE_SUB('{start_date_est}', {rolling_window_minus_one}) AND '{end_date_est}'
                    AND inv.product_status_group = 'A'           -- active products only
                    AND inv.web_eligibility_ind  = 1             -- web-eligible
                    AND inv.webstore_key = 6             -- DSG webstore
                    AND ds.ecode IS NOT NULL
                GROUP BY
                    inv.bod_inv_date,
                    inv.product_id,
                    ds.ecode
            """
        ).select("*")

        (
            ltr_stg_sku_costs.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_sku_costs"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_sku_costs")

    def create_ltr_stg_ecode_costs(self) -> None:
        ltr_stg_ecode_costs = self.spark.sql(
            f"""
                SELECT
                    ecode,
                    bod_inv_date_est,
                    AVG(avg_web_cost) AS avg_web_cost_ecode_level
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_sku_costs
                GROUP BY ecode, bod_inv_date_est
            """
        ).select("*")

        (
            ltr_stg_ecode_costs.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_ecode_costs"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_ecode_costs")

    def create_ltr_stg_order_profit(self) -> None:
        ltr_stg_order_profit = self.spark.sql(
            f"""
                SELECT
                    COALESCE(sc.bod_inv_date_est, ec.bod_inv_date_est) AS bod_inv_date_est,
                    ia.tran_date,
                    ia.inv_date_est,
                    ia.search_term,
                    -- Product
                    ia.ecode,
                    ia._sku,
                    -- Hit / session
                    ia.hitid,
                    ia._report_suite,
                    ia._webstore_key,
                    ia.mcvisid,
                    ia._visit_id,
                    ia._order_id,
                    -- Order metrics
                    ia._units,
                    ia._revenue,
                    -- Attribution / search metadata
                    ia._evar33,
                    ia._evar58,
                    -- Timestamp
                    ia.order_time_est,
                    -- SKU-level pricing (falls back to ecode-level cost if SKU cost unavailable)
                    sc.avg_web_price,
                    COALESCE(sc.avg_web_cost, ec.avg_web_cost_ecode_level) AS avg_web_cost,
                    -- Profit = revenue − (COALESCE(sku_cost, ecode_cost) × units)
                    (ia._revenue - (COALESCE(sc.avg_web_cost, ec.avg_web_cost_ecode_level) * ia._units)) AS profit
                FROM {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_init_orders_filtered ia
                -- Join on SKU (product_id) to get SKU-level pricing
                LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_sku_costs sc
                    ON  ia.inv_date_est = sc.bod_inv_date_est
                    AND ia._sku        = sc.product_id
                -- Fallback: join on ecode to get ecode-level average cost
                LEFT JOIN {self.config.source_catalog}.{self.config.source_schema}.ltr_stg_ecode_costs ec
                    ON  ia.inv_date_est = ec.bod_inv_date_est
                    AND ia.ecode = ec.ecode
                -- ensure at least one cost value is available for profit calculation
                WHERE sc.avg_web_cost IS NOT NULL OR ec.avg_web_cost_ecode_level IS NOT NULL
                -- Deduplicate: keep one row per hit (highest revenue line)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY ia.hitid
                    ORDER BY ia._revenue DESC
                ) = 1
            """
        ).select("*")

        (
            ltr_stg_order_profit.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_order_profit"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_order_profit")