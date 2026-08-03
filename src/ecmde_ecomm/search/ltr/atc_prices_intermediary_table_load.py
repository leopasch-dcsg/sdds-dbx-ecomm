from datetime import date
from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.common.logger import Logger
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation, LoadConfig


class LTRATCPricesIntermediaryLoad(LoadOperation):
    def __init__(
        self,
        config: LoadConfig,
        spark: SparkSession,
        start_date_est: date | None = None,
        end_date_est: date | None = None,
    ):
        super().__init__(config, spark, start_date_est, end_date_est)
        self._logger = Logger.logger(__class__.__name__)

    def batch_dataframe(self) -> DataFrame:
        if not self.start_date_est:
            raise RuntimeError("start_date_est is not set")
        if not self.end_date_est:
            raise RuntimeError("end_date_est is not set")
        if not self.config.lookback_days:
            raise RuntimeError("lookback_days is not set")

        lookback_days_minus_one = int(self.config.lookback_days) - 1

        self._logger.info(
            f"""
            Start Date ETC: {self.start_date_est}
            End Date ETC: {self.end_date_est}
            Lookback Days: {self.config.lookback_days}
            Lookback Days Minus One: {lookback_days_minus_one}
            """
        )

        sql = f"""
            WITH init_clk AS (
              SELECT
                    tran_date,
                    -- take 7 AM EST as the cutoff for using previous day's BOD data to get the price
                    CASE
                        WHEN EXTRACT(HOUR FROM FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(CAST(post_cust_hit_time_gmt AS BIGINT)), "America/New_York")) < 7
                            THEN DATE_SUB(tran_date, 1)
                            ELSE tran_date
                    END AS inv_date_est,
                    ecode,
                    _sku,
                    _evar2,
                    _event_evar2_instance,
                    _evar3,
                    SPLIT_PART(_evar63, "|", 1) AS dym_search_term, --spark sql starts counting at 1 in array functions
                    _event_cart_add,
                    _evar58,
                    hitid
               FROM entdata.clk.dks_web_only
              WHERE _report_suite = "dsg"
                and tran_date BETWEEN DATE_SUB(DATE("{self.start_date_est}"), {lookback_days_minus_one}) AND DATE("{self.end_date_est}")  -- add additional days for ATC events since we need to take rolling window estimates
                AND DATE(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(CAST(visit_start_time_gmt AS BIGINT)), "America/New_York")) >= DATE_SUB(DATE("{self.start_date_est}"), {lookback_days_minus_one})
                AND (_evar2 IS NOT NULL AND CAST(TRIM(_evar2) AS NUMERIC) IS NULL)
            ),
            /* Create a basic DYM mapping */
            dym_mapping AS (
                SELECT
                    raw_search_term,
                    FIRST_VALUE(dym_search_term) AS dym_search_term
                FROM (
                    SELECT
                        regexp_replace(lower(trim(_evar2)),"[^a-zA-Z0-9 ]", "") AS raw_search_term,
                        regexp_replace(lower(trim(dym_search_term)),"[^a-zA-Z0-9 ]", "") AS dym_search_term
                   FROM init_clk
                  WHERE _evar3 LIKE "%DYM%"
                    AND dym_search_term IS NOT NULL
                    AND _event_evar2_instance IS NOT NULL
                )
                GROUP BY raw_search_term
            ),
            /* Get search-ATC events */
            init_atc AS (
                SELECT
                    atc.tran_date,
                    atc.inv_date_est,
                    COALESCE(d.dym_search_term, atc.search_term) AS search_term,
                    atc.ecode,
                    atc._sku,
                    atc.hitid
                FROM (
                    SELECT
                        tran_date,
                        inv_date_est,
                        regexp_replace(lower(trim(_evar2)),"[^a-zA-Z0-9 ]", "") AS search_term,
                        ecode,
                        _sku,
                        hitid
                    FROM init_clk
                    WHERE _event_cart_add IS NOT NULL
                    AND ecode IS NOT NULL
                    AND _sku IS NOT NULL
                    AND _evar58 IN ("Internal Search", "Search Page Refinement", "Quick View - Search - SRLP", "Quick View - Search - srlp")
                ) atc
                LEFT JOIN dym_mapping d
                ON (d.raw_search_term = atc.search_term)
            ),
            /* Get sku-level pricing data for our time window */
            sku_prices AS (
                SELECT
                    inv.bod_inv_date AS bod_inv_date_est,
                    inv.product_number AS sku,
                    ds.ecode,
                    CAST(inv.web_price AS DOUBLE) AS web_price,
                    LOG(1 + CAST(inv.web_price AS DOUBLE)) AS log1p_web_price
                FROM entdata.ecm.inv_web_prod_bod_assortment inv -- in BQ and DBX
                JOIN entdata.prd.dks_sku ds -- in BQ and DBX
                USING (product_id)
                WHERE inv.bod_inv_date BETWEEN DATE_SUB(DATE("{self.start_date_est}"), {lookback_days_minus_one}) AND DATE("{self.end_date_est}")  -- make sure we include the windows near the start date
                and inv.web_price is not null
                AND inv.product_status_group = "A"
                AND inv.web_eligibility_ind = 1
                AND inv.webstore_key = 6
                AND ds.ecode IS NOT NULL
            ),
            /* Merge ATC events with sku-level price data */
            atc_prices AS (
                SELECT
                    ia.tran_date,
                    sp.bod_inv_date_est,
                    ia.search_term,
                    ia.ecode,
                    ia._sku,
                    ia.hitid,
                    sp.web_price,
                    sp.log1p_web_price
                FROM init_atc ia
                JOIN sku_prices sp
                ON (ia.inv_date_est <= sp.bod_inv_date_est AND ia._sku = sp.sku)
                QUALIFY ROW_NUMBER() OVER(PARTITION BY ia.hitid ORDER BY ia.inv_date_est - sp.bod_inv_date_est) = 1  -- dedupe any atc events with most recently recorded price
            )
            SELECT tran_date, bod_inv_date_est, search_term, ecode, _sku, hitid, web_price, log1p_web_price FROM atc_prices;
            """

        self._logger.info(f"Executing the SQL query for add to cart prices.")
        return self.spark.sql(sql)
