from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation



class LTRATCBaseLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:
        if not self.start_date_est:
            raise RuntimeError("start_date_est is not set")
        if not self.end_date_est:
            raise RuntimeError("end_date_est is not set")
        if not self.config.lookback_days:
            raise RuntimeError("lookback_days is not set")

        return self.spark.sql(
            f"""
                /* Configure dates for filtering data */
                WITH dates_est AS (
                    SELECT
                        DATE("{self.start_date_est}") AS start_date_est,
                        DATE("{self.end_date_est}") AS end_date_est,
                        {self.config.lookback_days} AS lookback_days
                ),
                /* Construct array of dates for which we have to calculate rolling window estimates */
                date_range_est AS (
                    SELECT
                        EXPLODE (
                            SEQUENCE(d.start_date_est, d.end_date_est, INTERVAL 1 DAY)
                        ) AS date_est
                    FROM dates_est d
                ),
                /* Generate pairs of search_terms and dates for the time windows */
                term_dates_est AS (
                    SELECT
                        d.date_est,
                        s.search_term
                    FROM (
                        SELECT DISTINCT search_term
                        FROM {self.config.source_table_qualified()}
                        WHERE 
                            CHAR_LENGTH(search_term) BETWEEN 2 AND 40 
                            AND RLIKE(search_term, "[a-z0-9]+")  -- add last mile filtering
                    ) s
                    CROSS JOIN date_range_est d
                ),
                /* Merge search_term/date pairs with the collected ATC events to establish rolling windows */
                atc_rolling_windows AS (
                    SELECT
                        td.date_est,
                        td.search_term,
                        ap.tran_date,
                        ap.hitid,
                        ap.web_price,
                        ap.log1p_web_price
                    FROM term_dates_est td
                    JOIN {self.config.source_table_qualified()} ap
                    ON (
                        (ap.tran_date BETWEEN DATE_SUB(td.date_est, ({self.config.lookback_days} - 1)) AND td.date_est)
                        AND td.search_term = ap.search_term
                    )
                )
                SELECT date_est, search_term, tran_date, hitid, web_price FROM atc_rolling_windows;
                """
        )
