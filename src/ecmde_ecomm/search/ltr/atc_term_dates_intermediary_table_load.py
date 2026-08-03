from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation



class LTRATCTermDatesIntermediaryLoad(LoadOperation):
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
        
                term_dates_est AS (
                    SELECT
                        d.date_est,
                        s.search_term,
                        s.ecode
                    FROM (
                        SELECT DISTINCT search_term, ecode
                        FROM {self.config.source_table_qualified()}
                        WHERE 
                            CHAR_LENGTH(search_term) BETWEEN 2 AND 40 
                            AND RLIKE(search_term, "[a-z0-9]+")  -- add last mile filtering
                    ) s
                    CROSS JOIN date_range_est d
                )
                SELECT date_est, search_term, ecode FROM term_dates_est;
            """
        )