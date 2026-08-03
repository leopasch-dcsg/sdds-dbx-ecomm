from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation



class LTRABBImpressionsAggLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:
        self.spark.sql("CLEAR CACHE")
        self.spark.sql("SET spark.sql.shuffle.partitions=auto")
        self.spark.sql(f"""
                        CREATE OR REPLACE TEMP VIEW counted_search_results AS ( 
                            SELECT 
                                tran_date,
                                search_term,
                                -- COUNT(*) AS search_count,
                                SUM(time_decay_search) AS time_decay_search_count
                            FROM (
                                SELECT DISTINCT
                                    DATE(FROM_UTC_TIMESTAMP(TIMESTAMP_MILLIS(time), "America/New_York")) AS tran_date,
                                    search_term,  -- this should already be normalized
                                    id, -- search event id            
                                    time_decay_search            
                                FROM {self.config.source_table_qualified()}
                            )
                            GROUP BY 
                                tran_date,
                                search_term
                        )
                        """)

        self.spark.sql(f"""
                        --get date, ecode, impression_counts
                        --one row per search_term/ecode per date
                        CREATE OR REPLACE TEMP VIEW counted_impressions AS ( 
                            SELECT  
                                tran_date,
                                search_term,
                                ecode,
                                COUNT(*) AS impression_count,
                                SUM(time_decay_impression) AS time_decay_impression_count,
                                ARRAY_AGG(impression_time) AS impression_times,
                                ARRAY_AGG(impression_age) AS impression_signal_age
                            FROM (
                                SELECT
                                    DATE(FROM_UTC_TIMESTAMP(TIMESTAMP_MILLIS(ls.time), "America/New_York")) AS tran_date,
                                    ls.search_term,
                                    i.id AS ecode,
                                    i.time AS impression_time,
                                    i.age AS impression_age,
                                    i.time_decay_impression,
                                    i.event_id AS impression_event_id
                                FROM {self.config.source_table_qualified()} ls
                                LATERAL VIEW EXPLODE(impressions) AS i
                            )  
                            GROUP BY
                                tran_date,
                                search_term,
                                ecode  
                        )  
                        """)

        return self.spark.sql(f"""
                        --count impressions per search term/ecode per day
                        SELECT
                            ci.tran_date,
                            ci.search_term,
                            ci.ecode,
                            csr.time_decay_search_count,
                            ci.impression_count,
                            ci.time_decay_impression_count,
                            ci.impression_times,
                            ci.impression_signal_age
                        FROM counted_impressions ci
                        LEFT JOIN counted_search_results csr
                            ON ci.tran_date = csr.tran_date
                            AND ci.search_term = csr.search_term
                        """)