from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation


class LTRABBLinkedSearchesLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:
        if not self.start_date_est:
            raise RuntimeError("start_date_est is not set")
        if not self.end_date_est:
            raise RuntimeError("end_date_est is not set")
        if not self.config.abb_rolling_window:
            raise RuntimeError("rolling_window is not set")

        self.spark.sql("CLEAR CACHE")
        self.spark.sql("SET spark.sql.shuffle.partitions=auto")

        self.create_ltr_stg_dates()

        start_date_est = self.spark.sql(
            f"""
                SELECT start_date_est
                FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dates
            """
        ).first()["start_date_est"]

        end_date_est = self.spark.sql(
            f"""
                        SELECT end_date_est
                        FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dates
                    """
        ).first()["end_date_est"]

        rolling_window_minus_one = self.spark.sql(
            f"""
                        SELECT rolling_window_minus_one
                        FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dates
                    """
        ).first()["rolling_window_minus_one"]

        start_ts_utc = self.spark.sql(
            f"""
                        SELECT start_ts_utc
                        FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dates
                    """
        ).first()["start_ts_utc"]

        end_ts_utc = self.spark.sql(
            f"""
                                SELECT end_ts_utc
                                FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dates
                            """
        ).first()["end_ts_utc"]

        self.create_ltr_stg_all_dates()
        self.create_ltr_stg_init_clk(start_date_est, end_date_est, rolling_window_minus_one)
        self.create_ltr_stg_init_searches(start_date_est, end_date_est, rolling_window_minus_one, start_ts_utc, end_ts_utc)
        self.create_ltr_stg_unrolled_impressions(rolling_window_minus_one, start_ts_utc, end_ts_utc)
        self.create_ltr_stg_counted_impressions()
        self.create_ltr_stg_qualified_searches()
        self.create_ltr_stg_grouped_impressions()
        self.create_ltr_stg_init_searches_and_impressions()
        self.create_ltr_stg_linked_searches()

        return self.spark.sql(
            f"""
                    SELECT * EXCEPT(violation_size)
                    FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_linked_searches
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY violation_size) = 1
                    """
        )

    def create_ltr_stg_dates(self):
        df = self.spark.sql(
            f"""
                                SELECT 
                                    CAST("{self.start_date_est}" AS DATE) AS start_date_est, 
                                    CAST("{self.end_date_est}" AS DATE) AS end_date_est,
                                    ({self.config.abb_rolling_window} - 1) AS rolling_window_minus_one,
                                    TO_UTC_TIMESTAMP("{self.start_date_est} 00:00:00", 'America/New_York') AS start_ts_utc,
                                    TO_UTC_TIMESTAMP("{self.end_date_est} 23:59:59", 'America/New_York')  AS end_ts_utc
                                """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dates"
            )
        )


    def create_ltr_stg_all_dates(self) -> None:
        df = self.spark.sql(
            f"""
                          SELECT date_est
                          FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_dates d
                          LATERAL VIEW EXPLODE(
                            SEQUENCE(
                              DATE_SUB(d.start_date_est, d.rolling_window_minus_one), 
                              d.end_date_est, 
                              INTERVAL 1 DAY
                            )
                          ) AS date_est
                        """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_all_dates"
            )
        )

    def create_ltr_stg_init_clk(self,start_date_est, end_date_est, rolling_window_minus_one) -> None:
        df = self.spark.sql(
            f"""
                        SELECT 
                            _visit_id, 
                            mcvisid, 
                            MIN(tran_date) AS visit_start_tran_date_est,
                            MIN(CAST(post_cust_hit_time_gmt AS BIGINT)) * 1000 AS start_time,
                            MAX(CAST(post_cust_hit_time_gmt AS BIGINT)) * 1000 AS end_time    
                        FROM entdata.clk.dks_web_only c
                        -- we must pull in additional data to ensure each date between start_date_est and end_date_est have an adequate rolling window size 
                        WHERE 
                            c.tran_date BETWEEN DATE_SUB('{start_date_est}', {rolling_window_minus_one}) AND '{end_date_est}'
                            AND CAST(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(visit_start_time_gmt AS BIGINT)), 'America/New_York') AS DATE) >= DATE_SUB('{start_date_est}', {rolling_window_minus_one})
                            AND c._report_suite = 'dsg' 
                            AND (c._prop33 LIKE 'NB-%' 
                                OR c._prop33 LIKE 'CB-%' 
                                OR c._prop33 IS NULL)
                        GROUP BY _visit_id, mcvisid
                        """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_clk"
            )
        )

    def create_ltr_stg_init_searches(self, start_date_est, end_date_est, rolling_window_minus_one, start_ts_utc, end_ts_utc) -> None:
        df = self.spark.sql(
            f"""
                            SELECT 
                                id, 
                                event_date_short AS date_utc,         
                                UNIX_MILLIS(event_timestamp_utc) AS time,
                                DATE_DIFF(DAY, event_timestamp_utc, CURRENT_TIMESTAMP()) AS search_age,
                                s.search_event.size AS page_size,
                                s.event_track.AdobeMCVID AS mcvisid,
                                TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(search_event.term), r"[^a-z0-9\.\_\-/ ]+", ""), r" {{2,}}", " ")) AS search_term
                            FROM prod_ent_silver_db.sdsc.ml_events s
                            WHERE s.event_date_short BETWEEN DATE_SUB(DATE('{start_ts_utc}'), {rolling_window_minus_one} ) AND DATE('{end_ts_utc}')
                            AND DATE(FROM_UTC_TIMESTAMP(s.event_timestamp_utc, "America/New_York")) BETWEEN DATE_SUB('{start_date_est}', {rolling_window_minus_one}) AND '{end_date_est}'  
                            AND s.search_event.type = 'SRLP'
                            AND s.type = 'S'
                            AND s.search_event.term IS NOT NULL
                            AND s.search_event.term != ''        
                            AND s.banner = 'DSG'
                            AND s.channel = 'WEB'
            """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_searches"
            )
        )

        self.spark.sql(f"OPTIMIZE {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_searches")


    def create_ltr_stg_unrolled_impressions(self, rolling_window_minus_one, start_ts_utc, end_ts_utc) -> None:
        df = self.spark.sql(
            f"""   
                        SELECT DISTINCT
                        s.id AS impression_event_id,
                        s.parent_id AS parent_id,
                        s.event_date_short AS date_utc, 
                        UNIX_MILLIS(s.event_timestamp_utc) AS impression_time_utc,
                        DATE_DIFF(DAY,s.event_timestamp_utc, CURRENT_TIMESTAMP()) AS impression_age,
                        s.event_track.AdobeMCVID AS mcvisid,
                        i.id AS impression_id,
                        i.type AS impression_type
                        FROM prod_ent_silver_db.sdsc.ml_events s
                        LATERAL VIEW EXPLODE(s.search_result.items) AS i  
                        WHERE CAST(s.event_date_short AS DATE) BETWEEN DATE_SUB(DATE('{start_ts_utc}'), {rolling_window_minus_one}) AND DATE('{end_ts_utc}')
                        AND s.event_timestamp_utc BETWEEN TIMESTAMP(DATE_SUB('{start_ts_utc}', {rolling_window_minus_one})) AND '{end_ts_utc}'
                        AND s.type = "I"
                        AND s.banner = 'DSG'
                        AND s.channel = 'WEB'
                        AND i.id IS NOT NULL
                        AND exists (SELECT 1 FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_searches where s.parent_id = ltr_stg_init_searches.id)
                               """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_unrolled_impressions"
            )
        )

    def create_ltr_stg_counted_impressions(self) -> None:
        df = self.spark.sql(
            f"""
                        SELECT 
                            parent_id,
                            COUNT(*) AS impression_count
                        FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_unrolled_impressions
                        GROUP BY parent_id
                        """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_counted_impressions"
            )
        )

    def create_ltr_stg_qualified_searches(self) -> None:
        df = self.spark.sql(
            f"""
                        SELECT 
                            i.id
                        FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_searches i
                        JOIN {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_counted_impressions c 
                        ON i.id = c.parent_id
                        WHERE c.impression_count <= i.page_size * 2
                        """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_qualified_searches"
            )
        )

    def create_ltr_stg_grouped_impressions(self) -> None:
        df = self.spark.sql(
            f"""
                        SELECT 
                            u.parent_id as parent_id,
                            MIN(u.date_utc) AS date_utc,
                            MIN(u.mcvisid) AS mcvisid,
                            ARRAY_COMPACT(
                                ARRAY_AGG(
                                    STRUCT(
                                        u.impression_event_id AS event_id, 
                                        u.impression_id AS id, 
                                        u.impression_time_utc AS time,
                                        u.impression_age AS age,
                                        ((1 + EXP(-0.18 * 30)) / (1 + EXP(0.18 * (u.impression_age-30)))) AS time_decay_impression
                                        )
                                    )
                                ) AS impressions
                        FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_unrolled_impressions u
                        INNER JOIN {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_qualified_searches q
                            ON q.id = u.parent_id
                        WHERE u.impression_type IN ('P', 'PP', 'SP')
                        GROUP BY u.parent_id
                        """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_grouped_impressions"
            )
        )

    def create_ltr_stg_init_searches_and_impressions(self) -> None:
        df = self.spark.sql(
            f"""
                        SELECT 
                            i.id, 
                            i.date_utc, 
                            i.time,
                            i.search_age, 
                            i.mcvisid, 
                            i.search_term, 
                            g.impressions
                        FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_grouped_impressions g
                        LEFT JOIN {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_searches i 
                        ON g.parent_id = i.id
                        """
        ).select("*")

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "false")
            .saveAsTable(
                f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_searches_and_impressions"
            )
        )

    def create_ltr_stg_linked_searches(self) -> None:
       df =  self.spark.sql(
            f"""
                        SELECT 
                            c.visit_start_tran_date_est,
                            c.mcvisid AS mc_visitor_id,
                            c._visit_id AS visit_id,
                            c.start_time,
                            c.end_time,    
                            s.date_utc AS search_date_utc,
                            s.time,
                            s.id,
                            s.search_term,
                            ((1 + EXP(-0.18 * 30)) / (1 + EXP(0.18 * (s.search_age-30)))) AS time_decay_search,
                            s.impressions.id AS impression_items,
                            s.impressions,
                            GREATEST(c.start_time - s.time, s.time - c.end_time, 0) AS violation_size
                        FROM {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_clk c
                        JOIN {self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_init_searches_and_impressions s 
                        ON ( 
                            c.mcvisid = s.mcvisid
                            AND s.time >= (c.start_time - 30 * 1000)  -- 30 second buffers
                            AND s.time <= (c.end_time + 30 * 1000)
                        )
                        """
        ).select("*")

       (
           df.write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "false")
           .saveAsTable(
               f"{self.config.destination_catalog}.{self.config.destination_schema}.ltr_stg_linked_searches"
           )
       )


