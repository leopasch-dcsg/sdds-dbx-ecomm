from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from ecmde_ecomm.common import AppendOperation


class SearchEventCTR(AppendOperation):

    def batch_dataframe(self) -> DataFrame:
        self.spark.sql("CLEAR CACHE")
        end_date = datetime.strptime(self.date_info.end_date_est, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=self.date_info.lookback_days)
        rolling_window = self.date_info.lookback_days - 1

        df = (self.spark.sql( f"""
                with search_event_base as(
                  select
                    tran_date as date_key,
                    _report_suite as webstore,
                    _visits as session_id,
                    case
                      when _prop2 = 'Search Results' then 'SRLP'
                    end as event_type,
                    trim(regexp_replace(lower(trim(_evar2)),"[^a-zA-Z0-9 ]", "")) as event_value,
                    substring_index(_prop11, ": ", 1) as facet
                  from entdata.clk.dks_web_only 
                  where 
                    tran_date between '{start_date}' and '{end_date}'
                    and date(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(CAST(visit_start_time_gmt as bigint)), "America/New_York")) >= '{start_date}'
                    and _report_suite in ('dsg', 'pbl', 'gg')
                    and _evar2 is not null
                    and _prop11 is not null
                    and _prop2 = 'Search Results'
                ),
                refined_search_event_base as(
                  Select
                    date_key,
                    webstore,
                    session_id,
                    event_type,
                    event_value,
                    facet
                  from search_event_base
                  where event_value != ''
                  group by date_key, webstore, session_id, event_type, event_value, facet
                ),
                if_stg_all_dates as(
                    select 
                        explode(
                            sequence(
                                to_date('{start_date}'),
                                to_date('{end_date}'),
                                interval 1 day
                            )
                        ) as date_est
                ),
                if_stg_init_clk as(
                  SELECT 
                      _visits, 
                      mcvisid,
                      _report_suite, 
                      MIN(tran_date) AS visit_start_tran_date_est,
                      MIN(CAST(post_cust_hit_time_gmt AS BIGINT)) AS start_time,
                      MAX(CAST(post_cust_hit_time_gmt AS BIGINT)) AS end_time    
                  FROM entdata.clk.dks_web_only c 
                  WHERE 
                      tran_date between '{start_date}' and '{end_date}'
                      and CAST(FROM_UTC_TIMESTAMP(FROM_UNIXTIME(CAST(visit_start_time_gmt AS BIGINT)), "America/New_York") AS DATE) >= '{start_date}'
                      AND c._report_suite in ('dsg', 'pbl', 'gg')
                      AND (c._prop33 LIKE 'NB-%' 
                          OR c._prop33 LIKE 'CB-%' 
                          OR c._prop33 IS NULL)
                  GROUP BY _visits, mcvisid, _report_suite
                ),
                if_stg_init_searches as (
                  SELECT *
                  FROM (
                      SELECT 
                          id, 
                          event_date_short AS date_utc,         
                          UNIX_SECONDS(event_timestamp_utc) AS time,
                          banner AS webstore,
                          s.search_event.size AS page_size,
                          s.event_track.AdobeMCVID AS mcvisid,
                          regexp_replace(lower(trim(s.search_event.term)),"[^a-zA-Z0-9 ]", "") AS search_term
                      FROM prod_ent_silver_db.sdsc.ml_events s
                      WHERE s.event_date_short BETWEEN DATE(to_utc_timestamp('{start_date} 00:00:00', 'America/New_York')) and date(to_utc_timestamp('{end_date} 23:59:59', 'America/New_York'))
                      AND DATE(FROM_UTC_TIMESTAMP(s.event_timestamp_utc, "America/New_York")) BETWEEN '{start_date}' and '{end_date}' 
                      AND s.search_event.type = 'SRLP'
                      AND s.type = 'S'
                      AND s.search_event.term IS NOT NULL        
                      AND s.banner in ('DSG', 'PL', 'GG')
                      AND s.channel = 'WEB'
                  )
                  WHERE search_term != ''
                ),
                if_stg_unrolled_impressions as 
                (
                  SELECT DISTINCT
                      s.id AS impression_event_id,
                      s.parent_id AS parent_id,
                      s.event_date_short AS date_utc, 
                      UNIX_SECONDS(s.event_timestamp_utc) AS impression_time,
                      s.event_track.AdobeMCVID AS mcvisid,
                      i.id AS impression_id
                  FROM prod_ent_silver_db.sdsc.ml_events s
                  LATERAL VIEW EXPLODE(s.search_result.items) AS i  
                  WHERE CAST(s.event_date_short AS DATE) BETWEEN date(to_utc_timestamp('{start_date} 00:00:00', 'America/New_York')) and date(to_utc_timestamp('{end_date} 23:59:59', 'America/New_York'))
                  AND UNIX_SECONDS(s.event_timestamp_utc) BETWEEN UNIX_SECONDS(TIMESTAMP(to_utc_timestamp('{start_date} 00:00:00', 'America/New_York'))) AND UNIX_SECONDS(TO_UTC_TIMESTAMP('{end_date} 23:59:59', 'America/New_York'))
                  AND s.type = "I"
                  AND exists (SELECT 1 FROM if_stg_init_searches where s.parent_id = if_stg_init_searches.id)
                ),
                if_stg_counted_impressions as(
                  SELECT 
                      parent_id,
                      COUNT(*) AS impression_count
                  FROM if_stg_unrolled_impressions
                  GROUP BY parent_id
                ),
                if_stg_qualified_searches as (
                  SELECT 
                      i.id
                  FROM if_stg_init_searches i
                  JOIN if_stg_counted_impressions c 
                  ON i.id = c.parent_id
                  WHERE c.impression_count <= i.page_size * 2
                ),
                if_stg_grouped_impressions as (
                  SELECT 
                      parent_id,
                      MIN(date_utc) AS date_utc,
                      ANY_VALUE(mcvisid) AS mcvisid,
                      COLLECT_LIST(
                          STRUCT(
                              impression_event_id AS event_id, 
                              impression_id AS id, 
                              impression_time AS time
                              )
                          ) AS impressions
                  FROM if_stg_unrolled_impressions
                  WHERE EXISTS (SELECT 1 FROM if_stg_qualified_searches WHERE if_stg_unrolled_impressions.parent_id = if_stg_qualified_searches.id)
                  GROUP BY parent_id
                ),
                if_stg_init_searches_and_impressions as (
                  SELECT 
                      i.id, 
                      i.date_utc, 
                      i.time, 
                      i.mcvisid, 
                      i.search_term, 
                      g.impressions
                  FROM  if_stg_init_searches i
                  LEFT JOIN if_stg_grouped_impressions g 
                  ON g.parent_id = i.id
                ),
                if_stg_linked_searches as (
                  SELECT
                    * except (violation_size)
                from(
                    SELECT 
                        c.visit_start_tran_date_est,
                        c._report_suite as webstore,
                        c.mcvisid AS mc_visitor_id,
                        c._visits AS visit_id,
                        c.start_time,
                        c.end_time,    
                        s.date_utc AS search_date_utc,
                        s.time,
                        s.id,
                        s.search_term,
                        s.impressions.id AS impression_items,
                        s.impressions,
                        GREATEST(c.start_time - s.time, s.time - c.end_time, 0) AS violation_size
                    FROM if_stg_init_clk c
                    JOIN if_stg_init_searches_and_impressions s 
                    ON ( 
                        c.mcvisid = s.mcvisid
                        AND s.time >= (c.start_time - 30)  -- 30 second buffers due to casting seconds to milliseconds
                        AND s.time <= (c.end_time + 30)
                        )
                    )
                QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY violation_size) = 1
                ),
                counted_search_results as(
                    SELECT 
                        *,        
                        COUNT(id) OVER (PARTITION BY webstore, search_term, tran_date) AS search_count
                    FROM (
                        SELECT
                            DATE(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(time), "America/New_York")) AS tran_date,
                            webstore,
                            search_term,  -- this should already be normalized
                            id, -- search event id            
                            impressions            
                        FROM if_stg_linked_searches
                    )
                ),
                unrolled_search_result_impressions as(
                    SELECT
                        csr.tran_date,
                        csr.webstore,
                        csr.search_term,
                        csr.search_count,
                        i.id AS impression_ecode,
                        i.time AS impression_time,
                        i.event_id AS impression_event_id
                    FROM counted_search_results csr
                    LATERAL VIEW EXPLODE(impressions) AS i 
                ),
                if_stg_impressions_agg as (
                  SELECT
                    tran_date,
                    webstore,
                    search_term,
                    FIRST(search_count) as search_count,
                    COUNT(*) AS impression_count
                  FROM unrolled_search_result_impressions
                  WHERE impression_ecode IS NOT NULL
                  AND impression_ecode != ''
                  GROUP BY 
                    tran_date,
                    webstore, 
                    search_term
                ),
                if_stg_init_clicks as (
                  SELECT
                      tran_date,
                      DATE(date_time_utc) AS tran_date_utc,
                      _report_suite,
                      mcvisid,
                      _visit_id,
                      hitid,
                      CAST(post_cust_hit_time_gmt AS BIGINT) AS clk_time,
                      regexp_replace(lower(trim(_evar2)),"[^a-zA-Z0-9 ]", "") AS search_term,
                      _evar2,
                      _evar58
                  FROM  entdata.clk.dks_web_only
                  WHERE tran_date BETWEEN '{start_date}' and '{end_date}'       
                  AND DATE(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(CAST(visit_start_time_gmt AS BIGINT)), "America/New_York")) >= '{start_date}'
                  AND ecode IS NOT NULL
                  AND _evar2 IS NOT NULL          
                  AND _prop2 = 'Product Detail'
                  AND _evar58 IN ('Internal Search', 'Search Page Refinement', 'Quick View - Search - SRLP', 'Quick View - Search - srlp')
                  AND _evar27 LIKE '%: Shopping: Search: Results'
                  AND post_page_event = '0'
                  AND _report_suite in ('dsg', 'pbl', 'gg')
                  AND (_prop33 LIKE 'NB-%'
                      OR _prop33 LIKE 'CB-%'
                      OR _prop33 IS NULL)
                ),
                if_stg_init_clicks_refined as (
                  SELECT
                    *,
                    LEAD(clk_time) OVER (PARTITION BY _visit_id ORDER BY clk_time) AS next_clk_time,
                    LAG(clk_time) OVER (PARTITION BY _visit_id ORDER BY clk_time) AS prev_clk_time
                FROM (
                    SELECT *            
                    FROM if_stg_init_clicks
                    WHERE search_term != ''
                    and _visit_id in (select session_id from refined_search_event_base)
                )
                ), 
                if_stg_init_clicks_and_facets as (
                    Select
                        c.*,
                        s.facet
                    from if_stg_init_clicks_refined c
                    JOIN refined_search_event_base s
                    ON c._visit_id = s.session_id
                    AND c.tran_date = s.date_key
                    AND c.search_term = s.event_value
                    AND c._report_suite = s.webstore
                ),
                if_stg_linked_clicks as (
                  SELECT 
                      c.tran_date,
                      c._report_suite,
                      c.mcvisid,
                      c._visit_id,
                      c.hitid,
                      MAX(ls.`time`) AS search_time,
                      MAX(c.search_term) AS search_term,
                      MAX(c.facet) AS facet
                  FROM if_stg_init_clicks_and_facets c
                  JOIN if_stg_linked_searches ls
                  ON 
                      c.mcvisid = ls.mc_visitor_id
                      AND c._visit_id = ls.visit_id
                      AND c.search_term = ls.search_term
                      AND ls.time <= c.clk_time
                      AND ((_evar58 NOT IN ('Quick View - Search - SRLP', 'Quick View - Search - srlp')
                          AND ls.time >= IFNULL(c.prev_clk_time, ls.start_time)) 
                      OR _evar58 IN ('Quick View - Search - SRLP', 'Quick View - Search - srlp'))
                  GROUP BY c.tran_date, c._report_suite, c.mcvisid, c._visit_id, c.hitid
                ),
                if_stg_click_counts as (
                  SELECT
                      tran_date,
                      _report_suite,
                      search_term,
                      facet,
                      IFNULL(COUNT(1), 0) AS daily_clicks
                  FROM if_stg_linked_clicks
                  GROUP BY 
                      tran_date,
                      _report_suite, 
                      search_term, 
                      facet
                ),
                if_stg_joined_impressions as (
                  SELECT
                      i.tran_date,
                      i.webstore,
                      i.search_term,
                      c.facet,
                      i.impression_count,
                      LEAST(IFNULL(c.daily_clicks, 0), i.impression_count) AS click_count  --limiting max number of clicks to number of impressions
                  FROM if_stg_impressions_agg i
                  CROSS JOIN dev_ecmde_db.jmoore_sandbox.if_stg_dates d
                  LEFT JOIN if_stg_click_counts c
                      ON c.tran_date = i.tran_date 
                      AND c.search_term = i.search_term
                      AND c._report_suite = i.webstore
                  where c.facet is not null
                ),
                if_stg_search_facets as (
                  select distinct webstore, search_term, facet
                  from if_stg_joined_impressions
                ),
                if_stg_search_facets_with_all_dates as (
                  select
                    ad.date_est as date_key,
                    sf.webstore as webstore,
                    sf.search_term as event_value,
                    sf.facet as facet,
                    IFNULL(ji.impression_count, 0) AS impression_count,
                    IFNULL(ji.click_count, 0) AS click_count
                  from if_stg_all_dates ad
                  cross join if_stg_search_facets sf
                  left join if_stg_joined_impressions ji
                    on ad.date_est = ji.tran_date
                    and sf.webstore = ji.webstore
                    and sf.search_term = ji.search_term
                    and sf.facet = ji.facet
                )
                select
                *
                from(
                  SELECT
                      date_key,
                      webstore,
                      'SRLP' as event_type,
                      event_value,
                      facet,
                      SUM(click_count) OVER (
                          PARTITION BY webstore, event_value, facet
                          ORDER BY date_key
                          ROWS BETWEEN {rolling_window} PRECEDING AND CURRENT ROW
                      ) AS total_clicks_last_x_days,
                      SUM(impression_count) OVER (
                          PARTITION BY webstore, event_value, facet
                          ORDER BY date_key
                          ROWS BETWEEN {rolling_window} PRECEDING AND CURRENT ROW
                      ) AS total_impressions_last_x_days,
                      total_clicks_last_x_days/total_impressions_last_x_days as ctr
                  FROM if_stg_search_facets_with_all_dates
                )
                where date_key = '{end_date}'
                """)
              .select("*"))

        return df