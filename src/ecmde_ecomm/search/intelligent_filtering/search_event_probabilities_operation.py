from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from ecmde_ecomm.common import AppendOperation


class SearchEventCountsAndProbabilities(AppendOperation):
    def batch_dataframe(self) -> DataFrame:
        self.spark.sql("CLEAR CACHE")
        end_date = datetime.strptime(self.date_info.end_date_est, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=self.date_info.lookback_days)
        rolling_window = self.date_info.lookback_days

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
                            tran_date between to_date('{start_date}') and to_date('{end_date}')
                            and date(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(CAST(visit_start_time_gmt as bigint)), "America/New_York")) >= to_date('{start_date}')
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
                    daily_search_event_counts as(
                        select
                          date_key,
                          webstore,
                          event_value,
                        count(distinct session_id) as daily_events
                        from refined_search_event_base
                        group by date_key, webstore, event_value
                    ),
                    global_daily_search_event_counts as(
                        select
                          date_key,
                          webstore,
                          sum(daily_events) as global_daily_events
                        from daily_search_event_counts
                        group by date_key, webstore
                    ),
                    search_event_facet_counts as(
                        select
                          date_key,
                          webstore,
                          event_value,
                          facet,
                          count(facet) as count_facet
                        from refined_search_event_base
                        group by date_key, webstore, event_value, facet
                    ),
                    if_search_term_facets as (
                      select distinct webstore, event_value, facet
                      from refined_search_event_base
                    ),
                    if_search_terms as (
                      select distinct webstore, event_value
                      from refined_search_event_base
                    ),
                    search_terms_with_all_dates as (
                      select
                        ad.date_est as date_key,
                        sf.webstore as webstore,
                        sf.event_value as event_value,
                        coalesce(dsc.daily_events, 0) as daily_events
                      from if_stg_all_dates ad
                      cross join if_search_terms sf
                      left join daily_search_event_counts dsc
                        on ad.date_est = dsc.date_key
                        and sf.event_value = dsc.event_value
                        and sf.webstore = dsc.webstore
                    ),
                    search_facets_with_all_dates as (
                      select
                        ad.date_est as date_key,
                        sf.webstore as webstore,
                        sf.event_value as event_value,
                        sf.facet as facet,
                        coalesce(sfc.count_facet, 0) as count_facet
                      from if_stg_all_dates ad
                      cross join if_search_term_facets sf
                      left join search_event_facet_counts sfc
                        on ad.date_est = sfc.date_key
                        and sf.event_value = sfc.event_value
                        and sf.webstore = sfc.webstore
                        and sf.facet = sfc.facet
                    ),
                    if_search_counts as (
                      select
                        date_key,
                        webstore,
                        event_value,
                        sum(daily_events)
                          over (partition by webstore, event_value order by cast(date_key as timestamp) range between interval {rolling_window} days preceding and current row) as search_events_last_x_days
                      from search_terms_with_all_dates
                    ),
                    if_search_counts_global as (
                    select
                      date_key,
                      webstore,
                      sum(global_daily_events)
                          over (partition by webstore order by cast(date_key as timestamp) range between interval {rolling_window} days preceding and current row) as search_events_last_x_days_global
                    from global_daily_search_event_counts
                    ),
                    if_search_facet_counts as (
                      select
                        date_key,
                        webstore,
                        event_value,
                        facet,
                        sum(count_facet)
                          over (partition by webstore, event_value, facet order by cast(date_key as timestamp) range between interval {rolling_window} days preceding and current row) as facet_count_last_x_days
                      from search_facets_with_all_dates
                    ),
                    if_search_prob_calcs as (
                    Select 
                      isfc.date_key,
                      isfc.webstore,
                      'SRLP' as event_type,
                      isfc.event_value,
                      isfc.facet,
                      isc.search_events_last_x_days,
                      round(isc.search_events_last_x_days/ iscg.search_events_last_x_days_global, 8) as p_event_last_x_days,
                      isfc.facet_count_last_x_days,
                      round(isfc.facet_count_last_x_days/ isc.search_events_last_x_days, 3) as p_facet_last_x_days
                    from if_search_facet_counts isfc
                    join if_search_counts isc
                      on isfc.date_key = isc.date_key
                      and isfc.webstore = isc.webstore
                      and isfc.event_value = isc.event_value
                    join if_search_counts_global iscg
                      on isfc.date_key = iscg.date_key
                      and isfc.webstore = iscg.webstore
                    where isfc.date_key = to_date('{end_date}')
                    )
                    select * from if_search_prob_calcs
                """)
              .select("*"))

        return df