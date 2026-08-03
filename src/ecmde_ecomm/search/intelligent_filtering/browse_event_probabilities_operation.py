from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from ecmde_ecomm.common import AppendOperation


class BrowseEventCountsAndProbabilities(AppendOperation):
    def batch_dataframe(self) -> DataFrame:
        self.spark.sql("CLEAR CACHE")
        end_date = datetime.strptime(self.date_info.end_date_est, "%Y-%m-%d").date()
        start_date = end_date - timedelta(days=self.date_info.lookback_days)
        rolling_window = self.date_info.lookback_days

        df = (self.spark.sql( f"""
                    with browse_event_base as(
                          select
                            tran_date as date_key,
                            _report_suite as webstore,
                            _visits as session_id,
                            case
                              when _prop2 = 'Family' then 'PLP'
                            end as event_type,
                            substring_index(substring_index(pagename, ": ", -1), " ", 1) as event_value,
                            substring_index(_prop11, ": ", 1) as facet
                          from entdata.clk.dks_web_only 
                          where 
                            tran_date between to_date('{start_date}') and to_date('{end_date}')
                            and date(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(CAST(visit_start_time_gmt as bigint)), "America/New_York")) >= to_date('{start_date}')
                            and _report_suite in ('dsg', 'pbl', 'gg')
                            and _evar2 is not null
                            and _prop11 is not null
                            and _prop2 = 'Family'
                    ),
                    refined_browse_event_base as(
                          Select
                            date_key,
                            webstore,
                            session_id,
                            event_type,
                            event_value,
                            facet
                          from browse_event_base
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
                    daily_browse_event_counts as(
                        select
                          date_key,
                          webstore,
                          event_value,
                        count(distinct session_id) as daily_events
                        from refined_browse_event_base
                        group by date_key, webstore, event_value
                    ),
                    global_daily_browse_event_counts as(
                        select
                          date_key,
                          webstore,
                          sum(daily_events) as global_daily_events
                        from daily_browse_event_counts
                        group by date_key, webstore
                    ),
                    browse_event_facet_counts as(
                        select
                          date_key,
                          webstore,
                          event_value,
                          facet,
                          count(facet) as count_facet
                        from refined_browse_event_base
                        group by date_key, webstore, event_value, facet
                    ),
                    if_family_page_facets as (
                      select distinct webstore, event_value, facet
                      from refined_browse_event_base
                    ),
                    if_family_pages as (
                      select distinct webstore, event_value
                      from refined_browse_event_base
                    ),
                    family_pages_with_all_dates as (
                      select
                        ad.date_est as date_key,
                        fp.webstore as webstore,
                        fp.event_value as event_value,
                        coalesce(dbc.daily_events, 0) as daily_events
                      from if_stg_all_dates ad
                      cross join if_family_pages fp
                      left join daily_browse_event_counts dbc
                        on ad.date_est = dbc.date_key
                        and fp.event_value = dbc.event_value
                        and fp.webstore = dbc.webstore
                    ),
                    browse_facets_with_all_dates as (
                      select
                        ad.date_est as date_key,
                        fpf.webstore as webstore,
                        fpf.event_value as event_value,
                        fpf.facet as facet,
                        coalesce(bfc.count_facet, 0) as count_facet
                      from if_stg_all_dates ad
                      cross join if_family_page_facets fpf
                      left join browse_event_facet_counts bfc
                        on ad.date_est = bfc.date_key
                        and fpf.event_value = bfc.event_value
                        and fpf.webstore = bfc.webstore
                        and fpf.facet = bfc.facet
                    ),
                    if_browse_counts as (
                      select
                        date_key,
                        webstore,
                        event_value,
                        sum(daily_events)
                          over (partition by webstore, event_value order by cast(date_key as timestamp) range between interval {rolling_window} days preceding and current row) as browse_events_last_x_days
                      from family_pages_with_all_dates
                    ),
                    if_browse_counts_global as (
                    select
                      date_key,
                      webstore,
                      sum(global_daily_events)
                          over (partition by webstore order by cast(date_key as timestamp) range between interval {rolling_window} days preceding and current row) as browse_events_last_x_days_global
                    from global_daily_browse_event_counts
                    ),
                    if_browse_facet_counts as (
                      select
                        date_key,
                        webstore,
                        event_value,
                        facet,
                        sum(count_facet)
                          over (partition by webstore, event_value, facet order by cast(date_key as timestamp) range between interval {rolling_window} days preceding and current row) as facet_count_last_x_days
                      from browse_facets_with_all_dates
                    ),
                    if_browse_prob_calcs as (
                    Select 
                      ibfc.date_key,
                      ibfc.webstore,
                      'PLP' as event_type,
                      ibfc.event_value,
                      ibfc.facet,
                      ibc.browse_events_last_x_days,
                      round(ibc.browse_events_last_x_days/ ibcg.browse_events_last_x_days_global, 8) as p_event_last_x_days,
                      ibfc.facet_count_last_x_days,
                      round(ibfc.facet_count_last_x_days/ ibc.browse_events_last_x_days, 3) as p_facet_last_x_days
                    from if_browse_facet_counts ibfc
                    join if_browse_counts ibc
                      on ibfc.date_key = ibc.date_key
                      and ibfc.webstore = ibc.webstore
                      and ibfc.event_value = ibc.event_value
                    join if_browse_counts_global ibcg
                      on ibfc.date_key = ibcg.date_key
                      and ibfc.webstore = ibcg.webstore
                    where ibfc.date_key = to_date('{end_date}')
                    )
                    select * from if_browse_prob_calcs
                """)
              .select("*"))

        return df
