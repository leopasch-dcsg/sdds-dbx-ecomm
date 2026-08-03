from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from ecmde_ecomm.common import AppendOperation


class SearchEventFinancials(AppendOperation):

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
                        base_attrs as (
                          select
                            ds.product_id,
                            ds.product_number as sku,
                            ds.product_identifying,
                            ds.ecode,
                            ds.brand,
                            dsc.sku_average_cost as curr_sku_average_cost,
                            dship.sku_length * dship.sku_width * dship.sku_height as sku_volume
                          from entdata.prd.dks_sku ds
                          left join entdata.prd.dks_sku_shipping dship
                          using (product_id)
                          left join entdata.prc.dks_sku_cost dsc
                          using (product_id)
                        ),
                        golabl_avg_costs as(
                        SELECT
                            product_id,
                            sku,
                            product_identifying,
                            ecode,
                            brand,
                            sku_volume,
                            curr_sku_average_cost,
                            CASE WHEN ecode IS NOT NULL THEN AVG(sku_volume) OVER(PARTITION BY ecode) END AS ecode_avg_sku_volume,
                            CASE WHEN product_identifying IS NOT NULL AND brand IS NOT NULL THEN AVG(sku_volume) OVER(PARTITION BY product_identifying, brand) END AS pia_brand_avg_sku_volume,
                            CASE WHEN product_identifying IS NOT NULL THEN AVG(sku_volume) OVER(PARTITION BY product_identifying) END AS pia_avg_sku_volume,
                            global_avg_sku_volume,
                            CASE WHEN ecode IS NOT NULL THEN AVG(curr_sku_average_cost) OVER(PARTITION BY ecode) END AS ecode_avg_curr_sku_average_cost,
                            CASE WHEN product_identifying IS NOT NULL AND brand IS NOT NULL THEN AVG(curr_sku_average_cost) OVER(PARTITION BY product_identifying, brand) END AS pia_brand_avg_curr_sku_average_cost,
                            CASE WHEN product_identifying IS NOT NULL THEN AVG(curr_sku_average_cost) OVER(PARTITION BY product_identifying) END AS pia_avg_curr_sku_average_cost,
                            global_avg_curr_sku_average_cost
                        FROM base_attrs
                        CROSS JOIN (
                              SELECT AVG(sku_volume) AS global_avg_sku_volume, AVG(curr_sku_average_cost) AS global_avg_curr_sku_average_cost FROM base_attrs
                          ) global_avg
                        ),
                        if_ecode_cost as (
                            SELECT
                              product_id,
                              sku,
                              product_identifying,
                              ecode,
                              COALESCE(curr_sku_average_cost, ecode_avg_curr_sku_average_cost, pia_brand_avg_curr_sku_average_cost, pia_avg_curr_sku_average_cost, global_avg_curr_sku_average_cost) AS curr_sku_average_cost
                            FROM golabl_avg_costs
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
                        if_search_term_facets as (
                          select distinct webstore, event_value, facet
                          from refined_search_event_base
                        ),
                        search_event_revenue as (
                          select
                            d.tran_date as date_key,
                            d._report_suite as webstore,
                            d._visits as session_id,
                            d._evar61 as sku,
                            d.product_id as ecode,
                            trim(regexp_replace(lower(trim(d._evar2)),"[^a-zA-Z0-9 ]", "")) as event_value,
                            d._revenue as gross_revenue,
                            d.units
                          from entdata.clk.dks_web_only d
                          where 
                          d.tran_date between to_date('{start_date}') and to_date('{end_date}')
                          and date(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(CAST(d.visit_start_time_gmt as bigint)), "America/New_York")) >= to_date('{start_date}')
                          and d._report_suite in ('dsg', 'pbl', 'gg')
                          and d.product_id not in ('Shipping', 'Tax')
                          and d._revenue is not null
                          and d._evar2 is not null
                          and d._visits in (select session_id from refined_search_event_base)
                        ),
                        search_event_facet_revenue as (
                          select
                            ser.*,
                            c.curr_sku_average_cost,
                            r.facet
                          from search_event_revenue ser
                          join refined_search_event_base r
                            on ser.session_id = r.session_id
                            and ser.event_value = r.event_value
                            and ser.webstore = r.webstore
                            and ser.date_key = r.date_key
                          join if_ecode_cost c
                            on ser.sku = c.sku
                        ),
                        search_event_facet_revenue_agg as(
                          select
                            date_key,
                            webstore,
                            event_value,
                            facet,
                            sum(units) as units,
                            sum(gross_revenue) as gross_revenue,
                            sum(curr_sku_average_cost) as cost
                          from search_event_facet_revenue
                          group by date_key, webstore, event_value, facet
                        ),
                        search_event_facet_revenue_all_dates as (
                          select
                            ad.date_est as date_key,
                            sf.webstore as webstore,
                            sf.event_value as event_value,
                            sf.facet as facet,
                            coalesce(sefr.units, 0) as units,
                            coalesce(sefr.gross_revenue, 0) as gross_revenue,
                            coalesce(sefr.cost, 0) as cost
                          from if_stg_all_dates ad
                          cross join if_search_term_facets sf
                          left join search_event_facet_revenue_agg sefr
                            on ad.date_est = sefr.date_key
                            and sf.webstore = sefr.webstore
                            and sf.event_value = sefr.event_value
                            and sf.facet = sefr.facet
                        ),
                        search_event_financials_calcs as (
                            select
                              date_key,
                              webstore,
                              event_value,
                              facet,
                              sum(units)
                                over(partition by webstore, event_value, facet order by date_key rows between {rolling_window} preceding and current row) as units_last_x_days,
                              sum(gross_revenue)
                                over(partition by webstore, event_value, facet order by date_key rows between {rolling_window} preceding and current row) as revenue_last_x_days,
                              sum(cost)
                                over(partition by webstore, event_value, facet order by date_key rows between {rolling_window} preceding and current row) as cost_last_x_days
                            from search_event_facet_revenue_all_dates
                        )
                          select
                            date_key,
                            webstore,
                            'SRLP' as event_type,
                            event_value,
                            facet,
                            ifnull(cost_last_x_days/units_last_x_days, 0) as avg_cost_last_x_days,
                            ifnull(revenue_last_x_days/units_last_x_days, 0) as avg_revenue_last_x_days,
                            ifnull(avg_revenue_last_x_days - avg_cost_last_x_days, 0) as avg_profit_last_x_days
                          from search_event_financials_calcs
                          where date_key = to_date('{end_date}')
                """)
              .select("*"))

        return df