from dataclasses import dataclass
from datetime import date, datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    asc,
    col,
    count_distinct,
    sum,
    nvl,
    lit,
)
from pyspark.sql.types import DecimalType, LongType

from ecmde_ecomm.common.errors import ExpectationNotMetError
from ecmde_ecomm.fulfillment.demand.model.dmd_decomp_rpt import FiscalDates
from ecmde_ecomm.fulfillment.model.channel_code import ChannelCode
from ecmde_ecomm.fulfillment.model.trans_type import TransType


@dataclass
class DemandDecompReportRepositoryConf:
    ecmde_silver_catalog: str
    ecmde_silver_schema: str
    entdata_catalog: str
    user_principal: str
    ddw_catalog: str
    ecomp_catalog: str
    ecom_dim_schema: str

@dataclass(frozen=True)
class DemandDecompReportDataSources:
    open_demand: DataFrame
    fulfilled: DataFrame
    cancels: DataFrame
    designated_demand: DataFrame


class DemandDecompReportRepository:
    def __init__(self, conf: DemandDecompReportRepositoryConf, spark: SparkSession):
        self.__conf = conf
        self.__spark = spark
        self.demand_indicator_flag_true = 1
        self.ship_confirmed_status = 36
        self.fulfilled_status_code = "F"
        self.presale_flg_true = 1
        self.fraud_pay_reason_code = "FP"
        self.customer_changed_mind_reason_code = "CT"
        self.inventory_reason_code = "INV"
        self.other_reason_code = "OTH"

    @property
    def _ecomp_dim_base(self) -> str:
        return f"{self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}"

    def date_range(self, start_date_etc: date) -> FiscalDates:

        start_ts = start_date_etc.strftime("%Y-%m-%d %H:%M:%S")
        date_dim = f"{self._ecomp_dim_base}.date_dim"

        query = f"""
            WITH base AS (
              SELECT *
              FROM {date_dim}
              WHERE calendar_date = to_timestamp('{start_ts}', 'yyyy-MM-dd HH:mm:ss')
            ),
            ty_start AS (
              SELECT date_id AS date_id_ty_start
              FROM {date_dim}
              WHERE date_id = (SELECT MIN(fiscal_year_begin_date) FROM base)
            ),
            ty_end AS (
              SELECT date_id AS date_id_ty_end
              FROM {date_dim}
              WHERE date_id = (SELECT date_id FROM base)
            ),
            ly_base AS (
              SELECT *
              FROM {date_dim}
              WHERE date_id = (SELECT date_id_ly FROM base)
            ),
            ly_start AS (
              SELECT date_id AS date_id_ly_start
              FROM {date_dim}
              WHERE date_id = (SELECT MIN(fiscal_year_begin_date) FROM ly_base)
            ),
            ly_end AS (
              SELECT date_id AS date_id_ly_end
              FROM {date_dim}
              WHERE date_id = (SELECT date_id FROM ly_base)
            )
            SELECT
              (SELECT date_id_ty_start FROM ty_start) AS date_id_ty_start,
              (SELECT date_id_ty_end   FROM ty_end)   AS date_id_ty_end,
              (SELECT date_id_ly_start FROM ly_start) AS date_id_ly_start,
              (SELECT date_id_ly_end   FROM ly_end)   AS date_id_ly_end
            """

        rows = self.__spark.sql(query).collect()
        return FiscalDates(rows[0].date_id_ly_start, rows[0].date_id_ly_end, rows[0].date_id_ty_start,
                           rows[0].date_id_ty_end)

    def open_demand_aggregate(self, fiscal_dates: FiscalDates) -> DataFrame:
        curr_alloc = self._open_demand_current_allocations(fiscal_dates)
        sku_shipments = self._open_demand_sku_shipments(fiscal_dates)
        fulfilled_orders = self._open_demand_fulfilled_orders(fiscal_dates)

        open_dmd = (
            curr_alloc.unionAll(sku_shipments)
            .unionAll(fulfilled_orders)
            .groupBy("order_date_key")
            .agg(
                sum("orig_tot_extended_amt").alias("open_order_amt"),
                count_distinct("order_header_key").alias("open_order_count"),
            )
            .select(
                col("order_date_key"),
                col("open_order_amt"),
                col("open_order_count"),
            )
        ).cache()

        yesterday_data = open_dmd.filter(
            col("order_date_key") == fiscal_dates.date_id_ty_end
        )
        if yesterday_data.isEmpty():
            raise ExpectationNotMetError(
                f"The open demand data set does not contain data for the required date range. Date ID TY End: {fiscal_dates.date_id_ty_end}"
            )

        return open_dmd

    def _open_demand_current_allocations(self, fiscal_dates: FiscalDates) -> DataFrame:
            base = self._ecomp_dim_base
            query = f"""
              select curr_alloc.order_date_key as order_date_key,
                     curr_alloc.order_header_key as order_header_key,
                     curr_alloc.extended_amt as orig_tot_extended_amt
                from {base}.order_sku_curr_alloc curr_alloc
                join {base}.order_header order_header
                  on curr_alloc.order_header_key = order_header.order_header_key
               where curr_alloc.units > 0
                 and order_header.demand_ind = {self.demand_indicator_flag_true}
                 and curr_alloc.order_date_key >= {fiscal_dates.date_id_ly_start}
            """
            return self.__spark.sql(query).cache()

    def _open_demand_sku_shipments(self, fiscal_dates: FiscalDates) -> DataFrame:

        base = self._ecomp_dim_base
        query = f"""
          select order_sku.order_date_key,
                 order_sku.order_header_key,
                 order_sku.orig_tot_extended_amt,
                 order_sku.orig_tot_units,
                 sku_shipment.shipped_units
            from {base}.order_sku_shipment sku_shipment
            join {base}.order_sku order_sku
              on sku_shipment.order_sku_key = order_sku.order_sku_key
            join {base}.order_header order_header
              on order_sku.order_header_key = order_header.order_header_key
           where order_sku.order_date_key >= {fiscal_dates.date_id_ly_start}
             and sku_shipment.fulfillment_date_key >= {fiscal_dates.date_id_ly_start}
             and order_header.demand_ind = {self.demand_indicator_flag_true}
             and order_sku.order_sku_status_key = {self.ship_confirmed_status}
             and sku_shipment.posted_date_key is null
        """

        sku_shipments = self.__spark.sql(query)

        return (
            sku_shipments
            .withColumn("orig_tot_units", nvl(col("orig_tot_units"), lit(0)))
            .withColumn(
                "orig_tot_extended_amt_recalculated",
                (col("orig_tot_extended_amt") / col("orig_tot_units")) * col("shipped_units"),
            )
            .select(
                col("order_date_key"),
                col("order_header_key"),
                col("orig_tot_extended_amt_recalculated").alias("orig_tot_extended_amt"),
            )
        ).cache()

    def _open_demand_fulfilled_orders(self, fiscal_dates: FiscalDates) -> DataFrame:
        base = self._ecomp_dim_base
        query = f"""
              select order_sku.order_date_key,
                     order_sku.order_header_key,
                     order_sku.orig_tot_extended_amt
                from {base}.order_sku order_sku
                join {base}.order_fulfill order_fulfill
                  on order_sku.web_ord_num = order_fulfill.web_ord_num
                 and order_sku.chain_key = order_fulfill.chain_key
                join {base}.order_header order_header
                  on order_sku.order_header_key = order_header.order_header_key
               where order_sku.order_date_key >= {fiscal_dates.date_id_ly_start}
                 and order_fulfill.fulfillment_date_key >= {fiscal_dates.date_id_ly_start}
                 and order_header.demand_ind = {self.demand_indicator_flag_true}
                 and order_sku.order_sku_status_key = {self.ship_confirmed_status}
                 and order_fulfill.channel_type_key = {ChannelCode.BOPIS.key}
                 and order_fulfill.fulfillment_status_cd = '{self.fulfilled_status_code}'
            """
        return self.__spark.sql(query).cache()

    def fulfilled_aggregate(self, fiscal_dates: FiscalDates) -> DataFrame:
        sku_active_record_status = "A"
        moosejaw_chain_key = 9
        backstock_location_code = "BS"
        query = f"""
                with base_ff_data as (
                  select txn_sku.txn_date_key,
                        order_header.order_date_key,
                        order_fulfill.fulfillment_date_key,
                        order_fulfill.fulfillment_location_cd,
                        order_fulfill.channel_type_key,
                        locations.location_type_cd,
                        txn_sku.trans_type_key,
                        txn_sku.extended_amt,
                        txn_sku.order_header_key,
                        txn_sku.order_fulfill_key
                    from {self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}.txn_order_sku txn_sku
                    join {self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}.order_fulfill order_fulfill
                      on txn_sku.order_fulfill_key = order_fulfill.order_fulfill_key
                     and (
                        (txn_sku.txn_date_key between {fiscal_dates.date_id_ty_start} and {fiscal_dates.date_id_ty_end}) or
                        (txn_sku.txn_date_key between {fiscal_dates.date_id_ly_start} and {fiscal_dates.date_id_ly_end})
                     )
                    join {self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}.order_header order_header
                      on txn_sku.order_header_key = order_header.order_header_key
                    left join {self.__conf.entdata_catalog}.loc.location locations
                      on order_fulfill.fulfillment_location_cd = locations.location_number
                   where upper(txn_sku.record_status) = '{sku_active_record_status}'
                     and order_fulfill.chain_key != {moosejaw_chain_key}
                     and txn_sku.order_header_key != -1
                ),
                ff_data as (
                  select bd.txn_date_key,
                         datediff(
                             case when bd.txn_date_key > 0 then to_date(cast(bd.txn_date_key as string), 'yyyyMMdd') end,
                             case when bd.order_date_key > 0 then to_date(cast(bd.order_date_key as string), 'yyyyMMdd') end
                         ) fulfillment_age_days,
                         bd.order_header_key,
                         case
                            when bd.channel_type_key = {ChannelCode.DC.key} and bd.location_type_cd = '{backstock_location_code}' then {ChannelCode.RDC.key} else bd.channel_type_key
                         end as channel_type_key,
                         bd.trans_type_key,
                         bd.extended_amt
                    from base_ff_data bd
                )
                select ff.txn_date_key,
                       count(distinct case when ff.trans_type_key = {TransType.Fulfilled.key} then ff.order_header_key else null end) co_count,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} then ff.extended_amt else 0 end) fulfilled_amt_by_fulfill_date,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.channel_type_key = {ChannelCode.SFS.key} then ff.extended_amt else 0 end) sfs_fulfilled_amt,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.channel_type_key = {ChannelCode.DC.key} then ff.extended_amt else 0 end) dc_fulfilled_amt,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.channel_type_key = {ChannelCode.RDC.key} then ff.extended_amt else 0 end) rdc_fulfilled_amt,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.channel_type_key = {ChannelCode.VDC.key} then ff.extended_amt else 0 end) vdc_fulfilled_amt,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.channel_type_key = {ChannelCode.BOPIS.key} then ff.extended_amt else 0 end) bopis_fulfilled_amt,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.channel_type_key = {ChannelCode.BOPL.key} then ff.extended_amt else 0 end) bopl_fulfilled_amt,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.channel_type_key = {ChannelCode.UNKNOWN.key} then ff.extended_amt else 0 end) unk_fulfilled_amt,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.fulfillment_age_days = 0 then ff.extended_amt else 0 end) fulfilled_sales_age_days_0,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.fulfillment_age_days = 1 then ff.extended_amt else 0 end) fulfilled_sales_age_days_1,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.fulfillment_age_days = 2 then ff.extended_amt else 0 end) fulfilled_sales_age_days_2,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.fulfillment_age_days = 3 then ff.extended_amt else 0 end) fulfilled_sales_age_days_3,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.fulfillment_age_days = 4 then ff.extended_amt else 0 end) fulfilled_sales_age_days_4,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.fulfillment_age_days = 5 then ff.extended_amt else 0 end) fulfilled_sales_age_days_5,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.fulfillment_age_days = 6 then ff.extended_amt else 0 end) fulfilled_sales_age_days_6,
                       sum(case when ff.trans_type_key = {TransType.Fulfilled.key} and ff.fulfillment_age_days >= 7 then ff.extended_amt else 0 end) fulfilled_sales_age_days_7,
                       sum(case when ff.trans_type_key = {TransType.Returned.key} then ff.extended_amt else 0 end) return_amt,
                       sum(case when ff.trans_type_key = {TransType.PriceAdjustment.key} then ff.extended_amt else 0 end) post_order_adjustment_amt
                  from ff_data ff
                group by ff.txn_date_key
                order by ff.txn_date_key;
                """
        fulfilled = self.__spark.sql(query).cache()
        yesterday_data = fulfilled.filter(
            col("txn_date_key") == fiscal_dates.date_id_ty_end
        )
        if yesterday_data.isEmpty():
            raise ExpectationNotMetError(
                f"The fulfilled data set does not contain data for the required date range. Date ID TY End: {fiscal_dates.date_id_ty_end}"
            )

        return fulfilled

    def cancels_aggregate(self, fiscal_dates: FiscalDates) -> DataFrame:
        query = f"""
                select order_header.order_date_key, 
                       sum(case when txn_order_sku.trans_type_key = {TransType.Fulfilled.key} then txn_order_sku.extended_amt else 0 end) as fulfilled_amt_by_order_date,
                       count(distinct case when txn_order_sku.trans_type_key = {TransType.Fulfilled.key} then order_header.order_header_key else null end) as fulfilled_orders_by_fulfill_date,
                       sum(case when txn_order_sku.trans_type_key = {TransType.Cancelled.key} and reasons.reason_category_desc = 'FRAUD / PAY AUTH' then txn_order_sku.extended_amt else 0 end) as fraud_cancel_amt,
                       sum(case when txn_order_sku.trans_type_key = {TransType.Cancelled.key} and reasons.reason_category_desc = 'CUSTOMER CHANGED MIND' then txn_order_sku.extended_amt else 0 end) as athlete_cancel_amt,
                       sum(case when txn_order_sku.trans_type_key = {TransType.Cancelled.key} and reasons.reason_category_desc = 'INVENTORY' then txn_order_sku.extended_amt else 0 end) as retailer_cancel_amt,
                       sum(case when txn_order_sku.trans_type_key = {TransType.Cancelled.key} and (reasons.reason_category_desc = 'OTHER' or reasons.reason_category_desc = 'UNKNOWN') then txn_order_sku.extended_amt else 0 end) as other_cancel_amt
                  from {self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}.txn_order_sku txn_order_sku
                  left join {self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}.reason reasons
                    on txn_order_sku.reason_key = reasons.reason_key
                   and reasons.is_current = 'C'
                  join {self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}.order_header order_header
                    on txn_order_sku.order_header_key = order_header.order_header_key
                 where order_header.demand_ind = {self.demand_indicator_flag_true}
                   and txn_order_sku.trans_type_key in ({TransType.Cancelled.key}, {TransType.Fulfilled.key})
                   and (
                        (order_header.order_date_key between {fiscal_dates.date_id_ty_start} and {fiscal_dates.date_id_ty_end}) or
                        (order_header.order_date_key between {fiscal_dates.date_id_ly_start} and {fiscal_dates.date_id_ly_end})
                   )
                   and (
                        (txn_order_sku.txn_date_key between {fiscal_dates.date_id_ty_start} and {fiscal_dates.date_id_ty_end}) or
                        (txn_order_sku.txn_date_key between {fiscal_dates.date_id_ly_start} and {fiscal_dates.date_id_ly_end})
                     )
                group by order_header.order_date_key
                """

        cancels = self.__spark.sql(query).cache()
        yesterday_data = cancels.filter(
            col("order_date_key") == fiscal_dates.date_id_ty_end
        )
        if yesterday_data.isEmpty():
            raise ExpectationNotMetError(
                f"The cancels data set does not contain data for the required date range. Date ID TY End: {fiscal_dates.date_id_ty_end}"
            )

        return cancels

    def designated_aggregate(self, fiscal_dates: FiscalDates) -> DataFrame:
        query = f"""
                select order_sku.order_date_key,
                       sum(order_sku.orig_tot_extended_amt) as designated_demand_amt,
                       count(distinct order_header.order_header_key) as co_count,
                       count(distinct case when order_sku.presale_flg = {self.presale_flg_true} then order_header.order_header_key else null end) as presale_co_count,
                       sum(case when order_sku.presale_flg = {self.presale_flg_true} then order_sku.orig_tot_extended_amt else 0 end) as presale_demand_amt,
                       sum(case when order_sku.channel_type_key = {ChannelCode.SFS.key} then order_sku.orig_tot_extended_amt else 0 end) as sfs_demand_sales_amt,
                       sum(case when order_sku.channel_type_key = {ChannelCode.DC.key} then order_sku.orig_tot_extended_amt else 0 end) as dc_demand_sales_amt,
                       sum(case when order_sku.channel_type_key = {ChannelCode.VDC.key} then order_sku.orig_tot_extended_amt else 0 end) as vdc_demand_sales_amt,
                       sum(case when order_sku.channel_type_key = {ChannelCode.BOPL.key} then order_sku.orig_tot_extended_amt else 0 end) as bopl_demand_sales_amt,
                       sum(case when order_sku.channel_type_key = {ChannelCode.BOPIS.key} then order_sku.orig_tot_extended_amt else 0 end) as bopis_demand_sales_amt,
                       sum(case when order_sku.channel_type_key = {ChannelCode.RDC.key} then order_sku.orig_tot_extended_amt else 0 end) as rdc_demand_sales_amt,
                       sum(case when order_sku.channel_type_key = {ChannelCode.MULTI.key} then order_sku.orig_tot_extended_amt else 0 end) as multi_demand_sales_amt,
                       sum(case when order_sku.channel_type_key = {ChannelCode.UNKNOWN.key} then order_sku.orig_tot_extended_amt else 0 end) as unk_demand_sales_amt
                  from {self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}.order_sku order_sku
                  join {self.__conf.ecomp_catalog}.{self.__conf.ecom_dim_schema}.order_header order_header
                    on order_sku.order_header_key = order_header.order_header_key
                 where order_header.demand_ind = {self.demand_indicator_flag_true}
                   and (
                        (order_header.order_date_key between {fiscal_dates.date_id_ty_start} and {fiscal_dates.date_id_ty_end}) or
                        (order_header.order_date_key between {fiscal_dates.date_id_ly_start} and {fiscal_dates.date_id_ly_end})
                   )
                group by order_sku.order_date_key
                """

        designated = self.__spark.sql(query).cache()
        yesterday_data = designated.filter(
            col("order_date_key") == fiscal_dates.date_id_ty_end
        )
        if yesterday_data.isEmpty():
            raise ExpectationNotMetError(
                f"The designated demand data set does not contain data for the required date range. Date ID TY End: {fiscal_dates.date_id_ty_end}"
            )

        return designated

    def demand_decomp_report(
        self,
        fiscal_dates: FiscalDates,
        batch_date_utc: datetime,
        data_sources: DemandDecompReportDataSources,
    ) -> DataFrame:
        designated_demand_agg = data_sources.designated_demand.alias("designated_dmd")
        cancel_agg = data_sources.cancels.alias("cancel_agg")
        fulfilled_agg = data_sources.fulfilled.alias("fulfilled_agg")
        open_dmd_agg = data_sources.open_demand.alias("open_dmd_agg")

        designated_row_count = designated_demand_agg.count()
        cancel_row_count = cancel_agg.count()
        fulfilled_row_count = fulfilled_agg.count()

        if not (designated_row_count == cancel_row_count == fulfilled_row_count):
            raise ExpectationNotMetError(
                f"The row counts between each of the primary data sets do not match. Designated Row Count: {designated_row_count}, Cancels Row Count: {cancel_row_count}, Fulfilled Row Count: {fulfilled_row_count}"
            )

        report_df = (
            designated_demand_agg.join(
                fulfilled_agg,
                designated_demand_agg.order_date_key == fulfilled_agg.txn_date_key,
                "left",
            )
            .join(
                cancel_agg,
                designated_demand_agg.order_date_key == cancel_agg.order_date_key,
                "left",
            )
            .join(
                open_dmd_agg,
                designated_demand_agg.order_date_key == open_dmd_agg.order_date_key,
                "left",
            )
            .select(
                col("designated_dmd.order_date_key").alias("date_key"),
                # Designated Demand Block
                nvl("designated_dmd.designated_demand_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("designated_demand_amt"),
                nvl("designated_dmd.presale_demand_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("presale_demand_amt"),
                nvl("designated_dmd.sfs_demand_sales_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("sfs_demand_sales_amt"),
                nvl("designated_dmd.dc_demand_sales_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("dc_demand_sales_amt"),
                nvl("designated_dmd.rdc_demand_sales_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("rdc_demand_sales_amt"),
                nvl("designated_dmd.vdc_demand_sales_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("vdc_demand_sales_amt"),
                nvl("designated_dmd.bopl_demand_sales_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("bopl_demand_sales_amt"),
                nvl("designated_dmd.bopis_demand_sales_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("bopis_demand_sales_amt"),
                nvl("designated_dmd.multi_demand_sales_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("multi_demand_sales_amt"),
                nvl("designated_dmd.unk_demand_sales_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("unk_demand_sales_amt"),
                nvl("designated_dmd.co_count", lit(0))
                .cast(LongType())
                .alias("co_count"),
                nvl("designated_dmd.presale_co_count", lit(0))
                .cast(LongType())
                .alias("presale_co_count"),
                # Cancels Aggregate Block
                nvl("cancel_agg.fulfilled_amt_by_order_date", lit(0))
                .cast(DecimalType(38, 10))
                .alias(
                    "fulfilled_amt_by_order_date"
                ),  # Column name is good. Comment is bad, column location in table makes no sense. This is by order date and not action.
                nvl("cancel_agg.fulfilled_orders_by_fulfill_date", lit(0))
                .cast(LongType())
                .alias(
                    "fulfilled_orders_by_fulfill_date"
                ),  # This is really fulfilled orders by order date, not fulfill date. We need to address this in column comments and potentially rename the column in the final table.
                nvl("cancel_agg.fraud_cancel_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fraud_cancel_amt"),
                nvl("cancel_agg.athlete_cancel_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("athlete_cancel_amt"),
                nvl("cancel_agg.retailer_cancel_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("retailer_cancel_amt"),
                nvl("cancel_agg.other_cancel_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("other_cancel_amt"),
                # Fulfilled Aggregate Block
                nvl("fulfilled_agg.fulfilled_amt_by_fulfill_date", lit(0))
                .cast(DecimalType(38, 10))
                .alias(
                    "fulfilled_amt_by_fulfill_date"
                ),  # Column name okay, comment bad, location in table not good.
                nvl("fulfilled_agg.co_count", lit(0))
                .cast(LongType())
                .alias(
                    "fulfilled_co_count"
                ),  # This is fulfilled co count by transaction date (aka action date). Column name is good but column comment is bad, also location in table makes no sense.
                nvl("fulfilled_agg.fulfilled_sales_age_days_0", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fulfilled_sales_age_days_0"),
                nvl("fulfilled_agg.fulfilled_sales_age_days_1", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fulfilled_sales_age_days_1"),
                nvl("fulfilled_agg.fulfilled_sales_age_days_2", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fulfilled_sales_age_days_2"),
                nvl("fulfilled_agg.fulfilled_sales_age_days_3", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fulfilled_sales_age_days_3"),
                nvl("fulfilled_agg.fulfilled_sales_age_days_4", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fulfilled_sales_age_days_4"),
                nvl("fulfilled_agg.fulfilled_sales_age_days_5", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fulfilled_sales_age_days_5"),
                nvl("fulfilled_agg.fulfilled_sales_age_days_6", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fulfilled_sales_age_days_6"),
                nvl("fulfilled_agg.fulfilled_sales_age_days_7", lit(0))
                .cast(DecimalType(38, 10))
                .alias("fulfilled_sales_age_days_7"),
                nvl("fulfilled_agg.sfs_fulfilled_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("sfs_fulfilled_amt"),
                nvl("fulfilled_agg.dc_fulfilled_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("dc_fulfilled_amt"),
                nvl("fulfilled_agg.rdc_fulfilled_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("rdc_fulfilled_amt"),
                nvl("fulfilled_agg.vdc_fulfilled_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("vdc_fulfilled_amt"),
                nvl("fulfilled_agg.bopis_fulfilled_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("bopis_fulfilled_amt"),
                nvl("fulfilled_agg.bopl_fulfilled_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("bopl_fulfilled_amt"),
                nvl("fulfilled_agg.unk_fulfilled_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("unassigned_fulfilled_amt"),
                nvl("fulfilled_agg.return_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("return_amt"),
                nvl("fulfilled_agg.post_order_adjustment_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("post_order_adjustment_amt"),
                # Open Demand Block
                nvl("open_dmd_agg.open_order_amt", lit(0))
                .cast(DecimalType(38, 10))
                .alias("open_order_amt"),
                nvl("open_dmd_agg.open_order_count", lit(0))
                .cast(LongType())
                .alias("open_order"),
                lit(batch_date_utc).alias("silver_created_on_utc"),
                lit(self.__conf.user_principal).alias("silver_created_by"),
            )
            .sort(asc("date_key"))
            .cache()
        )

        yesterday_data = report_df.filter(
            col("date_key") == fiscal_dates.date_id_ty_end
        )
        if yesterday_data.isEmpty():
            raise ExpectationNotMetError(
                f"The demand decomp report does not contain data for the required date range. Date ID TY End: {fiscal_dates.date_id_ty_end}"
            )

        report_row_count = report_df.count()
        if report_row_count != designated_row_count:
            raise ExpectationNotMetError(
                f"The final compiled report row count does not match the expected row count from the primary data sets. Report Row Count: {report_row_count}, Expected Row Count: {designated_row_count}"
            )

        return report_df

    def save(self, report_df: DataFrame):
        table_name = f"{self.__conf.ecmde_silver_catalog}.{self.__conf.ecmde_silver_schema}.demand_fulfill"

        self.__spark.sql(f"truncate table {table_name}")
        report_df.write.format("delta").mode("append").saveAsTable(table_name)
