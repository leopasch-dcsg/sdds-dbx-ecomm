from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadConfig, LoadOperation
from pyspark.sql import SparkSession
from datetime import date
from ecmde_ecomm.common.errors import NotSupportedError
from ecmde_ecomm.search.ltr.atc_base_table_load import LTRATCBaseLoad
from ecmde_ecomm.search.ltr.atc_bod_price_table_load import LTRATCBODPriceTableLoad
from ecmde_ecomm.search.ltr.atc_global_agg_table_load import LTRATCGlobalAggLoad
from ecmde_ecomm.search.ltr.atc_search_lvl_agg_table_load import LTRATCSearchLvlAggLoad
from ecmde_ecomm.search.ltr.atc_prices_intermediary_table_load import LTRATCPricesIntermediaryLoad
from ecmde_ecomm.search.ltr.atc_term_dates_intermediary_table_load import LTRATCTermDatesIntermediaryLoad
from ecmde_ecomm.search.ltr.atc_z_score_table_load import LTRATCZScoreLoad
from ecmde_ecomm.search.ltr.ltr_feature_agg_table_load import LTRFeatureAggLoad
from ecmde_ecomm.search.ltr.abb_linked_searches_table_load import LTRABBLinkedSearchesLoad
from ecmde_ecomm.search.ltr.abb_impressions_agg_table_load import LTRABBImpressionsAggLoad
from ecmde_ecomm.search.ltr.abb_atc_rate_table_load import LTRABBATCRateLoad
from ecmde_ecomm.search.ltr.abb_ctr_table_load import LTRABBCTRLoad
from ecmde_ecomm.search.ltr.abb_order_rate_table_load import LTRABBOrderRateLoad
from ecmde_ecomm.search.ltr.abb_profit_orders_table_load import LTRABBProfitOrdersLoad
from ecmde_ecomm.search.ltr.abb_positive_profit_rate_table_load import LTRABBPositiveProfitRateLoad

class LTRLoadOperationProvider:

    __LTR_ATC_TERM_DATES_INTERMEDIARY_TABLE = ("ltr_atc_term_dates_intermediary")
    __LTR_ATC_PRICES_INTERMEDIARY_TABLE = ("ltr_atc_prices_intermediary")
    __LTR_ATC_BASE_TABLE = ("ltr_atc_base")
    __LTR_ATC_GLOBAL_AGG_TABLE = ("ltr_atc_global_agg")
    __LTR_ATC_SEARCH_TERM_AGG_TABLE = ("ltr_atc_search_lvl_agg")
    __LTR_ATC_BOD_PRICE_TABLE = ("ltr_atc_bod_price_data")
    __LTR_ATC_Z_SCORE_TABLE = ("ltr_atc_zscore")
    __LTR_FEATURE_AGG_TABLE = ("ltr_feature_agg")
    __LTR_ABB_LINKED_SEARCHES_TABLE = ("ltr_abb_linked_searches")
    __LTR_ABB_IMPRESSIONS_AGG_TABLE = ("ltr_abb_impressions_agg")
    __LTR_ABB_ATC_RATE_TABLE = ("ltr_abb_atc_rate")
    __LTR_ABB_CTR_TABLE = ("ltr_abb_ctr")
    __LTR_ABB_ORDER_RATE_TABLE = ("ltr_abb_order_rate")
    __LTR_ABB_PROFIT_ORDERS_TABLE = ("ltr_abb_profit_orders")
    __LTR_ABB_POSITIVE_PROFIT_RATE = ("ltr_abb_positive_profit_rate")

    def __init__(
            self,
            config: LoadConfig,
            spark: SparkSession,
            start_date_est: date | None = None,
            end_date_est: date | None = None,
    ):
        self.config = config
        self.spark = spark
        self.start_date_est = start_date_est
        self.end_date_est = end_date_est

    @staticmethod
    def provider(config: LoadConfig, spark: SparkSession, start_date_est: date | None, end_date_est: date| None):
        return LTRLoadOperationProvider(config, spark, start_date_est, end_date_est)

    def operation(self) -> LoadOperation:
        match self.config.destination_table:
            case self.__LTR_ATC_BASE_TABLE:
                return LTRATCBaseLoad(self.config, self.spark, self.start_date_est, self.end_date_est)
            case self.__LTR_ATC_GLOBAL_AGG_TABLE:
                return LTRATCGlobalAggLoad(self.config, self.spark)
            case self.__LTR_ATC_SEARCH_TERM_AGG_TABLE:
                return LTRATCSearchLvlAggLoad(self.config, self.spark)
            case self.__LTR_ATC_BOD_PRICE_TABLE:
                return LTRATCBODPriceTableLoad(self.config, self.spark, self.start_date_est, self.end_date_est)
            case self.__LTR_ATC_PRICES_INTERMEDIARY_TABLE:
                return LTRATCPricesIntermediaryLoad(self.config, self.spark, self.start_date_est, self.end_date_est)
            case self.__LTR_ATC_TERM_DATES_INTERMEDIARY_TABLE:
                return LTRATCTermDatesIntermediaryLoad(self.config, self.spark, self.start_date_est, self.end_date_est)
            case self.__LTR_ATC_Z_SCORE_TABLE:
                return LTRATCZScoreLoad(self.config, self.spark)
            case self.__LTR_FEATURE_AGG_TABLE:
                return LTRFeatureAggLoad(self.config, self.spark)
            case self.__LTR_ABB_LINKED_SEARCHES_TABLE:
                return LTRABBLinkedSearchesLoad(self.config, self.spark, self.start_date_est, self.end_date_est)
            case self.__LTR_ABB_IMPRESSIONS_AGG_TABLE:
                return LTRABBImpressionsAggLoad(self.config, self.spark)
            case self.__LTR_ABB_ATC_RATE_TABLE:
                return LTRABBATCRateLoad(self.config, self.spark)
            case self.__LTR_ABB_CTR_TABLE:
                return LTRABBCTRLoad(self.config, self.spark)
            case self.__LTR_ABB_ORDER_RATE_TABLE:
                return LTRABBOrderRateLoad(self.config, self.spark)
            case self.__LTR_ABB_PROFIT_ORDERS_TABLE:
                return LTRABBProfitOrdersLoad(self.config, self.spark)
            case self.__LTR_ABB_POSITIVE_PROFIT_RATE:
                return LTRABBPositiveProfitRateLoad(self.config, self.spark)
            case _:
                raise NotSupportedError(
                    f"The specified LTR destination table {self.config.destination_table_qualified()} is not supported. "
                )