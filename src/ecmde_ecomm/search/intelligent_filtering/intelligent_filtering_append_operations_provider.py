from pyspark.sql import SparkSession
from ecmde_ecomm.common import Logger, NotSupportedError, AppendOperation, AppendOperationBuilder
from ecmde_ecomm.search.intelligent_filtering import (
    SearchEventCountsAndProbabilities,
    SearchEventFinancials,
    SearchEventCTR,
    BrowseEventCountsAndProbabilities,
    IntelligentFilteringMetricsModelAppend
)


class IntelligentFilteringAppendOperationsBuilder(AppendOperationBuilder):

    __IF_SEARCH_EVENT_COUNTS_AND_PROBABILITIES = "if_search_event_counts_probabilities"
    __IF_SEARCH_EVENT_FINANCIALS = "if_search_event_financials"
    __IF_SEARCH_EVENT_CTR = "if_search_event_ctr"
    __IF_BROWSE_EVENT_COUNTS_AND_PROBABILITIES = "if_browse_event_counts_probabilities"
    __IF_EVENT_METRICS_MODEL = "if_event_metrics_model"

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self._logger = Logger.logger(__class__.__name__)

    @staticmethod
    def builder(spark):
        return IntelligentFilteringAppendOperationsBuilder(spark)

    def operation(self) -> AppendOperation:
        self._logger.info(
            f"Building Append Operation for Intelligent Filtering Table: {self.dbx_destination.table}"
        )

        match self.dbx_destination.table:
            case self.__IF_SEARCH_EVENT_COUNTS_AND_PROBABILITIES:
                return SearchEventCountsAndProbabilities(
                    self.spark,
                    self.dbx_source,
                    self.dbx_destination,
                    self.date_info
                )
            case self.__IF_SEARCH_EVENT_FINANCIALS:
                return SearchEventFinancials(
                    self.spark,
                    self.dbx_source,
                    self.dbx_destination,
                    self.date_info
                )
            case self.__IF_SEARCH_EVENT_CTR:
                return SearchEventCTR(
                    self.spark,
                    self.dbx_source,
                    self.dbx_destination,
                    self.date_info
                )
            case self.__IF_BROWSE_EVENT_COUNTS_AND_PROBABILITIES:
                return BrowseEventCountsAndProbabilities(
                    self.spark,
                    self.dbx_source,
                    self.dbx_destination,
                    self.date_info
                )
            case self.__IF_EVENT_METRICS_MODEL:
                return IntelligentFilteringMetricsModelAppend(
                    self.spark,
                    self.dbx_source,
                    self.dbx_destination,
                    self.date_info
                )
            case _:
                raise NotSupportedError(
                    f"The specified databricks table {self.dbx_destination.table} is not supported."
                )