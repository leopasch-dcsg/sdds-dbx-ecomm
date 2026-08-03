from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from ecmde_ecomm.common.dq.dq_config import DQFrameworkConfig
from ecmde_ecomm.common.dq.results import DQCheckResult


class DQRule(ABC):
    @abstractmethod
    def evaluate(
        self,
        spark: SparkSession,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
    ) -> list[DQCheckResult]:
        pass
