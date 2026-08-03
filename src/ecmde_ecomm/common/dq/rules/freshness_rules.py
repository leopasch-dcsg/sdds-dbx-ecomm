from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import max as max_column

from ecmde_ecomm.common.dq.dq_config import DQFrameworkConfig, DQRuleConfig
from ecmde_ecomm.common.dq.dq_types import DQStatus
from ecmde_ecomm.common.dq.results import DQCheckResult
from ecmde_ecomm.common.dq.rules.base import DQRule


class FreshnessLagRule(DQRule):
    def __init__(self, rule: DQRuleConfig):
        self.rule = rule

    def evaluate(
        self,
        spark: SparkSession,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
    ) -> list[DQCheckResult]:
        row = dataframe.select(max_column(self.rule.freshness_column).alias("max_ts")).collect()[0]
        max_ts = row.max_ts

        if max_ts is None:
            lag_minutes = float("inf")
            status = DQStatus.FAIL
        else:
            now_utc = datetime.now(timezone.utc)
            lag_seconds = (now_utc - max_ts.replace(tzinfo=timezone.utc)).total_seconds()
            lag_minutes = lag_seconds / 60.0
            threshold = float(self.rule.freshness_minutes or 0)
            status = DQStatus.PASS if lag_minutes <= threshold else DQStatus.FAIL

        return [
            DQCheckResult(
                run_id=framework_config.run_id,
                domain=framework_config.domain,
                table_name=framework_config.table_name,
                rule_id=self.rule.rule_id,
                check_type=self.rule.check_type,
                status=status,
                severity=self.rule.severity,
                observed_value=lag_minutes,
                threshold_value=self.rule.freshness_minutes,
                message=f"Observed freshness lag is {lag_minutes} minutes.",
            )
        ]
