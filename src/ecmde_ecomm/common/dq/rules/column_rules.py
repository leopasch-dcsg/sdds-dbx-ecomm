from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr, regexp_extract

from ecmde_ecomm.common.dq.dq_config import DQFrameworkConfig, DQRuleConfig
from ecmde_ecomm.common.dq.dq_types import DQStatus
from ecmde_ecomm.common.dq.results import DQCheckResult
from ecmde_ecomm.common.dq.rules.base import DQRule
from ecmde_ecomm.common.dq.rules.comparators import evaluate_status


class NullThresholdRule(DQRule):
    def __init__(self, rule: DQRuleConfig):
        self.rule = rule

    def evaluate(
        self,
        spark: SparkSession,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
    ) -> list[DQCheckResult]:
        total_rows = dataframe.count()
        null_rows = dataframe.filter(col(self.rule.column_name).isNull()).count()

        ratio = 0.0
        if total_rows > 0:
            ratio = null_rows / total_rows

        threshold = self.rule.threshold_value or 0.0
        status = evaluate_status(ratio, threshold, self.rule.comparator)

        return [
            DQCheckResult(
                run_id=framework_config.run_id,
                domain=framework_config.domain,
                table_name=framework_config.table_name,
                rule_id=self.rule.rule_id,
                check_type=self.rule.check_type,
                status=status,
                severity=self.rule.severity,
                observed_value=ratio,
                threshold_value=threshold,
                message=f"Null ratio for {self.rule.column_name} is {ratio}.",
            )
        ]


class PatternThresholdRule(DQRule):
    def __init__(self, rule: DQRuleConfig):
        self.rule = rule

    def evaluate(
        self,
        spark: SparkSession,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
    ) -> list[DQCheckResult]:
        total_rows = dataframe.count()

        invalid_rows = (
            dataframe.filter(col(self.rule.column_name).isNotNull())
            .filter(regexp_extract(col(self.rule.column_name), self.rule.pattern, 0) == "")
            .count()
        )

        ratio = 0.0
        if total_rows > 0:
            ratio = invalid_rows / total_rows

        threshold = self.rule.threshold_value or 0.0
        status = evaluate_status(ratio, threshold, self.rule.comparator)

        return [
            DQCheckResult(
                run_id=framework_config.run_id,
                domain=framework_config.domain,
                table_name=framework_config.table_name,
                rule_id=self.rule.rule_id,
                check_type=self.rule.check_type,
                status=status,
                severity=self.rule.severity,
                observed_value=ratio,
                threshold_value=threshold,
                message=f"Pattern invalid ratio for {self.rule.column_name} is {ratio}.",
            )
        ]


class ConditionThresholdRule(DQRule):
    def __init__(self, rule: DQRuleConfig):
        self.rule = rule

    def evaluate(
        self,
        spark: SparkSession,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
    ) -> list[DQCheckResult]:
        total_rows = dataframe.count()
        violating_rows = dataframe.filter(expr(self.rule.condition_expr)).count()

        ratio = 0.0
        if total_rows > 0:
            ratio = violating_rows / total_rows

        threshold = self.rule.threshold_value or 0.0
        status = evaluate_status(ratio, threshold, self.rule.comparator)

        return [
            DQCheckResult(
                run_id=framework_config.run_id,
                domain=framework_config.domain,
                table_name=framework_config.table_name,
                rule_id=self.rule.rule_id,
                check_type=self.rule.check_type,
                status=status,
                severity=self.rule.severity,
                observed_value=ratio,
                threshold_value=threshold,
                message=(
                    f"Condition violation ratio for rule {self.rule.rule_id} is {ratio}."
                ),
            )
        ]
