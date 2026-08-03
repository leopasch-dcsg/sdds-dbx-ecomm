from pyspark.sql import DataFrame, SparkSession

from ecmde_ecomm.common.dq.dq_config import DQFrameworkConfig, DQRuleConfig
from ecmde_ecomm.common.dq.dq_types import DQAggregationType
from ecmde_ecomm.common.dq.dq_types import DQStatus
from ecmde_ecomm.common.dq.results import DQCheckResult
from ecmde_ecomm.common.dq.rules.base import DQRule
from ecmde_ecomm.common.dq.rules.comparators import evaluate_status


class QuarantineCountRule(DQRule):
    def __init__(self, rule: DQRuleConfig):
        self.rule = rule

    def evaluate(
        self,
        spark: SparkSession,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
    ) -> list[DQCheckResult]:
        source_count = dataframe.count()
        quarantine_table_name = self._quarantine_table_name(framework_config)
        quarantine_count = spark.table(quarantine_table_name).count()

        observed_value, threshold, metric_label = self._metric_values(
            source_count,
            quarantine_count,
        )
        status = evaluate_status(observed_value, threshold, self.rule.comparator)

        return [
            DQCheckResult(
                run_id=framework_config.run_id,
                domain=framework_config.domain,
                table_name=framework_config.table_name,
                rule_id=self.rule.rule_id,
                check_type=self.rule.check_type,
                status=status,
                severity=self.rule.severity,
                observed_value=observed_value,
                threshold_value=threshold,
                message=(
                    f"Quarantine {metric_label} is {observed_value}. Quarantine table: "
                    f"{quarantine_table_name}."
                ),
            )
        ]

    def _metric_values(
        self,
        source_count: int,
        quarantine_count: int,
    ) -> tuple[float, float, str]:
        threshold = self.rule.threshold_value or 0.0

        if self.rule.aggregation_type == DQAggregationType.COUNT:
            return float(quarantine_count), threshold, "count"

        ratio = 0.0
        if source_count > 0:
            ratio = quarantine_count / source_count

        return ratio, threshold, "ratio"

    def _quarantine_table_name(self, framework_config: DQFrameworkConfig) -> str:
        configured = self.rule.quarantine_table_fqn
        if configured is not None and len(configured.strip()) > 0:
            return configured.strip()

        return f"{framework_config.source_table_fqn}_quarantine"
