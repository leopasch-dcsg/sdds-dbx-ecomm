from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    count_distinct,
    expr,
    stddev_samp,
    sum as sum_column,
)

from ecmde_ecomm.common.dq.dq_config import DQFrameworkConfig, DQRuleConfig
from ecmde_ecomm.common.dq.dq_types import DQAggregationType, DQStatus
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.dq.results import DQCheckResult
from ecmde_ecomm.common.dq.rules.base import DQRule
from ecmde_ecomm.common.dq.rules.comparators import evaluate_status


class AggregateThresholdRule(DQRule):
    def __init__(self, rule: DQRuleConfig):
        self.rule = rule

    def evaluate(
        self,
        spark: SparkSession,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
    ) -> list[DQCheckResult]:
        aggregation_expr = self._build_aggregation_expression()
        group_columns = self.rule.group_by_columns

        if len(group_columns) > 0:
            observed_rows = (
                dataframe.groupBy(*group_columns)
                .agg(aggregation_expr.alias("observed_value"))
                .collect()
            )
            results = []
            for row in observed_rows:
                observed_value = row.observed_value
                threshold = self.rule.threshold_value or 0.0
                status = evaluate_status(float(observed_value), threshold, self.rule.comparator)
                dimension_key = "|".join([str(row[group]) for group in group_columns])
                results.append(
                    DQCheckResult(
                        run_id=framework_config.run_id,
                        domain=framework_config.domain,
                        table_name=framework_config.table_name,
                        rule_id=self.rule.rule_id,
                        check_type=self.rule.check_type,
                        status=status,
                        severity=self.rule.severity,
                        observed_value=float(observed_value),
                        threshold_value=threshold,
                        dimension_key=dimension_key,
                        message=(
                            f"Aggregate {self.rule.aggregation_type.value} observed "
                            f"{observed_value} for {dimension_key}."
                        ),
                    )
                )
            return results

        observed_value = dataframe.agg(aggregation_expr.alias("observed_value")).collect()[0][
            "observed_value"
        ]
        threshold = self.rule.threshold_value or 0.0
        status = evaluate_status(float(observed_value), threshold, self.rule.comparator)

        return [
            DQCheckResult(
                run_id=framework_config.run_id,
                domain=framework_config.domain,
                table_name=framework_config.table_name,
                rule_id=self.rule.rule_id,
                check_type=self.rule.check_type,
                status=status,
                severity=self.rule.severity,
                observed_value=float(observed_value),
                threshold_value=threshold,
                message=(
                    f"Aggregate {self.rule.aggregation_type.value} observed value is {observed_value}."
                ),
            )
        ]

    def _build_aggregation_expression(self):
        if self.rule.aggregation_type == DQAggregationType.COUNT:
            return count(expr("1"))
        if self.rule.aggregation_type == DQAggregationType.COUNT_DISTINCT:
            return count_distinct(col(self.rule.aggregation_column))
        if self.rule.aggregation_type == DQAggregationType.SUM:
            return sum_column(col(self.rule.aggregation_column))
        if self.rule.aggregation_type == DQAggregationType.AVG:
            return avg(col(self.rule.aggregation_column))
        if self.rule.aggregation_type == DQAggregationType.MEDIAN:
            return expr(f"percentile_approx({self.rule.aggregation_column}, 0.5)")
        if self.rule.aggregation_type == DQAggregationType.MODE:
            return expr(
                f"mode() within group (order by {self.rule.aggregation_column})"
            )
        if self.rule.aggregation_type == DQAggregationType.STDDEV:
            return stddev_samp(col(self.rule.aggregation_column))

        return count(expr("1"))


class ThreeSigmaRule(DQRule):
    def __init__(self, rule: DQRuleConfig):
        self.rule = rule

    def evaluate(
        self,
        spark: SparkSession,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
    ) -> list[DQCheckResult]:
        sigma_level = self._sigma_level()

        stats = dataframe.agg(
            avg(col(self.rule.aggregation_column)).alias("mean_value"),
            stddev_samp(col(self.rule.aggregation_column)).alias("stddev_value"),
            count(expr("1")).alias("row_count"),
        ).collect()[0]

        if stats.row_count < 30 or stats.stddev_value is None:
            return [
                DQCheckResult(
                    run_id=framework_config.run_id,
                    domain=framework_config.domain,
                    table_name=framework_config.table_name,
                    rule_id=self.rule.rule_id,
                    check_type=self.rule.check_type,
                    status=DQStatus.WARN,
                    severity=self.rule.severity,
                    observed_value=float(stats.row_count),
                    threshold_value=30,
                    message=(
                        f"{sigma_level}-sigma check skipped because sample size is too small."
                    ),
                )
            ]

        upper_bound = stats.mean_value + (sigma_level * stats.stddev_value)
        lower_bound = stats.mean_value - (sigma_level * stats.stddev_value)

        breaches = dataframe.filter(
            (col(self.rule.aggregation_column) > upper_bound)
            | (col(self.rule.aggregation_column) < lower_bound)
        ).count()

        status = DQStatus.PASS if breaches == 0 else DQStatus.FAIL

        return [
            DQCheckResult(
                run_id=framework_config.run_id,
                domain=framework_config.domain,
                table_name=framework_config.table_name,
                rule_id=self.rule.rule_id,
                check_type=self.rule.check_type,
                status=status,
                severity=self.rule.severity,
                observed_value=breaches,
                threshold_value=sigma_level,
                message=(
                    f"{sigma_level}-sigma bounds [{lower_bound}, {upper_bound}] with "
                    f"{breaches} outlier rows."
                ),
            )
        ]

    def _sigma_level(self) -> int:
        if self.rule.threshold_value is None:
            return 3

        sigma_level_raw = float(self.rule.threshold_value)
        if sigma_level_raw not in [1.0, 2.0, 3.0]:
            raise IllegalArgumentError(
                "For aggregation_type='sigma_3', threshold_value must be 1, 2, or 3."
            )

        return int(sigma_level_raw)
