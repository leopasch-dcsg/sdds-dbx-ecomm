import hashlib
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ecmde_ecomm.common.dq.dq_config import DQFrameworkConfig, DQRuleConfig
from ecmde_ecomm.common.dq.dq_types import DQCheckType, DQAggregationType, DQStatus
from ecmde_ecomm.common.dq.notifiers.base import DQNotifier
from ecmde_ecomm.common.dq.results import DQCheckResult
from ecmde_ecomm.common.dq.rules.aggregate_rules import (
    AggregateThresholdRule,
    ThreeSigmaRule,
)
from ecmde_ecomm.common.dq.rules.column_rules import (
    NullThresholdRule,
    PatternThresholdRule,
    ConditionThresholdRule,
)
from ecmde_ecomm.common.dq.rules.freshness_rules import FreshnessLagRule
from ecmde_ecomm.common.dq.rules.quarantine_rules import QuarantineCountRule


class DQEngine:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(
        self,
        dataframe: DataFrame,
        framework_config: DQFrameworkConfig,
        rules: list[DQRuleConfig],
        notifiers: list[DQNotifier] | None = None,
    ) -> list[DQCheckResult]:
        results: list[DQCheckResult] = []

        for rule in rules:
            evaluator = self._resolve_rule(rule)
            results.extend(evaluator.evaluate(self.spark, dataframe, framework_config))

        self._persist_results(framework_config, results)
        self._persist_summary(framework_config, results)

        if notifiers is not None:
            failed = [result for result in results if result.status == DQStatus.FAIL]
            for notifier in notifiers:
                channel = self._notification_channel(notifier)

                if len(failed) == 0:
                    self._persist_notification_log(
                        framework_config,
                        failed,
                        channel,
                        "skipped",
                        "No failed checks to notify.",
                    )
                    continue

                try:
                    notifier.notify(results)
                    self._persist_notification_log(
                        framework_config,
                        failed,
                        channel,
                        "success",
                        f"Notification sent for {len(failed)} failed check(s).",
                    )
                except Exception as exc:
                    self._persist_notification_log(
                        framework_config,
                        failed,
                        channel,
                        "failure",
                        str(exc),
                    )

        return results

    def _resolve_rule(self, rule: DQRuleConfig):
        if rule.check_type == DQCheckType.FRESHNESS:
            return FreshnessLagRule(rule)
        if rule.check_type == DQCheckType.NULL:
            return NullThresholdRule(rule)
        if rule.check_type == DQCheckType.PATTERN:
            return PatternThresholdRule(rule)
        if rule.check_type == DQCheckType.CONDITION:
            return ConditionThresholdRule(rule)
        if rule.check_type == DQCheckType.QUARANTINE_COUNT:
            return QuarantineCountRule(rule)
        if (
            rule.check_type == DQCheckType.AGGREGATE
            and rule.aggregation_type == DQAggregationType.SIGMA_3
        ):
            return ThreeSigmaRule(rule)
        return AggregateThresholdRule(rule)

    def _persist_results(
        self,
        framework_config: DQFrameworkConfig,
        results: list[DQCheckResult],
    ) -> None:
        if len(results) == 0:
            return

        rows = [result.as_dict() for result in results]
        dataframe = self.spark.createDataFrame(rows, schema=self._result_schema())
        dataframe.write.format("delta").mode("append").saveAsTable(
            framework_config.result_table_fqn
        )

    def _persist_summary(
        self,
        framework_config: DQFrameworkConfig,
        results: list[DQCheckResult],
    ) -> None:
        total = len(results)
        warn = len([result for result in results if result.status == DQStatus.WARN])
        failed = len([result for result in results if result.status == DQStatus.FAIL])

        summary = {
            "run_id": framework_config.run_id,
            "domain": framework_config.domain,
            "table_name": framework_config.table_name,
            "total_checks": total,
            "warn_checks": warn,
            "failed_checks": failed,
            "created_on_utc": datetime.now(timezone.utc),
        }

        self.spark.createDataFrame(
            [summary], schema=self._summary_schema()
        ).write.format("delta").mode("append").saveAsTable(
            framework_config.summary_table_fqn
        )

    def _persist_notification_log(
        self,
        framework_config: DQFrameworkConfig,
        failed: list[DQCheckResult],
        channel: str,
        status: str,
        detail: str,
    ) -> None:
        parts = framework_config.summary_table_fqn.split(".")
        if len(parts) < 3:
            return

        parts[-1] = "dq_notification_log"
        notification_table_fqn = ".".join(parts)

        payload_source = "|".join(
            sorted(
                [
                    f"{result.rule_id}:{result.status.value}:{result.message or ''}"
                    for result in failed
                ]
            )
        )
        payload_hash = hashlib.sha256(payload_source.encode("utf-8")).hexdigest()

        row = {
            "run_id": framework_config.run_id,
            "domain": framework_config.domain,
            "table_name": framework_config.table_name,
            "channel": channel,
            "payload_hash": payload_hash,
            "notification_status": status,
            "notification_detail": detail,
            "created_on_utc": datetime.now(timezone.utc),
        }

        self.spark.createDataFrame(
            [row], schema=self._notification_schema()
        ).write.format("delta").mode("append").saveAsTable(notification_table_fqn)

    @staticmethod
    def _notification_channel(notifier: DQNotifier) -> str:
        class_name = notifier.__class__.__name__.lower()
        if "email" in class_name:
            return "email"
        if "xmatters" in class_name:
            return "xmatters"
        return class_name

    @staticmethod
    def _result_schema() -> StructType:
        return StructType(
            [
                StructField("run_id", StringType(), nullable=False),
                StructField("domain", StringType(), nullable=False),
                StructField("table_name", StringType(), nullable=False),
                StructField("rule_id", StringType(), nullable=False),
                StructField("check_type", StringType(), nullable=False),
                StructField("status", StringType(), nullable=False),
                StructField("severity", StringType(), nullable=False),
                StructField("observed_value", StringType(), nullable=True),
                StructField("threshold_value", StringType(), nullable=True),
                StructField("dimension_key", StringType(), nullable=True),
                StructField("message", StringType(), nullable=True),
                StructField("created_on_utc", TimestampType(), nullable=False),
            ]
        )

    @staticmethod
    def _summary_schema() -> StructType:
        return StructType(
            [
                StructField("run_id", StringType(), nullable=False),
                StructField("domain", StringType(), nullable=False),
                StructField("table_name", StringType(), nullable=False),
                StructField("total_checks", IntegerType(), nullable=False),
                StructField("warn_checks", IntegerType(), nullable=False),
                StructField("failed_checks", IntegerType(), nullable=False),
                StructField("created_on_utc", TimestampType(), nullable=False),
            ]
        )

    @staticmethod
    def _notification_schema() -> StructType:
        return StructType(
            [
                StructField("run_id", StringType(), nullable=False),
                StructField("domain", StringType(), nullable=False),
                StructField("table_name", StringType(), nullable=False),
                StructField("channel", StringType(), nullable=False),
                StructField("payload_hash", StringType(), nullable=False),
                StructField("notification_status", StringType(), nullable=False),
                StructField("notification_detail", StringType(), nullable=True),
                StructField("created_on_utc", TimestampType(), nullable=False),
            ]
        )
