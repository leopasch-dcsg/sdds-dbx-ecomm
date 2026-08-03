import json
from dataclasses import dataclass
from pyspark.sql.functions import col

from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.dq.dq_types import (
    DQSeverity,
    DQCheckType,
    DQAggregationType,
    DQComparator,
)


@dataclass(frozen=True)
class DQFrameworkConfig:
    run_id: str
    domain: str
    table_name: str
    source_table_fqn: str
    result_table_fqn: str
    summary_table_fqn: str
    config_table_fqn: str | None
    fail_on_breach: bool
    source_filter_expr: str | None = None

    @staticmethod
    def from_notebook_params() -> "DQFrameworkConfig":
        run_id = NotebookUtil.notebook_param("dq_run_id", "")
        domain = NotebookUtil.notebook_param("dq_domain", "")
        table_name = NotebookUtil.notebook_param("dq_table_name", "")
        source_table_fqn = NotebookUtil.notebook_param("dq_source_table", "")
        result_table_fqn = NotebookUtil.notebook_param("dq_result_table", "")
        summary_table_fqn = NotebookUtil.notebook_param("dq_summary_table", "")
        config_table_fqn = NotebookUtil.notebook_param("dq_config_table")
        fail_on_breach_raw = NotebookUtil.notebook_param("dq_fail_on_breach", "false")
        source_filter_expr = NotebookUtil.notebook_param("dq_source_filter_expr")

        for value, name in [
            (run_id, "dq_run_id"),
            (domain, "dq_domain"),
            (table_name, "dq_table_name"),
            (source_table_fqn, "dq_source_table"),
            (result_table_fqn, "dq_result_table"),
            (summary_table_fqn, "dq_summary_table"),
        ]:
            if value is None or len(value.strip()) == 0:
                raise IllegalArgumentError(f"{name} is missing or empty.")

        return DQFrameworkConfig(
            run_id=run_id,
            domain=domain,
            table_name=table_name,
            source_table_fqn=source_table_fqn,
            result_table_fqn=result_table_fqn,
            summary_table_fqn=summary_table_fqn,
            config_table_fqn=config_table_fqn,
            fail_on_breach=fail_on_breach_raw.lower() == "true",
            source_filter_expr=source_filter_expr,
        )


@dataclass(frozen=True)
class DQRuleConfig:
    rule_id: str
    check_type: DQCheckType
    severity: DQSeverity
    column_name: str | None
    pattern: str | None
    threshold_value: float | None
    freshness_column: str | None
    freshness_minutes: int | None
    aggregation_type: DQAggregationType | None
    aggregation_column: str | None
    group_by_columns: list[str]
    quarantine_table_fqn: str | None
    condition_expr: str | None
    notify_email_recipients: str | None = None
    is_email_notify: bool = True
    is_xmatter_notify: bool = False
    comparator: DQComparator = DQComparator.LTE

    @staticmethod
    def from_json(json_payload: str) -> list["DQRuleConfig"]:
        if json_payload is None or len(json_payload.strip()) == 0:
            return []

        parsed = json.loads(json_payload)
        rules: list[DQRuleConfig] = []

        for row in parsed:
            rules.append(
                DQRuleConfig(
                    rule_id=row["rule_id"],
                    check_type=DQCheckType(row["check_type"]),
                    severity=DQSeverity(row.get("severity", "medium")),
                    column_name=row.get("column_name"),
                    pattern=row.get("pattern"),
                    threshold_value=row.get("threshold_value"),
                    freshness_column=row.get("freshness_column"),
                    freshness_minutes=row.get("freshness_minutes"),
                    aggregation_type=(
                        DQAggregationType(row["aggregation_type"])
                        if row.get("aggregation_type") is not None
                        else None
                    ),
                    aggregation_column=row.get("aggregation_column"),
                    group_by_columns=row.get("group_by_columns", []),
                    quarantine_table_fqn=row.get("quarantine_table_fqn"),
                    condition_expr=row.get("condition_expr"),
                    notify_email_recipients=row.get("notify_email_recipients"),
                    is_email_notify=row.get("is_email_notify", True),
                    is_xmatter_notify=row.get("is_xmatter_notify", False),
                    comparator=DQComparator(row.get("comparator", "lte")),
                )
            )

        return rules

    @staticmethod
    def load_rules(
        spark,
        framework_config: DQFrameworkConfig,
    ) -> list["DQRuleConfig"]:
        if framework_config.config_table_fqn is None:
            raise IllegalArgumentError(
                "dq_config_table is required for runtime DQ rule loading."
            )

        config_table_name = framework_config.config_table_fqn.strip()
        if len(config_table_name) == 0:
            raise IllegalArgumentError(
                "dq_config_table is empty. Provide a valid rule configuration table."
            )

        try:
            table_exists = spark.catalog.tableExists(config_table_name)
        except Exception:
            table_exists = False

        if not table_exists:
            raise IllegalArgumentError(
                f"DQ config table '{config_table_name}' does not exist."
            )

        rows = (
            spark.table(config_table_name)
            .filter(col("is_active"))
            .filter(col("domain") == framework_config.domain)
            .filter(col("table_name") == framework_config.table_name)
            .collect()
        )

        if len(rows) == 0:
            raise IllegalArgumentError(
                "No active DQ rules found for the configured domain and table."
            )

        return [
            DQRuleConfig(
                rule_id=row.rule_id,
                check_type=DQCheckType(row.check_type),
                severity=DQSeverity(row.severity),
                column_name=row.column_name,
                pattern=row.pattern,
                threshold_value=row.threshold_value,
                freshness_column=row.freshness_column,
                freshness_minutes=row.freshness_minutes,
                aggregation_type=(
                    DQAggregationType(row.aggregation_type)
                    if row.aggregation_type is not None
                    else None
                ),
                aggregation_column=row.aggregation_column,
                group_by_columns=(
                    [x.strip() for x in row.group_by_columns.split(",")]
                    if row.group_by_columns is not None
                    and len(row.group_by_columns) > 0
                    else []
                ),
                quarantine_table_fqn=row.quarantine_table_fqn,
                condition_expr=row.condition_expr,
                notify_email_recipients=getattr(row, "notify_email_recipients", None),
                is_email_notify=(
                    row.is_email_notify if row.is_email_notify is not None else True
                ),
                is_xmatter_notify=(
                    row.is_xmatter_notify
                    if row.is_xmatter_notify is not None
                    else False
                ),
                comparator=DQComparator(
                    row.comparator if row.comparator is not None else "lte"
                ),
            )
            for row in rows
        ]
