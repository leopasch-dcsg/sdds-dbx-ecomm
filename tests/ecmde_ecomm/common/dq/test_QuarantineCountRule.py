from unittest.mock import MagicMock

import pytest

from ecmde_ecomm.common.dq.dq_config import DQFrameworkConfig, DQRuleConfig
from ecmde_ecomm.common.dq.dq_types import (
    DQAggregationType,
    DQCheckType,
    DQComparator,
    DQSeverity,
)
from ecmde_ecomm.common.dq.rules.quarantine_rules import QuarantineCountRule


@pytest.mark.unit
class TestQuarantineCountRule:
    def _framework_config(self) -> DQFrameworkConfig:
        return DQFrameworkConfig(
            run_id="run-1",
            domain="fulfillment",
            table_name="stg_oso_order_header",
            source_table_fqn="dev_ent_silver_db.ecmde.stg_oso_order_header",
            result_table_fqn="dev_ent_silver_db.ecmde.dq_check_result",
            summary_table_fqn="dev_ent_silver_db.ecmde.dq_run_summary",
            config_table_fqn="dev_ent_silver_db.ecmde.dq_rule_config",
            fail_on_breach=False,
        )

    def _rule(self, aggregation_type):
        return DQRuleConfig(
            rule_id="quarantine_rule",
            check_type=DQCheckType.QUARANTINE_COUNT,
            severity=DQSeverity.HIGH,
            column_name=None,
            pattern=None,
            threshold_value=10,
            freshness_column=None,
            freshness_minutes=None,
            aggregation_type=aggregation_type,
            aggregation_column=None,
            group_by_columns=[],
            quarantine_table_fqn="dev_ent_silver_db.ecmde.order_header_quarantine",
            condition_expr=None,
            notify_email_recipients=None,
            is_email_notify=True,
            is_xmatter_notify=False,
            comparator=DQComparator.LTE,
        )

    def test_quarantine_count_uses_absolute_count_when_aggregation_count(self):
        spark = MagicMock()
        quarantine_dataframe = MagicMock()
        quarantine_dataframe.count.return_value = 11
        spark.table.return_value = quarantine_dataframe

        source_dataframe = MagicMock()
        source_dataframe.count.return_value = 1000000

        results = QuarantineCountRule(self._rule(DQAggregationType.COUNT)).evaluate(
            spark,
            source_dataframe,
            self._framework_config(),
        )

        assert len(results) == 1
        assert results[0].status.value == "fail"
        assert results[0].observed_value == 11.0
        assert "Quarantine count is 11.0" in results[0].message

    def test_quarantine_count_defaults_to_ratio_when_aggregation_missing(self):
        spark = MagicMock()
        quarantine_dataframe = MagicMock()
        quarantine_dataframe.count.return_value = 11
        spark.table.return_value = quarantine_dataframe

        source_dataframe = MagicMock()
        source_dataframe.count.return_value = 1000000

        rule = self._rule(None)
        rule = DQRuleConfig(**{**rule.__dict__, "threshold_value": 0.05})

        results = QuarantineCountRule(rule).evaluate(
            spark,
            source_dataframe,
            self._framework_config(),
        )

        assert len(results) == 1
        assert results[0].status.value == "pass"
        assert "Quarantine ratio is" in results[0].message
