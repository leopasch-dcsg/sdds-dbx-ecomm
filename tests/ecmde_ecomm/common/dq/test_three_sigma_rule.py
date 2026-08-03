import pytest

from ecmde_ecomm.common.dq.dq_config import DQRuleConfig
from ecmde_ecomm.common.dq.dq_types import DQAggregationType, DQCheckType, DQComparator, DQSeverity
from ecmde_ecomm.common.dq.rules.aggregate_rules import ThreeSigmaRule
from ecmde_ecomm.common.errors import IllegalArgumentError


@pytest.mark.unit
class TestThreeSigmaRule:
    def _rule(self, threshold_value):
        return DQRuleConfig(
            rule_id="sigma_rule",
            check_type=DQCheckType.AGGREGATE,
            severity=DQSeverity.HIGH,
            column_name=None,
            pattern=None,
            threshold_value=threshold_value,
            freshness_column=None,
            freshness_minutes=None,
            aggregation_type=DQAggregationType.SIGMA_3,
            aggregation_column="observed_col",
            group_by_columns=[],
            quarantine_table_fqn=None,
            condition_expr=None,
            notify_email_recipients=None,
            is_email_notify=True,
            is_xmatter_notify=False,
            comparator=DQComparator.LTE,
        )

    def test_sigma_level_defaults_to_three(self):
        rule = ThreeSigmaRule(self._rule(None))

        assert rule._sigma_level() == 3

    def test_sigma_level_supports_one_two_three(self):
        assert ThreeSigmaRule(self._rule(1))._sigma_level() == 1
        assert ThreeSigmaRule(self._rule(2))._sigma_level() == 2
        assert ThreeSigmaRule(self._rule(3))._sigma_level() == 3

    def test_sigma_level_rejects_unsupported_values(self):
        with pytest.raises(IllegalArgumentError):
            ThreeSigmaRule(self._rule(2.5))._sigma_level()

        with pytest.raises(IllegalArgumentError):
            ThreeSigmaRule(self._rule(0))._sigma_level()
