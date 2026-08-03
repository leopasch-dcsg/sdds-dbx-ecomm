import pytest

from ecmde_ecomm.common.dq import DQFrameworkConfig, DQRuleConfig
from ecmde_ecomm.common.dq.dq_types import DQAggregationType, DQCheckType, DQSeverity
from ecmde_ecomm.common.errors import IllegalArgumentError


@pytest.mark.unit
class TestDQConfig:
    def test_framework_config_from_notebook_params(self, monkeypatch):
        values = {
            "dq_run_id": "run-1",
            "dq_domain": "fulfillment",
            "dq_table_name": "stg_oso_order_fulfill",
            "dq_source_table": "dev_ent_silver_db.ecmde.stg_oso_order_fulfill",
            "dq_result_table": "dev_ent_silver_db.ecmde.dq_check_result",
            "dq_summary_table": "dev_ent_silver_db.ecmde.dq_run_summary",
            "dq_config_table": "dev_ent_silver_db.ecmde.dq_rule_config",
            "dq_fail_on_breach": "true",
            "dq_source_filter_expr": "business_date >= '2026-01-01'",
        }

        monkeypatch.setattr(
            "ecmde_ecomm.common.dq.dq_config.NotebookUtil.notebook_param",
            lambda key, default=None: values.get(key, default),
        )

        actual = DQFrameworkConfig.from_notebook_params()

        assert actual.run_id == "run-1"
        assert actual.fail_on_breach is True
        assert actual.source_filter_expr == "business_date >= '2026-01-01'"

    def test_framework_config_raises_when_required_param_missing(self, monkeypatch):
        values = {
            "dq_run_id": "",
            "dq_domain": "fulfillment",
            "dq_table_name": "stg_oso_order_fulfill",
            "dq_source_table": "dev_ent_silver_db.ecmde.stg_oso_order_fulfill",
            "dq_result_table": "dev_ent_silver_db.ecmde.dq_check_result",
            "dq_summary_table": "dev_ent_silver_db.ecmde.dq_run_summary",
        }

        monkeypatch.setattr(
            "ecmde_ecomm.common.dq.dq_config.NotebookUtil.notebook_param",
            lambda key, default=None: values.get(key, default),
        )

        with pytest.raises(IllegalArgumentError):
            DQFrameworkConfig.from_notebook_params()

    def test_rule_config_from_json(self):
        payload = (
            '[{"rule_id":"r1","check_type":"aggregate","severity":"high",'
            '"aggregation_type":"count","threshold_value":1},'
            '{"rule_id":"r2","check_type":"null","severity":"medium",'
            '"column_name":"sku_number","threshold_value":0.1}]'
        )

        actual = DQRuleConfig.from_json(payload)

        assert len(actual) == 2
        assert actual[0].check_type == DQCheckType.AGGREGATE
        assert actual[0].aggregation_type == DQAggregationType.COUNT
        assert actual[1].severity == DQSeverity.MEDIUM
