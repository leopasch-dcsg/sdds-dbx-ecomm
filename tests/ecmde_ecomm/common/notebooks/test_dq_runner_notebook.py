import importlib
import sys
import types
from types import SimpleNamespace

import pytest


class _FakeDataFrame:
    def __init__(self, filter_return=None):
        self.filter_calls = []
        self._filter_return = filter_return if filter_return is not None else self

    def filter(self, expression):
        self.filter_calls.append(expression)
        return self._filter_return


class _FakeSpark:
    def __init__(self, source_dataframe):
        self._source_dataframe = source_dataframe
        self.table_calls = []

    def table(self, table_fqn):
        self.table_calls.append(table_fqn)
        return self._source_dataframe


class _FakeDatabricksSession:
    spark = None

    class _Builder:
        @classmethod
        def getOrCreate(cls):
            return _FakeDatabricksSession.spark

    builder = _Builder()


def _load_notebook_module(monkeypatch, framework_config):
    module_name = "ecmde_ecomm.common.notebooks.dq_runner_notebook"
    sys.modules.pop(module_name, None)

    fake_runtime_module = types.ModuleType("databricks.sdk.runtime")
    fake_runtime_module.dbutils = SimpleNamespace()
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", fake_runtime_module)

    original_dq_module = importlib.import_module("ecmde_ecomm.common.dq")

    unfiltered_dataframe = _FakeDataFrame()
    filtered_dataframe = _FakeDataFrame()
    unfiltered_dataframe._filter_return = filtered_dataframe

    fake_spark = _FakeSpark(unfiltered_dataframe)
    _FakeDatabricksSession.spark = fake_spark

    fake_databricks_module = types.ModuleType("databricks")
    fake_connect_module = types.ModuleType("databricks.connect")
    fake_connect_module.DatabricksSession = _FakeDatabricksSession
    fake_databricks_module.connect = fake_connect_module
    monkeypatch.setitem(sys.modules, "databricks", fake_databricks_module)
    monkeypatch.setitem(sys.modules, "databricks.connect", fake_connect_module)

    class _FrameworkConfigStub:
        @staticmethod
        def from_notebook_params():
            return framework_config

    class _RuleConfigStub:
        @staticmethod
        def load_rules(spark, config):
            return [
                SimpleNamespace(
                    notify_email_recipients=None,
                    is_email_notify=False,
                    is_xmatter_notify=False,
                )
            ]

    captured_run = {}

    class _EngineStub:
        def __init__(self, spark):
            captured_run["spark"] = spark

        def run(self, source_dataframe, config, rule_configs, notifiers):
            captured_run["source_dataframe"] = source_dataframe
            captured_run["config"] = config
            captured_run["rule_configs"] = rule_configs
            captured_run["notifiers"] = notifiers
            return []

    monkeypatch.setattr(original_dq_module, "DQFrameworkConfig", _FrameworkConfigStub)
    monkeypatch.setattr(original_dq_module, "DQRuleConfig", _RuleConfigStub)
    monkeypatch.setattr(original_dq_module, "DQEngine", _EngineStub)

    importlib.import_module(module_name)

    return {
        "unfiltered_dataframe": unfiltered_dataframe,
        "filtered_dataframe": filtered_dataframe,
        "captured_run": captured_run,
        "spark": fake_spark,
    }


@pytest.mark.unit
class TestDQRunnerNotebook:
    def test_applies_source_filter_when_expression_is_configured(self, monkeypatch):
        framework_config = SimpleNamespace(
            source_table_fqn="dev_ent_silver_db.ecmde.sample_source",
            source_filter_expr="business_date >= '2026-01-01'",
            domain="fulfillment",
            table_name="sample_source",
            fail_on_breach=False,
        )

        actual = _load_notebook_module(monkeypatch, framework_config)

        assert actual["spark"].table_calls == ["dev_ent_silver_db.ecmde.sample_source"]
        assert actual["unfiltered_dataframe"].filter_calls == [
            "business_date >= '2026-01-01'"
        ]
        assert actual["captured_run"]["source_dataframe"] is actual["filtered_dataframe"]

    def test_skips_source_filter_when_expression_is_empty(self, monkeypatch):
        framework_config = SimpleNamespace(
            source_table_fqn="dev_ent_silver_db.ecmde.sample_source",
            source_filter_expr="",
            domain="fulfillment",
            table_name="sample_source",
            fail_on_breach=False,
        )

        actual = _load_notebook_module(monkeypatch, framework_config)

        assert actual["spark"].table_calls == ["dev_ent_silver_db.ecmde.sample_source"]
        assert actual["unfiltered_dataframe"].filter_calls == []
        assert (
            actual["captured_run"]["source_dataframe"]
            is actual["unfiltered_dataframe"]
        )
