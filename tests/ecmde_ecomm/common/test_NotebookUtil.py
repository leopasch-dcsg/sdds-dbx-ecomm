from ecmde_ecomm.common.util import NotebookUtil
from databricks.sdk.runtime import dbutils
from tests.ecmde_ecomm.fixtures import *


def get_notebook_params_from_args(*args):
    variable_value = args[0][0]

    if "kafka_schema_registry_use_latest_version" == variable_value:
        return "false"

    return None

@pytest.mark.unit
class TestNotebookUtil:
    def test_notebook_param_no_param(self, monkeypatch):

        def mock_get(*args):
            return get_notebook_params_from_args(*args)

        monkeypatch.setattr(dbutils.widgets, "get", mock_get)
        expected = "34"
        actual = NotebookUtil.notebook_param("placeholder", "34")
        assert expected == actual

    def test_notebook_param_with_missing_param(self, monkeypatch):
        def mock_get(*args):
            raise KeyError("key does not exist.")

        monkeypatch.setattr(dbutils.widgets, "get", mock_get)
        expected = "34"
        actual = NotebookUtil.notebook_param("placeholder", "34")
        assert expected == actual

    def test_notebook_param_no_param_no_default(self, monkeypatch):

        def mock_get(*args):
            return get_notebook_params_from_args(args)

        monkeypatch.setattr(dbutils.widgets, "get", mock_get)
        expected = None
        actual = NotebookUtil.notebook_param("placeholder")
        assert expected == actual

    def test_notebook_param_return(self, monkeypatch):

        def mock_get(*args):
            return get_notebook_params_from_args(args)

        monkeypatch.setattr(dbutils.widgets, "get", mock_get)
        expected = "false"
        actual = NotebookUtil.notebook_param("kafka_schema_registry_use_latest_version")
        assert expected == actual

    def test_spark_param(self, spark):
        spark.conf.set("test_param", "34")
        expected = "34"
        actual = NotebookUtil.spark_param(spark, "test_param", None)
        assert expected == actual

    def test_spark_param_default(self, spark):
        expected = "34"
        actual = NotebookUtil.spark_param(spark, "different_param", "34")
        assert expected == actual

        expected = None
        actual = NotebookUtil.spark_param(spark, "different_param", None)
        assert expected == actual
