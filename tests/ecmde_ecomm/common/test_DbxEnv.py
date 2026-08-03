from databricks.sdk.runtime import dbutils
from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.dbx.env import DbxEnv
from tests.ecmde_ecomm.fixtures import *


@pytest.mark.unit
class TestDbxEnv:
    @pytest.mark.parametrize(
        "env_input, expected",
        [
            ("dev", DbxEnv.DEV),
            ("DEV", DbxEnv.DEV),
            ("qa", DbxEnv.QA),
            ("QA", DbxEnv.QA),
            ("prod", DbxEnv.PROD),
            ("PROD", DbxEnv.PROD),
        ],
    )
    def test_from_notebook_params(self, env_input, expected, monkeypatch):
        def mock_dbutils_widgets_get(*args, **kwargs):
            return env_input

        monkeypatch.setattr(dbutils.widgets, "get", mock_dbutils_widgets_get)
        actual = DbxEnv.from_notebook_params()
        assert expected == actual

    @pytest.mark.parametrize(
        "env_input, expected",
        [
            ("dev", DbxEnv.DEV),
            ("DEV", DbxEnv.DEV),
            ("qa", DbxEnv.QA),
            ("QA", DbxEnv.QA),
            ("prod", DbxEnv.PROD),
            ("PROD", DbxEnv.PROD),
        ],
    )
    def test_from_spark_context(self, env_input, expected, spark):
        spark.conf.set("dbx_env", env_input)
        actual = DbxEnv.from_spark_context(spark)
        assert expected == actual

    def test_from_notebook_params_fail(self, monkeypatch):
        def mock_dbutils_widgets_get(*args, **kwargs):
            return "DOES NOT EXIST"

        monkeypatch.setattr(dbutils.widgets, "get", mock_dbutils_widgets_get)
        with pytest.raises(ElementNotFoundError):
            DbxEnv.from_notebook_params()

    def test_from_spark_context_fail(self, spark):
        spark.conf.set("dbx_env", "DOES NOT EXIST")
        with pytest.raises(ElementNotFoundError):
            DbxEnv.from_spark_context(spark)
