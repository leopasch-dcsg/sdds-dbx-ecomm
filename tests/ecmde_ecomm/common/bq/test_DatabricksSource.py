import pytest
from databricks.sdk.runtime import dbutils
from ecmde_ecomm.common.errors import IllegalArgumentError, ElementNotFoundError

from ecmde_ecomm.common.bq.egress import DatabricksSource


def get_dbutils_value(*args):
    param_name = args[0][0]

    match param_name:
        case "dbx_bq_source_catalog":
            return catalog_test_double()
        case "dbx_bq_source_schema":
            return schema_test_double()
        case "dbx_bq_source_table":
            return table_test_double()
        case _:
            raise ElementNotFoundError()


def catalog_test_double() -> str:
    return "catalog_name"


def schema_test_double() -> str:
    return "schema_name"


def table_test_double() -> str:
    return "table_name"


@pytest.fixture
def datasource() -> DatabricksSource:
    return DatabricksSource(
        catalog_test_double(),
        schema_test_double(),
        table_test_double(),
    )


@pytest.mark.unit
class TestDatabricksSource:
    def test_property_validation(self):
        with pytest.raises(IllegalArgumentError):
            DatabricksSource("", "schema", "table")

        with pytest.raises(IllegalArgumentError):
            DatabricksSource("catalog", "", "table")

        with pytest.raises(IllegalArgumentError):
            DatabricksSource("catalog", "schema", "")

    def test_fully_qualified_table(self, datasource):
        assert (
            f"{catalog_test_double()}.{schema_test_double()}.{table_test_double()}"
            == datasource.fully_qualified_table()
        )

    def test_from_notebook_params(self, datasource, monkeypatch):
        def mock_dbutils_widgets_get(*args):
            return get_dbutils_value(args)

        monkeypatch.setattr(dbutils.widgets, "get", mock_dbutils_widgets_get)
        actual = DatabricksSource.from_notebook_params()
        assert datasource == actual

    def test_from_notebook_params_error(self, monkeypatch):
        with pytest.raises(KeyError):
            DatabricksSource.from_notebook_params()
