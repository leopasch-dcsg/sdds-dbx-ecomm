import pytest
from databricks.sdk.runtime import dbutils
from ecmde_ecomm.common.errors import IllegalArgumentError, ElementNotFoundError

from ecmde_ecomm.common.bq.egress import BigQueryDestination


def get_dbutils_value(*args):
    param_name = args[0][0]

    match param_name:
        case "dbx_bq_destination_bucket":
            return bucket_test_double()
        case "dbx_bq_destination_parent_project_id":
            return parent_project_id_test_double()
        case "dbx_bq_destination_project_id":
            return project_id_test_double()
        case "dbx_bq_destination_dataset":
            return dataset_test_double()
        case "dbx_bq_destination_table":
            return table_test_double()
        case _:
            raise ElementNotFoundError()


@pytest.fixture
def destination() -> BigQueryDestination:
    return BigQueryDestination(
        bucket_test_double(),
        parent_project_id_test_double(),
        project_id_test_double(),
        dataset_test_double(),
        table_test_double(),
    )


def bucket_test_double() -> str:
    return "dbx-gcp-ecom-bucket"


def parent_project_id_test_double() -> str:
    return "gcp-dks-ecmde-sbox"


def project_id_test_double() -> str:
    return "gcp-dbx-ecomp-dev"


def dataset_test_double() -> str:
    return "ecm_incoming"


def table_test_double() -> str:
    return "promotion_header_dbx"


@pytest.mark.unit
class TestBigQueryDestination:
    def test_property_validation(self):
        with pytest.raises(IllegalArgumentError):
            BigQueryDestination(
                "",
                parent_project_id_test_double(),
                project_id_test_double(),
                dataset_test_double(),
                table_test_double(),
            )

        with pytest.raises(IllegalArgumentError):
            BigQueryDestination(
                bucket_test_double(),
                "",
                project_id_test_double(),
                dataset_test_double(),
                table_test_double(),
            )

        with pytest.raises(IllegalArgumentError):
            BigQueryDestination(
                bucket_test_double(),
                parent_project_id_test_double(),
                "",
                dataset_test_double(),
                table_test_double(),
            )

        with pytest.raises(IllegalArgumentError):
            BigQueryDestination(
                bucket_test_double(),
                parent_project_id_test_double(),
                project_id_test_double(),
                "",
                table_test_double(),
            )

        with pytest.raises(IllegalArgumentError):
            BigQueryDestination(
                bucket_test_double(),
                parent_project_id_test_double(),
                project_id_test_double(),
                dataset_test_double(),
                "",
            )

    def test_from_notebook_params(self, destination, monkeypatch):
        def mock_dbutils_widgets_get(*args):
            return get_dbutils_value(args)

        monkeypatch.setattr(dbutils.widgets, "get", mock_dbutils_widgets_get)
        actual = BigQueryDestination.from_notebook_params()
        assert destination == actual

    def test_from_notebook_params_error(self):
        with pytest.raises(KeyError):
            BigQueryDestination.from_notebook_params()
