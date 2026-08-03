from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.bq.egress import (
    BigQueryCredentials,
    BigQueryDestination,
    DatabricksSource,
    BigQueryEgressOperation,
)
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def credentials():
    return BigQueryCredentials(json_credentials_test_double())


def json_credentials_test_double() -> str:
    return """
        {
            "type": "service_account",
            "project_id": "gcp-dks-ecmde-sbox",
            "private_key_id": "8675309",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBLI81L\n-----END PRIVATE KEY-----\n",
            "client_email": "a-service-account@parent-project-id.iam.gserviceaccount.com",
            "client_id": "1234567890",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/a-service-account%40parent-project-id.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }
    """


@pytest.fixture
def destination() -> BigQueryDestination:
    return BigQueryDestination(
        bucket_test_double(),
        parent_project_id_test_double(),
        project_id_test_double(),
        dataset_test_double(),
        destination_table_test_double(),
    )


def bucket_test_double() -> str:
    return "dbx-gcp-ecom-bucket"


def parent_project_id_test_double() -> str:
    return "gcp-dks-ecmde-sbox"


def project_id_test_double() -> str:
    return "gcp-dbx-ecomp-dev"


def dataset_test_double() -> str:
    return "ecm_incoming"


def destination_table_test_double() -> str:
    return "promotion_header_dbx"


@pytest.fixture
def datasource() -> DatabricksSource:
    return DatabricksSource(
        catalog_test_double(),
        schema_test_double(),
        datasource_table_test_double(),
    )


def catalog_test_double() -> str:
    return "catalog_name"


def schema_test_double() -> str:
    return "schema_name"


def datasource_table_test_double() -> str:
    return "table_name"


@pytest.mark.unit
class TestBigQueryEgressOperationBuilder:
    def test_operation_throws_on_missing_options(
        self,
        spark: SparkSession,
        datasource: DatabricksSource,
        destination: BigQueryDestination,
        credentials: BigQueryCredentials,
    ):
        with pytest.raises(IllegalArgumentError):
            (
                BigQueryEgressOperation.builder(spark)
                .with_bigquery_destination(destination)
                .with_databricks_source(datasource)
                .operation()
            )

        with pytest.raises(IllegalArgumentError):
            (
                BigQueryEgressOperation.builder(spark)
                .with_bigquery_destination(destination)
                .with_bigquery_credentials(credentials)
                .operation()
            )

        with pytest.raises(IllegalArgumentError):
            (
                BigQueryEgressOperation.builder(spark)
                .with_databricks_source(datasource)
                .with_bigquery_credentials(credentials)
                .operation()
            )

    def test_operation_does_not_throw(
        self,
        spark: SparkSession,
        datasource: DatabricksSource,
        destination: BigQueryDestination,
        credentials: BigQueryCredentials,
    ):
        operation = (
            BigQueryEgressOperation.builder(spark)
            .with_bigquery_destination(destination)
            .with_databricks_source(datasource)
            .with_bigquery_credentials(credentials)
            .operation()
        )

        assert operation is not None
        assert operation.credentials == credentials
        assert operation.destination == destination
        assert operation.source == datasource
