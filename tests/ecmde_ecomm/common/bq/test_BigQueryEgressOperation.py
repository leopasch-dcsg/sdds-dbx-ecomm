import pytest
from pyspark.sql.types import StructType, IntegerType, StringType, StructField
from ecmde_ecomm.common.bq.egress import (
    BigQueryCredentials,
    BigQueryDestination,
    DatabricksSource,
    BigQueryEgressOperation,
)
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
        ]
    )


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


def json_credentials_encoded_test_double() -> str:
    return "CiAgICAgICAgewogICAgICAgICAgICAidHlwZSI6ICJzZXJ2aWNlX2FjY291bnQiLAogICAgICAgICAgICAicHJvamVjdF9pZCI6ICJnY3AtZGtzLWVjbWRlLXNib3giLAogICAgICAgICAgICAicHJpdmF0ZV9rZXlfaWQiOiAiODY3NTMwOSIsCiAgICAgICAgICAgICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS0KTUlJRXZnSUJMSTgxTAotLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tCiIsCiAgICAgICAgICAgICJjbGllbnRfZW1haWwiOiAiYS1zZXJ2aWNlLWFjY291bnRAcGFyZW50LXByb2plY3QtaWQuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICAgICAgICAgICAiY2xpZW50X2lkIjogIjEyMzQ1Njc4OTAiLAogICAgICAgICAgICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICAgICAgICAgICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAgICAgICAgICAgImF1dGhfcHJvdmlkZXJfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjEvY2VydHMiLAogICAgICAgICAgICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9hLXNlcnZpY2UtYWNjb3VudCU0MHBhcmVudC1wcm9qZWN0LWlkLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAgICAgICAgICAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKICAgICAgICB9CiAgICA="


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


@pytest.fixture
def operation(
    spark: SparkSession,
    datasource: DatabricksSource,
    destination: BigQueryDestination,
    credentials: BigQueryCredentials,
):
    return (
        BigQueryEgressOperation.builder(spark)
        .with_bigquery_credentials(credentials)
        .with_bigquery_destination(destination)
        .with_databricks_source(datasource)
        .operation()
    )


@pytest.mark.unit
class TestBigQueryEgressOperation:
    def test_spark_conf(self, spark: SparkSession, operation: BigQueryEgressOperation):
        operation._configure_spark()
        credentials_encoded = spark.conf.get("credentials")
        assert json_credentials_encoded_test_double() == credentials_encoded
