import pytest
from databricks.sdk.runtime import dbutils
from ecmde_ecomm.common.errors import IllegalArgumentError, ElementNotFoundError
from ecmde_ecomm.common.bq.egress import BigQueryCredentials
from tests.ecmde_ecomm.fixtures import *


def get_spark_param_value(*args):
    param_name = args[0][0]
    return get_value(param_name)


def get_dbutils_param_value(*args):
    param_name = args[0][0]
    return get_value(param_name)


def get_value(param_name: str):
    match param_name:
        case "azure_kv_scope":
            return "kv-dsg-ecmde-dbx"
        case "dbx_bq_credentials_json_credentials_key":
            return json_credentials_key_test_double()
        case _:
            raise ElementNotFoundError()


def get_secrets_value(*args):
    key = args[0][1]

    if key == json_credentials_key_test_double():
        return json_credentials_test_double()


@pytest.fixture
def credentials():
    return BigQueryCredentials(json_credentials_test_double())


def json_credentials_key_test_double() -> str:
    return "az-key-json-credentials"


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


def json_encoded_test_double() -> str:
    return "CiAgICAgICAgewogICAgICAgICAgICAidHlwZSI6ICJzZXJ2aWNlX2FjY291bnQiLAogICAgICAgICAgICAicHJvamVjdF9pZCI6ICJnY3AtZGtzLWVjbWRlLXNib3giLAogICAgICAgICAgICAicHJpdmF0ZV9rZXlfaWQiOiAiODY3NTMwOSIsCiAgICAgICAgICAgICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS0KTUlJRXZnSUJMSTgxTAotLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tCiIsCiAgICAgICAgICAgICJjbGllbnRfZW1haWwiOiAiYS1zZXJ2aWNlLWFjY291bnRAcGFyZW50LXByb2plY3QtaWQuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20iLAogICAgICAgICAgICAiY2xpZW50X2lkIjogIjEyMzQ1Njc4OTAiLAogICAgICAgICAgICAiYXV0aF91cmkiOiAiaHR0cHM6Ly9hY2NvdW50cy5nb29nbGUuY29tL28vb2F1dGgyL2F1dGgiLAogICAgICAgICAgICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAgICAgICAgICAgImF1dGhfcHJvdmlkZXJfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9vYXV0aDIvdjEvY2VydHMiLAogICAgICAgICAgICAiY2xpZW50X3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vcm9ib3QvdjEvbWV0YWRhdGEveDUwOS9hLXNlcnZpY2UtYWNjb3VudCU0MHBhcmVudC1wcm9qZWN0LWlkLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAgICAgICAgICAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKICAgICAgICB9CiAgICA="


@pytest.mark.unit
class TestBigQueryCredentials:
    def test_property_validation(self):
        with pytest.raises(IllegalArgumentError):
            BigQueryCredentials("")

    def test_from_spark_conf(self, credentials, spark, monkeypatch):
        def mock_spark_conf(*args):
            return get_spark_param_value(args)

        def mock_db_utils(*args):
            return get_secrets_value(args)

        monkeypatch.setattr(spark.conf, "get", mock_spark_conf)
        monkeypatch.setattr(dbutils.secrets, "get", mock_db_utils)

        actual = BigQueryCredentials.from_spark_conf(spark)
        assert credentials == actual

    def test_from_notebook_params(self, credentials, spark, monkeypatch):
        def mock_db_utils(*args):
            return get_dbutils_param_value(args)

        def mock_db_secrets(*args):
            return get_secrets_value(args)

        monkeypatch.setattr(dbutils.secrets, "get", mock_db_secrets)
        monkeypatch.setattr(dbutils.widgets, "get", mock_db_utils)

        actual = BigQueryCredentials.from_notebook_params()
        assert credentials == actual

    def test_json_credentials_b64encoded(self, credentials):
        assert json_encoded_test_double() == credentials.json_credentials_b64encoded()

    def test_from_notebook_params_with_error(self):
        with pytest.raises(KeyError):
            BigQueryCredentials.from_notebook_params()
