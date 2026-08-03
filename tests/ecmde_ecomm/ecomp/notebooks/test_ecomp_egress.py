import sys
import pytest
from tests.ecmde_ecomm.fixtures import spark

import ecmde_ecomm.common.spark
from ecmde_ecomm.common import CredentialUtil
from ecmde_ecomm.common.bq import (
    BigQueryCredentials,
    BigQueryDestination,
    DatabricksSource,
    BigQueryEgressOperation,
)
from ecmde_ecomm.ecomp.notebooks.bq.ecomp_egress import (
    parse_args,
    egress_spark_session,
    bigquery_credentials,
    bigquery_destination,
    databricks_source,
    egress_data,
    main,
)


def az_key_scope_test_double() -> str:
    return "kv-dsg-ecmde-dbx"


def source_catalog_test_double() -> str:
    return "dev_ecmde_db"


def source_schema_test_double() -> str:
    return "test_schema"


def source_table_test_double() -> str:
    return "test_table_name"


def bq_credentials_json_credentials_key_test_double() -> str:
    return "credential_key_name"


def bq_bucket_test_double() -> str:
    return "dbx-gcp-ecom-ddw-bucket"


def bq_parent_project_id_test_double() -> str:
    return "gcp-dks-ecmde-sbox"


def bq_project_id_test_double() -> str:
    return "gn-ddw-project01"


def bq_dataset_test_double() -> str:
    return "ecm_incoming"


def bq_table_test_double() -> str:
    return "pim_product_emast"


@pytest.fixture
def test_args() -> list[str]:
    return [
        "prog",
        "--azure_kv_scope",
        az_key_scope_test_double(),
        "--source_catalog",
        source_catalog_test_double(),
        "--source_schema",
        source_schema_test_double(),
        "--source_table",
        source_table_test_double(),
        "--bq_credentials_json_credentials_key",
        bq_credentials_json_credentials_key_test_double(),
        "--bq_bucket",
        bq_bucket_test_double(),
        "--bq_parent_project_id",
        bq_parent_project_id_test_double(),
        "--bq_project_id",
        bq_project_id_test_double(),
        "--bq_dataset",
        bq_dataset_test_double(),
        "--bq_table",
        bq_table_test_double(),
    ]


@pytest.mark.unit
class TestEcompEgress:
    def test_parse_args(self, test_args, monkeypatch):
        monkeypatch.setattr(sys, "argv", test_args)

        args = parse_args()
        assert args.azure_kv_scope == "kv-dsg-ecmde-dbx"
        assert args.source_catalog == "dev_ecmde_db"
        assert args.source_schema == "test_schema"
        assert args.source_table == "test_table_name"
        assert args.bq_credentials_json_credentials_key == "credential_key_name"
        assert args.bq_bucket == "dbx-gcp-ecom-ddw-bucket"
        assert args.bq_parent_project_id == "gcp-dks-ecmde-sbox"
        assert args.bq_project_id == "gn-ddw-project01"
        assert args.bq_dataset == "ecm_incoming"
        assert args.bq_table == "pim_product_emast"

    @pytest.mark.parametrize("expected", [BigQueryCredentials("{token='some_token'}")])
    def test_big_query_credentials(self, expected, test_args, monkeypatch):
        monkeypatch.setattr(sys, "argv", test_args)
        monkeypatch.setattr(
            CredentialUtil, "secret", lambda *args: expected.json_credentials
        )

        parsed_args = parse_args()
        credentials = bigquery_credentials(parsed_args)
        assert credentials == expected

    @pytest.mark.parametrize(
        "expected",
        [
            BigQueryDestination(
                bq_bucket_test_double(),
                bq_parent_project_id_test_double(),
                bq_project_id_test_double(),
                bq_dataset_test_double(),
                bq_table_test_double(),
            )
        ],
    )
    def test_big_query_destination(self, expected, test_args, monkeypatch):
        monkeypatch.setattr(sys, "argv", test_args)
        parsed_args = parse_args()
        destination = bigquery_destination(parsed_args)
        assert destination == expected

    @pytest.mark.parametrize(
        "expected",
        [
            DatabricksSource(
                source_catalog_test_double(),
                source_schema_test_double(),
                source_table_test_double(),
            )
        ],
    )
    def test_databricks_source(self, expected, test_args, monkeypatch):
        monkeypatch.setattr(sys, "argv", test_args)
        parsed_args = parse_args()
        source = databricks_source(parsed_args)
        assert source == expected

    def test_egress_spark_session(self, spark, monkeypatch):
        monkeypatch.setattr(
            ecmde_ecomm.common.spark.util, "spark_session", lambda *args: spark
        )

        session = egress_spark_session()
        assert "true" == session.conf.get("spark.sql.adaptive.enabled")
        assert "true" == session.conf.get(
            "spark.sql.adaptive.coalescePartitions.enabled"
        )
        assert "2000" == session.conf.get("spark.sql.shuffle.partitions")

    def test_egress_data(self, test_args, spark, monkeypatch):
        monkeypatch.setattr(sys, "argv", test_args)
        parsed_args = parse_args()

        monkeypatch.setattr(
            CredentialUtil, "secret", lambda *args: "{token='some_token'}"
        )

        monkeypatch.setattr(
            ecmde_ecomm.common.spark.util, "spark_session", lambda *args: spark
        )

        monkeypatch.setattr(
            ecmde_ecomm.ecomp.notebooks.bq.ecomp_egress,
            "operation",
            lambda *args: MockOperation(
                egress_spark_session(),
                bigquery_credentials(parsed_args),
                databricks_source(parsed_args),
                bigquery_destination(parsed_args),
            ),
        )

        assert (
            egress_data(
                egress_spark_session(),
                bigquery_credentials(parsed_args),
                bigquery_destination(parsed_args),
                databricks_source(parsed_args),
            )
            is None
        )

    def test_main(self, test_args, spark, monkeypatch):
        monkeypatch.setattr(sys, "argv", test_args)
        monkeypatch.setattr(
            CredentialUtil, "secret", lambda *args: "{token='some_token'}"
        )
        monkeypatch.setattr(
            ecmde_ecomm.common.spark.util, "spark_session", lambda *args: spark
        )

        monkeypatch.setattr(
            ecmde_ecomm.ecomp.notebooks.bq.ecomp_egress,
            "operation",
            lambda *args: MockOperation(
                egress_spark_session(),
                bigquery_credentials(parse_args()),
                databricks_source(parse_args()),
                bigquery_destination(parse_args()),
            ),
        )

        assert main() is None


class MockOperation(BigQueryEgressOperation):
    def execute(self) -> None:
        return None
