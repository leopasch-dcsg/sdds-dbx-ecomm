import pytest
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.dbx.kafka.conf import KafkaSchemaRegistryConfig


@pytest.fixture
def dbx_schema_registry_url_test() -> str:
    return "https://schem/registry/url.com"


@pytest.fixture
def schema_credentials_source() -> str:
    return "USER_INFO"


@pytest.fixture
def schema_credentials_user_info(
    dbx_schema_registry_user_test, dbx_schema_registry_password_test
) -> str:
    return f"{dbx_schema_registry_user_test}:{dbx_schema_registry_password_test}"


@pytest.fixture
def dbx_schema_registry_user_test() -> str:
    return "user_1"


@pytest.fixture
def dbx_schema_registry_password_test() -> str:
    return "password_1"


@pytest.fixture
def dbx_schema_registry_key_subject_test() -> str:
    return "schema_key_subject"


@pytest.fixture
def dbx_schema_registry_value_subject_test() -> str:
    return "schema_value_subject"


@pytest.mark.unit
class TestKafkaSchemaRegistryConfig:
    def test_no_url(
        self,
        dbx_schema_registry_user_test,
        dbx_schema_registry_password_test,
        dbx_schema_registry_key_subject_test,
        dbx_schema_registry_value_subject_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaSchemaRegistryConfig(
                "",
                dbx_schema_registry_user_test,
                dbx_schema_registry_password_test,
                dbx_schema_registry_key_subject_test,
                dbx_schema_registry_value_subject_test,
            )

    def test_no_user(
        self,
        dbx_schema_registry_url_test,
        dbx_schema_registry_password_test,
        dbx_schema_registry_key_subject_test,
        dbx_schema_registry_value_subject_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaSchemaRegistryConfig(
                dbx_schema_registry_url_test,
                "",
                dbx_schema_registry_password_test,
                dbx_schema_registry_key_subject_test,
                dbx_schema_registry_value_subject_test,
            )

    def test_no_password(
        self,
        dbx_schema_registry_url_test,
        dbx_schema_registry_user_test,
        dbx_schema_registry_key_subject_test,
        dbx_schema_registry_value_subject_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaSchemaRegistryConfig(
                dbx_schema_registry_url_test,
                dbx_schema_registry_user_test,
                "",
                dbx_schema_registry_key_subject_test,
                dbx_schema_registry_value_subject_test,
            )

    def test_no_key_subject(
        self,
        dbx_schema_registry_url_test,
        dbx_schema_registry_user_test,
        dbx_schema_registry_password_test,
        dbx_schema_registry_value_subject_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaSchemaRegistryConfig(
                dbx_schema_registry_url_test,
                dbx_schema_registry_user_test,
                dbx_schema_registry_password_test,
                "",
                dbx_schema_registry_value_subject_test,
            )

    def test_no_value_subject(
        self,
        dbx_schema_registry_url_test,
        dbx_schema_registry_user_test,
        dbx_schema_registry_password_test,
        dbx_schema_registry_key_subject_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaSchemaRegistryConfig(
                dbx_schema_registry_url_test,
                dbx_schema_registry_user_test,
                dbx_schema_registry_password_test,
                dbx_schema_registry_key_subject_test,
                "",
            )

    def test_schema_registry_conf(
        self,
        dbx_schema_registry_url_test,
        dbx_schema_registry_user_test,
        dbx_schema_registry_password_test,
        dbx_schema_registry_key_subject_test,
        dbx_schema_registry_value_subject_test,
        schema_credentials_source,
        schema_credentials_user_info,
    ):
        config = KafkaSchemaRegistryConfig(
            dbx_schema_registry_url_test,
            dbx_schema_registry_user_test,
            dbx_schema_registry_password_test,
            dbx_schema_registry_key_subject_test,
            dbx_schema_registry_value_subject_test,
        )

        assert (
            schema_credentials_source
            == config.schema_registry_conf()[
                "confluent.schema.registry.basic.auth.credentials.source"
            ]
        )
        assert (
            schema_credentials_user_info
            == config.schema_registry_conf()[
                "confluent.schema.registry.basic.auth.user.info"
            ]
        )
