import pytest
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.dbx.kafka.conf import KafkaConfig


@pytest.fixture
def dbx_topic_test() -> list[str]:
    return ["red", "blue", "purple"]


@pytest.fixture
def dbx_jaas_test() -> str:
    return "jaas"


@pytest.fixture
def dbx_bootstrap_server_test() -> str:
    return "server"


@pytest.fixture
def dbx_session_timeout_test() -> int:
    return 1000


@pytest.fixture
def dbx_fail_on_loss_test() -> bool:
    return False


@pytest.mark.unit
class TestKafkaConfig:
    def test_no_topic(
        self,
        dbx_jaas_test,
        dbx_bootstrap_server_test,
        dbx_session_timeout_test,
        dbx_fail_on_loss_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaConfig(
                [""],
                dbx_jaas_test,
                dbx_bootstrap_server_test,
                dbx_session_timeout_test,
                dbx_fail_on_loss_test,
            )

    def test_no_jaas_config(
        self,
        dbx_topic_test,
        dbx_bootstrap_server_test,
        dbx_session_timeout_test,
        dbx_fail_on_loss_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaConfig(
                dbx_topic_test,
                "",
                dbx_bootstrap_server_test,
                dbx_session_timeout_test,
                dbx_fail_on_loss_test,
            )

    def test_no_bootstrap_servers(
        self,
        dbx_topic_test,
        dbx_jaas_test,
        dbx_session_timeout_test,
        dbx_fail_on_loss_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaConfig(
                dbx_topic_test,
                dbx_jaas_test,
                "",
                dbx_session_timeout_test,
                dbx_fail_on_loss_test,
            )

    def test_no_session_timeout(
        self,
        dbx_topic_test,
        dbx_jaas_test,
        dbx_bootstrap_server_test,
        dbx_fail_on_loss_test,
    ):
        with pytest.raises(IllegalArgumentError):
            KafkaConfig(
                dbx_topic_test,
                dbx_jaas_test,
                dbx_bootstrap_server_test,
                0,
                dbx_fail_on_loss_test,
            )

    def test_subscribed_topics(
        self,
        dbx_topic_test,
        dbx_jaas_test,
        dbx_bootstrap_server_test,
        dbx_session_timeout_test,
        dbx_fail_on_loss_test,
    ):
        config = KafkaConfig(
            dbx_topic_test,
            dbx_jaas_test,
            dbx_bootstrap_server_test,
            dbx_session_timeout_test,
            dbx_fail_on_loss_test,
        )

        separator = ","
        expected = separator.join(dbx_topic_test)
        assert expected == config.subscribed_topics()
