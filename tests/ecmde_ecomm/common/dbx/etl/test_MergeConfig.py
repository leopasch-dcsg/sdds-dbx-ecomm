import pytest
from databricks.sdk.runtime import dbutils
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.dbx.env import DbxEnv
from ecmde_ecomm.common.dbx.etl.merge import MergeConfig


@pytest.fixture
def config() -> MergeConfig:
    return MergeConfig(
        "silver_catalog",
        "silver_schema",
        "silver_table",
        "gold_catalog",
        "gold_schema",
        "gold_table",
        "user_id",
    )


def get_dbutils_value(*args):
    param_name = args[0][0]
    match param_name:
        case "dbx_merge_source_catalog":
            return "silver_catalog"
        case "dbx_merge_source_schema":
            return "silver_schema"
        case "dbx_merge_source_table":
            return "silver_table"
        case "dbx_merge_destination_catalog":
            return "gold_catalog"
        case "dbx_merge_destination_schema":
            return "gold_schema"
        case "dbx_merge_destination_table":
            return "gold_table"
        case "dbx_env":
            return DbxEnv.DEV.environment
        case "dbx_user_id":
            return "user_id"
        case _:
            return None


@pytest.mark.unit
class TestMergeConfig:
    def test_config_with_missing_values(self):
        with pytest.raises(IllegalArgumentError):
            MergeConfig(
                "",
                "silver_schema",
                "silver_table",
                "gold_catalog",
                "gold_schema",
                "gold_table",
                "user_id",
            )

        with pytest.raises(IllegalArgumentError):
            MergeConfig(
                "silver_catalog",
                "",
                "silver_table",
                "gold_catalog",
                "gold_schema",
                "gold_table",
                "user_id",
            )

        with pytest.raises(IllegalArgumentError):
            MergeConfig(
                "silver_catalog",
                "silver_schema",
                "",
                "gold_catalog",
                "gold_schema",
                "gold_table",
                "user_id",
            )

        with pytest.raises(IllegalArgumentError):
            MergeConfig(
                "silver_catalog",
                "silver_schema",
                "silver_table",
                "",
                "gold_schema",
                "gold_table",
                "user_id",
            )

        with pytest.raises(IllegalArgumentError):
            MergeConfig(
                "silver_catalog",
                "silver_schema",
                "silver_table",
                "gold_catalog",
                "",
                "gold_table",
                "user_id",
            )

        with pytest.raises(IllegalArgumentError):
            MergeConfig(
                "silver_catalog",
                "silver_schema",
                "silver_table",
                "gold_catalog",
                "gold_schema",
                "",
                "user_id",
            )

        with pytest.raises(IllegalArgumentError):
            MergeConfig(
                "silver_catalog",
                "silver_schema",
                "silver_table",
                "gold_catalog",
                "gold_schema",
                "gold_table",
                "",
            )

    def test_silver_table_qualified(self, config):
        expected = (
            f"{config.source_catalog}.{config.source_schema}.{config.source_table}"
        )
        assert expected == config.source_table_qualified()

    def test_gold_table_qualified(self, config):
        expected = f"{config.destination_catalog}.{config.destination_schema}.{config.destination_table}"
        assert expected == config.destination_table_qualified()

    def test_config_from_notebook_params(self, config, monkeypatch):
        def mock_dbutils_widgets_get(*args, **kwargs):
            return get_dbutils_value(args)

        monkeypatch.setattr(dbutils.widgets, "get", mock_dbutils_widgets_get)
        actual = MergeConfig.from_notebook_params()
        assert config == actual
