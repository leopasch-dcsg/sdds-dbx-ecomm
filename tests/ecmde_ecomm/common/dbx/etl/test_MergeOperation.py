from datetime import datetime, timezone
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.dbx.etl.merge import (
    MergeOperation,
    MergeConfig,
)
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture(scope="module")
def schema_timestamp_type() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("silver_layer_timestamp", TimestampType(), False),
            StructField("silver_layer_update_timestamp", TimestampType(), True),
        ]
    )


@pytest.fixture(scope="module")
def schema_string_timestamp_type() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("silver_layer_timestamp", StringType(), False),
            StructField("silver_layer_update_timestamp", StringType(), True),
        ]
    )


@pytest.fixture
def schema_string_timestamp_type_data():
    return [(1, timestamp_str_test_double(), timestamp_str_test_double())]


@pytest.fixture
def schema_timestamp_type_data():
    return [(1, timestamp_test_double(), timestamp_test_double())]


@pytest.fixture
def operation_with_timestamp_watermark(spark: SparkSession) -> MergeOperation:
    config = MergeConfig(
        "source_catalog",
        "source_schema",
        "source_table",
        "gold_catalog",
        "gold_schema",
        "gold_table",
        "user_id",
    )

    watermark = Watermark(
        "dev_ecmde_db.common.watermarks",
        config.source_catalog,
        config.source_schema,
        config.source_table,
        spark,
    )
    return MergeOperationTestDouble(config, watermark, spark)


@pytest.fixture
def operation_with_string_watermark(spark: SparkSession) -> MergeOperation:
    config = MergeConfig(
        "source_catalog",
        "source_schema",
        "source_table",
        "gold_catalog",
        "gold_schema",
        "gold_table",
        "user_id",
    )

    watermark = Watermark(
        "dev_ecmde_db.common.watermarks",
        config.source_catalog,
        config.source_schema,
        config.source_table,
        spark,
    )
    return MergeOperationTestDouble(config, watermark, spark)


def timestamp_str_test_double() -> str:
    return "2024-08-28T16:00:00 UTC"


def timestamp_test_double() -> datetime:
    expected = "2024-08-28T16:00:00.000000+00:00"
    return datetime.strptime(expected, "%Y-%m-%dT%H:%M:%S.%f%z")


class MergeOperationTestDouble(MergeOperation):
    def __init__(self, config: MergeConfig, watermark: Watermark, spark: SparkSession):
        super().__init__(config, watermark, spark)

    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return ""

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return incremental_changeset

    def merge_updates(self, updates: DataFrame) -> None:
        return None


@pytest.mark.unit
class TestMergeOperation:
    def test_get_initial_silver_dataframe_timestamp_watermark_type(
        self,
        schema_timestamp_type: StructType,
        spark: SparkSession,
        operation_with_timestamp_watermark: MergeOperation,
        monkeypatch,
    ):
        self.perform_get_incremental_changeset_test(
            operation_with_timestamp_watermark,
            schema_timestamp_type,
            spark,
            monkeypatch,
        )

    def test_get_incremental_changeset_string_watermark_type(
        self,
        schema_string_timestamp_type: StructType,
        spark: SparkSession,
        operation_with_string_watermark: MergeOperation,
        monkeypatch,
    ):
        self.perform_get_incremental_changeset_test(
            operation_with_string_watermark,
            schema_string_timestamp_type,
            spark,
            monkeypatch,
        )

    @staticmethod
    def perform_get_incremental_changeset_test(
        operation: MergeOperation,
        schema: StructType,
        spark: SparkSession,
        monkeypatch,
    ):
        def mock_spark_sql(*args, **kwargs):
            return spark.createDataFrame([], schema)

        monkeypatch.setattr(operation.spark, "sql", mock_spark_sql)

        last_watermark_utc = datetime.fromtimestamp(0, timezone.utc)
        df = operation.get_incremental_changeset(
            last_watermark_utc,
        )

        actual_column_names = df.columns
        expected_column_names = schema.names

        intersection = set(actual_column_names).intersection(expected_column_names)
        assert len(intersection) == len(expected_column_names)
