import os
import time
import zoneinfo
import pytest

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.util import DateUtil
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def timestamp_ntz_test_double() -> str:
    return "2025-01-01T01:00:00"


@pytest.fixture
def timezone_test_double() -> str:
    return "America/New_York"


@pytest.fixture
def mock_dataframe(timestamp_ntz_test_double: str, spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("test_timestamp", StringType(), False),
        ]
    )

    data = [{"test_timestamp": timestamp_ntz_test_double}]
    return spark.createDataFrame(data, schema)


@pytest.mark.unit
class TestDateUtil:
    def test_make_timestamp_with_zone(
        self, mock_dataframe: DataFrame, timezone_test_double: str
    ):
        os.environ["TZ"] = "UTC"
        time.tzset()

        rows = (
            mock_dataframe.withColumn(
                "timestamp_utc",
                DateUtil.make_timestamp_with_zone(
                    col("test_timestamp"), timezone_test_double
                ),
            )
            .select("timestamp_utc")
            .collect()
        )

        etc_tz = zoneinfo.ZoneInfo("America/New_York")
        expected: datetime = datetime(2025, 1, 1, 1, 0, 0, tzinfo=etc_tz)
        actual: datetime = rows[0].timestamp_utc.astimezone(etc_tz)
        assert expected.isoformat() == actual.isoformat()

    def test_make_timestamp_with_zone_illegal_arguments(self):
        with pytest.raises(IllegalArgumentError):
            DateUtil.make_timestamp_with_zone(" ", "America/New_York")

        with pytest.raises(IllegalArgumentError):
            DateUtil.make_timestamp_with_zone("col_name", " ")
