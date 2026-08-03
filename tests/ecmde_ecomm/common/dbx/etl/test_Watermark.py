import calendar
from pyspark.sql.types import Row
from datetime import datetime, timezone
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from tests.ecmde_ecomm.fixtures import *


@pytest.fixture
def watermark_table_name_test_double() -> str:
    return "dev_ecmde_db.common.watermarks"


@pytest.fixture
def catalog_test_double() -> str:
    return "catalog_test_double"


@pytest.fixture
def schema_test_double() -> str:
    return "schema_test_double"


@pytest.fixture
def table_test_double() -> str:
    return "table_test_double"


@pytest.fixture
def watermark(
    spark,
    watermark_table_name_test_double: str,
    catalog_test_double: str,
    schema_test_double: str,
    table_test_double: str,
) -> Watermark:
    return Watermark(
        watermark_table_name_test_double,
        catalog_test_double,
        schema_test_double,
        table_test_double,
        spark,
    )


@pytest.mark.unit
class TestWatermark:
    def test_watermark_with_empty_watermark_table(
        self,
        spark,
        catalog_test_double,
        schema_test_double,
        table_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            Watermark(
                "",
                catalog_test_double,
                schema_test_double,
                table_test_double,
                spark,
            )

    def test_watermark_with_empty_catalog(
        self,
        spark,
        watermark_table_name_test_double,
        schema_test_double,
        table_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            Watermark(
                watermark_table_name_test_double,
                "",
                schema_test_double,
                table_test_double,
                spark,
            )

    def test_watermark_with_empty_schema(
        self,
        spark,
        watermark_table_name_test_double,
        catalog_test_double,
        schema_test_double,
        table_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            Watermark(
                watermark_table_name_test_double,
                catalog_test_double,
                "",
                table_test_double,
                spark,
            )

    def test_watermark_with_empty_table(
        self,
        spark,
        watermark_table_name_test_double,
        catalog_test_double,
        schema_test_double,
    ):
        with pytest.raises(IllegalArgumentError):
            Watermark(
                watermark_table_name_test_double,
                catalog_test_double,
                schema_test_double,
                "",
                spark,
            )

    def test_watermark_returning_default_datetime(
        self,
        spark,
        watermark,
        monkeypatch,
    ):
        def mock_get_watermark_result(*args, **kwargs):
            return []

        monkeypatch.setattr(
            watermark,
            "_get_watermark_result",
            mock_get_watermark_result,
        )

        expected_unix_epoch_utc = datetime.fromtimestamp(0, timezone.utc)
        actual = watermark.last_watermark_utc()
        assert expected_unix_epoch_utc == actual

    def test_watermark_return_existing_datetime(
        self,
        watermark,
        monkeypatch,
    ):
        expected = datetime.fromisoformat("2024-01-01T19:00:00.000000+00:00")

        def mock_get_watermark_result(*args, **kwargs):
            return [Row(watermark_epoch_sec_utc=calendar.timegm(expected.timetuple()))]

        monkeypatch.setattr(
            watermark,
            "_get_watermark_result",
            mock_get_watermark_result,
        )

        actual = watermark.last_watermark_utc()
        assert expected == actual

    def test_get_updates_df(self, watermark):
        expected = datetime.fromisoformat("2024-01-01T19:00:00.000000+00:00")
        expected_epoch_seconds = calendar.timegm(expected.timetuple())

        df = watermark._get_updates_df(expected_epoch_seconds)
        rows = df.collect()

        assert len(rows) == 1
        assert watermark.catalog == rows[0].catalog_name
        assert watermark.schema == rows[0].schema_name
        assert watermark.table == rows[0].table_name
        assert expected_epoch_seconds == rows[0].watermark_epoch_sec_utc
