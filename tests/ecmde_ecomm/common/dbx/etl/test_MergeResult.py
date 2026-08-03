import pytest
from datetime import datetime, timezone
from ecmde_ecomm.common.dbx.etl.merge import MergeResult
from ecmde_ecomm.common.errors import IllegalArgumentError


@pytest.mark.unit
class TestMergeResult:
    def test_empty_source_table(self):
        with pytest.raises(IllegalArgumentError):
            MergeResult(
                "",
                "catalog.schema.destination_table",
                0,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            )

    def test_empty_destination_table(self):
        with pytest.raises(IllegalArgumentError):
            MergeResult(
                "catalog.schema.source_table",
                "",
                0,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            )

    def test_source_table(self):
        result = MergeResult(
            "catalog.schema.source_table",
            "catalog.schema.destination_table",
            0,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )

        assert "catalog.schema.source_table" == result.source_table_qualified

    def test_destination_table(self):
        result = MergeResult(
            "catalog.schema.source_table",
            "catalog.schema.destination_table",
            0,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )

        assert "catalog.schema.destination_table" == result.destination_table_qualified

    def test_records_processed(self):
        expected = 1
        result = MergeResult(
            "catalog.schema.source_table",
            "catalog.schema.destination_table",
            expected,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )

        assert expected == result.records_processed

    def test_initial_watermark_timestamp_utc(self):
        expected = datetime.now(timezone.utc)
        result = MergeResult(
            "catalog.schema.source_table",
            "catalog.schema.destination_table",
            0,
            expected,
            datetime.now(timezone.utc),
        )

        assert expected == result.initial_watermark_timestamp_utc

    def test_next_watermark_timestamp_utc(self):
        expected = datetime.now(timezone.utc)
        result = MergeResult(
            "catalog.schema.source_table",
            "catalog.schema.destination_table",
            0,
            datetime.now(timezone.utc),
            expected,
        )

        assert expected == result.next_watermark_timestamp_utc
