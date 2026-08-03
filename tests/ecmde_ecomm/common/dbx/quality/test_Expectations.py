from py4j.protocol import Py4JJavaError
from tests.ecmde_ecomm.fixtures import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

from ecmde_ecomm.common.dbx.quality import expectation


@pytest.fixture
def schema() -> StructType:
    return StructType(
        [
            StructField("id", LongType(), False),
            StructField("name", StringType(), False),
            StructField("description", StringType(), True),
            StructField("use_case", StringType(), False),
        ]
    )


@pytest.fixture
def data(spark: SparkSession, schema: StructType) -> DataFrame:
    records = [
        (1, "Jerry", "Employee", "valid record."),
        (2, "Tammy", "", "invalid record with empty string description."),
        (3, "Terry", None, "invalid record with null description."),
        (4, "Jimmy", " ", "invalid record with whitespace description."),
        (5, "Margaret", "Employee", "valid record, but name not in list."),
    ]

    return spark.createDataFrame(records, schema=schema, verifySchema=True)


@pytest.mark.unit
class TestExpectations:
    def test_not_null(self, data: DataFrame) -> None:
        with pytest.raises(Py4JJavaError):
            (
                data.filter(data["id"] == 3)
                .select(expectation.not_null(data["description"]))
                .collect()
            )

        rows = (
            data.filter(data["id"] == 1)
            .select(expectation.not_null(data["description"]).alias("description"))
            .collect()
        )

        assert len(rows) == 1
        assert rows[0].description == "Employee"

    def test_not_empty(self, data: DataFrame) -> None:
        with pytest.raises(Py4JJavaError):
            (
                data.filter(data["id"] == 2)
                .select(expectation.not_empty(data["description"]))
                .collect()
            )

        with pytest.raises(Py4JJavaError):
            (
                data.filter(data["id"] == 3)
                .select(expectation.not_empty(data["description"], nullable=False))
                .collect()
            )

        with pytest.raises(Py4JJavaError):
            (
                data.filter(data["id"] == 4)
                .select(expectation.not_empty(data["description"], nullable=False))
                .collect()
            )

        rows = (
            data.filter(data["id"] == 1)
            .select(expectation.not_empty(data["description"]).alias("description"))
            .collect()
        )

        assert len(rows) == 1
        assert rows[0].description == "Employee"

    def test_one_of(self, data: DataFrame) -> None:
        with pytest.raises(Py4JJavaError):
            (
                data.filter(data["id"] == 5)
                .select(
                    expectation.one_of(
                        data["name"], ["Jerry", "Tammy", "Terry", "Timmy"]
                    ).alias("name")
                )
                .collect()
            )

        with pytest.raises(Py4JJavaError):
            (
                data.filter((data["id"] == 4) | (data["id"] == 5))
                .select(
                    expectation.one_of(
                        data["name"], ["Jerry", "Tammy", "Terry", "Timmy"]
                    ).alias("name")
                )
                .collect()
            )

        record = (
            data.filter((data["id"] == 4) | (data["id"] == 5))
            .select(
                expectation.one_of(data["name"], ["Margaret", "Jimmy"]).alias("name")
            )
            .orderBy("id")
            .collect()
        )

        assert len(record) == 2
        assert record[0].name == "Jimmy"
        assert record[1].name == "Margaret"
