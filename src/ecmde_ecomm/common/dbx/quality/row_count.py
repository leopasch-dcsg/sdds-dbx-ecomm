from pyspark.sql import SparkSession


class RowCount:
    def __init__(self, sql_statement: str, spark: SparkSession):
        # SQL statement must return a single column aliased as "record_count".
        self.sql_statement = sql_statement
        self.spark = spark

    def is_empty(self) -> bool:
        return self.get_row_count() == 0

    def is_not_empty(self) -> bool:
        return not self.is_empty()

    def expect_row_count(self, expected_row_count: int) -> bool:
        return self.get_row_count() == expected_row_count

    def get_row_count(self) -> int:
        result = self.spark.sql(self.sql_statement).collect()
        return result[0].record_count