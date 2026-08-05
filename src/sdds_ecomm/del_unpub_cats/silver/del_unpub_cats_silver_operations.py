from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window


class SilverUnpublishedCategoriesTransform:
    """
    Silver-layer transform for the `del_unpub_cats` pipeline.

    Reads the bronze delta table and produces a deduplicated, latest-state
    view keyed by `category_id`.
    """

    def __init__(
        self,
        spark: SparkSession,
        bronze_table: str,
        silver_table: str,
        dbx_user_id: str,
        batch_date_utc: datetime = None,
    ):
        self.spark = spark
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.dbx_user_id = dbx_user_id
        self.batch_date_utc = batch_date_utc or datetime.now(timezone.utc)

    def read(self) -> DataFrame:
        return self.spark.table(self.bronze_table)

    def transform(self, df: DataFrame) -> DataFrame:
        # Keep the newest row per category_id based on the source updated_at column.
        w = Window.partitionBy("category_id").orderBy(col("updated_at").desc())
        latest = (
            df.withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        return (
            latest.withColumn("processed_on_utc", lit(self.batch_date_utc))
            .withColumn("processed_by", lit(self.dbx_user_id))
        )

    def write(self, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(self.silver_table)
        )

    def execute(self) -> int:
        df = self.transform(self.read()).cache()
        count = df.count()
        self.write(df)
        return count
