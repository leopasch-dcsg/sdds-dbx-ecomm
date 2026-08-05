from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit


class GoldDeletableUnpublishedCategoriesTransform:
    """
    Gold-layer transform for the `del_unpub_cats` pipeline.

    Reads the silver latest-state view and outputs the set of category ids that
    are unpublished and eligible for deletion (i.e. `is_published = false` and
    unchanged for at least `stale_days` days).
    """

    def __init__(
        self,
        spark: SparkSession,
        silver_table: str,
        gold_table: str,
        dbx_user_id: str,
        stale_days: int = 30,
        batch_date_utc: datetime = None,
    ):
        self.spark = spark
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.dbx_user_id = dbx_user_id
        self.stale_days = stale_days
        self.batch_date_utc = batch_date_utc or datetime.now(timezone.utc)

    def read(self) -> DataFrame:
        return self.spark.table(self.silver_table)

    def transform(self, df: DataFrame) -> DataFrame:
        cutoff = self.spark.sql(
            f"SELECT current_timestamp() - INTERVAL {self.stale_days} DAYS AS cutoff"
        ).collect()[0]["cutoff"]

        return (
            df.filter(col("is_published") == lit(False))
            .filter(col("updated_at") <= lit(cutoff))
            .select(
                col("category_id"),
                col("category_name"),
                col("is_published"),
                col("updated_at"),
            )
            .withColumn("stale_days_threshold", lit(self.stale_days))
            .withColumn("flagged_on_utc", lit(self.batch_date_utc))
            .withColumn("flagged_by", lit(self.dbx_user_id))
        )

    def write(self, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(self.gold_table)
        )

    def execute(self) -> int:
        df = self.transform(self.read()).cache()
        count = df.count()
        self.write(df)
        return count
