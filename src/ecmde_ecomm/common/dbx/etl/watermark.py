import calendar
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row
from delta.tables import DeltaTable
from datetime import datetime, timezone
from ecmde_ecomm.common.errors import IllegalArgumentError


class Watermark:
    def __init__(
        self,
        watermark_table_name: str,
        catalog: str,
        schema: str,
        table: str,
        spark: SparkSession,
    ):
        if len(watermark_table_name.strip()) == 0:
            raise IllegalArgumentError("watermark_table_name cannot be empty string.")
        if len(catalog.strip()) == 0:
            raise IllegalArgumentError("catalog name cannot be empty string.")
        if len(schema.strip()) == 0:
            raise IllegalArgumentError("schema name cannot be empty string.")
        if len(table.strip()) == 0:
            raise IllegalArgumentError("table name cannot be empty string.")

        self.watermark_table_name = watermark_table_name
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.spark = spark

    def last_watermark_utc(
        self, default_timestamp_utc=datetime.fromtimestamp(0, timezone.utc)
    ) -> datetime:
        """
        Returns the last processed watermark date time in UTC when the data for the specified table was last processed.
        If a record does not exist in this table yet, then a default timestamp of the unix epoch is returned.
        """

        results = self._get_watermark_result()

        if len(results) == 0:
            return default_timestamp_utc

        watermark_utc = datetime.fromtimestamp(
            results[0].watermark_epoch_sec_utc, timezone.utc
        )

        return watermark_utc

    def update_watermark_timestamp(
        self, timestamp_utc=datetime.now(timezone.utc)
    ) -> None:
        """
        Updates the last processed timestamp for the defined watermark record configurations
        """
        timestamp_epoch_sec_utc = calendar.timegm(timestamp_utc.timetuple())
        self._perform_merge_with_updates(self._get_updates_df(timestamp_epoch_sec_utc))

    def _get_updates_df(self, timestamp_epoch_sec_utc: int) -> DataFrame:
        updates_df = self.spark.createDataFrame(
            [(self.catalog, self.schema, self.table, timestamp_epoch_sec_utc)],
            schema=[
                "catalog_name",
                "schema_name",
                "table_name",
                "watermark_epoch_sec_utc",
            ],
        )
        updates_df.createOrReplaceTempView("updates")
        return updates_df

    def _perform_merge_with_updates(self, updates: DataFrame) -> None:
        condition = f"""
            watermark.catalog_name = updates.catalog_name
            AND watermark.schema_name = updates.schema_name
            AND watermark.table_name = updates.table_name
            AND watermark.table_name = '{self.table}'
        """

        params = {
            "catalog_name": "updates.catalog_name",
            "schema_name": "updates.schema_name",
            "table_name": "updates.table_name",
            "watermark_epoch_sec_utc": "updates.watermark_epoch_sec_utc",
        }

        (
            DeltaTable.forName(self.spark, self.watermark_table_name)
            .alias("watermark")
            .merge(updates.alias("updates"), condition)
            .whenMatchedUpdate(set=params)
            .whenNotMatchedInsert(values=params)
            .execute()
        )

    def _get_watermark_result(self) -> list[Row]:
        statement = f"""
            select * 
              from {self.watermark_table_name} 
             where catalog_name = '{self.catalog}'
               and schema_name = '{self.schema}'
               and table_name = '{self.table}'
            """
        return self.spark.sql(statement).collect()
