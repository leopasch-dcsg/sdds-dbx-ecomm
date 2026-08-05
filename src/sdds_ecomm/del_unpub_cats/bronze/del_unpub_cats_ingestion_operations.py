from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from ecmde_ecomm.common.mysql.mysql_ingestion import MySqlIngestion


class BronzeUnpublishedCategoriesIngestion(MySqlIngestion):
    """
    Ingests rows from the MySQL source table that holds category publish state
    into the bronze Delta table for the `del_unpub_cats` pipeline.

    Appends rows updated since the last watermark and adds standard audit columns.
    """

    def perform_transforms(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("ingested_on_utc", lit(self.batch_date_utc))
            .withColumn("ingested_by", lit(self.config.dbx_user_id))
        )

    def write_to_destination(self, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(self.config.destination_table_qualified())
        )
