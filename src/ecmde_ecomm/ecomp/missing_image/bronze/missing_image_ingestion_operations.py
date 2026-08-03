from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from ecmde_ecomm.common.mysql.mysql_ingestion import MySqlIngestion


class BronzeBypassEcodeImageCheckIngestion(MySqlIngestion):
    """
    Ingests rows from MySQL `bypass_ecode_image_check` into the bronze Delta table.
    Appends all rows updated since the last watermark and adds audit columns.
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


class BronzeBypassColorImageCheckIngestion(MySqlIngestion):
    """
    Ingests rows from MySQL `bypass_color_image_check` into the bronze Delta table.
    Appends all rows updated since the last watermark and adds audit columns.
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
