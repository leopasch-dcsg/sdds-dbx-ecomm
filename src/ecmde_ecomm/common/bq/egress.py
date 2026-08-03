import base64
from dataclasses import dataclass
from enum import Enum
from datetime import timezone, datetime
from pyspark.sql import SparkSession, DataFrame
from databricks.sdk.runtime import dbutils
from ecmde_ecomm.common.dbx.etl import Watermark
from ecmde_ecomm.common import Logger, IllegalArgumentError


class LoadMode(str, Enum):
    DELTA = "delta"
    TRUNCATE_LOAD = "truncate_load"


@dataclass(frozen=True)
class BigQueryCredentials:
    json_credentials: str

    def __post_init__(self):
        if len(self.json_credentials.strip()) == 0:
            raise IllegalArgumentError("json_credentials is missing or empty.")

    @staticmethod
    def from_spark_conf(spark: SparkSession):
        azure_kv_scope = spark.conf.get("azure_kv_scope")

        json_creds_az_vault_key = spark.conf.get(
            "dbx_bq_credentials_json_credentials_key"
        )

        json_credentials = dbutils.secrets.get(azure_kv_scope, json_creds_az_vault_key)
        return BigQueryCredentials(json_credentials)

    @staticmethod
    def from_notebook_params():
        azure_kv_scope = dbutils.widgets.get("azure_kv_scope")

        json_creds_az_vault_key = dbutils.widgets.get(
            "dbx_bq_credentials_json_credentials_key"
        )

        json_credentials = dbutils.secrets.get(
            azure_kv_scope,
            json_creds_az_vault_key,
        )

        return BigQueryCredentials(json_credentials)

    def json_credentials_b64encoded(self) -> str:
        json_bytes = self.json_credentials.encode("utf-8")
        return base64.b64encode(json_bytes).decode("utf-8")


@dataclass(frozen=True)
class BigQueryDestination:
    bucket: str
    parent_project_id: str
    project_id: str
    dataset: str
    table: str

    def __post_init__(self):
        if len(self.bucket.strip()) == 0:
            raise IllegalArgumentError("bucket is missing or empty.")

        if len(self.parent_project_id.strip()) == 0:
            raise IllegalArgumentError("parent_project_id is missing or empty.")

        if len(self.project_id.strip()) == 0:
            raise IllegalArgumentError("project_id is missing or empty.")

        if len(self.dataset.strip()) == 0:
            raise IllegalArgumentError("dataset is missing or empty.")

        if len(self.table.strip()) == 0:
            raise IllegalArgumentError("table is missing or empty.")

    @staticmethod
    def from_notebook_params():
        """
        To use this method, the following notebook parameters should be defined
            * dbx_bq_destination_bucket: the name of the temporary storage bucket that DBX will write to.
            * dbx_bq_destination_parent_project_id: the parent project of your service account (where operations will originate from)
            * dbx_bq_destination_project_id: the name of the project where you data will be written to.
            * dbx_bq_destination_dataset: the name of the dataset that exists in the project being written to.
            * dbx_bq_destination_table: the name of the table that will be written to.

        :return: BigQueryDestination
        """
        bucket = dbutils.widgets.get("dbx_bq_destination_bucket")
        parent_project_id = dbutils.widgets.get("dbx_bq_destination_parent_project_id")
        project_id = dbutils.widgets.get("dbx_bq_destination_project_id")
        dataset = dbutils.widgets.get("dbx_bq_destination_dataset")
        table = dbutils.widgets.get("dbx_bq_destination_table")

        return BigQueryDestination(
            bucket, parent_project_id, project_id, dataset, table
        )


@dataclass(frozen=True)
class DatabricksSource:
    catalog: str
    schema: str
    table: str

    def __post_init__(self):
        if len(self.catalog.strip()) == 0:
            raise IllegalArgumentError("catalog is missing or empty.")

        if len(self.schema.strip()) == 0:
            raise IllegalArgumentError("schema is missing or empty.")

        if len(self.table.strip()) == 0:
            raise IllegalArgumentError("table is missing or empty.")

    @staticmethod
    def from_notebook_params():
        catalog = dbutils.widgets.get("dbx_bq_source_catalog")
        schema = dbutils.widgets.get("dbx_bq_source_schema")
        table = dbutils.widgets.get("dbx_bq_source_table")

        return DatabricksSource(catalog, schema, table)

    def fully_qualified_table(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


class BigQueryEgressOperation:

    def __init__(
        self,
        spark: SparkSession,
        credentials: BigQueryCredentials,
        source: DatabricksSource,
        destination: BigQueryDestination,
        watermark: Watermark | None = None,
        load_mode: LoadMode = LoadMode.TRUNCATE_LOAD,
    ):
        self.spark = spark
        self.credentials = credentials
        self.source = source
        self.destination = destination
        self.watermark = watermark
        self.load_mode = load_mode
        self.__logger = Logger.logger(__class__.__name__)

    @staticmethod
    def builder(spark: SparkSession):
        return BigQueryEgressOperationBuilder(spark)

    def execute(self) -> None:
        self.__logger.info("Execute Big Query Egress Operation")
        self._configure_spark()
        current_batch_date_utc = datetime.now(timezone.utc)

        self.__logger.info("Fetching source dataframe.")

        (
            self.get_source_dataframe(self._last_batch_date_utc())
            .write.format("bigquery")
            .option("temporaryGcsBucket", self.destination.bucket)
            .option("parentProject", self.destination.parent_project_id)
            .option("project", self.destination.project_id)
            .option("dataset", self.destination.dataset)
            .option("table", self.destination.table)
            .mode("overwrite")
            .save()
        )

        if self.load_mode == LoadMode.DELTA and self.watermark:
            self.update_watermark(current_batch_date_utc)

    def _configure_spark(self) -> None:
        self.spark.conf.set(
            "credentials", self.credentials.json_credentials_b64encoded()
        )

    def _last_batch_date_utc(self) -> datetime | None:
        if self.watermark:
            last_batch_date = self.watermark.last_watermark_utc()
            self.__logger.info(f"Last Batch Date: {last_batch_date}")
            return last_batch_date

        return None

    def get_source_dataframe(
        self, last_batch_date_utc: datetime | None = None
    ) -> DataFrame:
        """
        This method can be overridden to customize the source dataframe that will be written to BigQuery.

        :return: The dataframe containing the dataset that will be written to BigQuery.
        """
        return self.spark.sql(f"select * from {self.source.fully_qualified_table()}")

    def update_watermark(self, current_batch_date_utc: datetime) -> None:
        if self.watermark:
            self.watermark.update_watermark_timestamp(current_batch_date_utc)
            self.__logger.info(
                f"Updating Watermark Timestamp: {current_batch_date_utc}"
            )


class BigQueryEgressOperationBuilder:
    spark: SparkSession
    bigquery_credentials: BigQueryCredentials | None = None
    bigquery_destination: BigQueryDestination | None = None
    databricks_source: DatabricksSource | None = None
    watermark: Watermark | None = None
    load_mode: LoadMode = LoadMode.TRUNCATE_LOAD

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def with_bigquery_credentials(self, bigquery_credentials: BigQueryCredentials):
        self.bigquery_credentials = bigquery_credentials
        return self

    def with_bigquery_destination(self, bigquery_destination: BigQueryDestination):
        self.bigquery_destination = bigquery_destination
        return self

    def with_databricks_source(self, databricks_source: DatabricksSource):
        self.databricks_source = databricks_source
        return self

    def with_watermark(self, watermark: Watermark | None = None):
        self.watermark = watermark
        return self

    def with_load_mode(self, load_mode: LoadMode):
        self.load_mode = load_mode
        return self

    def operation(self) -> BigQueryEgressOperation:
        if self.bigquery_credentials is None:
            raise IllegalArgumentError(
                "Cannot create a BigQueryEgressOperation without credentials."
            )

        if self.bigquery_destination is None:
            raise IllegalArgumentError(
                "Cannot create a BigQueryEgressOperation without specifying the BQ destination."
            )

        if self.databricks_source is None:
            raise IllegalArgumentError(
                "Cannot create a BigQueryEgressOperation without specifying the Databricks source data."
            )

        return BigQueryEgressOperation(
            self.spark,
            self.bigquery_credentials,
            self.databricks_source,
            self.bigquery_destination,
            watermark=self.watermark,
            load_mode=self.load_mode,
        )
