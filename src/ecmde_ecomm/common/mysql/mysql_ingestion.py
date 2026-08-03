from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone

from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame, SparkSession

from ecmde_ecomm.common.dbx.env import DbxEnv
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.util import NotebookUtil


@dataclass(frozen=True)
class MySqlIngestResult:
    source_table: str
    destination_table_qualified: str
    records_processed: int
    initial_watermark_timestamp_utc: datetime
    next_watermark_timestamp_utc: datetime


@dataclass
class MySqlIngestConfig:
    jdbc_url: str
    username: str
    password: str
    source_table: str
    destination_catalog: str
    destination_schema: str
    destination_table: str
    watermark_column: str
    dbx_user_id: str
    dbx_env: DbxEnv = DbxEnv.DEV

    def __post_init__(self):
        if not self.jdbc_url.strip():
            raise IllegalArgumentError("jdbc_url is missing or empty.")
        if not self.username.strip():
            raise IllegalArgumentError("username is missing or empty.")
        if not self.destination_catalog.strip():
            raise IllegalArgumentError("destination_catalog is missing or empty.")
        if not self.destination_schema.strip():
            raise IllegalArgumentError("destination_schema is missing or empty.")
        if not self.destination_table.strip():
            raise IllegalArgumentError("destination_table is missing or empty.")
        if not self.watermark_column.strip():
            raise IllegalArgumentError("watermark_column is missing or empty.")
        if not self.dbx_user_id.strip():
            raise IllegalArgumentError("dbx_user_id is missing or empty.")

    def destination_table_qualified(self) -> str:
        return f"{self.destination_catalog}.{self.destination_schema}.{self.destination_table}"

    @staticmethod
    def from_notebook_params():
        azure_kv_scope = NotebookUtil.notebook_param("azure_kv_scope")
        jdbc_url = NotebookUtil.notebook_param("mysql_jdbc_url")
        username_key = NotebookUtil.notebook_param("mysql_username_key")
        password_key = NotebookUtil.notebook_param("mysql_password_key")
        username = dbutils.secrets.get(azure_kv_scope, username_key)
        password = dbutils.secrets.get(azure_kv_scope, password_key)
        source_table = NotebookUtil.notebook_param("dbx_source_table")
        destination_catalog = NotebookUtil.notebook_param("dbx_destination_catalog")
        destination_schema = NotebookUtil.notebook_param("dbx_destination_schema")
        destination_table = NotebookUtil.notebook_param("dbx_destination_table")
        watermark_column = NotebookUtil.notebook_param("mysql_watermark_column")
        dbx_user_id = NotebookUtil.notebook_param("dbx_user_id")
        dbx_env = DbxEnv.from_notebook_params()

        return MySqlIngestConfig(
            jdbc_url=jdbc_url,
            username=username,
            password=password,
            source_table=source_table,
            destination_catalog=destination_catalog,
            destination_schema=destination_schema,
            destination_table=destination_table,
            watermark_column=watermark_column,
            dbx_user_id=dbx_user_id,
            dbx_env=dbx_env,
        )


class MySqlIngestion(ABC):
    MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

    def __init__(
        self,
        config: MySqlIngestConfig,
        watermark: Watermark,
        spark: SparkSession,
        batch_date_utc: datetime = datetime.now(timezone.utc),
    ):
        self.__config = config
        self.__watermark = watermark
        self.__spark = spark
        self.__batch_date_utc = batch_date_utc

    @property
    def config(self) -> MySqlIngestConfig:
        return self.__config

    @property
    def watermark(self) -> Watermark:
        return self.__watermark

    @property
    def spark(self) -> SparkSession:
        return self.__spark

    @property
    def batch_date_utc(self) -> datetime:
        return self.__batch_date_utc

    def _read_from_jdbc(self, last_watermark_utc: datetime) -> DataFrame:
        watermark_str = last_watermark_utc.strftime("%Y-%m-%d %H:%M:%S")
        subquery = (
            f"(SELECT * FROM {self.config.source_table} "
            f"WHERE {self.config.watermark_column} > '{watermark_str}') t"
        )
        return (
            self.spark.read.format("jdbc")
            .option("url", self.config.jdbc_url)
            .option("dbtable", subquery)
            .option("user", self.config.username)
            .option("password", self.config.password)
            .option("driver", self.MYSQL_DRIVER)
            .load()
        )

    @abstractmethod
    def perform_transforms(self, df: DataFrame) -> DataFrame:
        """Apply domain-specific transforms and add audit columns."""
        pass

    @abstractmethod
    def write_to_destination(self, df: DataFrame) -> None:
        """Write the transformed DataFrame to the destination Delta table."""
        pass

    def execute(self) -> MySqlIngestResult:
        initial_watermark = self.watermark.last_watermark_utc()
        df = self._read_from_jdbc(initial_watermark)
        df = self.perform_transforms(df).cache()
        records = df.count()

        if records > 0:
            self.write_to_destination(df)

        next_watermark = self.batch_date_utc
        self.watermark.update_watermark_timestamp(next_watermark)

        return MySqlIngestResult(
            source_table=self.config.source_table,
            destination_table_qualified=self.config.destination_table_qualified(),
            records_processed=records,
            initial_watermark_timestamp_utc=initial_watermark,
            next_watermark_timestamp_utc=next_watermark,
        )
