from abc import ABC, abstractmethod
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.util import NotebookUtil


@dataclass(frozen=True)
class ElasticBatchInsertResult:
    source_table_qualified: str
    source_df: DataFrame

    def __post_init__(self):
        if len(self.source_table_qualified) == 0:
            raise IllegalArgumentError(
                "source_table_qualified is cannot be empty string."
            )


@dataclass(frozen=True)
class ElasticBatchInsertConfig:
    environment: str
    source_catalog: str
    source_schema: str
    source_table: str
    elastic_index: str
    elastic_user: str
    azure_scope: str
    elastic_host_dev: str
    elastic_password_dev: str
    elastic_host_prodauth: str
    elastic_password_prodauth: str
    elastic_host_qa: str
    elastic_password_qa: str
    elastic_host_prodeast: str
    elastic_password_prodeast: str
    elastic_host_prodeast_reserve: str
    elastic_password_prodeast_reserve: str
    elastic_host_prodwest: str
    elastic_password_prodwest: str
    elastic_host_prodwest_reserve: str
    elastic_password_prodwest_reserve: str

    def __post_init__(self):
        if len(self.source_catalog.strip()) == 0:
            raise IllegalArgumentError("source_catalog is missing or empty.")

        if len(self.source_schema.strip()) == 0:
            raise IllegalArgumentError("source_schema is missing or empty.")

        if len(self.source_table.strip()) == 0:
            raise IllegalArgumentError("source_table is missing or empty.")

        if len(self.elastic_index.strip()) == 0:
            raise IllegalArgumentError("elastic_index is missing or empty.")

        if len(self.elastic_user.strip()) == 0:
            raise IllegalArgumentError("elastic_user is missing or empty.")

        if len(self.environment.strip()) == 0:
            raise IllegalArgumentError("environment is missing or empty.")

    def source_table_qualified(self) -> str:
        return f"{self.source_catalog}.{self.source_schema}.{self.source_table}"

    @staticmethod
    def from_notebook_params():
        """
        Create an instance of ElasticBatchInsertConfig from a set of standard notebook parameters.
        """
        environment = NotebookUtil.notebook_param("dbx_env")
        source_catalog = NotebookUtil.notebook_param("source_catalog")
        source_schema = NotebookUtil.notebook_param("source_schema")
        source_table = NotebookUtil.notebook_param("source_table")
        azure_scope = NotebookUtil.notebook_param("azure_kv_scope")
        elastic_index = NotebookUtil.notebook_param("elastic_index")
        elastic_user = NotebookUtil.notebook_param("elastic_user_key")

        elastic_host_dev = NotebookUtil.notebook_param("elastic_host_dev")
        elastic_password_dev = NotebookUtil.notebook_param("elastic_password_key_dev")

        elastic_host_prodauth = NotebookUtil.notebook_param("elastic_host_prodauth")
        elastic_password_prodauth = NotebookUtil.notebook_param(
            "elastic_password_key_prodauth"
        )

        elastic_host_qa = NotebookUtil.notebook_param("elastic_host_qa")
        elastic_password_qa = NotebookUtil.notebook_param("elastic_password_key_qa")

        elastic_host_prodeast = NotebookUtil.notebook_param("elastic_host_prodeast")
        elastic_password_prodeast = NotebookUtil.notebook_param(
            "elastic_password_key_prodeast"
        )

        elastic_host_prodeast_reserve = NotebookUtil.notebook_param(
            "elastic_host_prodeast_reserve"
        )
        elastic_password_prodeast_reserve = NotebookUtil.notebook_param(
            "elastic_password_key_prodeast_reserve"
        )

        elastic_host_prodwest = NotebookUtil.notebook_param("elastic_host_prodwest")
        elastic_password_prodwest = NotebookUtil.notebook_param(
            "elastic_password_key_prodwest"
        )

        elastic_host_prodwest_reserve = NotebookUtil.notebook_param(
            "elastic_host_prodwest_reserve"
        )
        elastic_password_prodwest_reserve = NotebookUtil.notebook_param(
            "elastic_password_key_prodwest_reserve"
        )

        return ElasticBatchInsertConfig(
            environment,
            source_catalog,
            source_schema,
            source_table,
            elastic_index,
            elastic_user,
            azure_scope,
            elastic_host_dev,
            elastic_password_dev,
            elastic_host_prodauth,
            elastic_password_prodauth,
            elastic_host_qa,
            elastic_password_qa,
            elastic_host_prodeast,
            elastic_password_prodeast,
            elastic_host_prodeast_reserve,
            elastic_password_prodeast_reserve,
            elastic_host_prodwest,
            elastic_password_prodwest,
            elastic_host_prodwest_reserve,
            elastic_password_prodwest_reserve,
        )


class ElasticBatchInsert(ABC):

    def __init__(
        self,
        config: ElasticBatchInsertConfig,
        spark: SparkSession,
        host: str,
        password: str,
    ):
        self.config = config
        self.spark = spark
        self.host = host
        self.password = password

    def execute(self) -> ElasticBatchInsertResult:
        source_dataframe: DataFrame = self.batch_dataframe()
        (
            source_dataframe.write.format("org.elasticsearch.spark.sql")
            .option("es.nodes", self.host)
            .option("es.port", "443")
            .option("es.index.auto.create", "true")
            .option("es.net.ssl", "true")
            .option("es.nodes.wan.only", "true")
            .option("es.resource", self.config.elastic_index)
            .option("es.net.http.auth.user", self.config.elastic_user)
            .option("es.net.http.auth.pass", self.password)
            .mode("append")
            .save()
        )
        return ElasticBatchInsertResult(
            self.config.source_table_qualified(),
            source_dataframe,
        )

    @abstractmethod
    def batch_dataframe(self) -> DataFrame:
        """
        Return a data frame that contains data for LTR. The dataframe
        returned should match the shape of the table the data will be inserted into.
        """
        pass
