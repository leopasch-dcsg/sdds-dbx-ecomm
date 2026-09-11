from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession


class NotebookUtil:

    @staticmethod
    def text_widget(param: str, default: str, label: str) -> None:
        dbutils.widgets.text(param, default, label)

    @staticmethod
    def notebook_param(param: str, default: str | None = None) -> str | None:
        try:
            value = dbutils.widgets.get(param).strip()
            if len(value) == 0:
                value = default
            return value
        except Exception:
            return default

    @staticmethod
    def spark_param(
        spark: SparkSession, param: str, default: str | None = None
    ) -> str | None:
        return spark.conf.get(param, default)


def get_param(param_name):
    return NotebookUtil.notebook_param(param_name)


azure_secret_scope = get_param("azure_kv_scope")


def get_secret(param_name):
    key = get_param(param_name)
    return dbutils.secrets.get(azure_secret_scope, key)
