from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import make_timestamp, date_part, lit

from ecmde_ecomm.common.errors import (
    IllegalArgumentError,
    NotFoundError,
    ExpectationNotMetError,
)


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


class DateUtil:
    @staticmethod
    def make_timestamp_with_zone(column: Column | str, tzname: str) -> Column:
        if isinstance(column, str) and len(column.strip()) == 0:
            raise IllegalArgumentError("Column name cannot be null or empty.")

        if len(tzname.strip()) == 0:
            raise IllegalArgumentError("TZ name cannot be null or empty.")

        return make_timestamp(
            date_part(lit("YEAR"), column),
            date_part(lit("MONTH"), column),
            date_part(lit("DAY"), column),
            date_part(lit("HOUR"), column),
            date_part(lit("MINUTE"), column),
            date_part(lit("SECOND"), column),
            lit(tzname),
        )


class CredentialUtil:
    @staticmethod
    def secret(scope: str, key: str) -> str:
        if len(scope.strip()) == 0:
            raise IllegalArgumentError("scope is missing or empty.")
        if len(key.strip()) == 0:
            raise IllegalArgumentError("key is missing or empty.")

        try:
            value = dbutils.secrets.get(scope, key)
            if len(value) == 0:
                raise ExpectationNotMetError("Value is an empty string.")

            return value
        except Exception:
            raise NotFoundError("Value could not be found")
