from enum import Enum
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession
from ecmde_ecomm.common.errors import ElementNotFoundError
from ecmde_ecomm.common.util import NotebookUtil


class DbxEnv(Enum):
    DEV = "dev"
    QA = "qa"
    PROD = "prod"

    def __init__(self, environment):
        self.environment = environment

    @staticmethod
    def from_notebook_params():
        value = dbutils.widgets.get("dbx_env")
        return DbxEnv.__map_from_value(value)

    @staticmethod
    def from_spark_context(spark: SparkSession):
        value = spark.conf.get("dbx_env", DbxEnv.DEV.environment)
        return DbxEnv.__map_from_value(value)

    @staticmethod
    def __map_from_value(value: str):
        for env in DbxEnv:
            if env.environment.lower() == value.lower():
                return env

        raise ElementNotFoundError(
            f"The environment value {value} does not exist or is not a supported value. Ensure the configuration parameter dbx_env is set to a supported value 'dev', 'qa', or 'prod'"
        )


class DbxDataQuality(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    ECOMP = "ecomp"

    def __init__(self, quality):
        self.quality = quality

    @staticmethod
    def from_notebook_params():
        value = NotebookUtil.notebook_param("dbx_data_quality")
        return DbxDataQuality.__map_from_value(value)

    @staticmethod
    def from_spark_context(spark: SparkSession):
        value = NotebookUtil.spark_param("dbx_data_quality")
        return DbxDataQuality.__map_from_value(value)

    @staticmethod
    def __map_from_value(value: str | None):
        if value is None:
            raise ElementNotFoundError("dbx_data_quality parameter is required.")

        for quality in DbxDataQuality:
            if quality.quality == value.lower():
                return quality

        raise ElementNotFoundError(
            f"The environment value {value} does not exist or is not a supported value. Ensure the configuration parameter dbx_data_quality is set to a supported value 'bronze', 'silver', or 'gold'"
        )


class FMODEgressFlag(Enum):
    DBX = "DBX"
    ORACLE = "ORACLE"
    BOTH = "BOTH"
    NONE = "NONE"

    @staticmethod
    def from_notebook_params():
        val = NotebookUtil.notebook_param("fmodegress")
        for flag in FMODEgressFlag:
            if flag.value == val.upper():
                return flag
        raise ElementNotFoundError(f"Invalid fmodegress: {val}")
