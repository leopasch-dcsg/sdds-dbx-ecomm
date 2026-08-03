from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import convert_timezone, col, lit

from databricks.sdk.runtime import dbutils

from ecmde_ecomm.common.errors import NotSupportedError
from ecmde_ecomm.common.util import NotebookUtil


@dataclass
class OracleConfig:
    jdbc_url: str
    username: str
    password: str
    source_table: str
    destination_table: str
    convert_timestamps: bool = True
    trunc_load: bool = True

    @staticmethod
    def from_notebook_params():
        azure_secret_scope = NotebookUtil.notebook_param("azure_kv_scope")

        jdbc_url = NotebookUtil.notebook_param("oracle_jdbc_url")
        username = NotebookUtil.notebook_param("oracle_user_name")
        password = dbutils.secrets.get(
            azure_secret_scope, NotebookUtil.notebook_param("oracle_password_key")
        )
        source_table = NotebookUtil.notebook_param("dbx_source_table")
        destination_table = NotebookUtil.notebook_param("oracle_destination_table")
        convert_timestamps = (
            NotebookUtil.notebook_param("oracle_convert_timestamps", "true").lower()
            == "true"
        )
        trunc_load = (
            NotebookUtil.notebook_param("oracle_trunc_load", "true").lower() == "true"
        )

        return OracleConfig(
            jdbc_url,
            username,
            password,
            source_table,
            destination_table,
            convert_timestamps,
            trunc_load,
        )


class OracleEgress(ABC):
    def __init__(self, config: OracleConfig, spark: SparkSession):
        self.spark = spark
        self.config = config

    @abstractmethod
    def source_dataframe(self) -> DataFrame:
        """
        Return a dataframe with the data columns that you want to egress to oracle. Note that the column names must be
        identical to the column names in the destination Oracle table. Note that data types must also be Oracle compatible.

        Note that timestamp columns will automatically be converted to be zoned in America/New_York.
        """
        pass

    def execute(self) -> int:
        df = self.source_dataframe().cache()

        if self.config.convert_timestamps:
            df = OracleEgress.convert_timestamps(df)

        if self.config.trunc_load:
            self.trunc_load(df)
        else:
            raise NotSupportedError(
                "Truncate and load is the only operation supported at this time."
            )

        return df.count()

    @staticmethod
    def convert_timestamps(dataframe: DataFrame) -> DataFrame:
        df = dataframe
        for col_name, col_type in dataframe.dtypes:
            if col_type == "timestamp":
                df = df.withColumn(
                    col_name,
                    convert_timezone(
                        lit("UTC"), lit("America/New_York"), col(col_name)
                    ),
                )

        return df

    def trunc_load(self, dataframe: DataFrame):
        (
            dataframe.write.format("jdbc")
            .options(
                driver="oracle.jdbc.OracleDriver",
                url=self.config.jdbc_url,
                user=self.config.username,
                password=self.config.password,
                batchsize=10000,
                dbtable=self.config.destination_table,
            )
            .option("oracle.jdbc.timezoneAsRegion", "false")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )
