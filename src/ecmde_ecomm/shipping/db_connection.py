from pyspark.sql import SparkSession
from databricks.sdk.runtime import dbutils


class DBConnection:

    def __init__(self):
        self.spark = SparkSession.builder.appName("DBConnection").getOrCreate()

        self.ora_configs = {
            "driver": "oracle.jdbc.driver.OracleDriver",
            "az_vault_name": "kv-dsg-ecmde-dbx",
            "url": "jdbc:oracle:thin:@//ora-cs-p-4284.dcsg.com:1521/scip",
            "user": "az_dbx_ecomp_etl_ronly",
            "vault_secret_name": "az-dbx-ecomp-etl-ronly",
        }

    def get_oracle_connection(self):

        oracle_password = dbutils.secrets.get(
            self.ora_configs["az_vault_name"], self.ora_configs["vault_secret_name"]
        )

        return {
            "driver": self.ora_configs["driver"],
            "url": self.ora_configs["url"],
            "user": self.ora_configs["user"],
            "password": oracle_password,
        }

    def read_from_oracle(self, query: str):

        connection_details = self.get_oracle_connection()
        return (
            self.spark.read.format("jdbc")
            .options(
                driver=connection_details["driver"],
                url=connection_details["url"],
                user=connection_details["user"],
                password=connection_details["password"],
                dbtable=f"({query})",
                fetchsize="100000",
            )
            .load()
        )
