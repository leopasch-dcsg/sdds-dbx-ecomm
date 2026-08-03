# Databricks notebook source
# MAGIC %pip install elasticsearch==8.17.2
from databricks.sdk.runtime import dbutils

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk.runtime import display
from ecmde_ecomm.common.spark import spark_session
spark = spark_session()

# COMMAND ----------
from ecmde_ecomm.common.errors import NotSupportedError, ExpectationNotMetError
from ecmde_ecomm.common.util import CredentialUtil
from ecmde_ecomm.common.dbx.elastic.elastic_writer import (
    ElasticBatchInsertConfig,
)
from ecmde_ecomm.search.intelligent_filtering.if_dbx_to_elastic import ElasticDBXBatchInsert
from elasticsearch import Elasticsearch
from ecmde_ecomm.common.dbx.quality.row_count import RowCount
from ecmde_ecomm.common.util import NotebookUtil

# COMMAND ----------
config = ElasticBatchInsertConfig.from_notebook_params()
index_name = config.elastic_index


def get_elastic_host_info() -> list:
    match config.environment:
        case "PROD":
            es_user = config.elastic_user
            prodwest_host = config.elastic_host_prodwest
            prodwest_pass = CredentialUtil.secret(
                config.azure_scope, config.elastic_password_prodwest
            )
            prodwestR_host = config.elastic_host_prodwest_reserve
            prodwestR_pass = CredentialUtil.secret(
                config.azure_scope, config.elastic_password_prodwest_reserve
            )
            prodeast_host = config.elastic_host_prodeast
            prodeast_pass = CredentialUtil.secret(
                config.azure_scope, config.elastic_password_prodeast
            )
            prodeastR_host = config.elastic_host_prodeast_reserve
            prodeastR_pass = CredentialUtil.secret(
                config.azure_scope, config.elastic_password_prodeast_reserve
            )
            return [
                {"host": prodwest_host, "user": es_user, "pass": prodwest_pass},
                {"host": prodwestR_host, "user": es_user, "pass": prodwestR_pass},
                {"host": prodeast_host, "user": es_user, "pass": prodeast_pass},
                {"host": prodeastR_host, "user": es_user, "pass": prodeastR_pass},
            ]
        case "QA":
            es_user = config.elastic_user
            qa_host = config.elastic_host_qa
            qa_pass = CredentialUtil.secret(
                config.azure_scope, config.elastic_password_qa
            )
            prodauth_host = config.elastic_host_prodauth
            prodauth_pass = CredentialUtil.secret(
                config.azure_scope, config.elastic_password_prodauth
            )

            return [
                {"host": qa_host, "user": es_user, "pass": qa_pass},
                {"host": prodauth_host, "user": es_user, "pass": prodauth_pass},
            ]
        case "DEV":
            es_user = config.elastic_user
            dev_host = config.elastic_host_dev
            dev_pass = CredentialUtil.secret(
                config.azure_scope, config.elastic_password_dev
            )
            return [{"host": dev_host, "user": es_user, "pass": dev_pass}]
        case _:
            raise NotSupportedError(
                f"There is no Elastic Host info for the specified environment {config.environment}."
            )


def write_elastic(host_info) -> None:
    host_name = None
    end_date = NotebookUtil.notebook_param("end_date_est")
    try:
        for hosts in host_info:
            print(
                f"Writing to Elastic Host: {hosts['host']}, with user: {hosts['user']}"
            )

            host_name = hosts['host']

            my_basic_auth = (hosts["user"], hosts["pass"])
            es = Elasticsearch(hosts["host"], basic_auth=my_basic_auth)

            # 1. Delete the index if it exists
            if es.indices.exists(index=index_name):
                print(f"Deleting index: {index_name}")
                es.indices.delete(index=index_name)

            # 2. Create the index with mapping
            print(f"Creating index: {index_name}")
            es.indices.create(index=index_name)

            result = ElasticDBXBatchInsert(
                config, spark, hosts["host"], hosts["pass"], end_date
            ).execute()
            display(result.source_df)
            print(
                f"""
                Processing complete.
                Source Table: {result.source_table_qualified}
                Elastic Index: {index_name}
                """
            )
    except Exception as e:
        print(f"The job has failed while writing to Elastic Host: {host_name}", e)

record_count_query = f" SELECT COUNT(*) AS record_count FROM {config.source_table_qualified()}"
quality_check = RowCount(record_count_query, spark)
if quality_check.is_not_empty():
    host_info = get_elastic_host_info()
    write_elastic(host_info)
else:
    raise ExpectationNotMetError(f"There are no rows of data in {config.source_table_qualified()} to be sent over to Elastic.")
