import argparse
from pyspark.sql import SparkSession
from ecmde_ecomm.common import Logger, CredentialUtil
from ecmde_ecomm.common.spark import spark_session
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.bq.egress import (
    BigQueryEgressOperation,
    BigQueryCredentials,
    BigQueryDestination,
    DatabricksSource,
    LoadMode,
)
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.ecomp.bq.egress_operations import ECOMPBigQueryEgressOperationBuilder

__logger = Logger.logger("ECOMP BQ Egress")


def main():
    try:
        args = parse_args()
        spark = egress_spark_session()

        credentials = bigquery_credentials(args)
        destination = bigquery_destination(args)
        source = databricks_source(args)

        __logger.info(
            f"""
            Creating BigQuery Egress Operation:
            
            BQ Bucket: {destination.bucket}
            BQ Parent Project ID: {destination.parent_project_id}
            BQ Project ID: {destination.project_id}
            BQ Dataset: {destination.dataset}
            BQ Table: {destination.table}
            
            DBX Catalog: {source.catalog}
            DBX Schema: {source.schema}
            DBX Table: {source.table}
            Load Mode: {args.load_mode}
            Watermark Table: {args.watermark_table or '(none)'}
            """
        )

        egress_data(spark, credentials, destination, source, args)
    except Exception:
        __logger.error("Error performing egress to BQ operation.")
        raise


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--azure_kv_scope",
        type=str,
        required=True,
        help="The Azure Key Vault scope used to fetch secrets for this task.",
    )

    parser.add_argument(
        "--source_catalog",
        type=str,
        required=True,
        help="The source catalog that contains the schema and table we are going to egress to Big Query.",
    )

    parser.add_argument(
        "--source_schema",
        type=str,
        required=True,
        help="The source schema that contains the table we are going to egress to Big Query.",
    )

    parser.add_argument(
        "--source_table",
        type=str,
        required=True,
        help="The source table that we are going to egress to Big Query.",
    )

    parser.add_argument(
        "--bq_credentials_json_credentials_key",
        type=str,
        required=True,
        help="The key name used to fetch the BQ secrets from the Azure Key Vault.",
    )

    parser.add_argument(
        "--bq_bucket",
        type=str,
        required=True,
        help="The name of the bucket we are going to use during our egress operation to Big Query.",
    )

    parser.add_argument(
        "--bq_parent_project_id",
        type=str,
        required=True,
        help="The name of the parent project ID in BQ that our operations should flow from. Permissions in BQ are tied to the parent project ID.",
    )

    parser.add_argument(
        "--bq_project_id",
        type=str,
        required=True,
        help="The name of the BQ project ID where our table exists.",
    )

    parser.add_argument(
        "--bq_dataset",
        type=str,
        required=True,
        help="The name of the BQ dataset where our table exists.",
    )

    parser.add_argument(
        "--bq_table",
        type=str,
        required=True,
        help="The name of the BQ table used in the egress operation.",
    )

    parser.add_argument(
        "--load_mode",
        type=str,
        choices=[LoadMode.DELTA.value, LoadMode.TRUNCATE_LOAD.value],
        default=LoadMode.TRUNCATE_LOAD.value,
        help="delta = incremental (watermark); truncate_load = full load (no watermark).",
    )
    parser.add_argument(
        "--watermark_table",
        type=str,
        required=False,
        help="Fully qualified watermark table. Required if --load_mode=delta.",
    )

    args = parser.parse_args()
    return args


def egress_spark_session() -> SparkSession:
    spark = spark_session()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "2000")
    return spark


def bigquery_credentials(args) -> BigQueryCredentials:
    key_name = args.bq_credentials_json_credentials_key
    creds = CredentialUtil.secret(args.azure_kv_scope, key_name)
    return BigQueryCredentials(creds)


def bigquery_destination(args) -> BigQueryDestination:
    return BigQueryDestination(
        args.bq_bucket,
        args.bq_parent_project_id,
        args.bq_project_id,
        args.bq_dataset,
        args.bq_table,
    )


def databricks_source(args) -> DatabricksSource:
    return DatabricksSource(
        args.source_catalog,
        args.source_schema,
        args.source_table,
    )


def egress_data(
    spark: SparkSession,
    credentials: BigQueryCredentials,
    destination: BigQueryDestination,
    source: DatabricksSource,
    args=None,
) -> None:
    try:
        __logger.info("Creating BigQuery Egress Operation")
        operation(spark, credentials, destination, source, args).execute()
        __logger.info("BigQuery egress complete (mode=overwrite).")
    except Exception:
        __logger.exception("The job failed, see job output for error details.")
        raise


def operation(
    spark: SparkSession,
    credentials: BigQueryCredentials,
    destination: BigQueryDestination,
    source: DatabricksSource,
    args,
) -> BigQueryEgressOperation:
    load_mode = LoadMode(args.load_mode)
    wm_instance = None

    if load_mode == LoadMode.DELTA:
        if not args.watermark_table:
            raise IllegalArgumentError(
                "load_mode=delta requires --watermark_table (fully qualified)."
            )
        wm_instance = Watermark(
            watermark_table_name=args.watermark_table,
            catalog=destination.project_id,
            schema=destination.dataset,
            table=destination.table,
            spark=spark,
        )

    return (
        ECOMPBigQueryEgressOperationBuilder.builder(spark)
        .with_bigquery_credentials(credentials)
        .with_databricks_source(source)
        .with_bigquery_destination(destination)
        .with_watermark(wm_instance)
        .with_load_mode(load_mode)
        .operation()
    )
