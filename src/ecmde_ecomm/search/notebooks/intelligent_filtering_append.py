import argparse
from pyspark.sql import SparkSession
from ecmde_ecomm.common.spark import spark_session
from ecmde_ecomm.search.intelligent_filtering.intelligent_filtering_append_operations_provider import IntelligentFilteringAppendOperationsBuilder
from ecmde_ecomm.common import (
    DatabricksDestination,
    DatabricksSource,
    DateInfo,
    AppendOperation,
    Logger
)

__logger = Logger.logger("Intelligent Filtering Append Process")


def main():
    try:
        args = parse_args()
        spark = spark_session()


        destination = databricks_destination(args)
        source = databricks_source(args)
        dates = date_info(args)

        __logger.info(
            f"""
            Creating BigQuery Egress Operation:

            Source Catalog: {source.catalog}
            Source Schema: {source.schema}
            Destination Catalog: {destination.catalog}
            Destination Schema: {destination.schema}
            Destination Table: {destination.table}
            End Date: {dates.end_date_est}
            Lookback Window Size: {dates.lookback_days}
            """
        )

        append_data(spark, dates, destination, source)
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
        help="The source catalog that contains the schema and table we are going to pull data from.",
    )

    parser.add_argument(
        "--source_schema",
        type=str,
        required=True,
        help="The source schema that contains the table we are going to pull data from.",
    )

    parser.add_argument(
        "--destination_catalog",
        type=str,
        required=True,
        help="The destination catalog that contains the schema and table we are going to append data to.",
    )

    parser.add_argument(
        "--destination_schema",
        type=str,
        required=True,
        help="The destination schema that contains the table we are going to append data to.",
    )

    parser.add_argument(
        "--destination_table",
        type=str,
        required=True,
        help="The destination schema that contains the table we are going to append data to.",
    )

    parser.add_argument(
        "--lookback_days",
        type=int,
        required=True,
        help="The size of the lookback window",
    )

    parser.add_argument(
        "--end_date_est",
        type=str,
        required=True,
        help="The date we want to append to the final table.",
    )


    args = parser.parse_args()
    return args

def databricks_source(args) -> DatabricksSource:
    return DatabricksSource(
        args.source_catalog,
        args.source_schema,
    )

def databricks_destination(args) -> DatabricksDestination:
    return DatabricksDestination(
        args.destination_catalog,
        args.destination_schema,
        args.destination_table,
    )

def date_info(args) -> DateInfo:
    return DateInfo(
        args.end_date_est,
        args.lookback_days,
    )

def append_data(
        spark: SparkSession,
        dates: DateInfo,
        destination: DatabricksDestination,
        source: DatabricksSource,
) -> None:
    try:
        __logger.info("Creating Intelligent Filtering append operation")
        operation(spark, dates, destination, source).execute()
        __logger.info("Intelligent Filtering append operation complete.")
    except Exception:
        __logger.exception("The job failed, see job output for error details.")
        raise


def operation(
    spark: SparkSession,
    dates: DateInfo,
    destination: DatabricksDestination,
    source: DatabricksSource,
) -> AppendOperation:

    return (
        IntelligentFilteringAppendOperationsBuilder.builder(spark)
        .with_dbx_source(source)
        .with_dbx_destination(destination)
        .with_date_info(dates)
        .operation()
    )