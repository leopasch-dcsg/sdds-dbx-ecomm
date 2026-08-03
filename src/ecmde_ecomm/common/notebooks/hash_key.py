import argparse
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, sha2
from delta.tables import DeltaTable

from ecmde_ecomm.common.dbx.etl import merge_metrics
from ecmde_ecomm.common.spark import create_uuid5, spark_session
from ecmde_ecomm.common.logger import Logger


__logger = Logger.logger("Hashed Key Notebook")


def main():
    try:
        args = parse_args()
        spark = spark_session()

        source_table = fully_qualified_table_name(args)
        key_columns = parse_key_cols(args.key_columns)
        select_cols = sql_select_cols(key_columns)
        output_column = args.output_column

        __logger.info("Get source dataframe.")
        df = get_source_dataframe(source_table, select_cols, spark)

        __logger.info("Compute hashed UUID key.")
        df = (
            df.withColumn("tmp_concat_cols", concat_ws("||", *df.columns))
            .withColumn(output_column, create_uuid5(sha2(col("tmp_concat_cols"), 256)))
            .drop("tmp_concat_cols")
            .select("*")
        )

        condition = merge_condition(key_columns)

        __logger.info(f"Begin: Write to Delta table: {source_table}")
        (
            DeltaTable.forName(spark, source_table)
            .alias("target")
            .merge(source=df.alias("source"), condition=condition)
            .whenMatchedUpdate(
                set={f"target.{output_column}": f"source.{output_column}"}
            )
        ).execute()
        __logger.info(f"End: Write to Delta table: {source_table}")

        log_metrics(source_table)
    except Exception as e:
        __logger.error("Error creating hashed key.", exc_info=e)
        raise e


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source_catalog",
        type=str,
        required=True,
        help="The source catalog the table belongs to.",
    )
    parser.add_argument(
        "--source_schema",
        type=str,
        required=True,
        help="The source schema the table belongs to.",
    )
    parser.add_argument(
        "--source_table",
        type=str,
        required=True,
        help="Source table name that we want to compute a hashed key for.",
    )
    parser.add_argument(
        "--key_columns",
        type=str,
        required=True,
        help="A comma separate list of columns that we want to use to compute the hashed key.",
    )
    parser.add_argument(
        "--output_column",
        type=str,
        required=True,
        help="The column name to write the final result to.",
    )
    args = parser.parse_args()

    __logger.info(f"Source Catalog: {args.source_catalog}")
    __logger.info(f"Source Schema: {args.source_schema}")
    __logger.info(f"Source Table: {args.source_table}")
    __logger.info(f"Key Columns: {args.key_columns}")
    __logger.info(f"Output Column: {args.output_column}")

    return args


def fully_qualified_table_name(args) -> str:
    catalog = args.source_catalog
    schema = args.source_schema
    table = args.source_table

    return f"{catalog}.{schema}.{table}"


def parse_key_cols(key_columns: str) -> list[str]:
    normalized_cols = []
    for column in key_columns.split(","):
        normalized_cols.append(column.strip())

    return normalized_cols


def sql_select_cols(key_columns: list[str]) -> str:
    return ", ".join(key_columns)


def get_source_dataframe(
    table_name: str, key_columns: str, spark: SparkSession
) -> DataFrame:
    sql = f"""
    select {key_columns}
      from {table_name}
    """

    __logger.info(f"Source SQL: {sql}")
    return spark.sql(sql)


def merge_condition(key_columns: list[str]) -> str:
    condition = ""

    for index, value in enumerate(key_columns):
        if index == 0:
            condition = f"target.{value} = source.{value}"
            continue

        condition += f" AND target.{value} = source.{value}"

    __logger.info(f"Merge Condition: {condition}")
    return condition


def log_metrics(source_table: str):
    try:
        metrics = merge_metrics(source_table)
        __logger.info(
            f"""
            Merge Metrics for {source_table}
            New Table Version: {metrics.version}
            Target Rows Inserted: {metrics.targetRowsInserted}
            Target Rows Updated: {metrics.targetRowsUpdated}
            Target Rows Deleted: {metrics.targetRowsDeleted}
            Source Rows: {metrics.sourceRows}
            Scan Time MS: {metrics.scanTimeMs}
            Rewrite Time MS: {metrics.rewriteTimeMs}
            Execution Time MS: {metrics.executionTimeMs}
            """
        )
    except Exception as e:
        __logger.info("Unable to log merge metrics for this operation.", e)


if __name__ == "__main__":
    main()
