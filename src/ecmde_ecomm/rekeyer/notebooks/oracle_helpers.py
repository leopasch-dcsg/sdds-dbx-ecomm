from pyspark.sql.functions import date_part, lit, make_timestamp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import catalog
from datetime import datetime, timedelta

def update_timezone(df: DataFrame, target_timezone: str) -> DataFrame:
    for col_name, col_type in df.dtypes:
        if col_type == "timestamp":
            df = df.withColumn(
                col_name,
                make_timestamp(
                    years=date_part(lit("YEAR"), col_name),
                    months=date_part(lit("MONTH"), col_name),
                    days=date_part(lit("DAY"), col_name),
                    hours=date_part(lit("HOUR"), col_name),
                    mins=date_part(lit("MINUTE"), col_name),
                    secs=date_part(lit("SECOND"), col_name),
                    timezone=lit(target_timezone),  # "America/New_York"
                ),
            )

    return df


def oracle_query(spark:SparkSession, query:str, oracle_pw: str) -> DataFrame:
    df = (
        spark.read.format("jdbc")
        .options(
            driver="oracle.jdbc.driver.OracleDriver", #maven package com.oracle.ojdbc:ojdbc8:19.3.0.0
            url="jdbc:oracle:thin:@dkha0121.dcsg.com:1521/ecomp",
            user="az_ecomp_ronly",
            password=oracle_pw,
            dbtable=f"({query}) subq",
            fetchsize=1_000_000,
        )
        .option("oracle.jdbc.timezoneAsRegion", "false")
        .option("sessionInitStatement", "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
    )

    return update_timezone(df.load(),"America/New_York")