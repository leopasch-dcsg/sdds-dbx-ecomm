from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    current_timestamp,
    col,
    year,
    month, date_format
)
import time
import datetime
import pymysql
from dateutil.relativedelta import relativedelta

@dataclass(frozen=True)
class LineupEgressConfig:
    jdbc_url: str
    jdbc_host: str
    connection_properties: dict[str, str]


class LineupEgress:
    def __init__(self, spark: SparkSession, config: LineupEgressConfig):
        self.spark = spark
        self.jdbc_url = config.jdbc_url
        self.jdbc_host = config.jdbc_host
        self.connection_properties = config.connection_properties

    def write_table_trunc(
        self, catalog_table: str, mysql_table: str, mode: str = "overwrite"
    ):
        df = self.spark.table(catalog_table)
        (
            df.write.format("jdbc")
            .options(
                url=self.jdbc_url,
                dbtable=mysql_table,
                user=self.connection_properties.get("user"),
                password=self.connection_properties.get("password"),
                driver=self.connection_properties.get("driver"),
            )
            .mode(mode)
            .save()
        )

    def write_table_trunc_pim(
        self, catalog_table: str, mysql_table: str, mode: str = "overwrite"
    ):
        df = self.spark.table(catalog_table)
        df = df.withColumn("PIM_DATE_MODIFIED", current_timestamp())
        df = df.withColumn("EM_PRESALE_END_DATE", current_timestamp())
        df.write.jdbc(
            url=self.jdbc_url,
            table=mysql_table,
            mode=mode,
            properties=self.connection_properties,
        )

    def write_table_trunc_pmms(
        self, catalog_table: str, mysql_table: str, mode: str = "overwrite"
    ):
        df = self.spark.table(catalog_table)
        df = df.withColumn("DSG_TARGET_DATE", current_timestamp())
        df.write.jdbc(
            url=self.jdbc_url,
            table=mysql_table,
            mode=mode,
            properties=self.connection_properties,
        )

    def write_table_trunc_pmms_hist_batch(
        self,
        catalog_table: str,
        mysql_table: str,
        mode: str = "overwrite",
        max_retries: int = 3,
        retry_delay: int = 10,
    ):
        df = self.spark.table(catalog_table)

        # Replace nulls in DATE_LAST_MODIFIED with DATE_ADDED
        df = df.withColumn(
            "DATE_LAST_MODIFIED", coalesce("DATE_LAST_MODIFIED", "DATE_ADDED")
        )
        df = df.withColumn("GG_SETUP_DATE", current_timestamp())
        df = df.withColumn("DSG_TARGET_DATE", current_timestamp())
        df = df.withColumn("GG_IMAGE_DATE", current_timestamp())

        # Create yyyymm column (e.g., 201508)
        df = df.withColumn("yyyymm", date_format("DATE_LAST_MODIFIED", "yyyyMM"))

        # Get all distinct yyyymm values sorted
        yyyymm_list = (
            df.select("yyyymm")
            .distinct()
            .orderBy("yyyymm")
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        first = True  # Control truncate/overwrite only for the first iteration

        for yyyymm_val in yyyymm_list:
            print(f"\n📦 Writing data for partition yyyymm = {yyyymm_val}")
            retries = 0
            while retries < max_retries:
                try:
                    df_partition = (
                        df.filter(col("yyyymm") == yyyymm_val)
                        .drop("yyyymm")
                        .repartition(1)  # Optional: reduce JDBC pressure
                    )

                    # Write to MySQL (open-close connection per .save())
                    df_partition.write.format("jdbc").option(
                        "url", self.jdbc_url
                    ).option("dbtable", mysql_table).option(
                        "user", self.connection_properties["user"]
                    ).option(
                        "password", self.connection_properties["password"]
                    ).option(
                        "driver", self.connection_properties["driver"]
                    ).option(
                        "batchsize", 50000
                    ).option(
                        "isolationLevel", "READ_COMMITTED"
                    ).option(
                        "rewriteBatchedStatements", "true"
                    ).option(
                        "truncate", "true" if first and mode == "overwrite" else "false"
                    ).mode(
                        "overwrite" if first and mode == "overwrite" else "append"
                    ).save()

                    first = False  # Only truncate first time
                    break  # Success — exit retry loop

                except Exception as e:
                    retries += 1
                    print(
                        f"⚠️ Retry {retries}/{max_retries} for yyyymm = {yyyymm_val} due to error: {e}"
                    )
                    time.sleep(retry_delay)
                    if retries == max_retries:
                        raise RuntimeError(
                            f"❌ Failed to write partition yyyymm = {yyyymm_val} after {max_retries} retries."
                        ) from e

    def write_table_trunc_pim_hist_batch(
        self,
        catalog_table: str,
        mysql_table: str,
        mode: str = "overwrite",
        partition_column: str = "id",  # <--- NEW PARAMETER
        num_partitions: int = 8,  # <--- Optional: control parallelism
    ):
        df = self.spark.table(catalog_table)
        # Replace nulls in DATE_LAST_MODIFIED with DATE_ADDED
        df = df.withColumn(
            "DATE_LAST_MODIFIED", coalesce("DATE_LAST_MODIFIED", "DATE_ADDED")
        )

        min_val = df.agg({partition_column: "min"}).collect()[0][0]
        max_val = df.agg({partition_column: "max"}).collect()[0][0]

        df = df.withColumn("PIM_DATE_MODIFIED", current_timestamp())
        df = df.withColumn("EM_PRESALE_END_DATE", current_timestamp())
        df.write.format("jdbc").option("url", self.jdbc_url).option(
            "dbtable", mysql_table
        ).option("user", self.connection_properties["user"]).option(
            "password", self.connection_properties["password"]
        ).option(
            "driver", self.connection_properties["driver"]
        ).option(
            "batchsize", 10000
        ).option(
            "isolationLevel", "READ_COMMITTED"
        ).option(
            "rewriteBatchedStatements", "true"
        ).option(
            "truncate", "true" if mode == "overwrite" else "false"
        ).option(
            "numPartitions", num_partitions
        ).option(
            "partitionColumn", partition_column
        ).option(
            "lowerBound", min_val
        ).option(
            "upperBound", max_val
        ).mode(
            mode
        ).save()

    def write_table_trunc_hist_batch(
            self,
            catalog_table: str,
            mysql_table: str,
            partition_column: str ,  # <--- NEW PARAMETER
            mode: str = "overwrite",
            num_partitions: int = 8        # <--- Optional: control parallelism
        ):
            df = self.spark.table(catalog_table)
            # Replace nulls in DATE_LAST_MODIFIED with DATE_ADDED
            #df = df.withColumn("DATE_ADDED")
            if "DATE_LAST_MODIFIED" in df.columns and "DATE_ADDED" in df.columns:
                df = df.withColumn("DATE_LAST_MODIFIED", coalesce("DATE_LAST_MODIFIED", "DATE_ADDED"))
            min_val = df.agg({partition_column: "min"}).collect()[0][0]
            max_val = df.agg({partition_column: "max"}).collect()[0][0]
            (
                df.write
                .format("jdbc")
                .option("url", self.jdbc_url)
                .option("dbtable", mysql_table)
                .option("user", self.connection_properties["user"])
                .option("password", self.connection_properties["password"])
                .option("driver", self.connection_properties["driver"])
                .option("batchsize", 50000)
                .option("isolationLevel", "READ_COMMITTED")
                .option("rewriteBatchedStatements", "true")
                .option("truncate", "true" if mode == "overwrite" else "false")
                .option("numPartitions", num_partitions)
                .option("partitionColumn", partition_column)
                .option("lowerBound", min_val)
                .option("upperBound", max_val)
                .mode(mode)
                .save()
            )

    def write_table_trunc_hist_batch_iy(
        self,
        catalog_table: str,
        mysql_table: str,
        mode: str = "overwrite",
        max_retries: int = 3,
        retry_delay: int = 10,
    ):
        df = self.spark.table(catalog_table)

        # Replace nulls in DATE_LAST_MODIFIED with DATE_ADDED
        df = df.withColumn(
            "DATE_LAST_MODIFIED", coalesce("DATE_LAST_MODIFIED", "DATE_ADDED")
        )

        # Create yyyymm column (e.g., 201508)
        df = df.withColumn("yyyymm", date_format("DATE_LAST_MODIFIED", "yyyyMM"))

        # Get all distinct yyyymm values sorted
        yyyymm_list = (
            df.select("yyyymm")
            .distinct()
            .orderBy("yyyymm")
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        first = True  # Control truncate/overwrite only for the first iteration

        for yyyymm_val in yyyymm_list:
            print(f"\n📦 Writing data for partition yyyymm = {yyyymm_val}")
            retries = 0
            while retries < max_retries:
                try:
                    df_partition = (
                        df.filter(col("yyyymm") == yyyymm_val)
                        .drop("yyyymm")
                        .repartition(1)  # Optional: reduce JDBC pressure
                    )

                    # Write to MySQL (open-close connection per .save())
                    df_partition.write.format("jdbc").option(
                        "url", self.jdbc_url
                    ).option("dbtable", mysql_table).option(
                        "user", self.connection_properties["user"]
                    ).option(
                        "password", self.connection_properties["password"]
                    ).option(
                        "driver", self.connection_properties["driver"]
                    ).option(
                        "batchsize", 50000
                    ).option(
                        "isolationLevel", "READ_COMMITTED"
                    ).option(
                        "rewriteBatchedStatements", "true"
                    ).option(
                        "truncate", "true" if first and mode == "overwrite" else "false"
                    ).mode(
                        "overwrite" if first and mode == "overwrite" else "append"
                    ).save()

                    first = False  # Only truncate first time
                    break  # Success — exit retry loop

                except Exception as e:
                    retries += 1
                    print(
                        f"⚠️ Retry {retries}/{max_retries} for yyyymm = {yyyymm_val} due to error: {e}"
                    )
                    time.sleep(retry_delay)
                    if retries == max_retries:
                        raise RuntimeError(
                            f"❌ Failed to write partition yyyymm = {yyyymm_val} after {max_retries} retries."
                        ) from e

    def drop_partitions_pymysql(self, yyyymm_list, table_name):

        # Establish non-SSL connection
        conn = pymysql.connect(
            host=self.jdbc_host,
            user=self.connection_properties["user"],
            password=self.connection_properties["password"],
            database="ecom_dim",
            ssl={"ssl": {}}
        )
        cursor = conn.cursor()
    
        try:
            for yyyymm_val in yyyymm_list:
                partition_name = f"p{yyyymm_val}"  # e.g., p202309
                drop_sql = f"ALTER TABLE {table_name} TRUNCATE PARTITION {partition_name}"
                print(f"🧹 Dropping MySQL partition: {drop_sql}")
                cursor.execute(drop_sql)
                conn.commit()
                print(f"✅ Dropped partition: {partition_name}")
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"❌ Failed to drop partition {partition_name}: {e}")
        finally:
            cursor.close()
            conn.close()


    def write_table_trunc_hist_batch_2iy(
        self,
        catalog_table: str,
        mysql_table: str,
        mode: str = "append",
        max_retries: int = 3,
        retry_delay: int = 10
    ):
        df = self.spark.table(catalog_table)

        df = df.withColumn("DATE_LAST_MODIFIED", coalesce("DATE_LAST_MODIFIED", "DATE_ADDED"))
        df = df.withColumn("yyyymm", date_format("DATE_LAST_MODIFIED", "yyyyMM"))

        #yyyymm_list = df.select("yyyymm").distinct().orderBy("yyyymm").rdd.#flatMap(lambda x: x).collect()
        #recent_yyyymm_list = yyyymm_list[-2:]

        # Current month in yyyymm
        current = datetime.datetime.now()
        current_yyyymm = int(current.strftime("%Y%m"))

        # Previous month in yyyymm
        previous = current - relativedelta(months=1)
        previous_yyyymm = int(previous.strftime("%Y%m"))

        # Final list of last two yyyymm values
        recent_yyyymm_list = [previous_yyyymm, current_yyyymm]

        print(f"📆 Last 2 yyyymm partitions to reload: {recent_yyyymm_list}")

        # Delete from MySQL using JDBC
        self.drop_partitions_pymysql(yyyymm_list=recent_yyyymm_list, table_name=mysql_table)

        for yyyymm_val in recent_yyyymm_list:
            print(f"\n📦 Writing data for partition yyyymm = {yyyymm_val}")
            retries = 0
            while retries < max_retries:
                try:
                    df_partition = (
                        df.filter(col("yyyymm") == yyyymm_val)
                        .drop("yyyymm")
                        .repartition(1)
                    )

                    (
                        df_partition.write
                        .format("jdbc")
                        .option("url", self.jdbc_url)
                        .option("dbtable", mysql_table)
                        .option("user", self.connection_properties["user"])
                        .option("password", self.connection_properties["password"])
                        .option("driver", self.connection_properties["driver"])
                        .option("batchsize", 50000)
                        .option("isolationLevel", "READ_COMMITTED")
                        .option("rewriteBatchedStatements", "true")
                        .mode("append")
                        .save()
                    )

                    print(f"✅ Successfully wrote yyyymm = {yyyymm_val}")
                    break
                except Exception as e:
                    retries += 1
                    print(f"⚠️ Retry {retries}/{max_retries} for yyyymm = {yyyymm_val} due to error: {e}")
                    time.sleep(retry_delay)
                    if retries == max_retries:
                        raise RuntimeError(f"❌ Failed to write partition yyyymm = {yyyymm_val} after {max_retries} retries.") from e

    def load_custom_query_to_mysql_btch(
        self,
        sql_query: str,
        mysql_table: str,
        truncate_first: bool = True,
        num_partitions: int = 4,     # start low
        batch_size: int = 1000       # start low
    ):
        try:
            df = self.spark.sql(sql_query)
            print(f"📊 sample ok: {df.limit(1).count()}")
        except Exception as e:
            raise RuntimeError(f"❌ Failed running custom SQL: {e}")

        # materialize once (prevents recompute + repeated shuffles on retry)
        df = df.persist()
        row_cnt = df.count()
        #print(f"📊 Query returned {row_cnt} records | partitions(before): {df.rdd.getNumPartitions()}")

        # ✅ avoid shuffle; limit concurrent writers
        df = df.coalesce(num_partitions)
        #print(f"🔧 partitions(after): {df.rdd.getNumPartitions()}")
        print("DEBUG jdbc_host =", repr(self.jdbc_host))
        print("DEBUG jdbc_url  =", repr(self.jdbc_url))

        # truncate
        if truncate_first:
            import pymysql
            conn = pymysql.connect(
                host=self.jdbc_host,
                user=self.connection_properties["user"],
                password=self.connection_properties["password"],
                database=self.connection_properties.get("database", "ecom_dim"),
                ssl={"ssl": {}},
                autocommit=True
            )
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {mysql_table}")
            conn.close()

        try:
            (
                df.write.format("jdbc")
                .option("url", self.jdbc_url)
                .option("dbtable", mysql_table)
                .option("user", self.connection_properties["user"])
                .option("password", self.connection_properties["password"])
                .option("driver", self.connection_properties["driver"])
                .option("batchsize", batch_size)
                .option("rewriteBatchedStatements", "true")
                .option("isolationLevel", "READ_COMMITTED")
                .option("truncate", "false")
                .mode("append")
                .save()
            )
        finally:
            df.unpersist()

        print(f"✅ Loaded into {mysql_table}")