from databricks.sdk.runtime import dbutils
from urllib.parse import urlparse
import pymysql
from dataclasses import dataclass
from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

@dataclass(frozen=True)
class LineupDBXEgressConfig:
    jdbc_url: str
    connection_properties: dict[str, str]

class LineupDBXEgress:
    def __init__(self, spark: SparkSession, config: LineupDBXEgressConfig):
        self.spark = spark
        self.jdbc_url = config.jdbc_url
        self.connection_properties = config.connection_properties

    def read_from_mysql_parallel(
        self,
        mysql_table: str,
        partition_column: str,
        num_partitions: int = 64,
        where_clause: Optional[str] = None,
        fetchsize: int = 50000,
    ):
        wc = f" WHERE {where_clause} " if where_clause and where_clause.strip() else ""
        bounds_q = f"(SELECT MIN({partition_column}) AS lo, MAX({partition_column}) AS hi FROM {mysql_table}{wc}) AS b"
        bdf = self.spark.read.jdbc(self.jdbc_url, bounds_q, properties=self.connection_properties)
        r = bdf.collect()[0]
        lo, hi = int(r["lo"]), int(r["hi"])
        if lo == hi:
            hi = lo + 1

        if where_clause and where_clause.strip():
            dbtable = f"(SELECT * FROM {mysql_table} WHERE {where_clause}) AS sub"
        else:
            dbtable = mysql_table

        reader = (self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", dbtable)
            .option("fetchsize", str(fetchsize))
            .option("partitionColumn", partition_column)
            .option("lowerBound", str(lo))
            .option("upperBound", str(hi))
            .option("numPartitions", str(num_partitions))
        )
        for k, v in self.connection_properties.items():
            reader = reader.option(k, v)

        return reader.load()

    def execute_mysql_sp(self, sp_name: str, params: Optional[List[str]] = None):
        if not params:
            sql = f"{{CALL {sp_name}()}}"
        else:
            param_str = ",".join([f"'{p}'" for p in params])
            sql = f"{{CALL {sp_name}({param_str})}}"
        
        sc = self.spark.sparkContext
        jvm = sc._gateway.jvm
        conn = None
        
        try:
            props = jvm.java.util.Properties()
            for k, v in self.connection_properties.items():
                props.setProperty(k.strip(), str(v).strip())
            
            cleaned_url = self.jdbc_url.strip()
            
            print(f"Connecting to MySQL to execute {sp_name}...")
            conn = jvm.java.sql.DriverManager.getConnection(cleaned_url, props)
            
            # Use prepareCall for Stored Procedures
            cs = conn.prepareCall(sql)
            
            print(f"Executing SP (this will block until complete): {sp_name}")
            
            # .execute() returns true if the first result is a ResultSet
            # It blocks until the SP is finished.
            has_results = cs.execute()
            
            # Optional: Drain results to force synchronization
            if has_results:
                rs = cs.getResultSet()
                while rs.next():
                    pass # Just consuming the result set to ensure completion
            
            print(f"Successfully finished execution of {sp_name}")
            
        except Exception as e:
            print(f"Error during SP execution: {str(e)}")
            raise e
        finally:
            if conn:
                conn.close()

    @staticmethod
    def _host_port_from_jdbc(jdbc_url: str):
        # jdbc:mysql://host:3306/db?...
        u = jdbc_url.replace("jdbc:", "", 1)
        p = urlparse(u)
        return p.hostname, (p.port or 3306)
    
    def execute_mysql_sp_pymysql(self, sp_name: str, params: Optional[List] = None):
        host, port = self._host_port_from_jdbc(self.jdbc_url)

        db = self.connection_properties.get("database") or "ecom_dim"
        user = self.connection_properties["user"]
        pwd  = self.connection_properties["password"]

        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            database=db,
            ssl={"ssl": {}},
            autocommit=True
        )
        try:
            with conn.cursor() as cur:
                if params:
                    placeholders = ",".join(["%s"] * len(params))
                    cur.execute(f"CALL {sp_name}({placeholders})", params)
                else:
                    cur.execute(f"CALL {sp_name}()")

                while True:
                    if cur.description:
                        cur.fetchall()
                    if not cur.nextset():
                        break

            print(f"✅ SP completed: {sp_name}")
        finally:
            conn.close()
