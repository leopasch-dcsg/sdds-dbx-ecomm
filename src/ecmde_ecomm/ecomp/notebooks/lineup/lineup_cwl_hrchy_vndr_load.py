# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.ecomp.lineup import LineupEgress,LineupEgressConfig,LineupDBXEgress, LineupDBXEgressConfig
from ecmde_ecomm.common.util import CredentialUtil, NotebookUtil
from databricks.sdk.runtime import dbutils
import time

# Define notebook widgets for dynamic parameters
NotebookUtil.text_widget("azure_kv_scope", "", "Azure Key-Vault Scope")
NotebookUtil.text_widget("lineup_mysql_jdbc_url", "", "Lineup MYSQL JDBC URL")
NotebookUtil.text_widget("lineup_mysql_jdbc_host", "", "Lineup MYSQL JDBC HOST")
NotebookUtil.text_widget("ecomp_catalog", "", "ECOMP Catalog")

# Fetch parameters from notebook widgets
azure_kv_scope = NotebookUtil.notebook_param("azure_kv_scope")
jdbc_url = NotebookUtil.notebook_param("lineup_mysql_jdbc_url")
jdbc_host = NotebookUtil.notebook_param("lineup_mysql_jdbc_host")
ecomp_catalog = NotebookUtil.notebook_param("ecomp_catalog")

# Fetch secrets from Azure Key Vault
user = CredentialUtil.secret(azure_kv_scope, "lineup-mysql-user")
password = CredentialUtil.secret(azure_kv_scope, "lineup-mysql-password")

# Create config using fetched values
config = LineupEgressConfig(jdbc_host=jdbc_host,
    jdbc_url=jdbc_url,
    connection_properties={
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "rewriteBatchedStatements": "true",
        "isolationLevel": "READ_COMMITTED",
        "useServerPrepStmts": "true",
        "useCursorFetch": "true",
        "defaultFetchSize": "50000", "batchsize": "1000",
    }
)

egress = LineupEgress(spark, config)

egress.load_custom_query_to_mysql_btch(
    sql_query=f"""
    SELECT
    -- CAST(Product_Hierarchy_Key AS BIGINT) AS CWL_HIERARCHY_ID,

        {ecomp_catalog}.ecom.custom_concat_ifnull(
            ARRAY(
                LPAD(department_number, 3, '0') || '-' ||
                LPAD(sub_department_number, 3, '0') || '-' ||
                LPAD(class_number, 3, '0') || '-' ||
                LPAD(sub_class_number, 3, '0')
            )
        ) AS CWL_HIERARCHY_NAME,

        CAST(hierarchy_description AS STRING) AS DESCRIPTION,

        CAST(NULL AS BIGINT)        AS BUYER,
        CAST(NULL AS STRING)        AS BUYER_NAME,
        CAST(NULL AS STRING)        AS GENDER,
        CAST(NULL AS DECIMAL(5,1))  AS SHIP_LENGTH,
        CAST(NULL AS DECIMAL(5,1))  AS SHIP_WIDTH,
        CAST(NULL AS DECIMAL(5,1))  AS SHIP_HEIGHT,
        CAST(NULL AS DECIMAL(7,3))  AS SHIP_WEIGHT,
        CAST(NULL AS STRING)        AS COMMENTS,

        current_date()              AS DATE_ADDED,              -- MySQL DATE
        CAST('ECOM_DIM' AS STRING)  AS ADDED_BY,

        current_timestamp()         AS DATE_LAST_MODIFIED,      -- MySQL TIMESTAMP
        CAST('ECOM_DIM' AS STRING)  AS MODIFIED_BY,
        CAST('A' AS STRING)         AS RECORD_STATUS,

        CAST(NULL AS STRING)        AS SFS,
        CAST(NULL AS STRING)        AS BOPUIS,
        CAST(NULL AS STRING)        AS ISA,
        CAST(NULL AS STRING)        AS MORE_COLORS_AVAIL,

        {ecomp_catalog}.ecom.custom_concat_ifnull(
            ARRAY(
                CASE
                    WHEN division_number < 0 THEN '-9'
                    ELSE LPAD(division_number, 2, '0')
                END || '.' ||
                LPAD(department_number, 3, '0') || '.' ||
                LPAD(sub_department_number, 3, '0') || '.' ||
                LPAD(class_number, 3, '0') || '.' ||
                LPAD(sub_class_number, 3, '0')
            )
        ) AS HIERARCHY_CODE_DIV,

        {ecomp_catalog}.ecom.custom_concat_ifnull(
            ARRAY(
                LPAD(department_number, 3, '0') || '.' ||
                LPAD(sub_department_number, 3, '0') || '.' ||
                LPAD(class_number, 3, '0') || '.' ||
                LPAD(sub_class_number, 3, '0')
            )
        ) AS HIERARCHY_CODE_DEPT,

        CAST(division_number       AS BIGINT) AS DIVISION_NUMBER,
        CAST(department_number     AS BIGINT) AS DEPARTMENT_NUMBER,
        CAST(sub_department_number AS BIGINT) AS SUB_DEPARTMENT_NUMBER,
        CAST(class_number          AS BIGINT) AS CLASS_NUMBER,
        CAST(sub_class_number      AS BIGINT) AS SUB_CLASS_NUMBER,

        {ecomp_catalog}.ecom.custom_concat_ifnull(
            ARRAY(
                division_number || '.' ||
                department_number || '.' ||
                sub_department_number || '.' ||
                class_number || '.' ||
                sub_class_number || '-' ||
                hierarchy_description
            )
        ) AS CODE_AND_DESCRIPTION,

        CAST(NULL AS STRING) AS MERCHANDISER_TYPE,

        CAST(Product_Hierarchy_Key AS BIGINT) AS PRODUCT_HIERARCHY_KEY,

        CAST('Y' AS STRING) AS WEBSTORE_DSG,
        CAST('N' AS STRING) AS WEBSTORE_GG,
        CAST('N' AS STRING) AS WEBSTORE_FS,

        CAST(NULL AS STRING) AS IMAGE_TASK_TYPE,
        CAST(NULL AS STRING) AS APPAREL_FLG,

        CAST('N' AS STRING) AS WEBSTORE_PL,
        CAST('N' AS STRING) AS WEBSTORE_G3

    FROM (
        SELECT DISTINCT
            -1 AS Product_Hierarchy_Key,
            division_number,
            department_number,
            0 AS sub_department_number,
            0 AS class_number,
            0 AS sub_class_number,
            department_description AS hierarchy_description
        FROM {ecomp_catalog}.ecom_dim.product_hierarchy
        WHERE division_number > 0
        AND sub_department_number NOT IN (98, 99)
        AND class_number NOT IN (98, 99)
        AND record_status = 'A'
        AND is_current = 'C'

        UNION

        SELECT DISTINCT
            -1,
            division_number,
            department_number,
            sub_department_number,
            0,
            0,
            sub_department_description
        FROM {ecomp_catalog}.ecom_dim.product_hierarchy
        WHERE division_number > 0
        AND sub_department_number NOT IN (98, 99)
        AND class_number NOT IN (98, 99)
        AND record_status = 'A'
        AND is_current = 'C'

        UNION

        SELECT DISTINCT
            -1,
            division_number,
            department_number,
            sub_department_number,
            class_number,
            0,
            class_description
        FROM {ecomp_catalog}.ecom_dim.product_hierarchy
        WHERE division_number > 0
        AND sub_department_number NOT IN (98, 99)
        AND class_number NOT IN (98, 99)
        AND record_status = 'A'
        AND is_current = 'C'

        UNION

        SELECT DISTINCT
            Product_Hierarchy_Key,
            division_number,
            department_number,
            sub_department_number,
            class_number,
            sub_class_number,
            sub_class_description
        FROM {ecomp_catalog}.ecom_dim.product_hierarchy
        WHERE division_number > 0
        AND sub_department_number NOT IN (98, 99)
        AND class_number NOT IN (98, 99)
        AND record_status = 'A'
        AND is_current = 'C'
        ) full_h
        """,
        mysql_table="ecom_stg.stg_cwl_hierarchy",
        truncate_first=True
    )

egress.load_custom_query_to_mysql_btch(
    sql_query=f"""
  SELECT
    CAST(TRIM(v.VENDOR_NUMBER) AS BIGINT) AS VENDOR_NUMBER,
    CAST(v.VENDOR_DESC AS STRING)         AS VENDOR_NAME,

    CAST(NULL AS STRING)                  AS COMMENTS,

    current_date()                        AS DATE_ADDED,
    CAST('ECOM_DIM' AS STRING)            AS ADDED_BY,

    current_date()                        AS DATE_LAST_MODIFIED,
    CAST('ECOM_DIM' AS STRING)            AS MODIFIED_BY,

    CAST('A' AS STRING)                   AS RECORD_STATUS,

    CAST(NULL AS STRING)                  AS BRAND_COMMENT,
    CAST(NULL AS STRING)                  AS ADDRESS,

    CAST(NULL AS STRING)                  AS CONTACT_1,
    CAST(NULL AS STRING)                  AS EMAIL_1,
    CAST(NULL AS STRING)                  AS CONTACT_2,
    CAST(NULL AS STRING)                  AS EMAIL_2,
    CAST(NULL AS STRING)                  AS CONTACT_3,
    CAST(NULL AS STRING)                  AS EMAIL_3,
    CAST(NULL AS STRING)                  AS CONTACT_4,
    CAST(NULL AS STRING)                  AS EMAIL_4,

    CAST(NULL AS STRING)                  AS PHONE_NUMBER,
    CAST(NULL AS STRING)                  AS FTP_SITE,
    CAST(NULL AS STRING)                  AS USERNAME,
    CAST(NULL AS STRING)                  AS WEBSITES,
    CAST(NULL AS STRING)                  AS PDF_CATALOG,

    CAST(COALESCE(v.VENDOR_VDC_FLAG, 'N') AS STRING) AS VDC_ELIGIBLE,

    CAST('Y' AS STRING)                   AS WEBSTORE_DSG,
    CAST('N' AS STRING)                   AS WEBSTORE_GG,
    CAST('N' AS STRING)                   AS WEBSTORE_FS,
    CAST('N' AS STRING)                   AS WEBSTORE_PL,
    CAST('N' AS STRING)                   AS WEBSTORE_G3

FROM {ecomp_catalog}.ecom_dim.vendor v
WHERE v.VENDOR_STATUS = 'A'
  AND v.VENDOR_NUMBER IS NOT NULL
  AND TRIM(v.VENDOR_NUMBER) <> ''
  AND TRIM(v.VENDOR_NUMBER) RLIKE '^[0-9]+$'
 """,
    mysql_table="ecom_stg.stg_cwl_vendor",
    truncate_first=True)

sp_config = LineupDBXEgressConfig(
    jdbc_url=jdbc_url,
    connection_properties={
        "user": user,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver",
        "rewriteBatchedStatements": "true",
        "isolationLevel": "READ_COMMITTED",
        "useServerPrepStmts": "true",
        "useCursorFetch": "true",
        "defaultFetchSize": "50000",
        # SSL & Timeout Settings for Azure
        "useSSL": "true",
        "requireSSL": "true",
        "sslMode": "REQUIRED",
        "verifyServerCertificate": "false",
        "enabledTLSProtocols": "TLSv1.2",
        "connectTimeout": "30000",
        "socketTimeout": "600000",
        "tcpKeepAlive": "true",
        "serverTimezone": "UTC" # Equivalent to your sessionVariables time_zone
    }
)

sp_spark = SparkSession.builder.getOrCreate()

dbx_loader = LineupDBXEgress(sp_spark, sp_config)

start_time = time.time()
dbx_loader.execute_mysql_sp_pymysql("ecom_stg.sp_merge_cwl_hierarchy_from_stage")
end_time = time.time()

print(f"Total execution time: {end_time - start_time} seconds")

start_time = time.time()
dbx_loader.execute_mysql_sp_pymysql("ecom_stg.sp_merge_cwl_vendor_from_stage")
end_time = time.time()

print(f"Total execution time: {end_time - start_time} seconds")

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.hr_associate",
    partition_column="hr_associate_id",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.hr_associate")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.hr_associate")

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.hr_associate_role_vw",
    partition_column="HR_ASSOCIATE_ID",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.hr_associate_role")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.hr_associate_role")

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.hr_associate_preference",
    partition_column="hr_associate_preference_id",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.hr_associate_preference")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.hr_associate_preference")

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.cwl_hierarchy",
    partition_column="cwl_hierarchy_id",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.cwl_hierarchy")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.cwl_hierarchy")

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.cwl_vendor",
    partition_column="vendor_number",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.cwl_vendor")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.cwl_vendor")  

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.cwl_hierarchy_brand_assoc",
    partition_column="hr_associate_id",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.cwl_hierarchy_brand_assoc")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.cwl_hierarchy_brand_assoc")

egress.load_custom_query_to_mysql_btch(
    sql_query=f"""
  SELECT
  basequ.HR_ASSOCIATE_ID,
  basequ.HR_ASSOCIATE_CODE,
  basequ.HR_ASSOCIATE_NAME,
  basequ.SUPERVISOR_ID,
  ha_s.USER_CODE AS SUPERVISOR_CODE,
  basequ.VENDOR_NUMBER,
  basequ.DIVISION_NUMBER,
  basequ.DEPARTMENT_NUMBER,
  basequ.SUB_DEPARTMENT_NUMBER,
  basequ.CLASS_NUMBER,
  basequ.SUB_CLASS_NUMBER,
  basequ.PRODUCT_HIERARCHY_KEY,
  basequ.HIER_PRIORITY as priority,
  ha_s.hr_associate_name AS SUPERVISOR_NAME
FROM (
  SELECT
    incl.hr_associate_id,
    incl.user_code AS HR_ASSOCIATE_CODE,
    incl.hr_associate_name,
    incl.supervisor_id,
    incl.vendor_number,
    incl.division_number,
    incl.department_number,
    incl.sub_department_number,
    incl.class_number,
    incl.sub_class_number,
    incl.product_hierarchy_key,
    ROW_NUMBER() OVER (
      PARTITION BY incl.product_hierarchy_key, incl.vendor_number
      ORDER BY hier_priority, incl.date_last_modified DESC
    ) AS hier_priority
  FROM (
    SELECT
      chba.hr_associate_id,
      ha.hr_associate_name,
      ha.user_code,
      ha.supervisor_id,
      ph.primary_vendor_number AS vendor_number,
      ph.division_number,
      ph.department_number,
      ph.sub_department_number,
      ph.class_number,
      ph.sub_class_number,
      NVL(ph.product_hierarchy_key, -1) AS product_hierarchy_key,
      NVL(chba.date_last_modified, chba.date_added) AS date_last_modified,
      CASE
        WHEN chba.vendor_number <> '*' THEN 0
        WHEN ch.sub_class_number <> 0 THEN 1
        WHEN ch.class_number <> 0 THEN 2
        WHEN ch.sub_department_number <> 0 THEN 3
        WHEN ch.department_number <> 0 THEN 4
        ELSE 5
      END AS hier_priority
    FROM prod_ecmde_db.ecom.cwl_hierarchy_brand_assoc AS chba
    JOIN prod_ecmde_db.ecom.cwl_hierarchy AS ch
      ON ch.cwl_hierarchy_id = chba.cwl_hierarchy_id
    JOIN prod_ecmde_db.ecom.hr_associate AS ha
      ON ha.hr_associate_id = chba.hr_associate_id
    JOIN (
      SELECT /*+ NO_MERGE */ DISTINCT ph1.*, primary_vendor_number
      FROM prod_ecmde_db.ecom_dim.style AS sty
      JOIN prod_ecmde_db.ecom_dim.product_hierarchy AS ph1
        ON sty.product_hierarchy_key = ph1.product_hierarchy_key
      WHERE ph1.record_status = 'A'
        AND sty.record_status = 'A'
    ) AS ph
      ON ph.division_number = ch.division_number
     AND ph.department_number = CASE WHEN ch.department_number = 0 THEN ph.department_number ELSE ch.department_number END
     AND ph.sub_department_number = CASE WHEN ch.sub_department_number = 0 THEN ph.sub_department_number ELSE ch.sub_department_number END
     AND ph.class_number = CASE WHEN ch.class_number = 0 THEN ph.class_number ELSE ch.class_number END
     AND ph.sub_class_number = CASE WHEN ch.sub_class_number = 0 THEN ph.sub_class_number ELSE ch.sub_class_number END
     AND ph.primary_vendor_number = CASE WHEN chba.vendor_number = '*' THEN ph.primary_vendor_number ELSE chba.vendor_number END
    WHERE chba.record_status = 'A'
      AND ha.record_status = 'A'
      AND ch.record_status = 'A'
      AND task_code = '*'
      AND chba.hr_associate_id <> 1
      AND chba.inclusion_exclusion_code = 'I'
      AND chba.vendor_number <> '99999'
  ) AS incl
  LEFT JOIN (
    SELECT DISTINCT
      chbae.hr_associate_id,
      chbae.vendor_number,
      NVL(phe.product_hierarchy_key, -1) AS product_hierarchy_key,
      che.division_number AS excl_division_number
    FROM prod_ecmde_db.ecom.cwl_hierarchy_brand_assoc AS chbae
    JOIN prod_ecmde_db.ecom.cwl_hierarchy AS che
      ON che.cwl_hierarchy_id = chbae.cwl_hierarchy_id
    LEFT JOIN prod_ecmde_db.ecom_dim.product_hierarchy AS phe
      ON phe.division_number = che.division_number
     AND phe.record_status = 'A'
     AND phe.department_number = CASE WHEN che.department_number = 0 THEN phe.department_number ELSE che.department_number END
     AND phe.sub_department_number = CASE WHEN che.sub_department_number = 0 THEN phe.sub_department_number ELSE che.sub_department_number END
     AND phe.class_number = CASE WHEN che.class_number = 0 THEN phe.class_number ELSE che.class_number END
     AND phe.sub_class_number = CASE WHEN che.sub_class_number = 0 THEN phe.sub_class_number ELSE che.sub_class_number END
    WHERE chbae.record_status = 'A'
      AND che.record_status = 'A'
      AND chbae.task_code = '*'
      AND chbae.hr_associate_id <> 1
      AND chbae.inclusion_exclusion_code = 'E'
      AND chbae.vendor_number <> '99999'
  ) AS excl
    ON incl.hr_associate_id = excl.hr_associate_id
   AND incl.vendor_number =
       CASE
         WHEN excl.vendor_number = '*' THEN incl.vendor_number
         ELSE excl.vendor_number
       END
   AND incl.product_hierarchy_key =
       CASE
         WHEN excl.excl_division_number = 0 THEN incl.product_hierarchy_key
         ELSE excl.product_hierarchy_key
       END
  WHERE excl.hr_associate_id IS NULL
) AS basequ
LEFT JOIN prod_ecmde_db.ecom.hr_associate AS ha_s
  ON ha_s.hr_associate_id = basequ.supervisor_id
WHERE hier_priority = 1
 """,
    mysql_table="ecom.cwl_flomoe_ownership",
    truncate_first=True)

df = dbx_loader.read_from_mysql_parallel(
    mysql_table="ecom.cwl_flomoe_ownership",
    partition_column="hr_associate_id",  # <-- change to your numeric key
    num_partitions=64,
    fetchsize=50000
)

spark.sql(f"TRUNCATE TABLE {ecomp_catalog}.ecom.cwl_flomoe_ownership")
df.write.mode("append").insertInto(f"{ecomp_catalog}.ecom.cwl_flomoe_ownership")