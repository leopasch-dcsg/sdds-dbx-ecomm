# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from ecmde_ecomm.ecomp.lineup import LineupEgress,LineupEgressConfig
from ecmde_ecomm.common.util import CredentialUtil, NotebookUtil
from databricks.sdk.runtime import dbutils

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
WITH
/* =========================================================
   1) CORE WORKLIST AGG (STYLE + COLOR)
   ========================================================= */
cws_agg AS (
  SELECT
    cws.style_number,
    CASE
      WHEN UPPER(TRIM(cws.color)) IN ('NO COLOR', 'NO COL', 'NCOLR') THEN 'NCOLR'
      ELSE UPPER(TRIM(cws.color))
    END AS join_color,
    cws.color AS display_color,

    /* DATA_SOURCE */
    TRIM(BOTH ',' FROM CONCAT(
      CASE WHEN SUM(CASE WHEN cws.data_source = 'I' THEN 1 ELSE 0 END) > 0 THEN ',I' ELSE '' END,
      CASE WHEN SUM(CASE WHEN cws.data_source = 'P' THEN 1 ELSE 0 END) > 0 THEN ',P' ELSE '' END,
      CASE WHEN SUM(CASE WHEN cws.data_source = 'V' THEN 1 ELSE 0 END) > 0 THEN ',V' ELSE '' END
    )) AS data_source,

    /* GROUP UNITS */
    SUM(COALESCE(cws.eco_on_hand, 0) + COALESCE(cws.eco_on_order, 0) + COALESCE(cws.dsg_eco_in_transit, 0)) AS dsg_eco_group_units,
    SUM(COALESCE(cws.dsg_on_order, 0) + COALESCE(cws.dsg_on_hand, 0) + COALESCE(cws.dsg_bam_in_transit, 0)) AS dsg_bam_group_units,
    SUM(COALESCE(cws.dsg_vdc_on_hand, 0)) AS dsg_vdc_group_units,

    SUM(COALESCE(cws.gg_bam_on_order, 0) + COALESCE(cws.gg_bam_on_hand, 0) + COALESCE(cws.gg_bam_in_transit, 0)) AS gg_bam_group_units,
    SUM(COALESCE(cws.fs_bam_on_order, 0) + COALESCE(cws.fs_bam_on_hand, 0) + COALESCE(cws.fs_bam_in_transit, 0)) AS fs_bam_group_units,
    SUM(COALESCE(cws.pl_bam_on_order, 0) + COALESCE(cws.pl_bam_on_hand, 0) + COALESCE(cws.pl_bam_in_transit, 0)) AS pl_bam_group_units,

    /* PRICE ROLLUPS - DO NOT COALESCE TO 0 INSIDE MIN() */
    MIN(cws.dsg_price)    AS dsg_bam_price,
    MIN(cws.eco_price)    AS dsg_eco_price,
    MIN(cws.gg_bam_price) AS gg_bam_price,
    MIN(cws.gg_eco_price) AS gg_eco_price,
    MIN(cws.fs_bam_price) AS fs_bam_price,
    MIN(cws.fs_eco_price) AS fs_eco_price,

    /* STYLE UNITS */
    MAX(COALESCE(cws.style_units, 0))    AS dsg_style_units,
    MAX(COALESCE(cws.gg_style_units, 0)) AS gg_style_units,
    MAX(COALESCE(cws.fs_style_units, 0)) AS fs_style_units,
    MAX(COALESCE(cws.pl_style_units, 0)) AS pl_style_units,

    /* DSG_BAM_PROGRAM */
    COALESCE(
      TRIM(
        LEADING ','
        FROM prod_ecmde_db.ecom.custom_concat_ifnull(
          ARRAY(
            CASE
              WHEN SUM(
                COALESCE(cws.pl_bam_on_order, 0)
              + COALESCE(cws.fs_bam_on_order, 0)
              + COALESCE(cws.gg_bam_on_order, 0)
              + COALESCE(cws.dsg_on_order, 0)
              + COALESCE(cws.gg_eco_on_hand, 0)
              + COALESCE(cws.dc_on_hand, 0)
              + COALESCE(cws.dc_on_order, 0)
              + COALESCE(cws.dsg_bam_in_transit, 0)
              + COALESCE(cws.gg_bam_in_transit, 0)
              + COALESCE(cws.fs_bam_in_transit, 0)
              + COALESCE(cws.pl_bam_in_transit, 0)
              ) > 0 THEN ',B&M'
              ELSE NULL
            END,
            CASE
              WHEN SUM(
                COALESCE(cws.eco_on_hand, 0)
              + COALESCE(cws.eco_on_order, 0)
              + COALESCE(cws.dsg_eco_in_transit, 0)
              ) > 0 THEN ',eCom'
              ELSE NULL
            END,
            MAX(
              CASE
                WHEN cws.dsg_vdc NOT LIKE 'N%' THEN ',VDC'
                ELSE NULL
              END
            )
          )
        )
      ),
      'None'
    ) AS dsg_bam_program,

    /* DSG_VDC */
    CASE
      WHEN MAX(cws.dsg_vdc) IS NULL THEN NULL
      WHEN MAX(cws.dsg_vdc) = MIN(cws.dsg_vdc) THEN MAX(cws.dsg_vdc)
      WHEN MAX(cws.dsg_vdc) LIKE 'YES%' OR UPPER(MAX(cws.dsg_vdc)) LIKE '%PENDING' THEN 'YES-Mix'
      WHEN MIN(cws.dsg_vdc) LIKE 'N%' THEN 'NO-Mix'
      ELSE 'Mix'
    END AS dsg_vdc

  FROM prod_ecmde_db.ecom.content_worklist_sku cws
  WHERE cws.record_status = 'A'
  GROUP BY
    cws.style_number,
    CASE
      WHEN UPPER(TRIM(cws.color)) IN ('NO COLOR', 'NO COL', 'NCOLR') THEN 'NCOLR'
      ELSE UPPER(TRIM(cws.color))
    END,
    cws.color
),

/* =========================================================
   2) STYLE INFO + BRAND + VENDOR + HIERARCHY
   ========================================================= */
style_info AS (
  SELECT
    s.style_code,
    s.style_desc,
    s.emast_pim_ecode AS ecode,
    s.primary_vendor_number,
    s.primary_vendor_name,
    MIN(b.brand_desc) AS brand,
    ROUND(CASE WHEN s.average_cost > 0 THEN s.average_cost ELSE s.current_cost END, 2) AS cost_basis,

    ph.division_number,
    ph.department_number,
    ph.sub_department_number,
    ph.class_number,
    ph.sub_class_number,
    ph.sub_department_description AS sub_class
  FROM prod_ecmde_db.ecom_dim.style s
  JOIN prod_ecmde_db.ecom_dim.product_hierarchy ph
    ON ph.product_hierarchy_key = s.product_hierarchy_key
  LEFT JOIN prod_ecmde_db.ecom_dim.brand b
    ON b.brand_key = s.style_brand_key
  WHERE s.record_status = 'A'
  GROUP BY
    s.style_code,
    s.style_desc,
    s.emast_pim_ecode,
    s.primary_vendor_number,
    s.primary_vendor_name,
    ROUND(CASE WHEN s.average_cost > 0 THEN s.average_cost ELSE s.current_cost END, 2),
    ph.division_number,
    ph.department_number,
    ph.sub_department_number,
    ph.class_number,
    ph.sub_class_number,
    ph.sub_department_description
),

/* =========================================================
   3) ERD LOGIC
   ========================================================= */
erd_logic AS (
  SELECT
    s.style_code,
    CASE
      WHEN UPPER(TRIM(cc.color_code_desc)) IN ('NO COLOR', 'NO COL', 'NCOLR') THEN 'NCOLR'
      ELSE UPPER(TRIM(cc.color_code_desc))
    END AS join_color,
    MIN(dsu.last_min_po_erd) AS dks_min_erd_bigint
  FROM prod_ecmde_db.ecom_dim.dks_sku ds
  JOIN prod_ecmde_db.ecom_dim.dks_sku_units dsu
    ON ds.dks_sku_key = dsu.dks_sku_key
  JOIN prod_ecmde_db.ecom_dim.style s
    ON s.style_key = ds.style_key
  JOIN prod_ecmde_db.ecom_dim.color_code cc
    ON ds.dks_sku_color_code_key = cc.color_code_key
  WHERE dsu.last_min_po_erd IS NOT NULL
  GROUP BY
    s.style_code,
    CASE
      WHEN UPPER(TRIM(cc.color_code_desc)) IN ('NO COLOR', 'NO COL', 'NCOLR') THEN 'NCOLR'
      ELSE UPPER(TRIM(cc.color_code_desc))
    END
),

/* =========================================================
   4) MDM / WCS / WEB READY FLAGS
   ========================================================= */
mdm_info AS (
  SELECT
    s.style_code AS pmms_style_number,

    CASE
      WHEN pe.em_product_title IS NOT NULL
       AND COALESCE(pe.em_product_desc_flg, 'N') = 'Y'
      THEN 'Y' ELSE 'N'
    END AS mdm_copy_done,

    COALESCE(pe.wsc_ready_flg, 'N') AS mdm_emst_wcs,

    MAX(CASE pp.webstore_key WHEN 6 THEN COALESCE(pp.web_ready_flg, 'N') ELSE 'N' END) AS dsg_mdm_setup_done,
    MAX(CASE pp.webstore_key WHEN 2 THEN COALESCE(pp.web_ready_flg, 'N') ELSE 'N' END) AS gg_mdm_setup_done,
    MAX(CASE pp.webstore_key WHEN 4 THEN COALESCE(pp.web_ready_flg, 'N') ELSE 'N' END) AS fs_mdm_setup_done,
    MAX(CASE pp.webstore_key WHEN 7 THEN COALESCE(pp.web_ready_flg, 'N') ELSE 'N' END) AS pl_mdm_setup_done

  FROM prod_ecmde_db.ecom_dim.style s
  LEFT JOIN prod_ecmde_db.ecom_dim.pim_product_emast pe
    ON s.emast_pim_entity_id = pe.pim_product_emast_key
  LEFT JOIN prod_ecmde_db.ecom_dim.bridge_pim_product_style pps
    ON s.style_key = pps.style_key
   AND pps.is_current = 'C'
  LEFT JOIN prod_ecmde_db.ecom_dim.pim_product pp
    ON pps.pim_product_key = pp.pim_product_key
   AND pps.webstore_key = pp.webstore_key
   AND pp.record_status = 'A'
  WHERE s.record_status = 'A'
  GROUP BY
    s.style_code,
    pe.em_product_title,
    pe.em_product_desc_flg,
    pe.wsc_ready_flg
),

/* =========================================================
   5) OWNERSHIP
   ========================================================= */
ownership_info AS (
  SELECT
    si.style_code AS pmms_style_number,
    COALESCE(foc.hr_associate_code, 'Unassigned') AS ownership_assignee,
    COALESCE(foc.supervisor_code,   'Unassigned') AS ownership_supervisor
  FROM style_info si
  LEFT JOIN prod_ecmde_db.ecom.cwl_flomoe_ownership foc
    ON foc.department_number      = si.department_number
   AND foc.sub_department_number  = si.sub_department_number
   AND foc.class_number           = si.class_number
   AND foc.sub_class_number       = si.sub_class_number
   AND foc.vendor_number          = si.primary_vendor_number
),

/* =========================================================
   6) IMAGE DONE FLAG + FIRST IMAGE DATE + PIM VALUE CHAIN ELIGIBLE
   ========================================================= */
image_done_logic AS (
  SELECT
    cws.style_number,
    CASE
      WHEN UPPER(TRIM(cws.color)) IN ('NO COLOR', 'NO COL', 'NCOLR') THEN 'NCOLR'
      ELSE UPPER(TRIM(cws.color))
    END AS join_color,

    CASE
      WHEN MAX(COALESCE(by_sku.image_count, 0)) > 0 THEN 'Y'
      ELSE 'N'
    END AS dsg_image_done_ecode,

    MIN(by_sku.first_image_date) AS image_found_date,
    MIN(by_sku.pim_value_chain_eligible) AS gg_ecode
  FROM prod_ecmde_db.ecom.content_worklist_sku cws
  LEFT JOIN (
    SELECT
      ds.dks_sku_code,
      MAX(COALESCE(cl.image_cnt, 0)) AS image_count,
      MIN(cl.first_image_date)       AS first_image_date,
      MIN(
        CASE
          WHEN COALESCE(ds.em_sku_exclusive_to, '') = 'G3' THEN 'EXCLUSIVE'
          ELSE 'NOT ELIGIBLE'
        END
      ) AS pim_value_chain_eligible
    FROM prod_ecmde_db.ecom_dim.dks_sku_pim ds
    LEFT JOIN prod_ecmde_db.ecom_dim.pim_product_emast_color cl
      ON ds.pim_product_emast_color_key = cl.pim_product_emast_color_key
     AND cl.record_status = 'A'
    WHERE ds.record_status = 'A'
    GROUP BY ds.dks_sku_code
  ) by_sku
    ON cws.dks_sku = by_sku.dks_sku_code
  WHERE cws.record_status = 'A'
  GROUP BY
    cws.style_number,
    CASE
      WHEN UPPER(TRIM(cws.color)) IN ('NO COLOR', 'NO COL', 'NCOLR') THEN 'NCOLR'
      ELSE UPPER(TRIM(cws.color))
    END
)

/* =========================================================
   FINAL STAGE SELECT
   ========================================================= */
SELECT
  CAST(si.style_code AS STRING) AS pmms_style_number,
  CAST('Image' AS STRING)       AS task_type,
  CAST(ca.display_color AS STRING) AS task_code,
  CAST(ca.display_color AS STRING) AS site_color_code,

  CAST(ca.data_source AS STRING) AS data_source,

  CAST(si.style_desc AS STRING) AS style_description,
  CAST(si.ecode AS STRING)      AS ecode,
  CAST(si.brand AS STRING)      AS brand,

  CAST(si.primary_vendor_number AS BIGINT) AS vendor_number,
  CAST(si.primary_vendor_name   AS STRING) AS vendor,

  CAST(oi.ownership_assignee   AS STRING) AS ownership_assignee,
  CAST(oi.ownership_supervisor AS STRING) AS ownership_supervisor,

  CAST(COALESCE(mi.mdm_copy_done, 'N') AS STRING)       AS mdm_copy_done,
  CAST(COALESCE(mi.mdm_emst_wcs, 'N') AS STRING)        AS mdm_emst_wcs,
  CAST(COALESCE(mi.dsg_mdm_setup_done, 'N') AS STRING)  AS dsg_mdm_setup_done,
  CAST(COALESCE(mi.gg_mdm_setup_done, 'N') AS STRING)   AS gg_mdm_setup_done,
  CAST(COALESCE(mi.fs_mdm_setup_done, 'N') AS STRING)   AS fs_mdm_setup_done,
  CAST(COALESCE(mi.pl_mdm_setup_done, 'N') AS STRING)   AS pl_mdm_setup_done,

  CAST(ca.dsg_bam_program AS STRING) AS dsg_bam_program,

  /* Keep both forms for downstream MySQL stage */
  CAST(el.dks_min_erd_bigint AS STRING) AS min_erd,
  TO_DATE(CAST(el.dks_min_erd_bigint AS STRING), 'yyyyMMdd') AS dks_min_erd,

  CAST(si.sub_class AS STRING) AS description,
  CAST(si.cost_basis AS DECIMAL(13,2)) AS cost_basis,

  CAST(si.division_number       AS BIGINT) AS division_number,
  CAST(si.department_number     AS BIGINT) AS department_number,
  CAST(si.sub_department_number AS BIGINT) AS sub_department_number,
  CAST(si.class_number          AS BIGINT) AS class_number,
  CAST(si.sub_class_number      AS BIGINT) AS sub_class_number,

  CAST(ca.dsg_eco_group_units AS BIGINT) AS dsg_eco_group_units,
  CAST(ca.dsg_bam_group_units AS BIGINT) AS dsg_bam_group_units,
  CAST(ca.dsg_vdc_group_units AS BIGINT) AS dsg_vdc_group_units,
  CAST(ca.gg_bam_group_units  AS BIGINT) AS gg_bam_group_units,
  CAST(ca.fs_bam_group_units  AS BIGINT) AS fs_bam_group_units,
  CAST(ca.pl_bam_group_units  AS BIGINT) AS pl_bam_group_units,

  CAST(ca.dsg_bam_price AS DECIMAL(13,2)) AS dsg_bam_price,
  CAST(ca.dsg_eco_price AS DECIMAL(13,2)) AS dsg_eco_price,
  CAST(ca.gg_bam_price  AS DECIMAL(13,2)) AS gg_bam_price,
  CAST(ca.gg_eco_price  AS DECIMAL(13,2)) AS gg_eco_price,
  CAST(ca.fs_bam_price  AS DECIMAL(13,2)) AS fs_bam_price,
  CAST(ca.fs_eco_price  AS DECIMAL(13,2)) AS fs_eco_price,

  CAST(ca.dsg_style_units AS BIGINT) AS dsg_style_units,
  CAST(ca.gg_style_units  AS BIGINT) AS gg_style_units,
  CAST(ca.fs_style_units  AS BIGINT) AS fs_style_units,
  CAST(ca.pl_style_units  AS BIGINT) AS pl_style_units,

  CAST(ca.dsg_vdc AS STRING) AS dsg_vdc,

  CAST(idl.dsg_image_done_ecode AS STRING) AS dsg_image_done_ecode,
  CAST(idl.image_found_date     AS DATE)   AS image_found_date,
  CAST(idl.gg_ecode             AS STRING) AS gg_ecode,

  CAST('A' AS STRING)        AS record_status,
  CURRENT_TIMESTAMP()        AS date_added,
  CAST('DBX_LOAD' AS STRING) AS added_by,
  CURRENT_TIMESTAMP()        AS date_last_modified,
  CURRENT_USER()             AS modified_by

FROM cws_agg ca
JOIN style_info si
  ON ca.style_number = si.style_code
LEFT JOIN erd_logic el
  ON si.style_code = el.style_code
 AND ca.join_color = el.join_color
LEFT JOIN mdm_info mi
  ON mi.pmms_style_number = si.style_code
LEFT JOIN ownership_info oi
  ON oi.pmms_style_number = si.style_code
LEFT JOIN image_done_logic idl
  ON idl.style_number = ca.style_number
 AND idl.join_color   = ca.join_color
 """,
    mysql_table="ecom_stg.stg_cwl_pmms_style_task_all",
    truncate_first=True)