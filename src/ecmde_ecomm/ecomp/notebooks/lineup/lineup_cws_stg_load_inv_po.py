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
   Seed rows: recent V rows from content_worklist_sku
   ========================================================= */
v_seed AS (
  SELECT
    CAST(cws.dks_sku AS BIGINT)          AS dks_sku,
    CAST(cws.data_source AS STRING)      AS data_source,
    CAST(cws.vdc_associate_id AS BIGINT) AS vdc_associate_id,
    cws.date_added,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(cws.dks_sku AS BIGINT)
      ORDER BY cws.date_added DESC
    ) AS rn
  FROM {ecomp_catalog}.ecom.content_worklist_sku cws
  WHERE cws.data_source = 'V'
    AND cws.date_added >= current_timestamp() - INTERVAL 4 DAYS
    AND cws.dks_sku IS NOT NULL
),

/* =========================================================
   Keep latest V row per SKU
   ========================================================= */
v_src AS (
  SELECT
    dks_sku,
    data_source,
    vdc_associate_id
  FROM v_seed
  WHERE rn = 1
),

/* =========================================================
   SKU / Style / Hierarchy enrichment
   ========================================================= */
sku_dim AS (
  SELECT
    CAST(ds.dks_sku_code AS BIGINT) AS dks_sku,
    CAST(st.style_key AS BIGINT)    AS style_id,

    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(ph.division_number, '-', ph.division_description)
    ) AS division,

    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(ph.division_number, '.', ph.department_number, '-', ph.department_description)
    ) AS department,

    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(ph.division_number, '.', ph.department_number, '.', ph.sub_department_number, '-', ph.sub_department_description)
    ) AS sub_department,

    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(ph.division_number, '.', ph.department_number, '.', ph.sub_department_number, '.', ph.class_number, '-', ph.class_description)
    ) AS class,

    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(ph.division_number, '.', ph.department_number, '.', ph.sub_department_number, '.', ph.class_number, '.', ph.sub_class_number, '-', ph.sub_class_description)
    ) AS sub_class,

    st.primary_vendor_name   AS vendor,
    st.style_desc            AS style_name,
    ds.dks_sku_desc          AS product_description,
    st.vendor_style_number   AS style_number,
    ds.vendor_product_number AS vendor_product_number,
    cc.color_code_desc       AS color,
    CAST(ph.product_hierarchy_key AS BIGINT) AS product_hierarchy_id,

    CAST(st.size_group AS STRING) AS size_description,
    CAST(NULL AS BIGINT) AS vendor_number

  FROM {ecomp_catalog}.ecom_dim.dks_sku ds
  JOIN {ecomp_catalog}.ecom_dim.style st
    ON ds.style_key = st.style_key
   AND ds.product_number <> st.master_sku
  JOIN {ecomp_catalog}.ecom_dim.product_hierarchy ph
    ON st.product_hierarchy_key = ph.product_hierarchy_key
  LEFT JOIN {ecomp_catalog}.ecom_dim.color_code cc
    ON ds.dks_sku_color_code_key = cc.color_code_key
  WHERE ds.record_status = 'A'
    AND st.record_status = 'A'
),

/* =========================================================
   UPC lookup
   ========================================================= */
sku_upc AS (
  SELECT
    CAST(ds.dks_sku_code AS BIGINT) AS dks_sku,
    CASE
      WHEN MAX(u.upc_code) < 1000000000000
        THEN LPAD(CAST(MAX(u.upc_code) AS STRING), 12, '0')
      ELSE TRIM(CAST(MAX(u.upc_code) AS STRING))
    END AS upc
  FROM {ecomp_catalog}.ecom_dim.dks_sku ds
  JOIN {ecomp_catalog}.ecom_dim.upc u
    ON u.dks_sku_key = ds.dks_sku_key
   AND u.primary_upc_flag = '1'
   AND u.record_status = 'A'
  WHERE ds.record_status = 'A'
  GROUP BY ds.dks_sku_code
),

/* =========================================================
   Price lookup
   ========================================================= */
price_sr AS (
  SELECT
    CAST(pd.product_number AS BIGINT) AS dks_sku,
    COALESCE(sd.dsg_bm_retail, sd.original_retail)    AS dsg_price,
    COALESCE(sd.dsg_ecomm_retail, sd.original_retail) AS eco_price,
    COALESCE(sd.gg_bm_retail, sd.original_retail)     AS gg_bam_price,
    COALESCE(sd.gg_ecomm_retail, sd.original_retail)  AS gg_eco_price,
    COALESCE(sd.fas_bm_retail, sd.original_retail)    AS fs_bam_price,
    COALESCE(sd.fas_ecomm_retail, sd.original_retail) AS fs_eco_price
  FROM {ecomp_catalog}.ecom_dim.dks_sku pd
  JOIN {ecomp_catalog}.ecom_dim.style sd
    ON pd.style_key = sd.style_key
  WHERE pd.record_status = 'A'
    AND sd.record_status = 'A'
),

/* =========================================================
   Cost + route PO
   ========================================================= */
cost_sr AS (
  SELECT
    CAST(pd.product_number AS BIGINT) AS dks_sku,
    pd.route_po_flag AS route_po,
    sd.average_cost  AS avg_unit_cost,
    CASE
      WHEN sd.average_cost > 0 THEN sd.average_cost
      ELSE sd.current_cost
    END AS cost_basis
  FROM {ecomp_catalog}.ecom_dim.dks_sku pd
  JOIN {ecomp_catalog}.ecom_dim.style sd
    ON pd.style_key = sd.style_key
  WHERE pd.record_status = 'A'
    AND sd.record_status = 'A'
),

/* =========================================================
   VDC eligibility
   ========================================================= */
vdc_sr AS (
  SELECT
    CAST(dks_sku_code AS BIGINT) AS dks_sku,
    CASE
      WHEN pim_vdc_eligible_flg = 'Y' THEN 'YES'
      WHEN pim_vdc_eligible_flg = 'N' THEN 'NO'
      ELSE NULL
    END AS dsg_vdc
  FROM {ecomp_catalog}.ecom_dim.dks_sku
  WHERE record_status = 'A'
)

/* =========================================================
   FINAL SELECT
   Order aligned to stg_content_worklist_sku_vdc
   ========================================================= */
SELECT
  CAST(v.dks_sku AS BIGINT) AS DKS_SKU,
  CAST(NULL AS BIGINT) AS DSG_MIN_ERD,
  CAST(NULL AS BIGINT) AS DSG_MAX_ERD,
  CAST(NULL AS BIGINT) AS DSG_COUNT_ERD,
  CAST(NULL AS BIGINT) AS ECO_MIN_ERD,
  CAST(NULL AS BIGINT) AS ECO_MAX_ERD,
  CAST(NULL AS BIGINT) AS ECO_COUNT_ERD,

  CAST('V' AS STRING) AS DATA_SOURCE,
  CAST(s.division AS STRING) AS DIVISION,
  CAST(s.department AS STRING) AS DEPARTMENT,
  CAST(s.sub_department AS STRING) AS SUB_DEPARTMENT,
  CAST(s.class AS STRING) AS CLASS,
  CAST(s.sub_class AS STRING) AS SUB_CLASS,

  CAST(NULL AS STRING) AS BRAND,
  CAST(s.vendor AS STRING) AS VENDOR,
  CAST(s.style_name AS STRING) AS STYLE_NAME,
  CAST(s.product_description AS STRING) AS PRODUCT_DESCRIPTION,

  CAST(NULL AS BIGINT) AS GSI_SKU,
  CAST(u.upc AS STRING) AS UPC,
  CAST(s.vendor_product_number AS STRING) AS VENDOR_PRODUCT_NUMBER,

  CAST(NULL AS STRING) AS PID_STATUS,
  CAST(NULL AS STRING) AS SKU_STATUS,
  CAST(s.color AS STRING) AS COLOR,

  CAST(NULL AS STRING) AS GLOBAL_CAT_ID,
  CAST(NULL AS STRING) AS GLOBAL_CAT_NAME,
  CAST('NO' AS STRING) AS IS_MATCHED,

  CAST(s.style_number AS STRING) AS STYLE_NUMBER,
  CAST(NULL AS BIGINT) AS PID,

  CAST(p.dsg_price AS DECIMAL(13,2)) AS DSG_PRICE,
  CAST(p.eco_price AS DECIMAL(13,2)) AS ECO_PRICE,

  CAST(NULL AS STRING) AS E3_STYLE,
  CAST(NULL AS STRING) AS TW_SFS_STATUS,
  CAST(NULL AS STRING) AS TW_SFS_REASON,
  CAST(NULL AS STRING) AS NW_SFS_STATUS,
  CAST(NULL AS STRING) AS NW_SFS_REASON,

  CAST(NULL AS BIGINT) AS DSG_ON_HAND,
  CAST(NULL AS BIGINT) AS DSG_ON_ORDER,
  CAST(NULL AS BIGINT) AS ECO_ON_HAND,
  CAST(NULL AS BIGINT) AS ECO_ON_ORDER,

  CAST(NULL AS BIGINT) AS DSG_MIN_PO,
  CAST(NULL AS BIGINT) AS ECO_MIN_PO,
  CAST(NULL AS BIGINT) AS INVENTORY_DATE,
  CAST(NULL AS BIGINT) AS PRODUCT_ID,

  CAST(s.style_id AS BIGINT) AS STYLE_ID,

  CAST(NULL AS STRING) AS IMAGE_SOURCE,
  CAST('Missing' AS STRING) AS IMAGE_STATUS,

  CURRENT_TIMESTAMP() AS DATE_ADDED,
  CURRENT_USER()      AS ADDED_BY,
  CURRENT_TIMESTAMP() AS DATE_LAST_MODIFIED,
  CURRENT_USER()      AS MODIFIED_BY,

  CAST(NULL AS STRING) AS MODIFY_REASON,
  CAST('A' AS STRING) AS RECORD_STATUS,
  CAST(NULL AS STRING) AS PMMS_COLOR_CODE,
  CAST('U' AS STRING) AS HAS_IMAGE,

  CAST(COALESCE(vd.dsg_vdc, 'NE') AS STRING) AS VDC,
  CAST('NE' AS STRING) AS SFS,
  CAST('NE' AS STRING) AS BOPUIS,
  CAST('NE' AS STRING) AS ISA,

  CAST(c.route_po AS STRING) AS ROUTE_PO,
  CAST(NULL AS TIMESTAMP) AS SETUP_DATE,
  CAST(0 AS BIGINT) AS STYLE_UNITS,

  CAST(c.avg_unit_cost AS DECIMAL(15,4)) AS AVG_UNIT_COST,
  CAST(s.product_hierarchy_id AS BIGINT) AS PRODUCT_HIERARCHY_ID,

  CAST('Y' AS STRING) AS VDC_TYPE,
  CAST(NULL AS STRING) AS SFS_CALC,
  CAST(c.cost_basis AS DECIMAL(13,2)) AS COST_BASIS,

  CAST(NULL AS TIMESTAMP) AS FIRST_RECEIPT,
  CAST(NULL AS TIMESTAMP) AS LAST_RECEIPT,

  CAST(NULL AS BIGINT) AS GG_BAM_ON_HAND,
  CAST(NULL AS BIGINT) AS GG_BAM_ON_ORDER,
  CAST(NULL AS BIGINT) AS GG_BAM_MIN_ERD,
  CAST(NULL AS BIGINT) AS GG_ECO_ON_HAND,
  CAST(NULL AS BIGINT) AS GG_ECO_ON_ORDER,
  CAST(NULL AS BIGINT) AS GG_ECO_MIN_ERD,

  CAST(p.gg_eco_price AS DECIMAL(13,2)) AS GG_ECO_PRICE,
  CAST(p.gg_bam_price AS DECIMAL(13,2)) AS GG_BAM_PRICE,
  CAST(0 AS BIGINT) AS GG_STYLE_UNITS,

  CAST(NULL AS STRING) AS ECODE,
  CAST(NULL AS BIGINT) AS DSG_VDC_ON_HAND,
  CAST(s.size_description AS STRING) AS SIZE_DESCRIPTION,
  CAST(NULL AS BIGINT) AS BUYER_ID,
  CAST(v.vdc_associate_id AS BIGINT) AS VDC_ASSOCIATE_ID,
  CAST(NULL AS BIGINT) AS GG_VDC_ON_HAND,

  CAST(NULL AS BIGINT) AS FS_BAM_ON_HAND,
  CAST(NULL AS BIGINT) AS FS_BAM_ON_ORDER,
  CAST(p.fs_bam_price AS DECIMAL(13,2)) AS FS_BAM_PRICE,
  CAST(p.fs_eco_price AS DECIMAL(13,2)) AS FS_ECO_PRICE,
  CAST(0 AS BIGINT) AS FS_STYLE_UNITS,

  CAST(NULL AS STRING) AS GG_SFS,
  CAST(NULL AS STRING) AS GG_VDC,
  CAST(NULL AS STRING) AS FS_SFS,
  CAST(NULL AS STRING) AS FS_VDC,

  CAST(NULL AS BIGINT) AS FS_BAM_MIN_ERD,
  CAST(NULL AS BIGINT) AS FS_ECO_MIN_ERD,
  CAST(NULL AS BIGINT) AS FS_ECO_ON_ORDER,
  CAST(NULL AS BIGINT) AS FS_ECO_ON_HAND,
  CAST(NULL AS BIGINT) AS FS_VDC_ON_HAND,

  CAST(s.vendor_number AS BIGINT) AS VENDOR_NUMBER,

  CAST(NULL AS BIGINT) AS DSG_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS DSG_BAM_SHIP_TRANSFER,
  CAST(NULL AS BIGINT) AS GG_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS GG_BAM_SHIP_TRANSFER,
  CAST(NULL AS BIGINT) AS FS_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS FS_BAM_SHIP_TRANSFER,

  CAST(NULL AS BIGINT) AS DSG_ECO_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS DSG_ECO_SHIP_TRANSFER,
  CAST(NULL AS BIGINT) AS GG_ECO_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS GG_ECO_SHIP_TRANSFER,

  CAST(NULL AS STRING) AS DSG_SFS,
  CAST(NULL AS STRING) AS DSG_SFS_PREVIOUS,
  CAST(NULL AS STRING) AS GG_SFS_PREVIOUS,
  CAST(NULL AS STRING) AS FS_SFS_PREVIOUS,

  CAST(NULL AS STRING) AS DSG_EDC,
  CAST(NULL AS STRING) AS GG_EDC,
  CAST(NULL AS STRING) AS FS_EDC,

  CAST(NULL AS BIGINT) AS GG_OCE_UNITS,
  CAST(NULL AS BIGINT) AS LAB_ON_HAND,
  CAST(NULL AS BIGINT) AS LAB_ON_ORDER,
  CAST(NULL AS BIGINT) AS DC_ON_HAND,
  CAST(NULL AS BIGINT) AS DC_ON_ORDER,

  CAST(NULL AS BIGINT) AS DC_051_OO,
  CAST(NULL AS BIGINT) AS DC_351_OO,
  CAST(NULL AS BIGINT) AS DC_651_OO,
  CAST(NULL AS BIGINT) AS DC_851_OO,

  CAST(NULL AS BIGINT) AS DC_051_ERD,
  CAST(NULL AS BIGINT) AS DC_351_ERD,
  CAST(NULL AS BIGINT) AS DC_651_ERD,
  CAST(NULL AS BIGINT) AS DC_851_ERD,

  CAST(NULL AS STRING) AS DSG_BAM_PROGRAM,
  CAST(NULL AS STRING) AS GG_BAM_PROGRAM,
  CAST(NULL AS STRING) AS FS_BAM_PROGRAM,

  CAST(COALESCE(vd.dsg_vdc, 'NE') AS STRING) AS DSG_VDC,

  CAST(NULL AS BIGINT) AS WEB_OH_QTY,
  CAST(NULL AS BIGINT) AS DC_1051_OO,
  CAST(NULL AS BIGINT) AS DC_1051_ERD,
  CAST(NULL AS BIGINT) AS DC_951_OO,
  CAST(NULL AS BIGINT) AS DC_951_ERD,
  CAST(NULL AS BIGINT) AS RADIAL_DC_OH_QTY,
  CAST(NULL AS BIGINT) AS STORE_42_OH_QTY,

  CAST(NULL AS BIGINT) AS PL_BAM_ON_HAND,
  CAST(NULL AS BIGINT) AS PL_BAM_ON_ORDER,
  CAST(NULL AS BIGINT) AS PL_BAM_IN_TRANSIT,
  CAST(0 AS BIGINT) AS PL_STYLE_UNITS

FROM v_src v
LEFT JOIN sku_dim s
  ON s.dks_sku = v.dks_sku
LEFT JOIN sku_upc u
  ON u.dks_sku = v.dks_sku
LEFT JOIN price_sr p
  ON p.dks_sku = v.dks_sku
LEFT JOIN cost_sr c
  ON c.dks_sku = v.dks_sku
LEFT JOIN vdc_sr vd
  ON vd.dks_sku = v.dks_sku
    """,
    mysql_table="ecom_stg.stg_content_worklist_sku_vdc",
    truncate_first=True)

egress.load_custom_query_to_mysql_btch(
    sql_query=f"""
  WITH
/* =========================================================
   I – Inventory source (from STG_DDW_INV_PO_METRICS)
   ========================================================= */
inv_src AS (
  SELECT
    pd.dks_sku_code AS dks_sku,
    inv.product_id,
    sd.style_id,

    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(h.division_number, '-', h.division_description)
    ) AS division,
    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(h.division_number, '.', h.department_number, '-', h.department_description)
    ) AS department,
    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(h.division_number, '.', h.department_number, '.', h.sub_department_number, '-', h.sub_department_description)
    ) AS sub_department,
    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(h.division_number, '.', h.department_number, '.', h.sub_department_number, '.', h.class_number, '-', h.class_description)
    ) AS class,
    {ecomp_catalog}.ecom.custom_concat_ifnull(
      ARRAY(h.division_number, '.', h.department_number, '.', h.sub_department_number, '.', h.class_number, '.', h.sub_class_number, '-', h.sub_class_description)
    ) AS sub_class,

    sd.primary_vendor_name AS vendor,
    sd.style_desc          AS style_name,
    pd.dks_sku_desc        AS product_description,
    sd.vendor_style_number AS style_number,
    pd.vendor_product_number,
    cc.color_code_desc     AS color,
    h.product_hierarchy_key AS product_hierarchy_id,

    'I' AS data_source,

    CAST(DATE_FORMAT(CURRENT_DATE() - INTERVAL 1 DAY, 'yyyyMMdd') AS INT) AS inventory_date,

    CASE WHEN dsg_oh_qty < 0 THEN 0 ELSE dsg_oh_qty END               AS dsg_bam_on_hand,
    CASE WHEN dsg_oo_qty < 0 THEN 0 ELSE dsg_oo_qty END               AS dsg_bam_on_order,
    CASE WHEN dsg_intransit_qty < 0 THEN 0 ELSE dsg_intransit_qty END AS dsg_bam_in_transit,

    CASE WHEN ecom_dc_qty < 0 THEN 0 ELSE ecom_dc_qty END             AS dsg_eco_on_hand,
    CASE WHEN ecom_oo < 0 THEN 0 ELSE ecom_oo END                     AS dsg_eco_on_order,

    CASE WHEN gg_oh_qty < 0 THEN 0 ELSE gg_oh_qty END                 AS gg_bam_on_hand,
    CASE WHEN gg_oo_qty < 0 THEN 0 ELSE gg_oo_qty END                 AS gg_bam_on_order,
    CASE WHEN gg_intransit_qty < 0 THEN 0 ELSE gg_intransit_qty END   AS gg_bam_in_transit,

    CASE WHEN fs_oh_qty < 0 THEN 0 ELSE fs_oh_qty END                 AS fs_bam_on_hand,
    CASE WHEN fs_oo_qty < 0 THEN 0 ELSE fs_oo_qty END                 AS fs_bam_on_order,
    CASE WHEN fs_intransit_qty < 0 THEN 0 ELSE fs_intransit_qty END   AS fs_bam_in_transit,

    CASE WHEN pl_oh_qty < 0 THEN 0 ELSE pl_oh_qty END                 AS pl_bam_on_hand,
    CASE WHEN pl_oo_qty < 0 THEN 0 ELSE pl_oo_qty END                 AS pl_bam_on_order,
    CASE WHEN pl_intransit_qty < 0 THEN 0 ELSE pl_intransit_qty END   AS pl_bam_in_transit,

    CASE
      WHEN COALESCE(dc_qty, 0) + COALESCE(backstock_qty, 0) < 0 THEN 0
      ELSE COALESCE(dc_qty, 0) + COALESCE(backstock_qty, 0)
    END AS dc_on_hand,
    CASE WHEN dc_oo_qty < 0 THEN 0 ELSE dc_oo_qty END                 AS dc_on_order,
    CASE WHEN store_onhand_qty < 0 THEN 0 ELSE store_onhand_qty END   AS store_onhand_qty

  FROM {ecomp_catalog}.ecom.STG_DDW_INV_PO_METRICS inv
  INNER JOIN {ecomp_catalog}.ecom_dim.dks_sku pd
    ON pd.dks_sku_key = inv.product_id
  INNER JOIN {ecomp_catalog}.ecom_dim.style sd
    ON pd.style_key = sd.style_key
   AND pd.product_number <> sd.master_sku
  INNER JOIN {ecomp_catalog}.ecom_dim.product_hierarchy h
    ON sd.product_hierarchy_key = h.product_hierarchy_key
  JOIN {ecomp_catalog}.ecom_dim.color_code cc
    ON pd.dks_sku_color_code_key = cc.color_code_key
  WHERE
       dsg_oh_qty > 0 OR dsg_oo_qty > 0 OR dsg_intransit_qty > 0
    OR ecom_dc_qty > 0 OR ecom_oo > 0
    OR gg_oh_qty > 0 OR gg_oo_qty > 0 OR gg_intransit_qty > 0
    OR fs_oh_qty > 0 OR fs_oo_qty > 0 OR fs_intransit_qty > 0
    OR pl_oh_qty > 0 OR pl_oo_qty > 0 OR pl_intransit_qty > 0
),

/* =========================================================
   Common enrichments
   ========================================================= */
sku_upc AS (
  SELECT
    ds.dks_sku_code AS dks_sku,
    CASE
      WHEN MAX(u.upc_code) < 1000000000000
        THEN LPAD(CAST(MAX(u.upc_code) AS STRING), 12, '0')
      ELSE TRIM(CAST(MAX(u.upc_code) AS STRING))
    END AS upc
  FROM {ecomp_catalog}.ecom_dim.dks_sku ds
  JOIN {ecomp_catalog}.ecom_dim.upc u
    ON u.dks_sku_key = ds.dks_sku_key
   AND u.primary_upc_flag = '1'
   AND u.record_status = 'A'
  WHERE ds.record_status = 'A'
  GROUP BY ds.dks_sku_code
),

price_sr AS (
  SELECT
    pd.product_number AS dks_sku,
    COALESCE(sd.dsg_bm_retail, sd.original_retail)    AS dsg_price,
    COALESCE(sd.dsg_ecomm_retail, sd.original_retail) AS eco_price,
    COALESCE(sd.gg_bm_retail, sd.original_retail)     AS gg_bam_price,
    COALESCE(sd.gg_ecomm_retail, sd.original_retail)  AS gg_eco_price,
    COALESCE(sd.fas_bm_retail, sd.original_retail)    AS fs_bam_price,
    COALESCE(sd.fas_ecomm_retail, sd.original_retail) AS fs_eco_price
  FROM {ecomp_catalog}.ecom_dim.dks_sku pd
  JOIN {ecomp_catalog}.ecom_dim.style sd
    ON pd.style_key = sd.style_key
  WHERE pd.record_status = 'A'
    AND sd.record_status = 'A'
),

cost_sr AS (
  SELECT
    pd.product_number AS dks_sku,
    pd.route_po_flag  AS route_po,
    sd.average_cost   AS avg_unit_cost,
    CASE
      WHEN sd.average_cost > 0 THEN sd.average_cost
      ELSE sd.current_cost
    END AS cost_basis
  FROM {ecomp_catalog}.ecom_dim.dks_sku pd
  JOIN {ecomp_catalog}.ecom_dim.style sd
    ON pd.style_key = sd.style_key
  WHERE pd.record_status = 'A'
    AND sd.record_status = 'A'
),

vdc_sr AS (
  SELECT
    dks_sku_code AS dks_sku,
    CASE
      WHEN pim_vdc_eligible_flg = 'Y' THEN 'YES'
      WHEN pim_vdc_eligible_flg = 'N' THEN 'NO'
      ELSE NULL
    END AS dsg_vdc
  FROM {ecomp_catalog}.ecom_dim.dks_sku
  WHERE record_status = 'A'
)

/* =========================================================
   FINAL SELECT
   ========================================================= */
SELECT
  CAST(inv_src.dks_sku AS BIGINT) AS DKS_SKU,

  CAST(NULL AS BIGINT) AS DSG_MIN_ERD,
  CAST(NULL AS BIGINT) AS DSG_MAX_ERD,
  CAST(NULL AS BIGINT) AS DSG_COUNT_ERD,
  CAST(NULL AS BIGINT) AS ECO_MIN_ERD,
  CAST(NULL AS BIGINT) AS ECO_MAX_ERD,
  CAST(NULL AS BIGINT) AS ECO_COUNT_ERD,

  CAST(inv_src.data_source AS STRING) AS DATA_SOURCE,
  CAST(inv_src.division AS STRING) AS DIVISION,
  CAST(inv_src.department AS STRING) AS DEPARTMENT,
  CAST(inv_src.sub_department AS STRING) AS SUB_DEPARTMENT,
  CAST(inv_src.class AS STRING) AS CLASS,
  CAST(inv_src.sub_class AS STRING) AS SUB_CLASS,

  CAST(NULL AS STRING) AS BRAND,
  CAST(inv_src.vendor AS STRING) AS VENDOR,
  CAST(inv_src.style_name AS STRING) AS STYLE_NAME,
  CAST(inv_src.product_description AS STRING) AS PRODUCT_DESCRIPTION,

  CAST(NULL AS BIGINT) AS GSI_SKU,
  CAST(sku_upc.upc AS STRING) AS UPC,
  CAST(inv_src.vendor_product_number AS STRING) AS VENDOR_PRODUCT_NUMBER,

  CAST(NULL AS STRING) AS PID_STATUS,
  CAST(NULL AS STRING) AS SKU_STATUS,
  CAST(inv_src.color AS STRING) AS COLOR,

  CAST(NULL AS STRING) AS GLOBAL_CAT_ID,
  CAST(NULL AS STRING) AS GLOBAL_CAT_NAME,
  CAST('NO' AS STRING) AS IS_MATCHED,

  CAST(inv_src.style_number AS STRING) AS STYLE_NUMBER,
  CAST(inv_src.product_id AS BIGINT) AS PID,

  CAST(price_sr.dsg_price AS DECIMAL(13,2)) AS DSG_PRICE,
  CAST(price_sr.eco_price AS DECIMAL(13,2)) AS ECO_PRICE,

  CAST(NULL AS STRING) AS E3_STYLE,
  CAST(NULL AS STRING) AS TW_SFS_STATUS,
  CAST(NULL AS STRING) AS TW_SFS_REASON,
  CAST(NULL AS STRING) AS NW_SFS_STATUS,
  CAST(NULL AS STRING) AS NW_SFS_REASON,

  CAST(inv_src.dsg_bam_on_hand AS BIGINT) AS DSG_ON_HAND,
  CAST(inv_src.dsg_bam_on_order AS BIGINT) AS DSG_ON_ORDER,
  CAST(inv_src.dsg_eco_on_hand AS BIGINT) AS ECO_ON_HAND,
  CAST(inv_src.dsg_eco_on_order AS BIGINT) AS ECO_ON_ORDER,

  CAST(NULL AS BIGINT) AS DSG_MIN_PO,
  CAST(NULL AS BIGINT) AS ECO_MIN_PO,

  CAST(inv_src.inventory_date AS BIGINT) AS INVENTORY_DATE,

  CAST(inv_src.product_id AS BIGINT) AS PRODUCT_ID,
  CAST(inv_src.style_id AS BIGINT) AS STYLE_ID,

  CAST(NULL AS STRING) AS IMAGE_SOURCE,
  CAST('Missing' AS STRING) AS IMAGE_STATUS,

  CURRENT_TIMESTAMP() AS DATE_ADDED,
  CURRENT_USER()      AS ADDED_BY,
  CURRENT_TIMESTAMP() AS DATE_LAST_MODIFIED,
  CURRENT_USER()      AS MODIFIED_BY,

  CAST(NULL AS STRING) AS MODIFY_REASON,
  CAST('A' AS STRING)  AS RECORD_STATUS,
  CAST(NULL AS STRING) AS PMMS_COLOR_CODE,
  CAST('U' AS STRING)  AS HAS_IMAGE,

  CAST(COALESCE(vdc_sr.dsg_vdc, 'NE') AS STRING) AS VDC,
  CAST('NE' AS STRING) AS SFS,
  CAST('NE' AS STRING) AS BOPUIS,
  CAST('NE' AS STRING) AS ISA,

  CAST(cost_sr.route_po AS STRING) AS ROUTE_PO,
  CAST(NULL AS TIMESTAMP) AS SETUP_DATE,

  /* inventory-driven DSG style units */
  CAST(
      COALESCE(inv_src.dsg_bam_on_order, 0)
    + CASE
        WHEN COALESCE(inv_src.dsg_bam_on_hand, 0) + COALESCE(inv_src.dsg_bam_in_transit, 0) > 10
        THEN COALESCE(inv_src.dsg_bam_on_hand, 0) + COALESCE(inv_src.dsg_bam_in_transit, 0)
        ELSE 0
      END
    + COALESCE(inv_src.dsg_eco_on_order, 0)
    + COALESCE(inv_src.dsg_eco_on_hand, 0)
    + COALESCE(inv_src.dc_on_hand, 0)
    + COALESCE(inv_src.dc_on_order, 0)
  AS BIGINT) AS STYLE_UNITS,

  CAST(cost_sr.avg_unit_cost AS DECIMAL(15,4)) AS AVG_UNIT_COST,
  CAST(inv_src.product_hierarchy_id AS BIGINT) AS PRODUCT_HIERARCHY_ID,

  CAST('N' AS STRING) AS VDC_TYPE,
  CAST(NULL AS STRING) AS SFS_CALC,
  CAST(cost_sr.cost_basis AS DECIMAL(13,2)) AS COST_BASIS,

  CAST(NULL AS TIMESTAMP) AS FIRST_RECEIPT,
  CAST(NULL AS TIMESTAMP) AS LAST_RECEIPT,

  CAST(inv_src.gg_bam_on_hand AS BIGINT) AS GG_BAM_ON_HAND,
  CAST(inv_src.gg_bam_on_order AS BIGINT) AS GG_BAM_ON_ORDER,
  CAST(NULL AS BIGINT) AS GG_BAM_MIN_ERD,

  CAST(NULL AS BIGINT) AS GG_ECO_ON_HAND,
  CAST(NULL AS BIGINT) AS GG_ECO_ON_ORDER,
  CAST(NULL AS BIGINT) AS GG_ECO_MIN_ERD,

  CAST(price_sr.gg_eco_price AS DECIMAL(13,2)) AS GG_ECO_PRICE,
  CAST(price_sr.gg_bam_price AS DECIMAL(13,2)) AS GG_BAM_PRICE,

  CAST(
      COALESCE(inv_src.gg_bam_on_order, 0)
    + COALESCE(inv_src.gg_bam_on_hand, 0)
    + COALESCE(inv_src.gg_bam_in_transit, 0)
  AS BIGINT) AS GG_STYLE_UNITS,

  CAST(NULL AS STRING) AS ECODE,
  CAST(NULL AS BIGINT) AS DSG_VDC_ON_HAND,
  CAST(NULL AS STRING) AS SIZE_DESCRIPTION,
  CAST(NULL AS BIGINT) AS BUYER_ID,
  CAST(NULL AS BIGINT) AS VDC_ASSOCIATE_ID,
  CAST(NULL AS BIGINT) AS GG_VDC_ON_HAND,

  CAST(inv_src.fs_bam_on_hand AS BIGINT) AS FS_BAM_ON_HAND,
  CAST(inv_src.fs_bam_on_order AS BIGINT) AS FS_BAM_ON_ORDER,
  CAST(price_sr.fs_bam_price AS DECIMAL(13,2)) AS FS_BAM_PRICE,
  CAST(price_sr.fs_eco_price AS DECIMAL(13,2)) AS FS_ECO_PRICE,

  CAST(
      COALESCE(inv_src.fs_bam_on_order, 0)
    + COALESCE(inv_src.fs_bam_on_hand, 0)
    + COALESCE(inv_src.fs_bam_in_transit, 0)
  AS BIGINT) AS FS_STYLE_UNITS,

  CAST(NULL AS STRING) AS GG_SFS,
  CAST(NULL AS STRING) AS GG_VDC,
  CAST(NULL AS STRING) AS FS_SFS,
  CAST(NULL AS STRING) AS FS_VDC,

  CAST(NULL AS BIGINT) AS FS_BAM_MIN_ERD,
  CAST(NULL AS BIGINT) AS FS_ECO_MIN_ERD,
  CAST(NULL AS BIGINT) AS FS_ECO_ON_ORDER,
  CAST(NULL AS BIGINT) AS FS_ECO_ON_HAND,
  CAST(NULL AS BIGINT) AS FS_VDC_ON_HAND,

  CAST(NULL AS BIGINT) AS VENDOR_NUMBER,

  CAST(inv_src.dsg_bam_in_transit AS BIGINT) AS DSG_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS DSG_BAM_SHIP_TRANSFER,
  CAST(inv_src.gg_bam_in_transit AS BIGINT) AS GG_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS GG_BAM_SHIP_TRANSFER,
  CAST(inv_src.fs_bam_in_transit AS BIGINT) AS FS_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS FS_BAM_SHIP_TRANSFER,

  CAST(NULL AS BIGINT) AS DSG_ECO_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS DSG_ECO_SHIP_TRANSFER,
  CAST(NULL AS BIGINT) AS GG_ECO_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS GG_ECO_SHIP_TRANSFER,

  CAST(NULL AS STRING) AS DSG_SFS,
  CAST(NULL AS STRING) AS DSG_SFS_PREVIOUS,
  CAST(NULL AS STRING) AS GG_SFS_PREVIOUS,
  CAST(NULL AS STRING) AS FS_SFS_PREVIOUS,

  CAST(NULL AS STRING) AS DSG_EDC,
  CAST(NULL AS STRING) AS GG_EDC,
  CAST(NULL AS STRING) AS FS_EDC,

  CAST(NULL AS BIGINT) AS GG_OCE_UNITS,
  CAST(NULL AS BIGINT) AS LAB_ON_HAND,
  CAST(NULL AS BIGINT) AS LAB_ON_ORDER,

  CAST(inv_src.dc_on_hand AS BIGINT) AS DC_ON_HAND,
  CAST(inv_src.dc_on_order AS BIGINT) AS DC_ON_ORDER,

  CAST(NULL AS BIGINT) AS DC_051_OO,
  CAST(NULL AS BIGINT) AS DC_351_OO,
  CAST(NULL AS BIGINT) AS DC_651_OO,
  CAST(NULL AS BIGINT) AS DC_851_OO,

  CAST(NULL AS BIGINT) AS DC_051_ERD,
  CAST(NULL AS BIGINT) AS DC_351_ERD,
  CAST(NULL AS BIGINT) AS DC_651_ERD,
  CAST(NULL AS BIGINT) AS DC_851_ERD,

  CAST(NULL AS STRING) AS DSG_BAM_PROGRAM,
  CAST(NULL AS STRING) AS GG_BAM_PROGRAM,
  CAST(NULL AS STRING) AS FS_BAM_PROGRAM,

  CAST(COALESCE(vdc_sr.dsg_vdc, 'NE') AS STRING) AS DSG_VDC,

  CAST(inv_src.store_onhand_qty AS BIGINT) AS WEB_OH_QTY,

  CAST(NULL AS BIGINT) AS DC_1051_OO,
  CAST(NULL AS BIGINT) AS DC_1051_ERD,
  CAST(NULL AS BIGINT) AS DC_951_OO,
  CAST(NULL AS BIGINT) AS DC_951_ERD,

  CAST(NULL AS BIGINT) AS RADIAL_DC_OH_QTY,
  CAST(NULL AS BIGINT) AS STORE_42_OH_QTY,

  CAST(inv_src.pl_bam_on_hand AS BIGINT) AS PL_BAM_ON_HAND,
  CAST(inv_src.pl_bam_on_order AS BIGINT) AS PL_BAM_ON_ORDER,
  CAST(inv_src.pl_bam_in_transit AS BIGINT) AS PL_BAM_IN_TRANSIT,

  CAST(
      COALESCE(inv_src.pl_bam_on_order, 0)
    + COALESCE(inv_src.pl_bam_on_hand, 0)
    + COALESCE(inv_src.pl_bam_in_transit, 0)
  AS BIGINT) AS PL_STYLE_UNITS

FROM inv_src
LEFT JOIN sku_upc   ON sku_upc.dks_sku = inv_src.dks_sku
LEFT JOIN price_sr  ON price_sr.dks_sku = inv_src.dks_sku
LEFT JOIN cost_sr   ON cost_sr.dks_sku = inv_src.dks_sku
LEFT JOIN vdc_sr    ON vdc_sr.dks_sku = inv_src.dks_sku
    """,
    mysql_table="ecom_stg.stg_content_worklist_sku_inven",
    truncate_first=True
)

egress.load_custom_query_to_mysql_btch(
    sql_query=f"""
  WITH
/* =========================================================
   P – Purchase Orders rollup
   ========================================================= */
por AS (
  SELECT
    CAST(product_number AS BIGINT) AS dks_sku,

    MIN(product_id) AS product_id,
    MIN(style_id)   AS style_id,

    MIN(CASE WHEN po_loc = 'DSG' THEN expected_receipt_date END) AS dsg_min_erd,
    MAX(CASE WHEN po_loc = 'DSG' THEN expected_receipt_date END) AS dsg_max_erd,
    COUNT(CASE WHEN po_loc = 'DSG' THEN expected_receipt_date END) AS dsg_count_erd,
    MIN(CASE WHEN po_loc = 'DSG' THEN po_number END) AS dsg_min_po,

    MIN(CASE WHEN po_loc = 'ECO' THEN expected_receipt_date END) AS eco_min_erd,
    MAX(CASE WHEN po_loc = 'ECO' THEN expected_receipt_date END) AS eco_max_erd,
    COUNT(CASE WHEN po_loc = 'ECO' THEN expected_receipt_date END) AS eco_count_erd,
    MIN(CASE WHEN po_loc = 'ECO' THEN po_number END) AS eco_min_po,

    /* PO-based style units */
    SUM(CASE WHEN po_loc = 'DSG' THEN COALESCE(order_qty, 0) ELSE 0 END) AS dsg_style_units,
    SUM(CASE WHEN po_loc = 'ECO' THEN COALESCE(order_qty, 0) ELSE 0 END) AS eco_style_units,

    CAST('P' AS STRING) AS data_source,

    MIN(division)       AS division,
    MIN(department)     AS department,
    MIN(sub_department) AS sub_department,
    MIN(class)          AS class,
    MIN(sub_class)      AS sub_class,

    MIN(vendor)                AS vendor,
    MIN(style_name)            AS style_name,
    MIN(product_description)   AS product_description,
    MIN(color)                 AS color,
    MIN(style_number)          AS style_number,
    MIN(vendor_product_number) AS vendor_product_number,
    MIN(product_hierarchy_id)  AS product_hierarchy_id

  FROM (
    SELECT
      CAST(pd.product_number AS BIGINT) AS product_number,
      CAST(NVL(ps.dtl_product_id, ps.product_id) AS BIGINT) AS product_id,
      CAST(sd.style_key AS BIGINT) AS style_id,

      CASE
        WHEN NVL(ps.ecom_transfer_qty, 0) > 0 THEN 'ECO'
        ELSE 'DSG'
      END AS po_loc,

      CAST(ps.expected_receipt_date AS BIGINT) AS expected_receipt_date,
      CAST(ps.po_number AS BIGINT)             AS po_number,

      /* quantity used for PO-driven style-units */
      CAST(
        CASE
          WHEN NVL(ps.ecom_transfer_qty, 0) > 0 THEN NVL(ps.ecom_transfer_qty, 0)
          ELSE NVL(ps.quantity_ordered, 0)
        END
        AS BIGINT
      ) AS order_qty,

      {ecomp_catalog}.ecom.custom_concat_ifnull(
        ARRAY(h.division_number, '-', h.division_description)
      ) AS division,

      {ecomp_catalog}.ecom.custom_concat_ifnull(
        ARRAY(h.division_number,'.',h.department_number,'-',h.department_description)
      ) AS department,

      {ecomp_catalog}.ecom.custom_concat_ifnull(
        ARRAY(h.division_number,'.',h.department_number,'.',
              h.sub_department_number,'-',h.sub_department_description)
      ) AS sub_department,

      {ecomp_catalog}.ecom.custom_concat_ifnull(
        ARRAY(h.division_number,'.',h.department_number,'.',
              h.sub_department_number,'.',h.class_number,'-',h.class_description)
      ) AS class,

      {ecomp_catalog}.ecom.custom_concat_ifnull(
        ARRAY(h.division_number,'.',h.department_number,'.',
              h.sub_department_number,'.',h.class_number,'.',
              h.sub_class_number,'-',h.sub_class_description)
      ) AS sub_class,

      sd.primary_vendor_name AS vendor,
      sd.style_desc          AS style_name,
      pd.dks_sku_desc        AS product_description,
      sd.vendor_style_number AS style_number,
      pd.vendor_product_number,
      cc.color_code_desc      AS color,
      h.product_hierarchy_key AS product_hierarchy_id

    FROM {ecomp_catalog}.ecom.STG_DDW_Purchase_Orders ps
    JOIN {ecomp_catalog}.ecom_dim.dks_sku pd
      ON pd.dks_sku_key = NVL(ps.dtl_product_id, ps.product_id)
    JOIN {ecomp_catalog}.ecom_dim.style sd
      ON pd.style_key = sd.style_key
     AND pd.product_number <> sd.master_sku
    JOIN {ecomp_catalog}.ecom_dim.product_hierarchy h
      ON sd.product_hierarchy_key = h.product_hierarchy_key
    JOIN {ecomp_catalog}.ecom_dim.color_code cc
      ON pd.dks_sku_color_code_key = cc.color_code_key

    WHERE ps.po_status_code IN ('3','5')
      AND pd.status_code = 'A'
      AND pd.set_code = '0'
  ) x
  GROUP BY product_number
),

/* =========================================================
   UPC lookup
   ========================================================= */
sku_upc AS (
  SELECT
    ds.dks_sku_code AS dks_sku,
    CASE
      WHEN MAX(u.upc_code) < 1000000000000
        THEN LPAD(CAST(MAX(u.upc_code) AS STRING), 12, '0')
      ELSE TRIM(CAST(MAX(u.upc_code) AS STRING))
    END AS upc
  FROM {ecomp_catalog}.ecom_dim.dks_sku ds
  JOIN {ecomp_catalog}.ecom_dim.upc u
    ON u.dks_sku_key = ds.dks_sku_key
   AND u.primary_upc_flag = '1'
   AND u.record_status = 'A'
  WHERE ds.record_status = 'A'
  GROUP BY ds.dks_sku_code
),

/* =========================================================
   Price lookup
   ========================================================= */
price_sr AS (
  SELECT
    pd.product_number AS dks_sku,
    COALESCE(sd.dsg_bm_retail, sd.original_retail)    AS dsg_price,
    COALESCE(sd.dsg_ecomm_retail, sd.original_retail) AS eco_price,
    COALESCE(sd.gg_bm_retail, sd.original_retail)     AS gg_bam_price,
    COALESCE(sd.gg_ecomm_retail, sd.original_retail)  AS gg_eco_price,
    COALESCE(sd.fas_bm_retail, sd.original_retail)    AS fs_bam_price,
    COALESCE(sd.fas_ecomm_retail, sd.original_retail) AS fs_eco_price
  FROM {ecomp_catalog}.ecom_dim.dks_sku pd
  JOIN {ecomp_catalog}.ecom_dim.style sd
    ON pd.style_key = sd.style_key
  WHERE pd.record_status='A'
    AND sd.record_status='A'
),

/* =========================================================
   Cost + Route PO
   ========================================================= */
cost_sr AS (
  SELECT
    pd.product_number AS dks_sku,
    pd.route_po_flag  AS route_po,
    sd.average_cost   AS avg_unit_cost,
    CASE
      WHEN sd.average_cost > 0 THEN sd.average_cost
      ELSE sd.current_cost
    END AS cost_basis
  FROM {ecomp_catalog}.ecom_dim.dks_sku pd
  JOIN {ecomp_catalog}.ecom_dim.style sd
    ON pd.style_key = sd.style_key
  WHERE pd.record_status='A'
    AND sd.record_status='A'
),

/* =========================================================
   VDC initial assignment
   ========================================================= */
vdc_sr AS (
  SELECT
    dks_sku_code AS dks_sku,
    CASE
      WHEN pim_vdc_eligible_flg = 'Y' THEN 'YES'
      WHEN pim_vdc_eligible_flg = 'N' THEN 'NO'
      ELSE NULL
    END AS dsg_vdc
  FROM {ecomp_catalog}.ecom_dim.dks_sku
  WHERE record_status='A'
)

/* =========================================================
   FINAL SELECT
   ========================================================= */
SELECT
  CAST(por.dks_sku AS BIGINT) AS DKS_SKU,

  CAST(por.dsg_min_erd   AS BIGINT) AS DSG_MIN_ERD,
  CAST(por.dsg_max_erd   AS BIGINT) AS DSG_MAX_ERD,
  CAST(por.dsg_count_erd AS BIGINT) AS DSG_COUNT_ERD,

  CAST(por.eco_min_erd   AS BIGINT) AS ECO_MIN_ERD,
  CAST(por.eco_max_erd   AS BIGINT) AS ECO_MAX_ERD,
  CAST(por.eco_count_erd AS BIGINT) AS ECO_COUNT_ERD,

  CAST(por.data_source AS STRING) AS DATA_SOURCE,

  CAST(por.division       AS STRING) AS DIVISION,
  CAST(por.department     AS STRING) AS DEPARTMENT,
  CAST(por.sub_department AS STRING) AS SUB_DEPARTMENT,
  CAST(por.class          AS STRING) AS CLASS,
  CAST(por.sub_class      AS STRING) AS SUB_CLASS,

  CAST(NULL AS STRING) AS BRAND,

  CAST(por.vendor AS STRING) AS VENDOR,
  CAST(por.style_name AS STRING) AS STYLE_NAME,
  CAST(por.product_description AS STRING) AS PRODUCT_DESCRIPTION,

  CAST(NULL AS BIGINT) AS GSI_SKU,
  CAST(sku_upc.upc AS STRING) AS UPC,
  CAST(por.vendor_product_number AS STRING) AS VENDOR_PRODUCT_NUMBER,

  CAST(NULL AS STRING) AS PID_STATUS,
  CAST(NULL AS STRING) AS SKU_STATUS,

  CAST(por.color AS STRING) AS COLOR,

  CAST(NULL AS STRING) AS GLOBAL_CAT_ID,
  CAST(NULL AS STRING) AS GLOBAL_CAT_NAME,
  CAST('NO' AS STRING) AS IS_MATCHED,

  CAST(por.style_number AS STRING) AS STYLE_NUMBER,
  CAST(por.product_id   AS BIGINT) AS PID,

  CAST(price_sr.dsg_price AS DECIMAL(13,2)) AS DSG_PRICE,
  CAST(price_sr.eco_price AS DECIMAL(13,2)) AS ECO_PRICE,

  CAST(NULL AS STRING) AS E3_STYLE,
  CAST(NULL AS STRING) AS TW_SFS_STATUS,
  CAST(NULL AS STRING) AS TW_SFS_REASON,
  CAST(NULL AS STRING) AS NW_SFS_STATUS,
  CAST(NULL AS STRING) AS NW_SFS_REASON,

  CAST(NULL AS BIGINT) AS DSG_ON_HAND,
  CAST(NULL AS BIGINT) AS DSG_ON_ORDER,
  CAST(NULL AS BIGINT) AS ECO_ON_HAND,
  CAST(NULL AS BIGINT) AS ECO_ON_ORDER,

  CAST(por.dsg_min_po AS BIGINT) AS DSG_MIN_PO,
  CAST(por.eco_min_po AS BIGINT) AS ECO_MIN_PO,

  CAST(NULL AS BIGINT) AS INVENTORY_DATE,

  CAST(por.product_id AS BIGINT) AS PRODUCT_ID,
  CAST(por.style_id   AS BIGINT) AS STYLE_ID,

  CAST(NULL AS STRING) AS IMAGE_SOURCE,
  CAST('Missing' AS STRING) AS IMAGE_STATUS,

  CURRENT_TIMESTAMP() AS DATE_ADDED,
  CURRENT_USER()      AS ADDED_BY,
  CURRENT_TIMESTAMP() AS DATE_LAST_MODIFIED,
  CURRENT_USER()      AS MODIFIED_BY,

  CAST(NULL AS STRING) AS MODIFY_REASON,
  CAST('A' AS STRING)  AS RECORD_STATUS,

  CAST(NULL AS STRING) AS PMMS_COLOR_CODE,
  CAST('U' AS STRING)  AS HAS_IMAGE,

  CAST(COALESCE(vdc_sr.dsg_vdc, 'NE') AS STRING) AS VDC,

  CAST('NE' AS STRING) AS SFS,
  CAST('NE' AS STRING) AS BOPUIS,
  CAST('NE' AS STRING) AS ISA,

  CAST(cost_sr.route_po AS STRING) AS ROUTE_PO,

  CAST(NULL AS TIMESTAMP) AS SETUP_DATE,

  /* P-source unit fix */
  CAST(COALESCE(por.dsg_style_units, 0) + COALESCE(por.eco_style_units, 0) AS BIGINT) AS STYLE_UNITS,

  CAST(cost_sr.avg_unit_cost AS DECIMAL(15,4)) AS AVG_UNIT_COST,
  CAST(por.product_hierarchy_id AS BIGINT) AS PRODUCT_HIERARCHY_ID,

  CAST('N' AS STRING) AS VDC_TYPE,
  CAST(NULL AS STRING) AS SFS_CALC,

  CAST(cost_sr.cost_basis AS DECIMAL(13,2)) AS COST_BASIS,

  CAST(NULL AS TIMESTAMP) AS FIRST_RECEIPT,
  CAST(NULL AS TIMESTAMP) AS LAST_RECEIPT,

  CAST(NULL AS BIGINT) AS GG_BAM_ON_HAND,
  CAST(NULL AS BIGINT) AS GG_BAM_ON_ORDER,
  CAST(NULL AS BIGINT) AS GG_BAM_MIN_ERD,
  CAST(NULL AS BIGINT) AS GG_ECO_ON_HAND,
  CAST(NULL AS BIGINT) AS GG_ECO_ON_ORDER,
  CAST(NULL AS BIGINT) AS GG_ECO_MIN_ERD,

  CAST(price_sr.gg_eco_price AS DECIMAL(13,2)) AS GG_ECO_PRICE,
  CAST(price_sr.gg_bam_price AS DECIMAL(13,2)) AS GG_BAM_PRICE,

  CAST(0 AS BIGINT) AS GG_STYLE_UNITS,

  CAST(NULL AS STRING) AS ECODE,
  CAST(NULL AS BIGINT) AS DSG_VDC_ON_HAND,
  CAST(NULL AS STRING) AS SIZE_DESCRIPTION,
  CAST(NULL AS BIGINT) AS BUYER_ID,
  CAST(NULL AS BIGINT) AS VDC_ASSOCIATE_ID,
  CAST(NULL AS BIGINT) AS GG_VDC_ON_HAND,

  CAST(NULL AS BIGINT) AS FS_BAM_ON_HAND,
  CAST(NULL AS BIGINT) AS FS_BAM_ON_ORDER,

  CAST(price_sr.fs_bam_price AS DECIMAL(13,2)) AS FS_BAM_PRICE,
  CAST(price_sr.fs_eco_price AS DECIMAL(13,2)) AS FS_ECO_PRICE,

  CAST(0 AS BIGINT) AS FS_STYLE_UNITS,

  CAST(NULL AS STRING) AS GG_SFS,
  CAST(NULL AS STRING) AS GG_VDC,
  CAST(NULL AS STRING) AS FS_SFS,
  CAST(NULL AS STRING) AS FS_VDC,

  CAST(NULL AS BIGINT) AS FS_BAM_MIN_ERD,
  CAST(NULL AS BIGINT) AS FS_ECO_MIN_ERD,
  CAST(NULL AS BIGINT) AS FS_ECO_ON_ORDER,
  CAST(NULL AS BIGINT) AS FS_ECO_ON_HAND,
  CAST(NULL AS BIGINT) AS FS_VDC_ON_HAND,

  CAST(NULL AS BIGINT) AS VENDOR_NUMBER,

  CAST(NULL AS BIGINT) AS DSG_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS DSG_BAM_SHIP_TRANSFER,
  CAST(NULL AS BIGINT) AS GG_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS GG_BAM_SHIP_TRANSFER,
  CAST(NULL AS BIGINT) AS FS_BAM_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS FS_BAM_SHIP_TRANSFER,

  CAST(NULL AS BIGINT) AS DSG_ECO_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS DSG_ECO_SHIP_TRANSFER,
  CAST(NULL AS BIGINT) AS GG_ECO_IN_TRANSIT,
  CAST(NULL AS BIGINT) AS GG_ECO_SHIP_TRANSFER,

  CAST(NULL AS STRING) AS DSG_SFS,
  CAST(NULL AS STRING) AS DSG_SFS_PREVIOUS,
  CAST(NULL AS STRING) AS GG_SFS_PREVIOUS,
  CAST(NULL AS STRING) AS FS_SFS_PREVIOUS,

  CAST(NULL AS STRING) AS DSG_EDC,
  CAST(NULL AS STRING) AS GG_EDC,
  CAST(NULL AS STRING) AS FS_EDC,

  CAST(NULL AS BIGINT) AS GG_OCE_UNITS,
  CAST(NULL AS BIGINT) AS LAB_ON_HAND,
  CAST(NULL AS BIGINT) AS LAB_ON_ORDER,

  CAST(NULL AS BIGINT) AS DC_ON_HAND,
  CAST(NULL AS BIGINT) AS DC_ON_ORDER,

  CAST(NULL AS BIGINT) AS DC_051_OO,
  CAST(NULL AS BIGINT) AS DC_351_OO,
  CAST(NULL AS BIGINT) AS DC_651_OO,
  CAST(NULL AS BIGINT) AS DC_851_OO,

  CAST(NULL AS BIGINT) AS DC_051_ERD,
  CAST(NULL AS BIGINT) AS DC_351_ERD,
  CAST(NULL AS BIGINT) AS DC_651_ERD,
  CAST(NULL AS BIGINT) AS DC_851_ERD,

  CAST(NULL AS STRING) AS DSG_BAM_PROGRAM,
  CAST(NULL AS STRING) AS GG_BAM_PROGRAM,
  CAST(NULL AS STRING) AS FS_BAM_PROGRAM,

  CAST(COALESCE(vdc_sr.dsg_vdc, 'NE') AS STRING) AS DSG_VDC,

  CAST(NULL AS BIGINT) AS WEB_OH_QTY,

  CAST(NULL AS BIGINT) AS DC_1051_OO,
  CAST(NULL AS BIGINT) AS DC_1051_ERD,
  CAST(NULL AS BIGINT) AS DC_951_OO,
  CAST(NULL AS BIGINT) AS DC_951_ERD,

  CAST(NULL AS BIGINT) AS RADIAL_DC_OH_QTY,
  CAST(NULL AS BIGINT) AS STORE_42_OH_QTY,

  CAST(NULL AS BIGINT) AS PL_BAM_ON_HAND,
  CAST(NULL AS BIGINT) AS PL_BAM_ON_ORDER,
  CAST(NULL AS BIGINT) AS PL_BAM_IN_TRANSIT,
  CAST(0 AS BIGINT) AS PL_STYLE_UNITS

FROM por
LEFT JOIN sku_upc  ON sku_upc.dks_sku = por.dks_sku
LEFT JOIN price_sr ON price_sr.dks_sku = por.dks_sku
LEFT JOIN cost_sr  ON cost_sr.dks_sku = por.dks_sku
LEFT JOIN vdc_sr   ON vdc_sr.dks_sku = por.dks_sku
 """,
    mysql_table="ecom_stg.stg_content_worklist_sku_pordr",
    truncate_first=True)