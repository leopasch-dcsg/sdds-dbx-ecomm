CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_designate_dmd_vw
(
  ECOM_DESIGNATE_DMD_KEY,
  ECOM_WEBSTORE_KEY,
  PRODUCT_ID,
  STYLE_ID,
  ECOM_WEBSTORE_PRODUCT_KEY,
  ECOM_WEB_SKU_KEY,
  ECOM_OIA_DESIG_DATE_KEY,
  ECOM_OIA_DESIG_DMD_UNITS,
  ECOM_OIA_DESIG_DMD_AMT,
  ECOM_OIA_DESIG_DMD_SHIP_AMT,
  ECOM_OIA_DESIG_DMD_COST,
  ECOM_ORDER_SKU_KEY,
  ECOM_ORDER_HEADER_KEY,
  ECOM_ORD_DISTR_KEY,
  OIA_DATE_LAST_MODIFIED
) AS
SELECT
  a.init_alloc_key,
  a.chain_key AS ecom_webstore_key,
  a.dks_Sku_key AS ddw_product_id,
  a.style_key,
  a.product_key,
  a.web_Sku_key,
  a.order_date_key,
  units AS ecom_desig_dmd_units,
  extended_amt AS ecom_desig_dmd_amt,
  freight_amt AS ecom_desig_dmd_shipping_amt,
  units * a.average_cost AS ecom_desig_dmd_cost,
  a.order_skU_key,
  a.order_header_key,
  a.order_fulfill_Key,
  NVL(a.date_last_modified, a.date_added) AS OIA_DATE_LAST_MODIFIED
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_SKU_INIT_ALLOC AS a
WHERE
  a.order_date_key BETWEEN
    20210131
  AND
    CAST(DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP, 1), 'yyyyMMdd') AS DOUBLE)
  AND /*  a.order_date_key BETWEEN 20190203 and 20200201 */ units > 0
