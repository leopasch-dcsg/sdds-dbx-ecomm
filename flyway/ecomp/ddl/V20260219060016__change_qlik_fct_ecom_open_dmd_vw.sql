CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_open_dmd_vw
(
  ECOM_WEBSTORE_KEY,
  PRODUCT_ID,
  STYLE_ID,
  ECOM_WEBSTORE_PRODUCT_KEY,
  ECOM_WEB_SKU_KEY,
  ECOM_OCA_OPEN_DATE_KEY,
  ECOM_OCA_OPEN_DMD_UNITS,
  ECOM_OCA_OPEN_DMD_AMT,
  ECOM_OCA_OPEN_DMD_SHIP_AMT,
  ECOM_OCA_OPEN_DMD_COST,
  ECOM_ORDER_SKU_KEY,
  ECOM_ORDER_HEADER_KEY,
  ECOM_ORD_DISTR_KEY,
  OCA_DATE_LAST_MODIFIED,
  ECOM_ORD_OPEN_CODE
) AS
SELECT
  a.chain_key AS ecom_webstore_key,
  a.dks_Sku_key AS ddw_product_id,
  a.style_key,
  a.product_key,
  a.web_Sku_key,
  a.order_date_key,
  units,
  extended_amt,
  freight_amt,
  units * a.average_cost AS ecom_oca_open_dmd_cost,
  a.order_skU_key,
  a.order_header_key,
  a.order_fulfill_Key,
  NVL(a.date_last_modified, a.date_added) AS oca_date_last_modified,
  'OPEN' AS open_code
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_SKU_curr_ALLOC AS a
WHERE
  units > 0
  AND a.order_date_key >= 20210131
UNION ALL
SELECT
  a.webstore_key AS ecom_webstore_key,
  a.dks_Sku_key AS ddw_product_id,
  a.style_key,
  a.product_key,
  a.web_Sku_key,
  b.order_date_key,
  shipped_units,
  (b.orig_tot_extended_amt / NULLIF(orig_tot_units, 0)) * shipped_units,
  (orig_tot_freight_amt / NULLIF(orig_tot_units, 0)) * shipped_units,
  shipped_units * b.average_cost,
  a.order_skU_key,
  b.order_header_key,
  a.order_fulfill_Key,
  NVL(a.date_last_modified, a.date_added) AS oca_date_last_modified,
  'SHIPPED NOT POSTED'
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_SKU_shipment AS a
    JOIN prod_ecmde_db.ecom_Dim.order_sku AS b
      ON a.order_sku_key = b.order_sku_key
WHERE
  b.order_date_key >= 20210131
  AND b.order_SkU_status_key = 36
  AND a.fulfillment_date_key >= 20210131
  AND posted_date_key IS NULL
UNION ALL
SELECT
  a.chain_key AS ecom_webstore_key,
  a.dks_Sku_key AS ddw_product_id,
  a.style_key,
  a.product_key,
  a.web_Sku_key,
  a.order_date_key,
  orig_tot_units,
  orig_tot_extended_amt,
  orig_tot_freight_amt,
  orig_tot_units * a.average_cost,
  a.order_skU_key,
  a.order_header_key,
  MAX(order_fulfill_Key),
  NVL(a.date_last_modified, a.date_added) AS oca_date_last_modified,
  'SHIPPED NOT POSTED'
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_SKU AS a
    JOIN prod_ecmde_db.ecom_dim.order_fulfill AS b
      ON a.web_ord_num = b.web_ord_num
      AND a.CHAIN_KEY = b.chaiN_key
WHERE
  order_date_key >= 20210131
  AND order_SkU_status_key = 36
  AND b.channeL_type_key = 3
  AND B.FULFILLMENT_STATUS_CD = 'F'
  AND b.fulfillment_date_key >= 20210131
GROUP BY
  a.chain_key,
  a.dks_Sku_key,
  a.style_key,
  a.product_key,
  a.web_Sku_key,
  a.order_date_key,
  orig_tot_units,
  orig_tot_extended_amt,
  orig_tot_freight_amt,
  orig_tot_units * a.average_cost,
  a.order_skU_key,
  a.order_header_key,
  NVL(a.date_last_modified, a.date_added)
