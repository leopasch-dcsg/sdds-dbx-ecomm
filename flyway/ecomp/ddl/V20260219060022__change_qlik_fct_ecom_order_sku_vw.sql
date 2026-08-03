CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_order_sku_vw
(
  ECOM_ORDER_SKU_KEY,
  ECOM_WEBSTORE_KEY,
  PRODUCT_ID,
  STYLE_ID,
  ECOM_WEBSTORE_PRODUCT_KEY,
  ECOM_WEB_SKU_KEY,
  ECOM_OS_ORDER_SUBMIT_DATE_KEY,
  ECOM_ORDER_HEADER_KEY,
  ECOM_ORDER_SKU_LINE_CNT,
  ECOM_ORDER_SKU_ORIG_UNITS,
  ECOM_ORDER_SKU_ORIG_REV_AMT,
  ECOM_ORDER_SKU_ORIG_SHIP_AMT,
  ECOM_ORDER_SKU_ORIG_COST,
  ECOM_ORDER_SKU_ORIG_DISC_AMT,
  ECOM_ORD_SKU_ORIG_SHIPDISC_AMT,
  OS_DATE_LAST_MODIFIED
) AS
SELECT
  order_skU_key,
  chain_key AS ecom_webstore_key,
  dks_Sku_key AS ddw_product_id,
  style_key,
  product_key,
  NVL(
    CAST(
      prod_ecmde_db.ecom.custom_concat_ifnull(
        ARRAY(CAST(chain_key AS INT), '0', ABS(dks_sku))
      ) AS DOUBLE
    ),
    -1
  ) AS WEB_SKU_KEY,
  order_date_key AS ecom_order_submit_date_key,
  order_header_key,
  order_line_cnt,
  orig_tot_units,
  orig_tot_extended_amt,
  orig_tot_freight_amt,
  average_cost * orig_tot_units AS ecom_order_sku_orig_cost,
  orig_tot_ext_disc_amt,
  orig_tot_freight_disc_amt,
  date_last_modified
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_SKU
WHERE
  order_date_key BETWEEN
    20200202
  AND
    CAST(DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP, 1), 'yyyyMMdd') AS DOUBLE)
