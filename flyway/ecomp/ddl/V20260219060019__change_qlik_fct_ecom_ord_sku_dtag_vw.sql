CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_ord_sku_dtag_vw
(
  ECOM_WEBSTORE_KEY,
  PRODUCT_ID,
  STYLE_ID,
  ECOM_WEBSTORE_PRODUCT_KEY,
  ECOM_WEB_SKU_KEY,
  ECOM_ORDER_SUBMIT_DATE_KEY,
  ECOM_ORDER_SKU_KEY,
  ECOM_ORDER_HEADER_KEY,
  ECOM_DISCOUNT_GROUP,
  ECOM_ODS_PROMOTION_KEY,
  OSDT_DATE_LAST_MODIFIED
) AS
SELECT
  a.CHAIN_KEY,
  DKS_SKU_KEY,
  STYLE_KEY,
  PRODUCT_KEY,
  WEB_SKU_KEY,
  txn_date_key AS ORDER_DATE_KEY,
  ORDER_SKU_KEY,
  ORDER_HEADER_KEY,
  b.discount_group,
  a.promotioN_key,
  MAX(a.date_last_modified) AS OSDT_DATE_LAST_MODIFIED
FROM
  prod_ecmde_db.ecom_dim.txn_order_sku_adjustment AS a
    JOIN prod_ecmde_db.ecom_Dim.promotion_header AS b
      ON a.promotion_key = b.promotion_key
WHERE
  txn_Date_key >= 20210131
  AND discount_group IN ('TAG', 'EARN')
GROUP BY
  a.CHAIN_KEY,
  a.ORDER_HEADER_KEY,
  a.ORDER_SKU_KEY,
  a.txn_date_key,
  a.STYLE_KEY,
  a.DKS_SKU_KEY,
  a.WEB_SKU_KEY,
  a.PRODUCT_KEY,
  a.promotioN_key,
  b.discount_group
