CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_ord_sku_discs_vw
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
  ECOM_DMD_FREIGHT_ADJUST_AMT,
  ECOM_ODS_PROMOTION_KEY,
  ECOM_WEB_ADJUST_NOTES,
  ECOM_ODS_ADJUST_TYPE_KEY,
  ECOM_ADJ_CODE,
  OSDAS_DATE_LAST_MODIFIED,
  ECOM_PROMO_CODE
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
  'SHIPPING' AS discount_group,
  SUM(freight_adjust_AMT) AS freight_adjust_AMT,
  CASE
    WHEN trans_type_key = 13 THEN manual_disc_key
    ELSE NVL(a.promotion_key, -1)
  END AS promotioN_key,
  CASE
    WHEN COUNT(DISTINCT adjustment_notes) > 1 THEN 'MULTIPLE'
    ELSE MAX(adjustment_notes)
  END AS adjustment_notes,
  NVL(md.manual_disc_key, -1) AS manual_disc_key,
  CASE
    WHEN COUNT(DISTINCT NVL(source_reason_cd, '99999')) > 1 THEN 'MULTI'
    ELSE MAX(source_reason_cd)
  END AS source_reason_cd,
  MAX(a.date_last_modified) AS date_last_modified,
  MAX(a.promo_code) AS promo_code
FROM
  prod_ecmde_db.ecom_dim.txn_order_sku_adjustment AS a
    LEFT JOIN prod_ecmde_db.ecom_Dim.promotion_header AS b
      ON a.promotion_key = b.promotion_key
    LEFT JOIN prod_ecmde_db.Ecom_dim.manual_discount_lkup AS md
      ON md.manual_disc_desc = UPPER(a.adjustment_notes)
WHERE
  txn_Date_key >= 20210131
  AND freight_adjust_amt IS NOT NULL
  AND (
    discount_group IN ('SHIPPING')
    OR trans_type_key IN (13)
  )
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
  a.promotion_id,
  a.trans_type_key,
  md.manual_disc_key
