CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_ord_sku_disc_vw
(
  ECOM_ORD_SKU_DMD_ADJUST_KEY,
  ECOM_WEBSTORE_KEY,
  PRODUCT_ID,
  STYLE_ID,
  ECOM_WEBSTORE_PRODUCT_KEY,
  ECOM_WEB_SKU_KEY,
  ECOM_ORDER_SUBMIT_DATE_KEY,
  ECOM_ORDER_SKU_KEY,
  ECOM_ORDER_HEADER_KEY,
  ECOM_DISCOUNT_GROUP,
  ECOM_DMD_ADJUST_AMT,
  ECOM_ODS_PROMOTION_KEY,
  ECOM_WEB_ADJUST_NOTES,
  ECOM_ODS_ADJUST_TYPE_KEY,
  ECOM_ODS_WCS_PROMO_TAG_KEY,
  ECOM_DMD_ADJ_PCT_OF_ORDER_SKU,
  ECOM_ORD_SKU_ADJ_PRIORITY_NUM,
  ECOM_ADJ_CODE,
  OSDA_DATE_LAST_MODIFIED,
  ECOM_PROMO_CODE
) AS
WITH BEGIN_DATE AS (
  SELECT distinct
    CAST(DATE_FORMAT(TO_DATE(prev_dd.date_id, 'yyyyMMdd') - 7, 'yyyyMMdd') AS DOUBLE) AS date_id
  FROM
    prod_ecmde_db.ecom_dim.DATE_DIM AS cur_dd,
    prod_ecmde_db.ecom_dim.DATE_DIM AS prev_dd
  WHERE
    cur_dd.date_id = (
      SELECT distinct
        prev_dd.date_id
      FROM
        prod_ecmde_db.ecom_dim.DATE_DIM AS cur_dd,
        prod_ecmde_db.ecom_dim.DATE_DIM AS prev_dd
      WHERE
        cur_dd.date_id = CAST(
          DATE_FORMAT(DATE_TRUNC('DAY', CURRENT_TIMESTAMP()) - INTERVAL 1 DAY, 'yyyyMMdd') AS DOUBLE
        )
        AND cur_dd.fiscal_day_in_week = prev_dd.fiscal_day_in_week
        AND prev_dd.fiscal_year = SUBSTR(cur_dd.fiscal_prior_year_comp_week, 1, 4)
        AND prev_dd.fiscal_week_in_year = SUBSTR(cur_dd.fiscal_prior_year_comp_week, 5, 2)
    )
    AND cur_dd.fiscal_day_in_week = prev_dd.fiscal_day_in_week
    AND prev_dd.fiscal_year = SUBSTR(cur_dd.fiscal_prior_year_comp_week, 1, 4)
    AND prev_dd.fiscal_week_in_year = SUBSTR(cur_dd.fiscal_prior_year_comp_week, 5, 2)
)
SELECT
  order_sku_dmd_adjustment_key as ECOM_ORD_SKU_DMD_ADJUST_KEY,
  chain_key AS ECOM_WEBSTORE_KEY,
  dks_Sku_key AS PRODUCT_ID,
  style_key AS STYLE_ID,
  product_key AS ECOM_WEBSTORE_PRODUCT_KEY,
  NVL(
    CAST(
      prod_ecmde_db.ecom.custom_concat_ifnull(
        ARRAY(CAST(chain_key AS INT), '0', ABS(dks_sku))
      ) AS DOUBLE
    ),
    -1
  ) AS ECOM_WEB_SKU_KEY,
  order_date_key AS ECOM_ORDER_SUBMIT_DATE_KEY,
  order_skU_key as ECOM_ORDER_SKU_KEY,
  order_header_key as ECOM_ORDER_HEADER_KEY,
  discount_group as ECOM_DISCOUNT_GROUP,
  extended_adjust_amt as ECOM_DMD_ADJUST_AMT,
  CASE promotion_key
    WHEN
      -999
    THEN
      CASE
        WHEN
          source_reason_cd = 'PCM'
          AND manual_disc_key BETWEEN 3 AND 8
        THEN
          manual_disc_key + 10
        ELSE manual_disc_key
      END
    ELSE promotion_key
  END as ECOM_ODS_PROMOTION_KEY,
  adjustment_notes as ECOM_WEB_ADJUST_NOTES,
  trans_type_key as ECOM_ODS_ADJUST_TYPE_KEY,
  tag_promotioN_key as ECOM_ODS_WCS_PROMO_TAG_KEY,
  tot_order_discount_pct as ECOM_DMD_ADJ_PCT_OF_ORDER_SKU,
  NVL(margin_decomp_priority_num, 1) as ECOM_ORD_SKU_ADJ_PRIORITY_NUM,
  source_reason_cd as ECOM_ADJ_CODE,
  date_last_modified as OSDA_DATE_LAST_MODIFIED,
  promo_code as ECOM_PROMO_CODE
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_SKU_DMD_ADJUSTMENT
WHERE
  order_date_key >= (
    SELECT
      date_id
    from
      BEGIN_DATE
  )
UNION ALL
SELECT
  txn_seq_number as ECOM_ORD_SKU_DMD_ADJUST_KEY,
  chain_key as ECOM_WEBSTORE_KEY,
  dks_SkU_key as PRODUCT_ID,
  style_key as STYLE_ID,
  product_key as ECOM_WEBSTORE_PRODUCT_KEY,
  NVL(
    CAST(
      prod_ecmde_db.ecom.custom_concat_ifnull(
        ARRAY(CAST(chain_key AS INT), '0', ABS(dks_sku))
      ) AS DOUBLE
    ),
    -1
  ) AS ECOM_WEB_SKU_KEY,
  txn_date_key as ECOM_ORDER_SUBMIT_DATE_KEY,
  a.order_SkU_key as ECOM_ORDER_SKU_KEY,
  order_header_key as ECOM_ORDER_HEADER_KEY,
  'WCS PRICE UPLOAD' as ECOM_DISCOUNT_GROUP,
  extended_adjust_amt as ECOM_DMD_ADJUST_AMT,
  50 AS ECOM_ODS_PROMOTION_KEY,
  prod_ecmde_db.ecom.custom_concat_ifnull(
    ARRAY('WCS_UPLOAD_', string(promotion_id))
  ) as ECOM_WEB_ADJUST_NOTES,
  16 AS ECOM_ODS_ADJUST_TYPE_KEY,
  NULL as ECOM_ODS_WCS_PROMO_TAG_KEY,
  NULL as ECOM_DMD_ADJ_PCT_OF_ORDER_SKU,
  CASE
    WHEN b.order_sku_Key > 0 THEN 5
    ELSE 1
  END as ECOM_ORD_SKU_ADJ_PRIORITY_NUM,
  NULL as ECOM_ADJ_CODE,
  date_last_modified as OSDA_DATE_LAST_MODIFIED,
  a.promo_code as ECOM_PROMO_CODE
FROM
  prod_ecmde_db.ecom_dim.txn_order_sku_adjustment AS a
    LEFT JOIN (
      SELECT DISTINCT
        order_sku_key
      FROM
        prod_ecmde_db.ECOM_DIM.ORDER_SKU_DMD_ADJUSTMENT
      WHERE
        order_date_key >= (
          SELECT
            date_id
          from
            BEGIN_DATE
        )
    ) AS b
      ON A.ORDER_SKU_KEY = b.order_SkU_key
WHERE
  txn_date_key >= (
    SELECT
      date_id
    from
      BEGIN_DATE
  )
  AND trans_type_key IN (14, 16)
  AND NVL(extended_adjust_amt, 0) = 0
  AND promotion_key = -1
