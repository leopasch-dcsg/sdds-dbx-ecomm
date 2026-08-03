CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_return_vw
(
    ECOM_WEBSTORE_KEY,
    PRODUCT_ID,
    STYLE_ID,
    ECOM_WEBSTORE_PRODUCT_KEY,
    ECOM_WEB_SKU_KEY,
    ECOM_TX8_RETURN_DATE_KEY,
    ECOM_TX8_RETURN_CHANNEL,
    ECOM_TX8_RETURN_UNITS,
    ECOM_TX8_RETURN_AMT,
    ECOM_TX8_RETURN_COST,
    ECOM_TX8_RETURN_STORE_KEY,
    ECOM_ORDER_SKU_KEY,
    ECOM_ORDER_HEADER_KEY,
    ECOM_TX8_ORIG_ORD_DISTR_KEY,
    ECOM_TX8_RETURN_REASON_KEY,
    ECOM_TX8_RETURN_SOURCE_CD,
    TXN8_DATE_LAST_MODIFIED,
    ECOM_RETURN_HEADER_KEY,
    ECOM_RETURN_SOURCE,
    ECOM_ORIG_TRACKING_NUM,
    ECOM_ORIG_ORDER_DELIVERY_KEY
)
AS
SELECT
  CASE a.chain_key
    WHEN 1 THEN 6
    ELSE a.chain_key
  END AS ecom_webstore_key,
  a.dks_Sku_key AS ddw_product_id,
  a.style_key,
  a.product_key,
  NVL(
    CAST(
      prod_ecmde_db.ecom.custom_concat_ifnull(
        ARRAY(CAST(a.chain_key AS INT), '0', ABS(a.dks_sku))
      ) AS DOUBLE
    ),
    -1
  ) AS WEB_SKU_KEY,
  txn_date_key,
  CASE
    WHEN
      trans_type_key = 8
      AND a.data_source_key = 4
    THEN
      'In-Store'
    WHEN
      trans_type_key = 8
      AND (
        oh_type = 'B'
        OR chain_number IN (4, 5, 9)
      )
    THEN
      'Web DC'
    ELSE 'Curbside'
  END AS ecom_return_channel,
  CASE
    WHEN
      trans_type_key = 8
      AND a.data_source_key = 4
    THEN
      units * -1
    WHEN trans_type_key = 8 THEN units
    ELSE 0
  END AS ecom_return_units,
  CASE
    WHEN
      trans_type_key = 8
      AND a.data_source_key = 4
    THEN
      extended_amt * -1
    WHEN trans_type_key = 8 THEN extended_amt
    ELSE 0
  END AS ecom_return_amt,
  CASE
    WHEN
      trans_type_key = 8
      AND a.data_source_key = 4
    THEN
      units * -1 * average_cost
    WHEN trans_type_key = 8 THEN units * average_cost
    ELSE 0
  END AS ecom_return_cst,
  d.store_key,
  a.order_skU_key,
  a.order_header_key,
  a.order_fulfill_key,
  NVL(A.REASON_KEY, -1) AS ecom_return_reason_key,
  A.SOURCE_REASON_CD,
  a.date_added,
  return_header_key,
  NULL,
  NULL,
  NULL
FROM
  prod_ecmde_db.ECOM_DIM.txn_order_sku AS a
    LEFT JOIN prod_ecmde_db.ecom_dim.store AS d
      ON A.SOURCE_STORE_CD = D.STORE_NUMBER
        AND d.record_status = 'A'
WHERE
  a.trans_type_key = 8
  AND a.txn_date_key BETWEEN 20210131 AND 20221225
  AND a.record_status = 'A'
UNION ALL
SELECT
  CASE a.chain_key
    WHEN 1 THEN 6
    ELSE a.chain_key
  END AS ecom_webstore_key,
  a.dks_Sku_key AS ddw_product_id,
  a.style_key,
  a.product_key,
  NVL(
    CAST(
      prod_ecmde_db.ecom.custom_concat_ifnull(
        ARRAY(CAST(a.chain_key AS INT), '0', ABS(a.dks_sku))
      ) AS DOUBLE
    ),
    -1
  ) AS WEB_SKU_KEY,
  txn_date_key,
  CASE
    WHEN
      (
        oh_type = 'B'
        OR chain_number IN (4, 5, 9)
      )
    THEN
      'Web DC'
    WHEN a.return_source IN ('endzone', 'legacy-pos') THEN 'In-Store'
    WHEN
      (
        NOT return_delivery_key IS NULL
        OR fedex_flg = 'Y'
      )
    THEN
      'Store Mail-In'
    WHEN a.return_source LIKE 'curbside%' THEN 'Curbside'
    ELSE 'Unknown'
  END AS ecom_return_channel,
  units AS ecom_return_units,
  extended_amt AS ecom_return_amt,
  units * average_cost AS ecom_return_cst,
  d.store_key,
  a.order_skU_key,
  a.order_header_key,
  a.order_fulfill_key,
  NVL(A.REASON_KEY, -1),
  A.SOURCE_REASON_CD,
  a.date_added,
  a.return_header_key,
  a.return_source,
  tracking_number,
  a.order_delivery_key
FROM
  prod_ecmde_db.ECOM_DIM.txn_order_sku AS a
    LEFT JOIN prod_ecmde_db.ecom_dim.store AS d
      ON A.SOURCE_STORE_CD = D.STORE_NUMBER
      AND d.record_status = 'A'
    LEFT JOIN prod_ecmde_db.ecom_dim.return_header AS b
      ON a.return_header_key = b.return_header_key
WHERE
  a.trans_type_key = 8
  AND a.txn_date_key BETWEEN
    20221226
  AND
    CAST(DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP, 1), 'yyyyMMdd') AS DOUBLE)
  AND a.record_status = 'A'