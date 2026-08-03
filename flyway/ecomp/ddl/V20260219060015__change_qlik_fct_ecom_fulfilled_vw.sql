CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_fulfilled_vw
(
  ECOM_WEBSTORE_KEY,
  PRODUCT_ID,
  STYLE_ID,
  ECOM_WEBSTORE_PRODUCT_KEY,
  ECOM_WEB_SKU_KEY,
  ECOM_TX6_FULFILL_POST_DATE_KEY,
  ECOM_TX6_FULFILL_UNITS,
  ECOM_TX6_FULFILL_AMT,
  ECOM_TX6_FULFILL_COST,
  ECOM_TX6_FULFILL_SHIP_AMT,
  ECOM_ORDER_SKU_KEY,
  ECOM_ORDER_HEADER_KEY,
  ECOM_ORD_DISTR_KEY,
  ECOM_PKG_DELIVERY_KEY,
  TXN6_DATE_LAST_MODIFIED,
  ECOM_TX6_CS_REP,
  ECOM_TX6_SHIPPED_DTTM,
  ECOM_TX6_SHIPPED_DATE_KEY
) AS
SELECT
  a.chain_key AS ECOM_WEBSTORE_KEY,
  a.dks_sku_key AS PRODUCT_ID,
  a.style_key AS STYLE_ID,
  a.product_key AS ECOM_WEBSTORE_PRODUCT_KEY,
  NVL(
    CAST(
      prod_ecmde_db.ecom.custom_concat_ifnull(
        ARRAY(CAST(a.chain_key AS INT), '0', ABS(a.dks_sku))
      ) AS DOUBLE
    ),
    - 1.0
  ) AS ECOM_WEB_SKU_KEY,
  a.txn_date_key AS ECOM_TX6_FULFILL_POST_DATE_KEY,
  a.units AS ECOM_TX6_FULFILL_UNITS,
  a.extended_amt AS ECOM_TX6_FULFILL_AMT,
  a.units * a.average_cost AS ECOM_TX6_FULFILL_COST,
  CAST(NULL AS DECIMAL(10, 0)) AS ECOM_TX6_FULFILL_SHIP_AMT,
  a.order_sku_key AS ECOM_ORDER_SKU_KEY,
  a.order_header_key AS ECOM_ORDER_HEADER_KEY,
  a.order_fulfill_key AS ECOM_ORD_DISTR_KEY,
  a.order_delivery_key AS ECOM_PKG_DELIVERY_KEY,
  a.date_added AS TXN6_DATE_LAST_MODIFIED,
  SUBSTR(
    b.cust_service_rep_code,
    1,
    NVL(NULLIF(INSTR(b.cust_service_rep_code, '@') - 1, -1), 20)
  ) AS ECOM_TX6_CS_REP,
  NVL(d.actual_shipped_dttm, c.min_ship_dttm) AS ECOM_TX6_SHIPPED_DTTM,
  NVL(d.fulfillment_date_key, c.fulfillment_date_key) AS ECOM_TX6_SHIPPED_DATE_KEY
FROM
  prod_ecmde_db.ecom_dim.txn_order_sku a
    LEFT JOIN prod_ecmde_db.ecom_dim.cust_service_rep b
      ON a.txn_cust_service_rep_key = b.cust_service_rep_key
    LEFT JOIN prod_ecmde_db.ecom_dim.order_fulfill c
      ON a.order_fulfill_key = c.order_fulfill_key
    LEFT JOIN prod_ecmde_db.ecom_dim.order_delivery d
      ON a.order_delivery_key = d.order_delivery_key
WHERE
  a.trans_type_key = 6
  AND a.txn_date_key BETWEEN
    20230129
  AND
    CAST(DATE_FORMAT(DATE_SUB(current_date(), 1), 'yyyyMMdd') AS BIGINT)
  AND a.record_status = 'A'
