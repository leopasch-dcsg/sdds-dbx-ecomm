CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_order_header_vw
(
    ECOM_WEBSTORE_KEY,
    ECOM_OHF_ORDER_SUBMIT_DATE_KEY,
    ECOM_ORDER_HEADER_KEY,
    ECOM_ORDER_LINE_CNT,
    ECOM_ORDER_ORIG_UNITS,
    ECOM_ORDER_ORIG_REV_AMT,
    ECOM_ORDER_ORIG_SHIP_AMT,
    ECOM_ORDER_ORIG_COST,
    ECOM_ORDER_ORIG_DISC_AMT,
    ECOM_ORDER_ORIG_SHIPDISC_AMT,
    ECOM_ORDER_SKU_CNT,
    OHF_DATE_LAST_MODIFIED,
    ECOM_ORDER_PRICE_TYPE
)
AS
SELECT
  a.chain_key AS ecom_webstore_key,
  order_date_key AS ecom_order_submit_date_key,
  order_header_key,
  SUM(order_line_cnt) AS ecom_order_line_cnt,
  SUM(orig_tot_units) AS ecom_order_orig_units,
  SUM(orig_tot_extended_amt) AS ecom_order_orig_rev_amt,
  SUM(orig_tot_freight_amt) AS ecom_order_orig_ship_amt,
  SUM(average_cost * orig_tot_units) AS ecom_order_orig_cost,
  SUM(orig_tot_ext_disc_amt) AS ecom_order_orig_disc_amt,
  SUM(orig_tot_freight_disc_amt) AS ecom_order_orig_shipdisc_amt,
  COUNT(*) AS ecom_order_sku_cnt,
  MAX(a.date_last_modified) AS ohf_date_last_modified,
  CASE COUNT(DISTINCT clearance_type_flag)
    WHEN 1 THEN MAX(clearance_type_flag)
    ELSE 'Mixed'
  END AS order_price_type
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_SKU AS a
    LEFT JOIN prod_ecmde_db.ECOM_DIM.CLEARANCE_TYPE AS b
      ON a.clearance_type_key = b.clearance_Type_key
WHERE
  order_date_key BETWEEN
    20200202
  AND
    CAST(DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP, 1), 'yyyyMMdd') AS DOUBLE)
GROUP BY
  a.chain_key,
  order_date_key,
  order_header_key