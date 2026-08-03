CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_order_head_ff_vw
(
  ECOM_WEBSTORE_KEY,
  ECOM_ORDER_HEADER_KEY,
  ECOM_ORDER_FF_UNITS,
  ECOM_ORDER_FF_AMT,
  ECOM_ORDER_STORE_RETURN_UNITS,
  ECOM_ORDER_DC_RETURN_UNITS,
  ECOM_ORDER_STORE_RETURN_AMT,
  ECOM_ORDER_DC_RETURN_AMT,
  OHFF_DATE_LAST_MODIFIED
) AS
SELECT
  MAX(a.chain_key) AS chain_key,
  a.order_header_key,
  SUM(
    CASE trans_type_key
      WHEN 6 THEN units
      ELSE 0
    END
  ) AS fulfill_units,
  SUM(
    CASE trans_type_key
      WHEN 6 THEN extended_amt
      ELSE 0
    END
  ) AS fulfill_amt,
  SUM(
    CASE
      WHEN
        trans_type_key = 8
        AND a.data_source_key = 4
      THEN
        units * -1
      ELSE 0
    END
  ) AS ecom_store_return_units,
  SUM(
    CASE
      WHEN
        trans_type_key = 8
        AND NVL(a.data_source_key, -1) <> 4
      THEN
        units
      ELSE 0
    END
  ) AS ecom_dc_return_units,
  SUM(
    CASE
      WHEN
        trans_type_key = 8
        AND a.data_source_key = 4
      THEN
        extended_amt * -1
      ELSE 0
    END
  ) AS ecom_store_return_amt,
  SUM(
    CASE
      WHEN
        trans_type_key = 8
        AND NVL(a.data_source_key, -1) <> 4
      THEN
        extended_amt
      ELSE 0
    END
  ) AS ecom_dc_return_amt,
  MAX(a.date_last_modified) AS ohff_date_last_modified
FROM
  prod_ecmde_db.ECOM_DIM.txn_order_Sku AS a
WHERE
  trans_type_key IN (6, 8)
  AND txn_date_key BETWEEN
    20200202
  AND
    CAST(DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP, 1), 'yyyyMMdd') AS DOUBLE)
  AND a.record_status = 'A'
GROUP BY
  a.order_header_key
