CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_kpi_ecom_alloc_vw
(
    ECOM_KEA_DO_CREATE_DATE_KEY,
    ECOM_KEA_ECOM_CHANNEL_KEY,
    ECOM_KEA_WEBSTORE_KEY,
    ECOM_KEA_ALLOC_UNITS,
    ECOM_KEA_ALLOC_AMT,
    ECOM_KEA_DECLINED_UNITS,
    ECOM_KEA_DECLINED_AMT,
    ECOM_KEA_FULFILLED_UNITS,
    ECOM_KEA_FULFILLED_AMT,
    ECOM_KEA_OPEN_UNITS,
    ECOM_KEA_OPEN_AMT,
    ECOM_KEA_ABANDONED_UNITS,
    ECOM_KEA_ABANDONED_AMT,
    ECOM_KEA_CUST_CANC_UNITS,
    ECOM_KEA_CUST_CANC_AMT,
    ECOM_KEA_OTH_CANC_UNITS,
    ECOM_KEA_OTH_CANC_AMT
)
AS
SELECT
  txn_date_key,
  channel_type_key,
  a.chain_key,
  SUM(
    CASE
      WHEN trans_type_key = 3 THEN units
    END
  ) AS allocated_units,
  SUM(
    CASE
      WHEN trans_type_key = 3 THEN extended_amt
    END
  ) AS allocated_amt,
  SUM(
    CASE
      WHEN
        a.data_source_key != 23 AND
        NVL(source_reason_cd, 'x') NOT IN ('030', '055', '070', '100', '200', '065')
      THEN
        DECLINE_UNITS
      WHEN
        a.data_source_key = 23 AND
        NVL(source_reason_cd, 'x') NOT IN ('-530', '-531', '-538', '510', '520')
      THEN
        DECLINE_UNITS        
    END
  ) AS declined_units,
  SUM(
    CASE
      WHEN
        DECLINE_UNITS > 0
        AND a.data_source_key != 23
        AND NVL(source_reason_cd, 'x') NOT IN ('030', '055', '070', '100', '200', '065')
      THEN
        ROUND(A.DECLINE_UNITS * (extended_amt / NULLIF(a.units, 0)), 2)
      WHEN
        DECLINE_UNITS > 0 
        AND a.data_source_key = 23
        AND NVL(source_reason_cd, 'x') NOT IN ('-530', '-531', '-538', '510', '520')
      THEN
        ROUND(A.DECLINE_UNITS * (extended_amt / NULLIF(a.units, 0)), 2)        
    END
  ) AS declined_amt,
  SUM(
    CASE
      WHEN fulfillment_status_cd = 'F' THEN units - NVL(A.DECLINE_UNITS, 0)
    END
  ) AS fulfilled_units,
  SUM(
    CASE
      WHEN
        fulfillment_status_cd = 'F'
      THEN
        ROUND((a.units - NVL(A.DECLINE_UNITS, 0)) * (extended_amt / NULLIF(a.units, 0)), 2)
    END
  ) AS fulfilled_amt,
  SUM(
    CASE
      WHEN fulfillment_status_cd = 'A' THEN units - NVL(A.DECLINE_UNITS, 0)
    END
  ) AS open_units,
  SUM(
    CASE
      WHEN
        fulfillment_status_cd = 'A'
      THEN
        ROUND((a.units - NVL(A.DECLINE_UNITS, 0)) * (extended_amt / NULLIF(a.units, 0)), 2)
    END
  ) AS open_amt,
  SUM(
    CASE
      WHEN a.DATA_SOURCE_KEY != 23 
        AND source_reason_cd IN ('200', '065') THEN DECLINE_UNITS
      WHEN a.DATA_SOURCE_KEY = 23 
        AND source_reason_cd IN ('-530', '510') THEN DECLINE_UNITS
    END
  ) AS abandoned_units,
  SUM(
    CASE
      WHEN a.data_source_key != 23 AND
        source_reason_cd IN ('200', '065')
      THEN
        ROUND(A.DECLINE_UNITS * (extended_amt / NULLIF(a.units, 0)), 2)
      WHEN a.data_source_key = 23 AND
        source_reason_cd IN ('-530', '510')
      THEN
        ROUND(A.DECLINE_UNITS * (extended_amt / NULLIF(a.units, 0)), 2)        
    END
  ) AS abandoned_amt,
  SUM(
    CASE
      WHEN source_reason_cd IN ('030', '055', '070', '100') 
        AND a.data_source_key != 23 THEN DECLINE_UNITS
      WHEN source_reason_cd IN ('-531', '-538', '520') 
        AND a.data_source_key = 23 THEN DECLINE_UNITS
    END
  ) AS customer_can_req_units,
  SUM(
    CASE
      WHEN a.data_source_key != 23 AND
        source_reason_cd IN ('030', '055', '070', '100')
      THEN
        ROUND(A.DECLINE_UNITS * (extended_amt / NULLIF(a.units, 0)), 2)
      WHEN a.data_source_key = 23 AND
        source_reason_cd IN ('-531', '-538', '520')
      THEN
        ROUND(A.DECLINE_UNITS * (extended_amt / NULLIF(a.units, 0)), 2)        
    END
  ) AS customer_can_req_amt,
  SUM(
    CASE
      WHEN
        trans_type_key = 3
        AND fulfillment_status_cd = 'X'
      THEN
        a.units - NVL(A.DECLINE_UNITS, 0)
    END
  ) AS oth_cancelled_units,
  SUM(
    CASE
      WHEN
        trans_type_key = 3
        AND fulfillment_status_cd = 'X'
      THEN
        ROUND((a.units - NVL(A.DECLINE_UNITS, 0)) * (extended_amt / NULLIF(a.units, 0)), 2)
    END
  ) AS oth_cancelled_amt
FROM
  prod_ecmde_db.ecom_dim.txn_order_sku AS a
    JOIN prod_ecmde_db.ecom_dim.order_fulfill AS b
      ON a.order_fulfill_key = b.order_fulfill_key
WHERE
  trans_type_key = 3
  AND txn_date_key >= 20210131
GROUP BY
  channel_type_key,
  txn_date_key,
  a.chain_key