CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_cancelled_dmd_vw
(
  ECOM_WEBSTORE_KEY,
  PRODUCT_ID,
  STYLE_ID,
  ECOM_WEBSTORE_PRODUCT_KEY,
  ECOM_WEB_SKU_KEY,
  ECOM_TX5_CANCEL_DATE_KEY,
  ECOM_TX5_CANC_DMD_FF_CHAN_KEY,
  ECOM_TX5_CANCEL_DMD_UNITS,
  ECOM_TX5_CANCEL_DMD_AMT,
  ECOM_TX5_CANCEL_DMD_COST,
  ECOM_TX5_DESIG_CAN_STORE_KEY,
  ECOM_TX5_DESIG_CANC_VENDOR_KEY,
  ECOM_ORDER_SKU_KEY,
  ECOM_ORDER_HEADER_KEY,
  ECOM_TX5_CANCEL_REASON_KEY,
  ECOM_TX5_CANCEL_SOURCE_CD,
  TXN5_DATE_LAST_MODIFIED,
  ECOM_TX5_CANCEL_SOURCE_SYSTEM,
  ECOM_TX5_CANCEL_DTTM
) AS
SELECT
  a.chain_key AS ecom_webstore_key,
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
  c.channel_type_key AS ecom_fulfill_channel_key,
  CASE
    WHEN SUM(units) > orig_tot_units THEN orig_tot_units
    ELSE SUM(units)
  END AS ecom_cancel_dmd_units,
  CASE
    WHEN SUM(extended_amt) > orig_tot_extended_amt THEN orig_tot_extended_amt
    ELSE SUM(extended_amt)
  END AS ecom_cancel_dmd_amt,
  CASE
    WHEN SUM(units) > orig_tot_units THEN orig_tot_units
    ELSE SUM(units)
  END
  * c.average_cost AS ecom_cancel_dmd_cost,
  NVL(d.store_key, -999) AS ecom_desig_can_store_key,
  NVL(d.vendor_key, -999) AS ecom_desig_canc_vendor_key,
  a.order_skU_key,
  a.order_header_key,
  NVL(A.REASON_KEY, -1) AS ecom_cancel_reason_key,
  A.SOURCE_REASON_CD AS ecom_cancel_source_cd,
  MAX(a.date_added) AS TXN5_DATE_LAST_MODIFIED,
  MAX(
    CASE
      WHEN A.TOTAL_AMT IS NULL THEN 'EOM'
      ELSE 'WCS'
    END
  ) AS ecom_cancel_source_system,
  MIN(
    prod_ecmde_db.ecom_dim.GET_DATE_FROM_STRING(
      CONCAT(CAST(txn_date_key AS BIGINT), time_24),
      'yyyyMMddHH:mm:ss'
    )
  ) AS ecom_cancel_dttm
FROM
  prod_ecmde_db.ECOM_DIM.txn_order_sku AS a
    JOIN prod_ecmde_db.ecom_dim.order_sku AS c
      ON a.order_sku_key = c.order_sku_key
    LEFT JOIN prod_ecmde_db.ecom_dim.order_fulfill AS d
      ON C.ORDER_FULFILL_NUMBER = d.ORDER_FULFILL_NUMBER
      AND c.chain_key = d.chain_key
    JOIN prod_ecmde_db.ecom_Dim.time_dim AS e
      ON a.txn_time_key = e.time_key
WHERE
  a.trans_type_key = 5
  AND a.txn_date_key BETWEEN
    20210131
  AND
    CAST(DATE_FORMAT(DATE_SUB(CURRENT_TIMESTAMP, 1), 'yyyyMMdd') AS DOUBLE)
  AND c.order_date_key >= 20210131
GROUP BY
  a.chain_key,
  a.dks_Sku_key,
  a.style_key,
  a.product_key,
  NVL(
    CAST(
      prod_ecmde_db.ecom.custom_concat_ifnull(
        ARRAY(CAST(a.chain_key AS INT), '0', ABS(a.dks_sku))
      ) AS DOUBLE
    ),
    -1
  ),
  txn_date_key,
  c.channel_type_key,
  NVL(d.store_key, -999),
  NVL(d.vendor_key, -999),
  a.order_skU_key,
  a.order_header_key,
  orig_tot_units,
  orig_tot_extended_amt,
  c.average_cost,
  A.REASON_KEY,
  A.SOURCE_REASON_CD
