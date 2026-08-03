CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_kpi_ecom_settle_vw
(
    ECOM_KESE_SETTLE_DATE_KEY,
    ECOM_KESE_ECOM_CHANNEL_KEY,
    ECOM_KESE_WEBSTORE_KEY,
    ECOM_KESE_SETTLE_UNITS,
    ECOM_KESE_SETTLE_AMT,
    ECOM_KESE_DC_RETURN_UNITS,
    ECOM_KESE_DC_RETURN_AMT,
    ECOM_KESE_STORE_RET_UNITS,
    ECOM_KESE_STORE_RET_AMT,
    ECOM_KESE_PA_AMT
)
AS
SELECT
  txn_date_key,
  NVL(
    CASE
      WHEN
        (
          trans_type_key = 8
          AND NVL(a.data_source_key, -1) <> 4
          AND NOT return_source IN ('endzone', 'legacy-pos')
        )
        AND c.oh_type IN ('S')
      THEN
        4
      WHEN
        (
          trans_type_key = 8
          AND NVL(a.data_source_key, -1) <> 4
          AND NOT return_source IN ('endzone', 'legacy-pos')
        )
      THEN
        5
      WHEN
        b.channel_type_key = 5
        AND b.fulfillment_location_cd LIKE '%52'
      THEN
        7
      ELSE b.channel_type_key
    END,
    -1
  ) AS ecom_channel_key,
  a.chain_key,
  SUM(
    CASE
      WHEN trans_type_key = 6 THEN units
      ELSE 0
    END
  ) AS fulfill_units,
  SUM(
    CASE
      WHEN trans_type_key = 6 THEN extended_amt
      ELSE 0
    END
  ) AS fulfill_amt,
  SUM(
    CASE
      WHEN
        trans_type_key = 8
        AND a.data_source_key = 4
        AND txn_date_key <= 20221225
      THEN
        0
      WHEN
        trans_type_key = 8
        AND return_source IN ('endzone', 'legacy-pos')
        AND txn_date_key > 20221225
      THEN
        0
      WHEN trans_type_key = 8 THEN units
      ELSE 0
    END
  ) AS dc_return_units,
  SUM(
    CASE
      WHEN
        trans_type_key = 8
        AND a.data_source_key = 4
        AND txn_date_key <= 20221225
      THEN
        0
      WHEN
        trans_type_key = 8
        AND return_source IN ('endzone', 'legacy-pos')
        AND txn_date_key > 20221225
      THEN
        0
      WHEN trans_type_key = 8 THEN extended_amt
      ELSE 0
    END
  ) AS dc_return_amt,
  SUM(
    CASE
      WHEN
        trans_type_key = 8
        AND a.data_source_key = 4
        AND txn_date_key <= 20221225
      THEN
        units * -1
      WHEN
        trans_type_key = 8
        AND return_source IN ('endzone', 'legacy-pos')
        AND txn_date_key > 20221225
      THEN
        units
      ELSE 0
    END
  ) AS store_return_units,
  SUM(
    CASE
      WHEN
        trans_type_key = 8
        AND a.data_source_key = 4
        AND txn_date_key <= 20221225
      THEN
        extended_amt * -1
      WHEN
        trans_type_key = 8
        AND return_source IN ('endzone', 'legacy-pos')
        AND txn_date_key > 20221225
      THEN
        extended_amt
      ELSE 0
    END
  ) AS store_return_amt,
  SUM(
    CASE
      WHEN trans_type_key = 11 THEN extended_amt
      ELSE 0
    END
  ) AS price_adj_amt
FROM
  prod_ecmde_db.ecom_dim.txn_order_sku AS a
    LEFT JOIN prod_ecmde_db.ecom_Dim.order_fulfill AS b
      ON a.order_fulfill_key = b.order_Fulfill_key
    LEFT JOIN prod_ecmde_db.ecom_Dim.store AS c
      ON prod_ecmde_db.ECOM_DIM.GET_NUMBER_FROM_STRING(a.source_store_cd) = c.store_number
        AND c.record_status = 'A'
  WHERE a.txn_date_key >= (
    SELECT
      fiscal_year_begin_date
    FROM
      prod_ecmde_db.ecom_Dim.date_dim
    WHERE
      date_key = (
        SELECT
          date_id_ly
        FROM
          prod_ecmde_db.ecom_dim.date_dim
        WHERE
          date_code = DATE_TRUNC('DAY', CURRENT_TIMESTAMP() - INTERVAL 1 DAY)
      )
  )
  AND a.txn_date_key < CAST(DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd') AS DOUBLE)
  AND trans_type_key IN (6, 8, 11)
  AND a.record_status = 'A'
GROUP BY
  txn_date_key,
  a.chain_key,
  NVL(
    CASE
      WHEN
        (
          trans_type_key = 8
          AND NVL(a.data_source_key, -1) <> 4
          AND NOT return_source IN ('endzone', 'legacy-pos')
        )
        AND c.oh_type IN ('S')
      THEN
        4
      WHEN
        (
          trans_type_key = 8
          AND NVL(a.data_source_key, -1) <> 4
          AND NOT return_source IN ('endzone', 'legacy-pos')
        )
      THEN
        5
      WHEN
        b.channel_type_key = 5
        AND b.fulfillment_location_cd LIKE '%52'
      THEN
        7
      ELSE b.channel_type_key
    END,
    -1
  )