CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_ecom_return_header_vw
(
  ECOM_RETURN_HEADER_KEY,
  ECOM_RETURN_DATE_KEY,
  ECOM_RETURN_CO_NUM,
  ECOM_RETURN_TYPE,
  ECOM_RETURN_TRAN_AMT,
  ECOM_RETURN_RELEASE_NUM,
  ECOM_RETURN_TRAN_UNITS,
  ECOM_RETURN_TRAN_SKU_CNT,
  ECOM_RETURN_MESSAGE_SRC,
  ECOM_RETURN_TRACKING_NUM,
  ECOM_RETURN_FEDEX_FLG,
  ECOM_RETURN_PROCESSED_DTTM,
  ECOM_RETURN_DELIVERY_DTTM
) AS
SELECT
  return_header_key,
  tran_date,
  web_order_num,
  return_type,
  return_tran_amt,
  release_num,
  return_tran_units,
  return_tran_skU_cnt,
  CASE message_source
    WHEN 'SO-RT' THEN 'EOM'
    ELSE 'Other'
  END AS ECOM_RETURN_MESSAGE_SRC,
  NULL AS ECOM_RETURN_TRACKING_NUM,
  NULL AS ECOM_RETURN_FEDEX_FLG,
  NULL AS ECOM_RETURN_PROCESSED_DTTM,
  NULL AS ECOM_RETURN_DELIVERY_DTTM
FROM
  prod_ecmde_db.ECOM_DIM.return_header
WHERE
  tran_date BETWEEN 20210131 AND 20221225
UNION ALL
SELECT
  return_header_key,
  tran_date,
  web_order_num,
  return_type,
  return_tran_amt,
  release_num,
  return_tran_units,
  return_tran_skU_cnt,
  return_source,
  CASE
    WHEN
      return_type = 'DC'
    THEN
      COALESCE(
        return_tracking_num,
        CASE
          WHEN fedex_tracking_number <> 'Multiple' THEN fedex_tracking_number
        END,
        'Not Supplied'
      )
    WHEN
      return_source NOT IN ('endzone', 'legacy-pos')
    THEN
      NVL(return_tracking_num, fedex_tracking_number)
  END AS ECOM_RETURN_TRACKING_NUM,
  CASE
    WHEN
      return_source NOT IN ('endzone', 'legacy-pos')
      AND return_tracking_num IS NULL
    THEN
      fedex_flg
  END AS ECOM_RETURN_FEDEX_FLG,
  min_return_process_date AS ECOM_RETURN_PROCESSED_DTTM,
  CASE
    WHEN
      return_source NOT IN ('endzone', 'legacy-pos')
    THEN
      NVL(return_delivery_date, fedex_delivery_date)
  END AS ECOM_RETURN_DELIVERY_DTTM
FROM
  prod_ecmde_db.ECOM_DIM.return_header
WHERE
  tran_date > 20221225
