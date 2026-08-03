CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_content_dates_vw
(
  PRODUCT_ID,
  CONTENT_COMPLETE_DATE_TYPE,
  CONTENT_COMPLETE_DATE
) AS
SELECT
  dks_SkU_key AS product_id,
  complete_date_type AS content_complete_date_type,
  complete_date AS content_complete_date
FROM
  (
    SELECT
      ds1.dks_SKU_key,
      'COPY' AS complete_date_type,
      DATE_TRUNC('DAY', GREATEST(product_title_date, product_desc_date)) AS complete_date
    FROM
      prod_ecmde_db.ECOM_DIM.DKS_SKU AS ds1
        JOIN prod_ecmde_db.ecom_dim.style AS sty
          ON ds1.style_key = sty.style_key
          AND sty.record_status = 'A'
        JOIN prod_ecmde_db.ECOM_DIM.PIM_PRODUCT_EMAST AS wr
          ON STY.EMAST_PIM_ENTITY_ID = WR.PIM_PRODUCT_EMAST_KEY
    WHERE
      ds1.record_status = 'A'
      AND set_code = '0'
      AND color_code <> '9999'
      AND (
        product_title_date IS NOT NULL
        AND product_desc_date IS NOT NULL
      )
    UNION ALL
    SELECT
      ds1.dks_SKU_key,
      'WCSREADY' AS complete_date_type,
      TO_DATE(wsc_ready_date_key, 'yyyyMMdd') AS complete_date
    FROM
      prod_ecmde_db.ECOM_DIM.DKS_SKU AS ds1
        JOIN prod_ecmde_db.ecom_dim.style AS sty
          ON ds1.style_key = sty.style_key
          AND sty.record_status = 'A'
        JOIN prod_ecmde_db.ECOM_DIM.PIM_PRODUCT_EMAST AS wr
          ON STY.EMAST_PIM_ENTITY_ID = WR.PIM_PRODUCT_EMAST_KEY
    WHERE
      ds1.record_status = 'A'
      AND set_code = '0'
      AND color_code <> '9999'
      AND NVL(wsc_ready_date_key, -1) > 0
    UNION ALL
    SELECT
      ds1.dks_SKU_key,
      'IMAGE' AS complete_date_type,
      DATE_TRUNC('DAY', first_image_date) AS complete_date
    FROM
      prod_ecmde_db.ECOM_DIM.DKS_SKU AS ds1
        JOIN prod_ecmde_db.ECOM_DIM.dks_SkU_pim AS sp
          ON SP.dks_SKU_key = ds1.dks_Sku_key
        JOIN prod_ecmde_db.ECOM_DIM.PIM_PRODUCT_EMAST_COLOR AS ec
          ON SP.PIM_PRODUCT_EMAST_COLOR_KEY = EC.PIM_PRODUCT_EMAST_COLOR_KEY
    WHERE
      ds1.record_status = 'A'
      AND set_code = '0'
      AND color_code <> '9999'
      AND first_image_date IS NOT NULL
    UNION ALL
    SELECT
      ds1.dks_SKU_key,
      'TOTAL' AS complete_date_type,
      DATE_TRUNC(
        'DAY',
        GREATEST(
          product_title_date,product_desc_date,first_image_date,
          TO_DATE(wsc_ready_date_key, 'yyyyMMdd')
        )
      ) AS complete_date
    FROM
      prod_ecmde_db.ECOM_DIM.DKS_SKU AS ds1
        JOIN prod_ecmde_db.ecom_dim.style AS sty
          ON ds1.style_key = sty.style_key
          AND sty.record_status = 'A'
        JOIN prod_ecmde_db.ECOM_DIM.PIM_PRODUCT_EMAST AS wr
          ON STY.EMAST_PIM_ENTITY_ID = WR.PIM_PRODUCT_EMAST_KEY
        LEFT JOIN prod_ecmde_db.ECOM_DIM.dks_SkU_pim AS sp
          ON SP.dks_SKU_key = ds1.dks_Sku_key
        LEFT JOIN prod_ecmde_db.ECOM_DIM.PIM_PRODUCT_EMAST_COLOR AS ec
          ON SP.PIM_PRODUCT_EMAST_COLOR_KEY = EC.PIM_PRODUCT_EMAST_COLOR_KEY
    WHERE
      ds1.record_status = 'A'
      AND set_code = '0'
      AND color_code <> '9999'
      AND first_image_date IS NOT NULL
      AND NVL(wsc_ready_date_key, -1) > 0
      AND (
        product_title_date IS NOT NULL
        AND product_desc_date IS NOT NULL
      )
  );
