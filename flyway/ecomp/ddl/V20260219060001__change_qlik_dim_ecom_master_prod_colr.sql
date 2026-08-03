CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_ecom_master_prod_colr
(
  ECOM_EMAST_PIM_PROD_COLOR_KEY,
  EMAST_PC_ECODE,
  EMAST_PC_COLOR,
  ECOM_EMAST_PIM_ENTITY_ID,
  EMAST_PC_IMAGE_CNT,
  EMAST_PC_SWATCH_CNT,
  EMAST_PC_MODEL_CNT,
  EMAST_PC_MAIN_CNT,
  EMAST_PC_MAIN_MODEL_CNT,
  EMAST_PC_ALT_CNT,
  EMAST_PC_SWATCH_FILE,
  EMAST_PC_MAIN_FILE,
  EMAST_PC_STL_CNT,
  EMAST_PC_LFS_CNT,
  EMAST_PC_HAS_IMAGE_FLG,
  EMAST_PC_FIRST_IMAGE_DATE,
  EMAST_PC_MIN_SKU_RECEIPT_DT,
  EMAST_PC_IMAGE_DELIVERY_GRP,
  EMAST_PC_FRD_TO_IMGD_DAYS,
  EMAST_PC_FRD_TO_IMGD_WKS,
  EMAST_PC_VIDEO_CNT,
  EMAST_PC_SCENE7_IMG_CNT
) AS
SELECT
  clr.pim_product_emast_color_key,
  ecode,
  color,
  pim_product_emast_key,
  image_cnt,
  swatch_cnt,
  model_cnt,
  main_cnt,
  main_model_cnt,
  alt_cnt,
  swatch_file,
  main_file,
  shop_the_look_cnt,
  lifestyle_img_cnt,
  CASE
    WHEN image_cnt > 0 THEN 'Y'
    ELSE 'N'
  END AS color_image_flg,
  first_image_date,
  prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd') AS first_receipt_Date,
  CASE
    WHEN
      prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd') < DATE_TRUNC(
        'DAY',
        NVL(first_image_date, CURRENT_TIMESTAMP())
      )
    THEN
      'Late'
    WHEN
      prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd') >= DATE_TRUNC(
        'DAY',
        first_image_date
      )
    THEN
      'Early'
    WHEN
      first_image_date IS NULL
      AND NOT first_receipt_Date IS NULL
    THEN
      'Late'
    ELSE 'Unknown Receipt'
  END AS delivery_grp,
  CAST(
    (
      DATE_TRUNC('DAY', first_image_date)
      - prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd')
    ) AS INT
  ) AS days_diff,
  CASE
    WHEN
      prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd') >= DATE_TRUNC(
        'DAY',
        first_image_date
      )
    THEN
      'Early/On-Time'
    WHEN
      CAST(
        (
          DATE_TRUNC('DAY', first_image_date)
          - prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd')
        ) AS INT
      )
      / 7 < 1
    THEN
      'Less Than 1 Week'
    WHEN
      CAST(
        (
          DATE_TRUNC('DAY', first_image_date)
          - prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd')
        ) AS INT
      )
      / 7 < 3
    THEN
      '1-2 Weeks'
    WHEN
      CAST(
        (
          DATE_TRUNC('DAY', first_image_date)
          - prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd')
        ) AS INT
      )
      / 7 < 5
    THEN
      '3-4 Weeks'
    WHEN
      CAST(
        (
          DATE_TRUNC('DAY', first_image_date)
          - prod_ecmde_db.ECOM_DIM.GET_DATE_FROM_STRING(first_receipt_Date, 'yyyyMMdd')
        ) AS INT
      )
      / 7 > 4
    THEN
      '4+ Weeks'
    WHEN
      first_image_date IS NULL
      AND first_receipt_Date IS NOT NULL
    THEN
      'Image Date Missing'
    ELSE 'Unknown Receipt'
  END AS receipt_status,
  video_cnt,
  scene7_img_cnt
FROM
  prod_ecmde_db.ecom_dim.pim_product_emast_color AS clr
    LEFT JOIN (
      SELECT
        A.PIM_PRODUCT_EMAST_COLOR_KEY,
        MIN(first_receipt_Date) AS first_receipt_Date
      FROM
        prod_ecmde_db.ecom_Dim.dks_sku_pim AS a
          JOIN prod_ecmde_db.ecom_dim.dks_SkU_units AS b
            ON a.dks_SkU_key = b.dks_SkU_key
      WHERE
        NOT PIM_PRODUCT_EMAST_COLOR_KEY IS NULL
      GROUP BY
        A.PIM_PRODUCT_EMAST_COLOR_KEY
    ) AS rct
      ON clr.PIM_PRODUCT_EMAST_COLOR_KEY = rct.PIM_PRODUCT_EMAST_COLOR_KEY
WHERE
  record_status = 'A'
  OR clr.pim_product_emast_color_key = -1;
