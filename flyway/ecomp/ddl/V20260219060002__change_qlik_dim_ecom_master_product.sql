CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_ecom_master_product
(
  ECOM_EMAST_PIM_ENTITY_ID,
  EMAST_ECODE,
  EMAST_PIM_PROD_CREATE_DATE_KEY,
  EMAST_PIM_PROD_ALT_IMG_CNT,
  EMAST_PIM_PROD_SWATCH_IMG_CNT,
  EMAST_PIM_PRODUCT_COLOR_CNT,
  EMAST_HAS_MAST_PROD_DESC_FLG,
  EMAST_PROD_WSC_READY_FLG,
  EMAST_PROD_WCS_READY_DATE_KEY,
  EMAST_PROD_BUYABLE_FLG,
  EMAST_PROD_DISPLAY_FLG,
  EMAST_PROD_DISPLAY_DT_KEY,
  EMAST_PROD_DISPLAY_DT,
  EMAST_PROD_COO,
  EMAST_PROD_TITLE,
  EMAST_PROD_GENDER,
  EMAST_PROD_BRAND,
  EMAST_PROD_DISPLAY_STYLE,
  EMAST_ODS_PRODUCT_STATUS,
  EMAST_ODS_PROD_STATUS_GRP,
  EMAST_DSG_PROD_SORT_DATE_KEY,
  EMAST_DSG_PROD_SORT_DATE,
  EMAST_DSG_READY_FLG,
  EMAST_FNS_READY_FLG,
  EMAST_GG_READY_FLG,
  EMAST_PL_READY_FLG,
  EMAST_DSG_PIM_CATALOG_FLG,
  EMAST_FNS_PIM_CATALOG_FLG,
  EMAST_GG_PIM_CATALOG_FLG,
  EMAST_PRESALE_FLG,
  EMAST_PROMO_EXCL_ORDER_FLG,
  EMAST_PROMO_EXCL_ITEM_FLG,
  EMAST_PIM_ACTIVITY,
  EMAST_PIM_MAIN_IMAGE_CNT,
  EMAST_PIM_MODEL_IMG_CNT,
  EMAST_PIM_MAIN_MODEL_IMG_CNT,
  EMAST_PIM_IMG_CNT,
  EMAST_WCS_UNIQUE_ID,
  EMAST_PIM_FULL_IMAGE,
  EMAST_PRESALE_END_DATE,
  EMAST_PIM_STL_IMG_CNT,
  EMAST_HAS_FULL_IMAGE_FLG,
  EMAST_PRODUCT_IDENTITY,
  EMAST_FOOTWARE_APP_FLG,
  EMAST_PROD_ECOM_GENDER,
  EMAST_PROD_HAS_SPECS_FLG,
  EMAST_SUSTAINABILTY_DTL,
  EMAST_REVIEW_CNT,
  EMAST_HAS_REVIEW_FLG,
  EMAST_SIZE_CHART,
  EMAST_PIM_BODY_INC_CNT
) AS
SELECT
  PIM_PRODUCT_EMAST_KEY AS ecom_EMAST_PIM_ENTITY_ID,
  PIM_PRODUCT_EMAST_CODE AS ecom_ecode,
  NVL(
    CAST(DATE_FORMAT(PIM_DATE_CREATED, 'yyyyMMdd') AS DOUBLE),
    -1
  ) AS ecom_pim_prod_create_date_key,
  NVL(CURR_ALT_IMAGE_CNT, 0) AS ecom_pim_prod_alt_img_cnt,
  NVL(CURR_SW_IMAGE_CNT, 0) AS ecom_pim_prod_swatch_img_cnt,
  NVL(COLOR_CNT, 0) AS ecom_pim_product_color_cnt,
  NVL(EM_PRODUCT_DESC_FLG, 'N') AS ecom_has_mast_product_desc_flg,
  NVL(WSC_READY_FLG, 'N') AS ecom_prod_wsc_ready_Flg,
  NVL(WSC_READY_date_key, -1) AS ecom_prod_wcs_ready_date_key,
  NVL(PRODUCT_BUYABLE_FLG, 'Y') AS ecom_mast_prod_buyable_Flg,
  NVL(EM_PRODUCT_DISPLAY_FLG, 'Y') AS ecom_mast_prod_display_flg,
  NVL(PRODUCT_DISPLAY_DATE_KEY, -1) AS ecom_mast_prod_display_dt_key,
  NVL(c.date_code, TO_DATE('01-Jan-1900', 'dd-MMM-yyyy')) AS ecom_mast_prod_display_dt,
  COUNTRY_OF_ORIGIN AS ecom_mast_prod_coo,
  NVL(EM_PRODUCT_TITLE, '!No Value') AS ecom_mast_prod_title,
  NVL(EM_GENDER_BY_AGE, '!No Value') AS ecom_mast_prod_gender,
  EM_PRODUCT_BRAND AS ecom_mast_prod_brand,
  NVL(DSG_STYLE, '!No Value') AS ecom_mast_prod_display_style,
  NVL(EM_PRODUCT_STATUS, 'S') AS ecom_mast_ods_product_status,
  CASE SUBSTR(EM_PRODUCT_STATUS, 1, 1)
    WHEN 'A' THEN 'Active'
    ELSE 'Inactive'
  END AS ecom_mast_ods_prod_status_grp,
  NVL(product_sort_date_key, -1) AS ecom_dsg_product_sort_date_key,
  DATE_TRUNC('DAY', b.date_code) AS ecom_dsg_product_sort_date,
  NVL(DSG_ready_FLG, 'N') AS ecom_mast_dsg_ready_flg,
  NVL(FS_ready_FLG, 'N') AS ecom_mast_fns_ready_flg,
  NVL(GG_ready_FLG, 'N') AS ecom_mast_gg_ready_flg,
  NVL(PL_ready_FLG, 'N') AS ecom_mast_gg_ready_flg,
  NVL(DSG_FLG, 'N') AS ecom_mast_dsg_pim_catalog_flg,
  NVL(FS_FLG, 'N') AS ecom_mast_fns_pim_catalog_flg,
  NVL(GG_FLG, 'N') AS ecom_mast_gg_pim_catalog_flg,
  NVL(EM_PRESALE_FLG, 'N') AS ecom_mast_presale_flg,
  NVL(promo_exclusion_order_flg, 'N') AS ecom_mast_promo_excl_order_flg,
  NVL(promo_exclusion_item_flg, 'N') AS ecom_mast_promo_excl_item_flg,
  NVL(em_activity, '!No Value') AS ecom_mast_pim_activity,
  NVL(main_IMAGE_CNT, 0) AS ecom_mast_pim_main_IMAGE_CNT,
  NVL(model_IMAGE_CNT, 0) AS ecom_mast_pim_model_IMg_CNT,
  NVL(model_main_IMAGE_CNT, 0) AS ecom_mast_pim_main_mod_IMg_CNT,
  NVL(CURR_IMAGE_CNT, 0) AS ecom_mast_pim_IMg_CNT,
  wcs_catentry_id,
  NVL(full_image, '!No Value') AS ecom_mast_pim_full_image,
  em_presale_end_Date,
  NVL(shop_the_look_IMAGE_CNT, 0) AS ecom_mast_pim_stl_IMg_CNT,
  CASE NVL(full_image, '!No Value')
    WHEN '!No Value' THEN 'N'
    ELSE 'Y'
  END AS has_full_image_flg,
  NVL(em_product_identity, '!No Value') AS ecom_mast_product_identity,
  CASE stackd_flg
    WHEN 'E' THEN 'Exclusive'
    WHEN 'R' THEN 'Regular'
    ELSE 'N'
  END AS ecom_mast_footware_app_flg,
  NVL(A.EM_ECOM_GENDER, '!No Value') AS ecom_prod_ecom_gender,
  NVL(EM_has_specs_FLG, 'N') AS ecom_has_specs_flg,
  NVL(mu.em_sustainability, '!No Value') AS em_sustainability,
  review_count,
  CASE
    WHEN review_count > 0 THEN 'Y'
    ELSE 'N'
  END AS has_reviews_flg,
  NVL(SIZE_CHART, '!No Value') AS SIZE_CHART,
  NVL(body_inclusivity_cnt, 0) AS body_inclusivity_cnt
FROM
  prod_ecmde_db.ecom_dim.pim_product_emast AS a
    LEFT JOIN prod_ecmde_db.ecom_Dim.date_dim AS b
      ON a.product_sort_date_key = b.date_key
    LEFT JOIN prod_ecmde_db.ecom_dim.date_dim AS c
      ON a.PRODUCT_DISPLAY_DATE_KEY = c.date_key
    LEFT JOIN (
      SELECT
        mdm_entity_id,
        ARRAY_JOIN(COLLECT_LIST(attribute_value), ' , ') AS em_sustainability
      FROM
        (
          SELECT
            mdm_entity_id,
            attribute_value
          FROM
            prod_ecmde_db.ECOM_DIM.STG_MDM_MASTER_CATALOG_ATTR_mu
          WHERE
            attribute_id = 5503
            AND mdm_record_status = 'A'
          ORDER BY
            sort_id NULLS LAST
        ) AS ordered_mu
      GROUP BY
        mdm_entity_id
    ) AS mu
      ON A.PIM_PRODUCT_EMAST_KEY = mu.mdm_entity_id
    LEFT JOIN (
      SELECT
        product_code,
        MAX(review_count) AS review_count
      FROM
        prod_ecmde_db.ecom_dim.web_product
      WHERE
        record_status = 'A'
        AND chain_key IN (2, 4, 6)
        AND NOT review_count IS NULL
      GROUP BY
        product_code
    ) AS rv
      ON rv.product_code = PIM_PRODUCT_EMAST_CODE
WHERE
  a.record_status = 'A';
