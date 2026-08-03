CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_ecom_sku_vw
(
  PRODUCT_ID,
  ECOM_SKU_TYPE,
  ECOM_SKU_PIM_COLOR,
  ECOM_SKU_SEA_GRND_SHIP_CHARGE,
  ECOM_SKU_SEA_DAY1_SHIP_CHARGE,
  ECOM_SKU_SEA_DAY2_SHIP_CHARGE,
  ECOM_SKU_SEA_CURB_SHIP_CHARGE,
  ECOM_SKU_SEA_THR_SHIP_CHARGE,
  ECOM_SKU_SEA_ROC_SHIP_CHARGE,
  ECOM_SKU_SEA_ASSEM_SHIP_CHARGE,
  ECOM_SKU_SEA_STD_SHIP_CHARGE,
  ECOM_SKU_SEA_SHIP_CLASS,
  ECOM_SKU_PIM_COLOR_ALT_IMG_CNT,
  ECOM_SKU_PIM_COLOR_SW_IMG_CNT,
  ECOM_SKU_VDC_ACTIVE,
  ECOM_SKU_PIM_VDC_ELIGIBILE_FLG,
  ECOM_SKU_VDC_VENDOR_NUMBER,
  ECOM_SKU_VDC_VENDOR_NAME,
  ECOM_SKU_WEB_PRIMARY_UPC,
  ECOM_SKU_PIM_PROD_DISPLAY_DTTM,
  ECOM_SKU_PIM_PROD_DISP_DT_KEY,
  ECOM_SKU_PIM_COLR_MAIN_IMG_CNT,
  ECOM_SKU_PIM_COLR_MODL_IMG_CNT,
  ECOM_SKU_PIMCLR_MAINMODIMG_CNT,
  ECOM_SKU_SEA_SHIP_RESTRICTED,
  ECOM_SKU_SFS_ELIGIBLE_FLG,
  ECOM_SKU_RDC_ELIGIBLE_FLG,
  ECOM_SKU_EMAST_PIM_ENTITY_ID,
  ECOM_ODS_STYLE_STATUS_DETAIL,
  ECOM_SKU_SPORTS_TEAM,
  ECOM_ODS_STYLE_STATUS,
  ECOM_DDW_STYLE_ORIGINAL_RETAIL,
  ECOM_DDW_ROUTE_PO,
  ECOM_EMAST_PIM_PROD_COLOR_KEY,
  ECOM_SKU_EM_PRESALE_FLG,
  ECOM_SKU_COLOR_FAMILY,
  ECOM_SKU_PRESALE_END_DATE,
  ECOM_SKU_PIM_PROD_DISPLAY_DATE,
  SKU_MDM_STATUS,
  SKU_MDM_LAST_DELETED_DTTM,
  ECOM_SKU_HEIGHT,
  ECOM_SKU_LENGTH,
  ECOM_SKU_WEIGHT,
  ECOM_SKU_WIDTH,
  ECOM_SKU_DIM_TYPE,
  ECOM_STYLE_PRODUCT_IDENTITY,
  ECOM_EOM_PRESALE_END_DTTM,
  ECOM_EOM_PRESALE_ID,
  ECOM_SKU_SEA_STD_SHIP_WINDOW,
  ECOM_SKU_SEA_HAZMAT,
  ECOM_SKU_COMING_SOON_DTTM,
  ECOM_KIDS_SHOE_SIZE_GRP,
  ECOM_YOUTH_AGE_RANGE,
  ECOM_STYLE_SELLING_TYPE,
  ECOM_MD_SUSTAINABILITY_DTL,
  ECOM_SHIP_LOC_RESTRICTION,
  ECOM_EM_SKU_LEAD_TIME,
  ECOM_SKU_G3_ELIGIBLE,
  ECOM_SKU_EXCLUSIVE_TO,
  ECOM_SKU_G3_NO_SHOW,
  ECOM_SKU_APE_EXCL,
  ECOM_SKU_SEA_GTGT_FLG,
  ECOM_SKU_PVA_ERROR_FLG,
  ECOM_SKU_DKS_OH_FLG
) AS
SELECT
  a.DKS_SKU_KEY AS PRODUCT_ID,
  b.sku_type_desc AS ECOM_SKU_TYPE,
  NVL(C.PIM_COLOR, '!No Value') AS ECOM_PIM_COLOR,
  D.GROUND_SHIP_CHARGE AS SEA_GROUND_SHIP_CHARGE,
  D.`1DAY_SHIP_CHARGE` AS SEA_DAY1_SHIP_CHARGE,
  D.`2DAY_SHIP_CHARGE` AS SEA_DAY2_SHIP_CHARGE,
  D.CURBSIDE_SHIP_CHARGE AS SEA_CURBSIDE_SHIP_CHARGE,
  D.THRESHOLD_SHIP_CHARGE AS SEA_THRESHOLD_SHIP_CHARGE,
  D.ROC_SHIP_CHARGE AS SEA_ROC_SHIP_CHARGE,
  D.ASSEMBLY_SHIP_CHARGE AS SEA_ASSEMBLY_SHIP_CHARGE,
  CASE d.ship_class
    WHEN 'LTL'
    THEN COALESCE(curbside_ship_charge, threshold_ship_charge, roc_ship_charge, assembly_shiP_charge)
    ELSE COALESCE(ground_ship_charge, `2DAY_SHIP_CHARGE`, `1DAY_SHIP_CHARGE`)
  END AS SEA_standard_ship_charge,
  CASE UPPER(d.ship_class)
    WHEN 'P'
    THEN 'Parcel'
    WHEN 'OP'
    THEN 'OverSize Parcel'
    ELSE NVL(d.ship_class, 'No Value')
  END AS SEA_ship_class,
  NVL(C.ALT_IMAGE_CNT, 0) AS ECOM_PIM_COLOR_ALT_IMG_CNT,
  NVL(C.SW_IMAGE_CNT, 0) AS ECOM_PIM_COLOR_SWATCH_IMG_CNT,
  NVL(d.vn_active, 'N') AS ECOM_VDC_ACTIVE,
  NVL(A.PIM_VDC_ELIGIBLE_FLG, 'N') AS ECOM_PIM_vdc_eligibile_flg,
  d.facility_number AS ECOM_VDC_VENDOR_NUMBER,
  d.facility_name AS ECOM_VDC_VENDOR_NAME,
  WEB_PRIMARY_UPC AS ECOM_WEB_PRIMARY_UPC,
  NVL(C.PRODUCT_DISPLAY_DTTM,
  CAST('1900-01-01 00:00:00' AS TIMESTAMP)) AS ECOM_PIM_PRODUCT_DISPLAY_DTTM,
  CAST(DATE_FORMAT(C.PRODUCT_DISPLAY_DTTM, 'yyyyMMdd') AS DOUBLE) AS ECOM_PIM_PROD_DISP_DT_KEY,
  NVL(main_image_cnt, 0) AS ECOM_PIM_COLOR_MAIN_IMG_CNT,
  NVL(model_image_cnt, 0) AS ECOM_PIM_COLOR_MODEL_IMG_CNT,
  NVL(model_main_image_cnt, 0) AS ECOM_PIM_COLR_MAIN_MOD_IMG_CNT,
  NVL(is_ship_restricted, 'N') AS SEA_IS_SHIP_RESTRICTED,
  NVL(sfs_eligible_Flg, 'N') AS ECOM_SFS_ELIGIBLE_FLG,
  NVL(rdc_eligible_flg, 'N') AS ECOM_RDC_ELIGIBLE_FLG,
  NVL(E.EMAST_PIM_ENTITY_ID, -1) AS ECOM_EMAST_PIM_ENTITY_ID,
  NVL(E.STYLE_STATUS_EODS, 'S') AS ECOM_ODS_STYLE_STATUS_DETAIL,
  NVL(c.sports_team, '!No Value') AS ECOM_SKU_SPORTS_TEAM,
  CASE SUBSTR(style_status_eods, 1, 1) WHEN 'A' THEN 'Active' ELSE 'Inactive' END AS eods_style_status_grp,
  E.ORIGINAL_RETAIL,
  NVL(A.ROUTE_PO_FLAG, '!No Value') AS ECOM_DDW_ROUTE_PO,
  NVL(PIM_PRODUCT_EMAST_COLOR_KEY, -1) AS ECOM_EMAST_PIM_PROD_COLOR_KEY,
  NVL(em_sku_presale_flg, 'N') AS ECOM_SKU_EM_PRESALE_FLG,
  NVL(c.color_family, '!No Value') AS ECOM_SKU_COLOR_FAMILY,
  c.presale_end_date,
  NVL(DATE_TRUNC('DAY', C.PRODUCT_DISPLAY_DTTM), TO_DATE('01-Jan-1900','dd-MMM-yyyy')) AS ECOM_SKU_PIM_PROD_DISPLAY_DATE,
  'A' AS SKU_MDM_STATUS,
  CURRENT_TIMESTAMP() AS SKU_MDM_LAST_DELETED_DTTM,
  D.SKU_HEIGHT AS ECOM_SKU_HEIGHT,
  D.SKU_LENGTH AS ECOM_SKU_LENGTH,
  D.SKU_WEIGHT AS ECOM_SKU_WEIGHT,
  D.SKU_WIDTH AS ECOM_SKU_WIDTH,
  CASE D.DIM_TYPE
    WHEN 'A'
    THEN 'Actual'
    WHEN 'C'
    THEN 'Average'
    ELSE NVL(d.dim_type, '!No Value')
  END AS ECOM_SKU_DIM_TYPE,
  NVL(e.pim_product_identity, '!No Value') AS ECOM_STYLE_PRODUCT_IDENTITY,
  eom_presale_end_dttm AS ECOM_EOM_PRESALE_END_DTTM,
  presale_id AS ECOM_EOM_PRESALE_ID,
  CASE d.ship_class
    WHEN 'LTL'
    THEN COALESCE(curbside_shp_window, threshold_shp_window, roc_shp_window, assembly_shp_window)
    ELSE COALESCE(ground_shp_window, `2DAY_SHP_WINDOW`, `1DAY_SHP_WINDOW`)
  END AS SEA_standard_ship_window,
  NVL(D.HAZMAT_FLAG, 'N') AS ECOM_SKU_SEA_HAZMAT,
  C.COMING_SOON_END_DTTM AS COMING_SOON_END_DTTM,
  NVL(kids_shoe_size_grp, '!No Value') AS ECOM_KIDS_SHOE_SIZE_GRP,
  NVL(C.YOUTH_AGE_RANGE, '!No Value') AS ECOM_YOUTH_AGE_RANGE,
  CASE
    WHEN ph.division_number = 12
    THEN 'TEAM ROOM'
    WHEN ph.sub_department_number IN (99)
    THEN 'AUDIT'
    WHEN ph.sub_department_number IN (98) OR ph.class_number IN (98)
    THEN 'RESERVE'
    WHEN ph.department_number IN ('157', '222')
    OR prod_ecmde_db.ecom.custom_concat_ifnull(ARRAY(LPAD(ph.department_number, 3, '0'),'.',LPAD(ph.sub_department_number, 3, '0'))) IN ('135.008', '130.010')
    THEN 'USED/PREOWNED'
    WHEN UPPER(sub_department_description) LIKE '%SPEC%ORD%'
    OR UPPER(class_description) LIKE '%SPEC%ORD%'
    OR UPPER(sub_class_description) LIKE '%SPEC%ORD%'
    THEN 'SPECIAL ORDER'
    WHEN UPPER(class_description) LIKE '%DISP%DEMO%'
    THEN 'DISPLAY/DEMO'
    WHEN UPPER(class_description) LIKE '%MSR%'
    THEN 'DISCONTINUED'
    WHEN ph.division_description IN ('NOT AVAILABLE')
    OR ph.department_number < 100
    OR ph.department_number > 900
    THEN 'NON-MERCH'
    ELSE 'SELLING STOCK'
  END AS style_selling_type,
  NVL(md_sustainability, '!No Value') AS ECOM_MD_SUSTAINABILITY_DTL,
  d.location_id,
  c.em_sku_lead_time,
  NVL(C.EM_SKU_G3_ELIGIBLE_FLG, 'N') AS ECOM_SKU_G3_ELIGIBLE,
  NVL(em_Sku_exclusive_to, '!No Value') AS ECOM_SKU_EXCLUSIVE_TO,
  NVL(C.EM_SKU_G3_not_ELIGIBLE_FLG, 'N') AS ECOM_SKU_G3_NO_SHOW,
  NVL(ape_exclusive, '!No Value') AS ECOM_SKU_APE_EXCLUSIVE,
  NVL(d.gtgt_eligible, 'N') AS ECOM_SKU_SEA_GTGT_FLG,
  NVL(c.em_sku_pva_error_flg, 'N') AS ECOM_SKU_PVA_ERROR_FLG,
  CASE WHEN du.store_onhand_qty > 0 OR du.ecom_dc_oh_qty > 0 THEN 'Y' ELSE 'N' END AS ECOM_SKU_DKS_OH_FLG
FROM prod_ecmde_db.ecom_dim.DKS_SKU AS a
LEFT JOIN prod_ecmde_db.ecom_dim.sku_type AS b ON A.SKU_TYPE = B.SKU_TYPE_CODE
LEFT JOIN prod_ecmde_db.ecom_dim.dks_Sku_pim AS c ON A.dks_Sku_key = c.dks_Sku_key
LEFT JOIN prod_ecmde_db.ECOM_DIM.DKS_SKU_ship AS d ON A.dks_Sku_key = d.dks_Sku_key
JOIN prod_ecmde_db.ecom_dim.style AS e ON A.style_key = e.style_key
LEFT JOIN (
  SELECT
    prod_ecmde_db.ecom_dim.GET_NUMBER_FROM_STRING(item_name) AS item_name,
    MIN(availability_dttm) AS eom_presale_end_dttm,
    MIN(object_id) AS presale_id
  FROM prod_ecmde_db.ECOM.STG_EOM_PRESALE_INVENTORY
  WHERE disposition_id = 16619
  GROUP BY item_name
) AS f ON A.dks_Sku_code = f.item_name
JOIN prod_ecmde_db.ecom_Dim.product_hierarchy AS ph ON e.product_hierarchy_key = ph.product_hierarchy_key
LEFT JOIN (
  SELECT
    mdm_entity_id,
    array_join(collect_list(attribute_value),',') AS md_sustainability
  FROM (
    SELECT
      mdm_entity_id,
      attribute_value,
      sort_id
    FROM prod_ecmde_db.ECOM_DIM.STG_dks_MASTER_CATALOG_ATTR_mu
    WHERE attribute_id = 5367 AND mdm_record_status = 'A'
    ORDER BY sort_id NULLS LAST
  )
  GROUP BY mdm_entity_id
) AS mu ON e.pim_entity_id = mu.mdm_entity_id
LEFT JOIN prod_ecmde_db.ecom_dim.dks_sku_units AS du ON A.dks_Sku_key = du.dks_Sku_key
WHERE
  color_code <> '9999'
  AND a.record_Status = 'A'
  AND e.record_status = 'A'
  AND ph.record_status = 'A';
