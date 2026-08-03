CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_ecom_ods_promo_vw
(
  ECOM_ODS_PROMOTION_KEY,
  ECOM_SOURCE_PROMOTION_ID,
  ECOM_WEBSTORE_KEY,
  ECOM_PROMOTION_SOURCE,
  ECOM_SOURCE_PROMO_DESC,
  ECOM_PROMO_DISCOUNT_APPL_LEVEL,
  ECOM_PROMO_CODED,
  PPS_EVENT_ID,
  PPS_EVENT_DESC,
  PPS_EVENT_TYPE_DESC,
  PPS_PROMO_ID,
  PPS_PROMO_DESC,
  EPIC_PROMO_TYPE,
  EPIC_COMP_FLAG,
  EPIC_MEDIA,
  EPIC_PROMO_SUB,
  EPIC_PROMO_MAIN,
  ECOM_CS_FLG,
  ECOM_FLASH_FLG,
  ECOM_ODS_DISCOUNT_GRP,
  ECOM_ODS_DISCOUNT_GRP_DETAIL,
  ECOM_PROMO_START_DT,
  ECOM_PROMO_END_DT,
  PP_PROMO_SCHEME,
  ECOM_PROMO_ADMIN_NAME,
  CUSTOM_FAN_SHOP_FLAG,
  ECOM_WCS_PROMOTION_TYPE,
  ECOM_PROMO_INCLUSIONS_TYPE,
  ECOM_MKT_CHANNEL_ADMIN_NAME,
  ECOM_WCS_PURCH_CHANNEL
) AS
SELECT
  ph.PROMOTION_KEY,
  PROMOTION_ID,
  CHAIN_KEY,
  CASE ph.data_source_key
    WHEN 16 THEN 'PPS'
    WHEN 13 THEN 'WCS'
    ELSE 'OTH'
  END AS ECOM_PROMOTION_SOURCE,
  NVL(updated_desc, PROMOTION_DESC) AS ECOM_SOURCE_PROMO_DESC,
  DISCOUNT_ORDER_LEVEL,
  PROMOTION_CLASS,
  event_id,
  event_desc,
  pp.event_type_desc,
  pp.pps_promo_id,
  pp.pps_promo_desc,
  NVL(ph.epic_promo_type, '!No Value!') AS EPIC_PROMO_TYPE,
  ph.epic_comp_flag,
  NVL(ph.epic_media, '!No Value!') AS EPIC_MEDIA,
  NVL(PS.PROMO_HIER_SUB_DESC, '!No Value!') AS EPIC_PROMO_SUB,
  NVL(PM.PROMO_HIER_MAIN_DESC, '!No Value!') AS EPIC_PROMO_MAIN,
  CASE cs_ind
    WHEN 1 THEN 'Y'
    ELSE 'N'
  END AS ECOM_CS_FLG,
  CASE flash_sale_ind
    WHEN 1 THEN 'Y'
    WHEN 2 THEN 'Y'
    ELSE 'N'
  END AS ECOM_FLASH_FLG,
  discount_group,
  discount_group_detail,
  COALESCE(
    wcs_start_date, PP.EVENT_START_DATE,
    TO_DATE(NULLIF(CAST(PH.START_DATE_KEY AS INT), -1), 'yyyyMMdd')
  ) AS ECOM_PROMO_START_DT,
  NVL(
    wcs_end_Date, PP.EVENT_END_DATE
  ) AS ECOM_PROMO_END_DT,
  PP.PPS_PROMO_SCHEME_DESC,
  upd_admin_name,
  CASE
    WHEN
      UPPER(upd_admin_name) LIKE '%FAN%SHOP%'
      OR UPPER(upd_admin_name) LIKE '%FAN%GEAR%'
    THEN
      'Y'
    ELSE 'N'
  END AS fan_shop_flag,
  wcs_promotion_type,
  CASE discount_group_detail
    WHEN 'SIMPLE' THEN 'Style/Sku'
    ELSE wcs_inclusions_type
  END AS promotion_inclusions_type,
  mkt_channel_admin_name,
  wcs_purch_cond_channel_type
FROM
  prod_ecmde_db.ecom_dim.promotion_header AS ph
    LEFT JOIN prod_ecmde_db.ECOM_dim.promotion_event AS pp
      ON PH.PPS_PROMO_ID = PP.PPS_PROMO_ID
      AND PH.PPS_EVENT_ID = pp.event_id
    LEFT JOIN prod_ecmde_db.ECOM_DIM.PROMOTION_HIERARCHY_SUB AS ps
      ON PH.PROMO_HIER_SUB_KEY = PS.PROMO_HIER_SUB_KEY
    LEFT JOIN prod_ecmde_db.ECOM_DIM.PROMOTION_HIERARCHY_main AS pm
      ON Pm.PROMO_HIER_main_KEY = PS.PROMO_HIER_main_KEY
    LEFT JOIN (
      SELECT
        a.promotioN_key,
        COALESCE(b.shortdesc, b.field2, b.field1) AS updated_desc,
        C.ADMINSTVENAME AS upd_admin_name
      FROM
        prod_ecmde_db.ecoM_Dim.promotion_header AS a
          LEFT JOIN prod_ecmde_db.WEB_STAGE.px_description AS b
            ON a.promotioN_id = b.px_promotion_id
          LEFT JOIN prod_ecmde_db.WEB_STAGE.px_promoauth AS c
            ON a.promotion_id = prod_ecmde_db.ecom_dim.GET_NUMBER_FROM_STRING(c.px_promotioN_id)
      WHERE
        chain_key IN (2, 4, 6, 7)
        AND data_source_key = 13
    ) AS upd
      ON ph.promotion_key = upd.promotion_key
WHERE
  chain_key IN (2, 4, 6, 7)
UNION ALL
SELECT
  manual_disc_key,
  NULL,
  NULL,
  CASE SUBSTR(manual_disc_desc, 1, 3)
    WHEN 'MAN' THEN 'CVCC'
    WHEN 'CSR' THEN 'SOLEPANEL'
    ELSE 'WCS'
  END,
  manual_Disc_desc,
  CASE
    WHEN manual_disc_desc LIKE '%ITEM%' THEN 'Item'
    ELSE 'Order'
  END,
  CASE SUBSTR(manual_disc_desc, 1, 3)
    WHEN 'MAN' THEN 'REP ADJ'
    WHEN 'CSR' THEN 'REP ADJ'
    ELSE 'CERTIFICATE'
  END,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  CASE SUBSTR(manual_disc_desc, 1, 3)
    WHEN 'MAN' THEN 'R'
    WHEN 'CSR' THEN 'R'
  END,
  'N',
  NULL,
  CASE SUBSTR(manual_disc_desc, 1, 3)
    WHEN 'MAN' THEN 'REP ADJUSTMENT'
    WHEN 'CSR' THEN 'REP ADJUSTMENT'
    ELSE 'LOYALTY'
  END,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
FROM
  prod_ecmde_db.ECOM_DIM.MANUAL_DISCOUNT_LKUP
UNION ALL
SELECT
  manual_disc_key + 10,
  NULL,
  NULL,
  CASE SUBSTR(manual_disc_desc, 1, 3)
    WHEN 'MAN' THEN 'CVCC'
    WHEN 'CSR' THEN 'SOLEPANEL'
    ELSE 'WCS'
  END,
  manual_Disc_desc,
  CASE
    WHEN manual_disc_desc LIKE '%ITEM%' THEN 'Item'
    ELSE 'Order'
  END,
  CASE SUBSTR(manual_disc_desc, 1, 3)
    WHEN 'MAN' THEN 'REP ADJ'
    WHEN 'CSR' THEN 'REP ADJ'
    ELSE 'CERTIFICATE'
  END,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  CASE SUBSTR(manual_disc_desc, 1, 3)
    WHEN 'MAN' THEN 'R'
    WHEN 'CSR' THEN 'R'
  END,
  'N',
  NULL,
  CASE SUBSTR(manual_disc_desc, 1, 3)
    WHEN 'MAN' THEN 'PCM'
    WHEN 'CSR' THEN 'PCM'
    ELSE 'LOYALTY'
  END,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
FROM
  prod_ecmde_db.ECOM_DIM.MANUAL_DISCOUNT_LKUP
WHERE
  manual_disc_key BETWEEN 3 AND 8
UNION ALL
SELECT
  50,
  -1,
  NULL,
  'WCS',
  'WCS Price Upload',
  'Item',
  'MANUAL UPLOAD',
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  'N',
  'N',
  NULL,
  'MANUAL UPLOAD',
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
