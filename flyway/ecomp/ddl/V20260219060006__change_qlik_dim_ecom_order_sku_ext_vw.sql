CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_ecom_order_sku_ext_vw
(
  ECOM_ORDER_SKU_KEY,
  ECOM_OS_PRIMARY_DISCOUNT_DTL,
  ECOM_OS_PRIMARY_DISCOUNT_LGRP,
  ECOM_OS_PRIMARY_DISCOUNT_HGRP,
  OS_DATE_LAST_MODIFIED,
  ECOM_PRIMARY_DISCOUNT_KEY,
  ECOM_PRIMARY_DISC_PROMO_ID,
  ECOM_PRIMARY_DISC_PROMO_DESC,
  ECOM_PRIMARY_DISC_EVENT
) AS
SELECT
  a.order_sku_key,
  CASE
    WHEN
      a.MARGIN_DECOMP_CLASSIFICATION > 0
    THEN
      CASE d.discount_group_detail
        WHEN 'SITEWIDE' THEN '02.1-Global Distro'
        ELSE '02.2-Limited Distro'
      END
    WHEN
      a.MARGIN_DECOMP_CLASSIFICATION = -65
    THEN
      prod_ecmde_db.ecom.custom_concat_ifnull(
        ARRAY(
          b.MARGIN_DECOMP_CLASS_CODE,
          ': ',
          SUBSTR(c.clr_color_desc, 1, LENGTH(c.clr_color_desc) - 3)
        )
      )
    ELSE b.MARGIN_DECOMP_CLASS_CODE
  END AS ECOM_OS_PRIMARY_DISCOUNT_DTL,
  CASE
    WHEN
      a.MARGIN_DECOMP_CLASSIFICATION > 0
    THEN
      CASE d.discount_group_detail
        WHEN 'SITEWIDE' THEN '02.1-Global Distro'
        ELSE '02.2-Limited Distro'
      END
    ELSE b.MARGIN_DECOMP_CLASS_low_grp
  END AS ECOM_OS_PRIMARY_DISCOUNT_LGRP,
  CASE
    WHEN a.MARGIN_DECOMP_CLASSIFICATION > 0 THEN '02-Large Scale Promo'
    ELSE b.MARGIN_DECOMP_CLASS_high_grp
  END AS ECOM_OS_PRIMARY_DISCOUNT_HGRP,
  a.date_last_modified AS date_last_modified,
  NVL(a.MARGIN_DECOMP_CLASSIFICATION, -1) AS ECOM_PRIMARY_DISCOUNT_KEY,
  e.md_promotion_id,
  e.md_promotion_desc,
  e.md_event_name
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_SKU AS a
    LEFT JOIN prod_ecmde_db.ECOM_DIM.MARGIN_DECOMP_CLASS_LKUP AS b
      ON NVL(a.MARGIN_DECOMP_CLASSIFICATION, -1) = b.MARGIN_DECOMP_CLASS_KEY
    LEFT JOIN prod_ecmde_db.ecom_Dim.clr_color_lkup AS c
      ON a.clr_color_code = c.clr_color_code
    LEFT JOIN prod_ecmde_db.ecom_Dim.promotioN_header AS d
      ON a.MARGIN_DECOMP_CLASSIFICATION = d.promotion_key
    LEFT JOIN prod_ecmde_db.ecom_Dim.order_sku_flash_event AS e
      ON a.order_sku_key = e.order_sku_key
WHERE
  order_date_key >= 20200202
