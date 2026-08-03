CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_ecom_order_sku_vw
(
  ECOM_ORDER_SKU_KEY,
  ECOM_OS_FF_CHANNEL_KEY,
  ECOM_OS_CLEARANCE_TYPE_KEY,
  ECOM_OS_WEB_PRICE_ORDER,
  ECOM_OS_AVG_COST_PER_UNIT_ORD,
  ECOM_OS_CUSTOMER_EDD_KEY,
  ECOM_OS_WEB_SUBMIT_DTTM,
  ECOM_OS_VDC_PO_NUMBER,
  ECOM_OS_SHIP_TO_STATE,
  ECOM_OS_SHIP_TO_ZIP,
  ECOM_OS_CUST_REQ_FF_MODE_KEY,
  ECOM_OS_PRIMARY_DISCOUNT_DTL,
  ECOM_OS_PRIMARY_DISCOUNT_LGRP,
  ECOM_OS_PRIMARY_DISCOUNT_HGRP,
  OS_DATE_LAST_MODIFIED,
  ECOM_ODS_ORDER_SKU_STATUS_KEY,
  ECOM_OS_HOT_MARKET_FLG,
  ECOM_OS_PRESALE_FLG,
  ECOM_OS_MIN_DO_CREATE_DTTM,
  ECOM_OS_SPECIAL_ORDER_FLG,
  ECOM_OS_ACTUAL_FF_CHANNEL_KEY,
  ECOM_OS_ACTUAL_FF_LOCATION_CNT,
  ECOM_OS_IN_CART_ADJ_FLG,
  ECOM_OS_PRODUCT_ID,
  ECOM_OS_WEB_ORDER_NUM,
  ECOM_OS_CLR_COLOR,
  ECOM_PRIMARY_DISCOUNT_KEY,
  ECOM_BOPIS_STORE,
  ECOM_BOPIS_SAVE_FLG,
  ECOM_GTGT_FLG,
  ECOM_GTGT_DATE
) AS
SELECT
  a.order_skU_key,
  a.channel_type_key AS ecom_fulfill_channel_key,
  a.clearance_type_key,
  a.web_price,
  a.average_cost,
  promise_date,
  prod_ecmde_db.ecom_Dim.GET_DATE_FROM_STRING(
    webstore_order_date,
    'yyyy/MM/dd HH:mm:ss'
  )AS ecom_os_web_submit_dttm,
  po_number,
  ship_to_state,
  SUBSTR(ship_to_zip, 1, 5) AS ecom_os_ship_to_zip,
  cust_fulfillment_mode_key,
  cast(NULL as decimal) AS ecom_os_primary_discount_dtl,
  cast(NULL as decimal) AS ecom_os_primary_discount_lgrp,
  cast(NULL as decimal) AS ecom_os_primary_discount_hgrp,
  a.date_last_modified AS date_last_modified,
  a.order_sku_status_key,
  CASE a.hot_market_flg
    WHEN 1 THEN 'Y'
    ELSE 'N'
  END AS ecom_os_hot_market_flg,
  CASE a.presale_flg
    WHEN 1 THEN 'Y'
    ELSE 'N'
  END AS ecom_os_presale_flg,
  a.min_do_create_dttm AS min_do_create_dttm,
  CASE a.order_line_type_key
    WHEN 60 THEN 'Y'
    ELSE 'N'
  END AS ecom_os_special_order_flg,
  NVL(a.actual_ff_channel_key, -1) AS ecom_os_actual_ff_channel_key,
  a.actual_ff_location_cnt,
  CASE
    WHEN a.orig_tot_ext_disc_amt <> 0 THEN 'Y'
    ELSE 'N'
  END AS ecom_os_in_cart_adj_flg,
  dks_SkU_key,
  web_ord_num,
  NVL(c.clr_color_desc, '!No Value') AS ecom_os_clr_color,
  NVL(a.margin_decomp_classification, -1) AS ecom_primary_discount_key,
  a.bopis_store,
  CASE
    WHEN a.bopis_save_the_sale_flg = 'S' THEN 'Y'
    ELSE a.bopis_save_the_sale_flg
  END AS bopis_save_the_sale_flg,
  CASE a.gtgt_ind
    WHEN 1 THEN 'Y'
    ELSE 'N'
  END AS gtgt_flg,
  a.gtgt_date AS gtgt_date
FROM
  prod_ecmde_db.ecom_dim.order_sku AS a
    LEFT JOIN prod_ecmde_db.ecom_Dim.clr_color_lkup AS c
      ON a.clr_color_code = c.clr_color_code
WHERE
  order_date_key >= 20200202
