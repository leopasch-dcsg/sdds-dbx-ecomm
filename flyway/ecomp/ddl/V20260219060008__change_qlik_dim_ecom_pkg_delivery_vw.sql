CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_ecom_pkg_delivery_vw
(
  ECOM_PKG_DELIVERY_KEY,
  ECOM_PKG_TRACKING_NUMBER,
  ECOM_PKG_DELIVERY_STATUS_CD,
  ECOM_PKG_SHIP_DATE_KEY,
  ECOM_PKG_FULFILL_LOCATION_CD,
  ECOM_PKG_SHIP_DTTM,
  ECOM_PKG_TYPE,
  ECOM_PKG_CARRIER_KEY,
  ECOM_PKG_FULFILL_MODE_KEY,
  ECOM_PKG_LOCAL_DELIV_DATE_KEY,
  ECOM_PKG_LOCAL_ORIGIN_DATE_KEY,
  ECOM_PKG_SCI_LPN_ID,
  ECOM_PKG_DISTR_ORDER_NBR,
  ECOM_PKG_SETTLE_RELEASE_NBR,
  ECOM_PKG_LOCAL_DELIVERY_DTTM,
  ECOM_PKG_LOCAL_ORIGIN_DTTM,
  ECOM_PKG_UPS_ZONE,
  ECOM_PKG_MANIFEST_DTTM,
  ECOM_PKG_LOCAL_ATT_DELIV_DTTM,
  ECOM_PKG_LOCAL_USPS_DTTM,
  ECOM_PKG_LOCAL_MANIFEST_DTTM,
  ECOM_PKG_SHIP_UP_DOWN_GRADE_CD,
  OD_DATE_LAST_MODIFIED,
  ECOM_PKG_EASTERN_DELIVERY_DTTM,
  ECOM_PKG_CARRIER_BILLED_LBS,
  ECOM_PKG_LPN_CREATE_DTTM,
  ECOM_PKG_PARCEL_ZONE,
  ECOM_SHIP_UNITS_TYPE,
  ECOM_FREIGHT_APPLIED_FLG,
  ECOM_PKG_ADD_HANDLING_FLG
) AS
SELECT
  order_delivery_key,
  NVL(a.tracking_number, 'Unknown') AS ecom_pkg_tracking_number,
  delivery_status_cd,
  a.fulfillment_date_key,
  a.fulfillment_location_cd,
  actual_shipped_dttm,
  package_type_descr,
  a.carrier_key,
  a.fulfillment_mode_key,
  a.delivery_date_key,
  a.pickup_date_key,
  sci_lpn_id,
  a.order_fulfill_number,
  release_number,
  delivery_dttm,
  origin_scan_dttm,
  ups_zone,
  manifest_dttm,
  attempted_delivery_dttm,
  usps_scan_dttm,
  manifest_scan_dttm,
  ship_upgrade_cd,
  NVL(a.date_last_modified, a.date_added) AS od_date_last_modified,
  NVL(attempted_DELIVERY_DTTM, a.DELIVERY_DTTM) AS ecom_pkg_eastern_delivery_dttm,
  /* prod_ecmde_db.ECOM.get_local_time ( */
  /* NVL (attempted_DELIVERY_DTTM, a.DELIVERY_DTTM), */
  /* delivery_scan_state), */
  A.CARRIER_BILLED_WEIGHT_LBS,
  A.LPN_CREATE_DTTM AS LPN_CREATE_DTTM,
  CASE NVL(ups_zone, 'xxxx')
    WHEN 'xxxx' THEN 'Unknown'
    ELSE
      CASE prod_ecmde_db.ECOM.GET_GROUND_ZONE_EQUIVALENT(ups_zone)
        WHEN 0 THEN 'Other'
        ELSE CAST(prod_ecmde_db.ECOM.GET_GROUND_ZONE_EQUIVALENT(ups_zone) AS STRING)
      END
  END AS ecom_pkg_parcel_zone,
  CASE
    WHEN shipped_qty > 1 THEN 'Multi-Unit Box'
    WHEN shipped_qty = 1 THEN 'Single-Unit Box'
    ELSE 'Unknown'
  END AS ecom_ship_units_type,
  CASE
    WHEN NVL(carrier_freight_cost, -1) > -1 THEN 'Y'
    ELSE 'N'
  END AS freight_applied_flg,
  CASE
    WHEN carrier_adh_charge_amt > 0 THEN 'Y'
    ELSE 'N'
  END AS additional_handling_flg
FROM
  prod_ecmde_db.ECOM_DIM.ORDER_delivery AS a
WHERE
  a.fulfillment_date_key >= 20210101
  OR a.order_delivery_key = -1
