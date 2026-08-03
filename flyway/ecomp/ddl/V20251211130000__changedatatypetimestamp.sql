
ALTER TABLE ${ecom_dim_schema}.dks_sku_pim
  SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

ALTER TABLE ${ecom_dim_schema}.dks_sku_pim
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');


ALTER TABLE ${ecom_dim_schema}.dks_sku_pim
  ADD COLUMN presale_end_date_TEMP TIMESTAMP_NTZ
    AFTER presale_end_date;

UPDATE ${ecom_dim_schema}.dks_sku_pim
SET presale_end_date_TEMP = to_timestamp_ntz(presale_end_date);

ALTER TABLE ${ecom_dim_schema}.dks_sku_pim
  DROP COLUMN presale_end_date;

ALTER TABLE ${ecom_dim_schema}.dks_sku_pim
  RENAME COLUMN presale_end_date_TEMP TO presale_end_date;



ALTER TABLE ${ecom_dim_schema}.promotion_header
  SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

ALTER TABLE ${ecom_dim_schema}.promotion_header
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');


ALTER TABLE ${ecom_dim_schema}.promotion_header
  ADD COLUMN wcs_start_date_TEMP TIMESTAMP_NTZ
    AFTER wcs_start_date;

ALTER TABLE ${ecom_dim_schema}.promotion_header
  ADD COLUMN wcs_end_date_TEMP TIMESTAMP_NTZ
    AFTER wcs_end_date;

UPDATE ${ecom_dim_schema}.promotion_header
SET wcs_start_date_TEMP = to_timestamp_ntz(wcs_start_date),
    wcs_end_date_TEMP   = to_timestamp_ntz(wcs_end_date);

ALTER TABLE ${ecom_dim_schema}.promotion_header
  DROP COLUMN wcs_start_date;

ALTER TABLE ${ecom_dim_schema}.promotion_header
  DROP COLUMN wcs_end_date;

ALTER TABLE ${ecom_dim_schema}.promotion_header
  RENAME COLUMN wcs_start_date_TEMP TO wcs_start_date;

ALTER TABLE ${ecom_dim_schema}.promotion_header
  RENAME COLUMN wcs_end_date_TEMP TO wcs_end_date;
