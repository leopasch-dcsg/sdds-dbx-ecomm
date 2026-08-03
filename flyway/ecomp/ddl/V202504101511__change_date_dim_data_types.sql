ALTER TABLE ${ecom_dim_schema}.date_dim
SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

ALTER TABLE ${ecom_dim_schema}.date_dim SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE ${ecom_dim_schema}.date_dim ADD COLUMN calendar_date_TEMP TIMESTAMP_NTZ
    after calendar_date;

ALTER TABLE ${ecom_dim_schema}.date_dim ADD COLUMN date_code_TEMP DATE
    after date_code;

UPDATE ${ecom_dim_schema}.date_dim
SET calendar_date_TEMP = to_timestamp_ntz(to_date(calendar_date));

UPDATE ${ecom_dim_schema}.date_dim
SET date_code_TEMP = to_date(date_code);

ALTER TABLE ${ecom_dim_schema}.date_dim
DROP COLUMN calendar_date;

ALTER TABLE ${ecom_dim_schema}.date_dim
DROP COLUMN date_code;

ALTER TABLE ${ecom_dim_schema}.date_dim
RENAME COLUMN calendar_date_TEMP TO calendar_date;

ALTER TABLE ${ecom_dim_schema}.date_dim
RENAME COLUMN date_code_TEMP TO date_code;