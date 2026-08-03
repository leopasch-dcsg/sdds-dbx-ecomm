-- RADEF-2850: order_service_line table enhancements for reporting
-- Add cancel_units and cancel_price columns, remove csr_agent column

ALTER TABLE ${ecom_dim_schema}.order_service_line
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE ${ecom_dim_schema}.order_service_line
ADD COLUMNS (
    cancel_units INT COMMENT 'Units cancelled for this record',
    cancel_price DECIMAL(18,2) COMMENT 'Price of the cancelled items'
);

ALTER TABLE ${ecom_dim_schema}.order_service_line
ALTER COLUMN cancel_units AFTER return_price;

ALTER TABLE ${ecom_dim_schema}.order_service_line
ALTER COLUMN cancel_price AFTER cancel_units;

ALTER TABLE ${ecom_dim_schema}.order_service_line
DROP COLUMN csr_agent;
