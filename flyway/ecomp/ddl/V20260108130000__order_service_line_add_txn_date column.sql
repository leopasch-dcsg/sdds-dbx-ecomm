ALTER TABLE ${ecom_dim_schema}.order_service_line
ADD COLUMNS (
    transaction_date TIMESTAMP COMMENT 'Transaction date/time for this service line (UTC)'
)  ;

ALTER TABLE ${ecom_dim_schema}.order_service_line
ALTER COLUMN transaction_date AFTER service_type;
