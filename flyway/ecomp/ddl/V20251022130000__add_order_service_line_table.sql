CREATE TABLE IF NOT EXISTS ${ecom_dim_schema}.order_service_line
(
    web_ord_num        BIGINT        NOT NULL COMMENT 'Web order associated with this service line item',
    product_sku        BIGINT        NOT NULL COMMENT 'Product SKU associated with this service line item',
    webstore_key       BIGINT        NOT NULL COMMENT 'The normalized web store name key value. It maps to ecom_dim.webstore key value',
    service_sku        STRING        NOT NULL COMMENT 'Service SKU associated with the type of service',
    aos_associate_id   STRING                  COMMENT 'Associate ID field if available',
    csr_agent          STRING                  COMMENT 'CSR agent ID value if available',
    units              INT           NOT NULL COMMENT 'The quantity of units requested by the customer for this order.',
    service_price      DECIMAL(18,2) NOT NULL COMMENT 'Price of the service associated',
    service_tax        DECIMAL(18,2) NOT NULL COMMENT 'Estimated tax associated with the service',
    return_units       INT                     COMMENT 'Units returned for this record',
    return_price       DECIMAL(18,2)           COMMENT 'Price of the return items',
    service_type       STRING                  COMMENT 'Type of service',
    date_added         TIMESTAMP     NOT NULL COMMENT 'Record creation timestamp UTC',
    added_by           STRING        NOT NULL COMMENT 'Record created by',
    date_last_modified TIMESTAMP     NOT NULL COMMENT 'Last modified timestamp UTC',
    modified_by        STRING                 COMMENT 'Record last modified by',

    CONSTRAINT order_service_line_pk PRIMARY KEY (
        web_ord_num,
        product_sku,
        webstore_key,
        service_sku
    )
)
CLUSTER BY (date_added, web_ord_num, product_sku, webstore_key)
COMMENT 'This table contains the order number, SKU, associate IDs, and pricing/tax details for all services purchased.'
;
