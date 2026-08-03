CREATE TABLE  ${ecom_schema}.CO_DO_BRIDGE_INCREMENTAL(
    DISTRIBUTION_ORDER_NBR VARCHAR(50) COMMENT 'The DO numbers thats been created by eom (system generated number) ',
    SCI_ORDER_ID DECIMAL(18,0) COMMENT 'Auto increment key created by OSO when they created DO number ',
    MIN_PURCHASE_ORDER_ID DECIMAL(18,0)  NOT NULL  COMMENT 'EOM system genereated id at each co level we will take the minimum purchase_order_id.',
    CUSTOMER_ORDER_NBR VARCHAR(50)  NOT NULL COMMENT 'The actual Athlete order number created by oso it has eom /newman order numbers with fr sequnce number added.',
    PURCHASE_ORDERS_ID   DECIMAL(18,0) NOT NULL COMMENT 'The system generated Id at each co level as per eom as fulfillment partner this can be multiple per co if the realloc is sent to eom.',
    WEBSTORE VARCHAR(50)  COMMENT 'Webstore Athlete ordered.' ,
    CO_STATUS_ID  DECIMAL(18,0) NOT NULL  COMMENT 'Status id  of the CO.',
    CO_STATUS_DESCRIPTION   VARCHAR(100)  NOT NULL  COMMENT 'Status desc  of the CO.',
    ORDER_CONFIRMED_DATE  TIMESTAMP NOT NULL COMMENT 'The order confirmed date.',
    IS_CO_CANCELLED  INT NOT NULL COMMENT 'CO cancelled flag.',
    IS_PURCHASE_ORDERS_CONFIRMED  INT NOT NULL COMMENT 'Purchase order confirmed flag.',
    INITIAL_ORDER_CREATE_DATE  TIMESTAMP  NOT NULL COMMENT 'Intial order create date',
    ORDER_LAST_UPDATED_DATE  TIMESTAMP  NOT NULL COMMENT 'Last update status of the order received time.',
    ORIGINAL_CUSTOMER_ORDER_NBR VARCHAR(50)  COMMENT 'The Athlete order number without the fr number which we receive as part of EOM as fulfillment partner ',
    DO_CREATE_DTTM TIMESTAMP  COMMENT 'The Athlete order number without the fr number which we receive as part of EOM as fulfillment partner.',
    DO_LAST_UPDATED_DTTM  TIMESTAMP COMMENT 'Date do updated. '  
)  CLUSTER BY (CUSTOMER_ORDER_NBR,DISTRIBUTION_ORDER_NBR)
COMMENT 'The table has all the DO numbers for  CO (including the  fr number) for EOM as fr';