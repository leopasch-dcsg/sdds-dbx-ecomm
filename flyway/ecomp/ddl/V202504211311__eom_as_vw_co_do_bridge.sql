
CREATE OR REPLACE  VIEW ${ecom_schema}.VW_CO_DO_BRIDGE
(
   DISTRIBUTION_ORDER_NBR  COMMENT 'The DO numbers thats been created by eom (system generated number) ',
    SCI_ORDER_ID ,
    MIN_PURCHASE_ORDER_ID  COMMENT 'EOM system genereated id at each co level we will take the minimum purchase_order_id.',
    CUSTOMER_ORDER_NBR COMMENT 'The actual customer order number created by oso it has eom /newman order numbers with fr sequnce number added.',
    PURCHASE_ORDERS_ID  COMMENT 'The system generated Id at each co level as per eom as fulfillment partner this can be multiple per co if the realloc is sent to eom.',
    WEBSTORE  COMMENT 'Web store ordered from' ,
    CO_STATUS_ID  COMMENT 'Status id  of the CO.',
    CO_STATUS_DESCRIPTION  COMMENT 'Status desc  of the CO.',
    ORDER_CONFIRMED_DATE  COMMENT 'The order confirmed date.',
    IS_CO_CANCELLED  COMMENT 'CO cancelled flag.',
    IS_PURCHASE_ORDERS_CONFIRMED  COMMENT 'Purchase order confirmed flag.',
    INITIAL_ORDER_CREATE_DATE   COMMENT 'Intial order create date',
    ORDER_LAST_UPDATED_DATE  COMMENT 'Last update status of the order received time.',
    ORIGINAL_CUSTOMER_ORDER_NBR  COMMENT 'The customer order number without the fr number which we receive as part of EOM as fulfillment partner ',
    DO_CREATE_DTTM  COMMENT 'The customer order number without the fr number which we receive as part of EOM as fulfillment partner.',
    DO_LAST_UPDATED_DTTM  COMMENT 'Date do updated. '
)
AS

    SELECT DISTINCT
           ORDERS.DISTRIBUTION_ORDER_NBR,
           ORDERS.ORDER_ID
               AS SCI_ORDER_ID,
           MIN (
               CO.PURCHASE_ORDERS_ID)
           OVER (
               PARTITION BY COALESCE (CO_REF.REF_FIELD7,
                                      CO.CUSTOMER_ORDER_NBR)
               )
               AS MIN_PURCHASE_ORDER_ID,
           CO.CUSTOMER_ORDER_NBR,
           CO.PURCHASE_ORDERS_ID,
           CO.WEBSTORE,
           CO.CO_STATUS_ID,
           CO.CO_STATUS_DESCRIPTION,
           CO.ORDER_CONFIRMED_DATE,
           CO.IS_CO_CANCELLED,
           CO.IS_PURCHASE_ORDERS_CONFIRMED,
           CO.INITIAL_ORDER_CREATE_DATE,
           CO.ORDER_LAST_UPDATED_DATE,
           CO_REF.REF_FIELD7
               AS ORIGINAL_CUSTOMER_ORDER_NBR,
           ORDERS.DO_CREATE_DTTM,
           ORDERS.DO_LAST_UPDATED_DTTM
      FROM ${ecomp_catalog}.ECOM.EOM_PURCHASE_ORDERS  co
           LEFT JOIN ${ecomp_catalog}.DOMP.DOM_PO_REF_FIELDS co_ref
               ON co.PURCHASE_ORDERS_ID = co_ref.PURCHASE_ORDERS_ID
           LEFT JOIN ${ecomp_catalog}.ECOM.EOM_ORDERS orders
               ON co.PURCHASE_ORDERS_ID = orders.PURCHASE_ORDER_ID;