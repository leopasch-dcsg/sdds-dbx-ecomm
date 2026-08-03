insert into ${ecom_dim_schema}.fulfillment_mode
(
    fulfillment_mode_key,
    fulfillment_mode_code,
    fulfillment_mode_desc,
    data_source_key,
    date_last_modified,
    modified_by,
    added_by,
    carrier_key,
    shipping_definition,
    service_level,
    service_level_description
)
values
    (
        306,
        'STANDARD',
        'Ground',
        23,
        current_timestamp(),
        session_user(),
        session_user(),
        51,
        'Ground',
        'Ground',
        'Ground Shipping'
    ),
    (
        307,
        'PO_BOX',
        'USPS PO Box',
        23,
        current_timestamp(),
        session_user(),
        session_user(),
        51,
        'SmartPost',
        'SmartPST',
        'FedEx SmartPost'
    ),
    (
        308,
        'EXPEDITED',
        '2-Day',
        23,
        current_timestamp(),
        session_user(),
        session_user(),
        51,
        'Two-Day',
        'Two Day',
        'FedEx Two Day shipping'
    ),
    (
        309,
        'EXPRESS',
        'Next Day',
        23,
        current_timestamp(),
        session_user(),
        session_user(),
        51,
        'One-Day',
        'Next Day',
        'Next Day shipping'
    );

insert into ${ecom_dim_schema}.fulfillment_mode
(
    fulfillment_mode_key,
    fulfillment_mode_code,
    fulfillment_mode_desc,
    data_source_key,
    date_last_modified,
    modified_by,
    added_by,
    carrier_key,
    shipping_definition,
    service_level
)
values
    (
        311,
        'ROOM_OF_CHOICE',
        'LTL - Room Choice',
        23,
        current_timestamp(),
        session_user(),
        session_user(),
        -1,
        'LTL',
        'ROC'
    ),
    (
        312,
        'THRESHOLD',
        'LTL- Threshold',
        23,
        current_timestamp(),
        session_user(),
        session_user(),
        -1,
        'LTL',
        'THRESHLD'
    ),
    (
        313,
        'DELIVERY_ASSEMBLY',
        'LTL - Room of choice with assembly',
        23,
        current_timestamp(),
        session_user(),
        session_user(),
        -1,
        'LTL',
        'ASSEMBLE'
    );

insert into ${ecom_dim_schema}.fulfillment_mode
(
    fulfillment_mode_key,
    fulfillment_mode_code,
    fulfillment_mode_desc,
    data_source_key,
    date_last_modified,
    modified_by,
    added_by,
    carrier_key,
    shipping_definition,
    service_level,
    service_level_description,
    ship_mode,
    ship_class
)
values
    (
        310,
        'SAMEDAY',
        'Same Day',
        23,
        current_timestamp(),
        session_user(),
        session_user(),
        74,
        'Same Day',
        'DDSD',
        'DoorDash - AX',
        'SameDay',
        'SAMEDAY'
    );

UPDATE ${ecom_dim_schema}.fulfillment_mode
SET
    carrier_key = 51,
    shipping_definition = 'SmartPost' ,
    service_level = 'SmartPST',
    service_level_description = 'FedEx SmartPost',
    date_last_modified = current_timestamp(),
    modified_by = session_user()
WHERE fulfillment_mode_key = 86;

UPDATE ${ecom_dim_schema}.fulfillment_mode
SET
    carrier_key = 51,
    shipping_definition = 'Ground' ,
    service_level = 'Ground',
    service_level_description = 'Ground Shipping',
    date_last_modified = current_timestamp(),
    modified_by = session_user()
WHERE fulfillment_mode_key = 48;

UPDATE ${ecom_dim_schema}.fulfillment_mode
SET
    fulfillment_mode_desc= 'LTL - Curbside (Basic)',
    carrier_key = -1,
    shipping_definition = 'LTL' ,
    service_level = 'CURBSIDE',
    date_last_modified = current_timestamp(),
    modified_by = session_user()
WHERE fulfillment_mode_key = 115;