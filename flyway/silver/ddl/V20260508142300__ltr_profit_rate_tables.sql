CREATE TABLE ltr_abb_profit_orders (
    bod_inv_date_est DATE COMMENT 'Beginning-of-day inventory date (EST) — date column for rolling lookback window filtering downstream',
    tran_date DATE COMMENT 'Transaction date',
    inv_date_est DATE COMMENT 'Inventory date (EST); 7 AM Eastern cutoff — orders before 7 AM use prior day''s BOD pricing',
    search_term STRING COMMENT 'Normalized search query',
    ecode STRING COMMENT 'Product identifier',
    sku STRING COMMENT 'SKU',
    hitid STRING COMMENT 'Hit identifier',
    report_suite STRING COMMENT 'Report suite identifier',
    webstore_key STRING COMMENT 'Webstore key',
    mcvisid STRING COMMENT 'Adobe Cloud visitor ID',
    visit_id STRING COMMENT 'Visit identifier',
    order_id STRING COMMENT 'Order identifier',
    units DOUBLE COMMENT 'Units ordered',
    revenue DOUBLE COMMENT 'Revenue from the order',
    evar58 STRING COMMENT 'Last-touch attribution eVar',
    evar33 STRING COMMENT 'First-touch attribution eVar',
    order_time TIMESTAMP COMMENT 'Order timestamp (EST)',
    avg_web_price DOUBLE COMMENT 'BOD web price for the SKU',
    avg_web_cost DOUBLE COMMENT 'SKU-level cost (actual order cost or rollup)',
    profit DOUBLE COMMENT 'revenue - (avg_web_cost × units); can be negative'
)
    USING delta
    COMMENT 'This table contains searches, orders resulting from those searches, and the observed revenue/profit from each order. Also includes metadata such
            as order timestamps. This table is a prerequisite for the downstream positive profit rate feature query'
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_clickstream (
    tran_date DATE,
    inv_date_est DATE,
    ecode STRING,
    _sku BIGINT,
    _report_suite STRING,
    _webstore_key INT,
    mcvisid STRING,
    _visit_id STRING,
    _order_id BIGINT,
    hitid STRING,
    duplicate_purchase STRING,
    _units DOUBLE,
    _revenue DOUBLE,
    _evar2 STRING,
    _event_evar2_instance DOUBLE,
    _evar3 STRING,
    dym_search_term STRING,
    _evar58 STRING,
    _evar33 STRING,
    order_time_est TIMESTAMP
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_dym_mapping (
    raw_search_term STRING,
    dym_search_term STRING
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_orders_filtered (
    tran_date DATE,
    inv_date_est DATE,
    search_term STRING,
    ecode STRING,
    _sku BIGINT,
    hitid STRING,
    _report_suite STRING,
    _webstore_key INT,
    mcvisid STRING,
    _visit_id STRING,
    _order_id BIGINT,
    _units DOUBLE,
    _revenue DOUBLE,
    _evar33 STRING,
    _evar58 STRING,
    order_time_est TIMESTAMP
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_base_attrs (
    product_id BIGINT,
    sku BIGINT,
    product_identifying STRING,
    ecode STRING,
    brand STRING,
    curr_sku_average_cost DECIMAL(38,4)
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_sku_cost_rollup (
    product_id BIGINT,
    sku BIGINT,
    product_identifying STRING,
    ecode STRING,
    curr_sku_average_cost DECIMAL(38,4)
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_sku_costs (
    bod_inv_date_est DATE,
    product_id BIGINT,
    ecode STRING,
    avg_web_price DECIMAL(22,6),
    avg_web_cost DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_ecode_costs (
    ecode STRING,
    bod_inv_date_est DATE,
    avg_web_cost_ecode_level DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_profit (
    bod_inv_date_est DATE,
    tran_date DATE,
    inv_date_est DATE,
    search_term STRING,
    ecode STRING,
    _sku BIGINT,
    hitid STRING,
    _report_suite STRING,
    _webstore_key INT,
    mcvisid STRING,
    _visit_id STRING,
    _order_id BIGINT,
    _units DOUBLE,
    _revenue DOUBLE,
    _evar33 STRING,
    _evar58 STRING,
    order_time_est TIMESTAMP,
    avg_web_price DECIMAL(22,6),
    avg_web_cost DOUBLE,
    profit DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_abb_positive_profit_rate (
    lagged_feature_date_est DATE,
    search_term STRING,
    ecode STRING,
    time_decay_positive_profit_rate_posterior DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_profit_rate_dates (
    feature_date_est DATE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_config_filtered_profit_orders (
    bod_inv_date_est DATE,
    tran_date DATE,
    inv_date_est DATE,
    search_term STRING,
    ecode STRING,
    sku STRING,
    hitid STRING,
    report_suite STRING,
    webstore_key STRING,
    mcvisid STRING,
    visit_id STRING,
    order_id STRING,
    units DOUBLE,
    revenue DOUBLE,
    evar58 STRING,
    evar33 STRING,
    order_time TIMESTAMP,
    avg_web_price DOUBLE,
    avg_web_cost DOUBLE,
    profit DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_all_orders (
    bod_inv_date_est DATE,
    report_suite STRING,
    webstore_key STRING,
    mcvisid STRING,
    visit_id STRING,
    order_id STRING,
    order_time_est TIMESTAMP,
    ecode STRING,
    sku STRING,
    search_term STRING,
    profit DOUBLE,
    prev_order_time_est TIMESTAMP
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_profit_orders_linked_searches (
    search_time_est TIMESTAMP,
    mc_visitor_id STRING,
    visit_id STRING,
    search_term STRING,
    ecode_impression STRING
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_attributed_orders (
    bod_inv_date_est DATE,
    webstore_key STRING,
    order_id STRING,
    search_time_est TIMESTAMP,
    search_date_est DATE,
    order_time_est TIMESTAMP,
    ecode STRING,
    sku STRING,
    search_term STRING,
    profit DOUBLE,
    positive_profit DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_windowed_orders (
    lagged_feature_date_est DATE,
    bod_inv_date_est DATE,
    webstore_key STRING,
    order_id STRING,
    search_time_est TIMESTAMP,
    search_date_est DATE,
    order_time_est TIMESTAMP,
    ecode STRING,
    sku STRING,
    search_term STRING,
    profit DOUBLE,
    positive_profit DOUBLE,
    signal_age INT
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_time_decayed_orders (
    lagged_feature_date_est DATE,
    search_time_est TIMESTAMP,
    ecode STRING,
    search_term STRING,
    positive_profit DOUBLE,
    signal_age INT,
    time_decay_weight DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_feature_date_aggs (
    lagged_feature_date_est DATE,
    search_term STRING,
    ecode STRING,
    time_decay_positive_profit_order_count DOUBLE,
    sorted_struct ARRAY<STRUCT<positive_profit: DOUBLE, search_time_est: TIMESTAMP, signal_age: INT, time_decay_weight: DOUBLE>>
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_positive_profit_arrays (
    lagged_feature_date_est DATE,
    search_term STRING,
    ecode STRING,
    positive_profit_values ARRAY<DOUBLE>,
    time_decay_positive_profit_update_weights ARRAY<DOUBLE>
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_rolling_impressions (
    feature_date_est DATE,
    search_term STRING,
    ecode STRING,
    impression_signal_age ARRAY<BIGINT>
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_time_decayed_impressions (
    feature_date_est DATE,
    search_term STRING,
    ecode STRING,
    time_decay_impression_count DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_joined_features (
    lagged_feature_date_est DATE,
    search_term STRING,
    ecode STRING,
    time_decay_impression_count DOUBLE,
    time_decay_positive_profit_order_count DOUBLE,
    positive_profit_values ARRAY<DOUBLE>,
    time_decay_positive_profit_update_weights ARRAY<DOUBLE>
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_posterior_updates (
    lagged_feature_date_est DATE,
    search_term STRING,
    ecode STRING,
    time_decay_positive_profit_rate_posterior DOUBLE
)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.columnMapping.mode' = 'name',
        'delta.minWriterVersion' = '7');


ALTER TABLE ltr_feature_agg ADD COLUMN positive_profit_rate MAP<STRING, DOUBLE> after avg_web_price_atc_z_score_shifted;