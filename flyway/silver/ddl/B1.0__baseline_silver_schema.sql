CREATE TABLE co_stage_order_message_cancel (
        message_key STRING NOT NULL,
        message_dttm TIMESTAMP NOT NULL,
        silver_created_by STRING NOT NULL,
        silver_created_on_utc TIMESTAMP NOT NULL,
        cancel_source STRING,
        cancel_agent STRING,
        cancel_dttm TIMESTAMP,
        cancel_reason STRING,
        order_number STRING NOT NULL,
        order_state STRING NOT NULL,
        order_source STRING NOT NULL,
        order_channel STRING NOT NULL,
        aosstorenumber STRING,
        order_type STRING,
        order_placed_dttm TIMESTAMP,
        order_last_update_dttm TIMESTAMP,
        identity_id STRING,
        auth_id STRING,
        loyalty_acct_id STRING,
        address1 STRING,
        address2 STRING,
        address3 STRING,
        city STRING,
        state STRING,
        zip STRING,
        country STRING,
        reward_cert_codes STRING,
        sku STRING NOT NULL,
        external_item_id STRING,
        order_line_num STRING NOT NULL,
        order_line_unit_seq STRING NOT NULL,
        order_line_state STRING NOT NULL,
        order_line_last_update_dttm TIMESTAMP,
        original_price STRING,
        discount STRING,
        purchase_price STRING,
        est_unit_tax STRING,
        upc STRING,
        est_delivery_date TIMESTAMP,
        guarenteedtogetthere_date TIMESTAMP,
        line_item_type STRING,
        ship_sku STRING,
        ship_upc STRING,
        ship_mode STRING,
        ship_location_id STRING,
        ship_carrier STRING,
        ship_class STRING,
        original_ship_charge STRING,
        ship_discount STRING,
        ship_charge STRING,
        ship_address1 STRING,
        ship_address2 STRING,
        ship_address3 STRING,
        ship_city STRING,
        ship_state STRING,
        ship_zip STRING,
        tax_product_code STRING,
        est_ship_tax_shp_dtl STRING,
        date_added TIMESTAMP,
        unit_cancel_source STRING,
        unit_cancel_dttm TIMESTAMP,
        unit_cancel_reason STRING,
        CONSTRAINT `co_stage_order_message_cancel_pk` PRIMARY KEY (`order_number`, `order_source`, `order_state`, `order_line_state`, `order_channel`, `sku`, `order_line_num`, `order_line_unit_seq`))
    USING delta
    COMMENT 'This table records orders that have been cancelled.'
    CLUSTER BY (silver_created_on_utc,order_placed_dttm,cancel_dttm,cancel_source)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE quarantine_reasons (
        code INT NOT NULL COMMENT 'The reason code for why a record was quarantined.',
        description STRING NOT NULL COMMENT 'A human readable description of why a record was quarantined.',
        created_on_utc TIMESTAMP DEFAULT current_timestamp COMMENT 'The UTC timestamp when the record was created.',
        created_by STRING NOT NULL COMMENT 'The user ID of the individual that create the record.',
        CONSTRAINT `quarantine_reasons_pk` PRIMARY KEY (`code`))
    USING delta
    COMMENT 'This table records the list of reason codes for why a record would be quarantined.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.allowColumnDefaults' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE co_stage_order_message_cancel_quarantine (
        message_key STRING NOT NULL,
        quarantine_code INT NOT NULL,
        message_dttm TIMESTAMP NOT NULL,
        silver_created_by STRING NOT NULL,
        silver_created_on_utc TIMESTAMP NOT NULL,
        order_number STRING NOT NULL,
        order_state STRING NOT NULL,
        order_source STRING NOT NULL,
        order_channel STRING NOT NULL,
        aosstorenumber STRING,
        order_type STRING,
        order_placed_dttm TIMESTAMP,
        order_last_update_dttm TIMESTAMP,
        identity_id STRING,
        auth_id STRING,
        loyalty_acct_id STRING,
        address1 STRING,
        address2 STRING,
        address3 STRING,
        city STRING,
        state STRING,
        zip STRING,
        country STRING,
        reward_cert_codes STRING,
        sku STRING NOT NULL,
        external_item_id STRING,
        order_line_num STRING NOT NULL,
        order_line_unit_seq STRING NOT NULL,
        order_line_state STRING NOT NULL,
        order_line_last_update_dttm TIMESTAMP,
        original_price STRING,
        discount STRING,
        purchase_price STRING,
        est_unit_tax STRING,
        upc STRING,
        est_delivery_date TIMESTAMP,
        guarenteedtogetthere_date TIMESTAMP,
        line_item_type STRING,
        ship_sku STRING,
        ship_upc STRING,
        ship_mode STRING,
        ship_location_id STRING,
        ship_carrier STRING,
        ship_class STRING,
        original_ship_charge STRING,
        ship_discount STRING,
        ship_charge STRING,
        ship_address1 STRING,
        ship_address2 STRING,
        ship_address3 STRING,
        ship_city STRING,
        ship_state STRING,
        ship_zip STRING,
        tax_product_code STRING,
        est_ship_tax_shp_dtl STRING,
        cancel_source STRING,
        cancel_agent STRING,
        cancel_dttm TIMESTAMP,
        cancel_reason STRING,
        date_added TIMESTAMP,
        unit_cancel_source STRING,
        unit_cancel_dttm TIMESTAMP,
        unit_cancel_reason STRING,
        CONSTRAINT `co_stage_order_message_cancel_quarantine_pk` PRIMARY KEY (`order_number`, `order_source`, `order_state`, `order_line_state`, `order_channel`, `sku`, `order_line_num`, `order_line_unit_seq`),
        CONSTRAINT `order_message_cancel_fk01` FOREIGN KEY (`quarantine_code`) REFERENCES quarantine_reasons (`code`))
    USING delta
    COMMENT 'This table records orders that have been cancelled but have been placed in quarantine due to malformed or incorrect data.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE co_stage_order_message_discount (
        message_key STRING NOT NULL,
        message_dttm TIMESTAMP NOT NULL,
        silver_created_by STRING NOT NULL,
        silver_created_on_utc TIMESTAMP NOT NULL,
        order_number STRING NOT NULL,
        external_item_id STRING NOT NULL,
        order_line_unit_seq STRING NOT NULL,
        discount STRING,
        applicable_discount_amount STRING,
        discount_level STRING NOT NULL,
        discount_name STRING NOT NULL,
        discount_desc STRING,
        cart_desc STRING,
        discount_ext_id STRING NOT NULL,
        discount_type STRING NOT NULL,
        discount_code STRING,
        discount_applied_dttm TIMESTAMP,
        date_added TIMESTAMP,
        CONSTRAINT `co_stage_order_message_discount_pk` PRIMARY KEY (`order_number`, `order_line_unit_seq`, `external_item_id`, `discount_level`, `discount_name`, `discount_ext_id`, `discount_type`))
    USING delta
    COMMENT 'Silver table for discounts applied at the order line level.'
    CLUSTER BY (silver_created_on_utc,external_item_id)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE co_stage_order_message_fulfill (
        message_key STRING NOT NULL,
        message_dttm TIMESTAMP NOT NULL,
        silver_created_by STRING NOT NULL,
        silver_created_on_utc TIMESTAMP NOT NULL,
        date_added TIMESTAMP NOT NULL,
        order_number STRING NOT NULL,
        order_state STRING NOT NULL,
        order_source STRING NOT NULL,
        order_type STRING,
        order_placed_dttm TIMESTAMP,
        order_last_update_dttm TIMESTAMP,
        sku STRING NOT NULL,
        external_item_id STRING,
        order_line_num STRING NOT NULL,
        order_line_unit_seq STRING NOT NULL,
        order_line_state STRING NOT NULL,
        order_line_last_update_dttm TIMESTAMP,
        purchase_price STRING,
        return_price STRING,
        est_unit_tax STRING,
        est_delivery_date TIMESTAMP,
        start_est_delivery_date TIMESTAMP,
        guarenteedtogetthere_date TIMESTAMP,
        line_item_type STRING,
        fulfillment_type STRING,
        purchase_order STRING,
        fulfill_location_id STRING,
        fulfill_address1 STRING,
        fulfill_address2 STRING,
        fulfill_address3 STRING,
        fulfill_city STRING,
        fulfill_state STRING,
        fulfill_zip STRING,
        fulfill_order_id STRING,
        fulfill_shipped_dttm TIMESTAMP,
        ship_sku STRING,
        ship_upc STRING,
        ship_mode STRING,
        ship_location_id STRING,
        ship_carrier STRING,
        ship_class STRING,
        ship_tracking_num STRING,
        ship_charge STRING,
        ship_address1 STRING,
        ship_address2 STRING,
        ship_address3 STRING,
        ship_city STRING,
        ship_state STRING,
        ship_zip STRING,
        tax_product_code STRING,
        est_ship_tax_shp_dtl STRING,
        return_tracking_num STRING,
        return_reason STRING,
        return_location STRING,
        return_dttm TIMESTAMP,
        return_label_creation_date TIMESTAMP,
        return_unreceipted STRING,
        return_fraud_check_id STRING,
        return_pickup_date TIMESTAMP,
        return_delivery_date TIMESTAMP,
        return_process_date TIMESTAMP,
        return_source STRING,
        unit_cancel_source STRING,
        unit_cancel_dttm TIMESTAMP,
        unit_cancel_reason STRING,
        CONSTRAINT `co_stage_order_message_fulfill_pk` PRIMARY KEY (`order_number`, `order_source`, `order_state`, `order_line_state`, `sku`, `order_line_num`, `order_line_unit_seq`))
    USING delta
    COMMENT 'This table records order fulfillment and related shipment details.'
    CLUSTER BY (silver_created_on_utc,order_state,order_source)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE co_stage_order_message_fulfill_quarantine (
        message_key STRING NOT NULL,
        quarantine_code INT NOT NULL,
        message_dttm TIMESTAMP NOT NULL,
        silver_created_by STRING NOT NULL,
        silver_created_on_utc TIMESTAMP NOT NULL,
        date_added TIMESTAMP NOT NULL,
        order_number STRING NOT NULL,
        order_state STRING NOT NULL,
        order_source STRING NOT NULL,
        order_type STRING,
        order_placed_dttm TIMESTAMP,
        order_last_update_dttm TIMESTAMP,
        sku STRING NOT NULL,
        external_item_id STRING,
        order_line_num STRING NOT NULL,
        order_line_unit_seq STRING NOT NULL,
        order_line_state STRING NOT NULL,
        order_line_last_update_dttm TIMESTAMP,
        purchase_price STRING,
        return_price STRING,
        est_unit_tax STRING,
        est_delivery_date TIMESTAMP,
        start_est_delivery_date TIMESTAMP,
        guarenteedtogetthere_date TIMESTAMP,
        line_item_type STRING,
        fulfillment_type STRING,
        purchase_order STRING,
        fulfill_location_id STRING,
        fulfill_address1 STRING,
        fulfill_address2 STRING,
        fulfill_address3 STRING,
        fulfill_city STRING,
        fulfill_state STRING,
        fulfill_zip STRING,
        fulfill_order_id STRING,
        fulfill_shipped_dttm TIMESTAMP,
        ship_sku STRING,
        ship_upc STRING,
        ship_mode STRING,
        ship_location_id STRING,
        ship_carrier STRING,
        ship_class STRING,
        ship_tracking_num STRING,
        ship_charge STRING,
        ship_address1 STRING,
        ship_address2 STRING,
        ship_address3 STRING,
        ship_city STRING,
        ship_state STRING,
        ship_zip STRING,
        tax_product_code STRING,
        est_ship_tax_shp_dtl STRING,
        return_tracking_num STRING,
        return_reason STRING,
        return_location STRING,
        return_dttm TIMESTAMP,
        return_label_creation_date TIMESTAMP,
        return_unreceipted STRING,
        return_fraud_check_id STRING,
        return_pickup_date TIMESTAMP,
        return_delivery_date TIMESTAMP,
        return_process_date TIMESTAMP,
        return_source STRING,
        unit_cancel_source STRING,
        unit_cancel_dttm TIMESTAMP,
        unit_cancel_reason STRING,
        CONSTRAINT `co_stage_order_message_fulfill_quarantine_pk` PRIMARY KEY (`order_number`, `order_source`, `order_state`, `order_line_state`, `sku`, `order_line_num`, `order_line_unit_seq`),
        CONSTRAINT `order_message_fulfill_fk01` FOREIGN KEY (`quarantine_code`) REFERENCES quarantine_reasons (`code`))
    USING delta
    COMMENT 'This table records fulfilled orders that have been placed in quarantine due to malformed or incorrect data.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE co_stage_order_message_placed (
        message_key STRING NOT NULL,
        message_dttm TIMESTAMP NOT NULL,
        silver_created_by STRING NOT NULL,
        silver_created_on_utc TIMESTAMP NOT NULL,
        order_number STRING NOT NULL,
        order_state STRING NOT NULL,
        order_source STRING NOT NULL,
        order_channel STRING NOT NULL,
        aosstorenumber STRING,
        order_type STRING,
        order_placed_dttm TIMESTAMP,
        order_last_update_dttm TIMESTAMP,
        identity_id STRING,
        auth_id STRING,
        loyalty_acct_id STRING,
        address1 STRING,
        address2 STRING,
        address3 STRING,
        city STRING,
        state STRING,
        zip STRING,
        country STRING,
        reward_cert_codes STRING,
        sku STRING NOT NULL,
        external_item_id STRING,
        order_line_num STRING NOT NULL,
        order_line_unit_seq STRING NOT NULL,
        order_line_state STRING NOT NULL,
        order_line_last_update_dttm TIMESTAMP,
        original_price STRING,
        discount STRING,
        purchase_price STRING,
        est_unit_tax STRING,
        upc STRING,
        est_delivery_date TIMESTAMP,
        guarenteedtogetthere_date TIMESTAMP,
        line_item_type STRING,
        ship_sku STRING,
        ship_upc STRING,
        ship_mode STRING,
        ship_location_id STRING,
        ship_carrier STRING,
        ship_class STRING,
        original_ship_charge STRING,
        ship_discount STRING,
        ship_charge STRING,
        ship_address1 STRING,
        ship_address2 STRING,
        ship_address3 STRING,
        ship_city STRING,
        ship_state STRING,
        ship_zip STRING,
        tax_product_code STRING,
        est_ship_tax_shp_dtl STRING,
        date_added TIMESTAMP,
        CONSTRAINT `order_message_placed_pk` PRIMARY KEY (`order_number`, `order_source`, `order_state`, `order_line_state`, `order_channel`, `sku`, `order_line_num`, `order_line_unit_seq`))
    USING delta
    COMMENT 'This table records orders placed and the state of those orders.'
    CLUSTER BY (silver_created_on_utc,order_state,sku)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE co_stage_order_message_placed_payment (
        message_key STRING NOT NULL,
        message_dttm TIMESTAMP NOT NULL,
        order_number STRING NOT NULL,
        order_last_update_dttm TIMESTAMP,
        payment_type STRING NOT NULL,
        card_number STRING,
        authorized_amount STRING,
        silver_created_by STRING NOT NULL,
        silver_created_on_utc TIMESTAMP NOT NULL,
        silver_updated_by STRING NOT NULL,
        silver_updated_on_utc TIMESTAMP NOT NULL,
        CONSTRAINT `co_stage_order_message_placed_payment_pk` PRIMARY KEY (`order_number`, `payment_type`))
    USING delta
    CLUSTER BY (order_number,silver_created_on_utc,silver_updated_on_utc)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE co_stage_order_message_placed_quarantine (
        message_key STRING NOT NULL,
        quarantine_code INT NOT NULL,
        message_dttm TIMESTAMP,
        silver_created_by STRING NOT NULL,
        silver_created_on_utc TIMESTAMP NOT NULL,
        order_number STRING NOT NULL,
        order_state STRING NOT NULL,
        order_source STRING NOT NULL,
        order_channel STRING NOT NULL,
        aosstorenumber STRING,
        order_type STRING,
        order_placed_dttm TIMESTAMP,
        order_last_update_dttm TIMESTAMP,
        identity_id STRING,
        auth_id STRING,
        loyalty_acct_id STRING,
        address1 STRING,
        address2 STRING,
        address3 STRING,
        city STRING,
        state STRING,
        zip STRING,
        country STRING,
        reward_cert_codes STRING,
        sku STRING NOT NULL,
        external_item_id STRING,
        order_line_num STRING NOT NULL,
        order_line_unit_seq STRING NOT NULL,
        order_line_state STRING NOT NULL,
        order_line_last_update_dttm TIMESTAMP,
        original_price STRING,
        discount STRING,
        purchase_price STRING,
        est_unit_tax STRING,
        upc STRING,
        est_delivery_date TIMESTAMP,
        guarenteedtogetthere_date TIMESTAMP,
        line_item_type STRING,
        ship_sku STRING,
        ship_upc STRING,
        ship_mode STRING,
        ship_location_id STRING,
        ship_carrier STRING,
        ship_class STRING,
        original_ship_charge STRING,
        ship_discount STRING,
        ship_charge STRING,
        ship_address1 STRING,
        ship_address2 STRING,
        ship_address3 STRING,
        ship_city STRING,
        ship_state STRING,
        ship_zip STRING,
        tax_product_code STRING,
        est_ship_tax_shp_dtl STRING,
        date_added TIMESTAMP,
        CONSTRAINT `order_message_placed_quarantine_pk` PRIMARY KEY (`order_number`, `order_source`, `order_state`, `order_line_state`, `order_channel`, `sku`, `order_line_num`, `order_line_unit_seq`),
        CONSTRAINT `order_message_placed_fk01` FOREIGN KEY (`quarantine_code`) REFERENCES quarantine_reasons (`code`))
    USING delta
    COMMENT 'This table records orders placed that have been quarantined due to malformed or incorrect data.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE demand_fulfill (
        date_key BIGINT NOT NULL COMMENT 'The date key represents the calendar date in numeric form YYYYMMDD; for all metrics that have been aggregated to the day level for demand vs. fulfilled decomp report. The date range for the data in this table is all of fiscal LY to current.',
        designated_demand_amt DECIMAL(38,10) COMMENT 'The total designated demand sales amount for the given date.',
        presale_demand_amt DECIMAL(38,10) COMMENT 'The designated presale demand sales amount for the given date.',
        sfs_demand_sales_amt DECIMAL(38,10) NOT NULL DEFAULT 0 COMMENT 'The designated ship from store demand sales amount for the given date.',
        dc_demand_sales_amt DECIMAL(38,10) NOT NULL DEFAULT 0 COMMENT 'The designated DC demand sales amount for the given date.',
        rdc_demand_sales_amt DECIMAL(38,10) NOT NULL DEFAULT 0 COMMENT 'The designated RDC demand sales amount for the given date.',
        vdc_demand_sales_amt DECIMAL(38,10) NOT NULL DEFAULT 0 COMMENT 'The designated VDC demand sales amount for the given date.',
        bopl_demand_sales_amt DECIMAL(38,10) NOT NULL DEFAULT 0 COMMENT 'The designated BOPL demand sales amount for the given date.',
        bopis_demand_sales_amt DECIMAL(38,10) NOT NULL DEFAULT 0 COMMENT 'The designated BOPIS demand sales amount for the given date.',
        multi_demand_sales_amt DECIMAL(38,10) NOT NULL DEFAULT 0 COMMENT 'The designated MULTI demand sales amount for the given date.',
        unk_demand_sales_amt DECIMAL(38,10) NOT NULL DEFAULT 0 COMMENT 'The designated UNKNOWN demand sales amount for the given date.',
        co_count BIGINT COMMENT 'The total number of demand orders for the given date.',
        presale_co_count BIGINT COMMENT 'The total number of demand pre-sales orders for the given date.',
        fraud_cancel_amt DECIMAL(38,10) COMMENT 'The sum of transactions marked as fraud for the given date.',
        athlete_cancel_amt DECIMAL(38,10) COMMENT 'The sum of transactions that were cancelled by the athlete for the given date.',
        retailer_cancel_amt DECIMAL(38,10) COMMENT 'The sum of transactions that were cancelled by the retailer for the given date.',
        other_cancel_amt DECIMAL(38,10) COMMENT 'The sum of transactions that were cancelled for other reasons for the given date.',
        fulfilled_amt_by_fulfill_date DECIMAL(38,10) COMMENT 'The sum of sales that were fulfilled for the given date.',
        fulfilled_orders_by_fulfill_date BIGINT COMMENT 'The number of orders that were fulfilled  for the given date.',
        fulfilled_sales_age_days_0 DECIMAL(38,10) COMMENT 'The sum of sales for the current date_key that were fulfilled within the same day for the given date.',
        fulfilled_sales_age_days_1 DECIMAL(38,10) COMMENT 'The sum of sales for the current date_key that were fulfilled within the one day for the given date.',
        fulfilled_sales_age_days_2 DECIMAL(38,10) COMMENT 'The sum of sales for the current date_key that were fulfilled within the two days for the given date.',
        fulfilled_sales_age_days_3 DECIMAL(38,10) COMMENT 'The sum of sales for the current date_key that were fulfilled within the three days for the given date.',
        fulfilled_sales_age_days_4 DECIMAL(38,10) COMMENT 'The sum of sales for the current date_key that were fulfilled within the four days for the given date.',
        fulfilled_sales_age_days_5 DECIMAL(38,10) COMMENT 'The sum of sales for the current date_key that were fulfilled within five days for the given date.',
        fulfilled_sales_age_days_6 DECIMAL(38,10) COMMENT 'The sum of sales for the current date_key that were fulfilled within six days for the given date.',
        fulfilled_sales_age_days_7 DECIMAL(38,10) COMMENT 'The sum of sales for the current date_key that were fulfilled within seven days or more for the given date.',
        sfs_fulfilled_amt DECIMAL(38,10) COMMENT 'The sum of sales fulfilled for the given date that were fulfilled as ship from store.',
        dc_fulfilled_amt DECIMAL(38,10) COMMENT 'The sum of sales for the given date fulfilled from a DC.',
        rdc_fulfilled_amt DECIMAL(38,10) COMMENT 'The sum of sales for the given date fulfilled from an RDC.',
        vdc_fulfilled_amt DECIMAL(38,10) COMMENT 'The sum of sales for the given date fulfilled as vendor direct (VDC).',
        bopis_fulfilled_amt DECIMAL(38,10) COMMENT 'The sum of sales for the given date fulfilled as BOPIS.',
        bopl_fulfilled_amt DECIMAL(38,10) COMMENT 'The sum of sales for the given date fulfilled as BOPL.',
        unassigned_fulfilled_amt DECIMAL(38,10) COMMENT 'The sum of sales for the given date that were fulfilled without a classification.',
        return_amt DECIMAL(38,10) COMMENT 'The sum of sales for the given date that were returns.',
        post_order_adjustment_amt DECIMAL(38,10) COMMENT 'The sum of sales for the given date that had a post order price adjustment.',
        open_order_amt DECIMAL(38,10) COMMENT 'The sum of all sales that have not been fulfilled.',
        open_order BIGINT COMMENT 'The total number of orders that have not been fulfilled.',
        fulfilled_amt_by_order_date DECIMAL(38,10) COMMENT 'The sum of all sales fulfilled for the given order date, as of “yesterday” and the corresponding date for LY.',
        fulfilled_co_count BIGINT COMMENT 'The number of orders fulfilled for the given date.',
        silver_created_on_utc TIMESTAMP COMMENT 'The timestamp of when this record was entered into the datalake.',
        silver_created_by STRING COMMENT 'The ID of the user that ran the job/pipeline that created this record.',
        CONSTRAINT `demand_fulfill_pk` PRIMARY KEY (`date_key`))
    USING delta
    COMMENT 'This table represents the summarized metrics for demand vs fulfilled sales aggregated daily for all of fiscal LY to current date.'
    CLUSTER BY (date_key)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.columnMapping.mode' = 'name',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.enableTypeWidening' = 'true',
      'delta.feature.allowColumnDefaults' = 'supported',
      'delta.feature.appendOnly' = 'supported',
      'delta.feature.columnMapping' = 'supported',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.typeWidening-preview' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE ltr_abb_atc_rate (
        lagged_feature_date_est DATE NOT NULL COMMENT 'The date associated with the data in each row.',
        search_term STRING NOT NULL COMMENT 'The search string being counted.',
        ecode STRING NOT NULL COMMENT 'The ecode being counted for impressions.',
        prior_alpha DOUBLE NOT NULL,
        prior_beta DOUBLE NOT NULL,
        alpha_n DOUBLE NOT NULL,
        beta_n DOUBLE NOT NULL,
        signed_kl_divergence DOUBLE,
        signed_js_divergence_shifted DOUBLE,
        CONSTRAINT `ltr_abb_atc_rate_pk` PRIMARY KEY (`lagged_feature_date_est`, `search_term`, `ecode`))
    USING delta
    PARTITIONED BY (lagged_feature_date_est)
    COMMENT 'Aggregates add-to-cart events and impressions over `lookback_days` day rolling windows and computes posterior predictive estimates for the ATC-rate for each date/search_term/ecode combo. Dates are ET-derived.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.columnMapping' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_abb_ctr (
        lagged_feature_date_est DATE NOT NULL COMMENT 'The date associated with the data in each row.',
        search_term STRING NOT NULL COMMENT 'The search string being counted.',
        ecode STRING NOT NULL COMMENT 'The ecode being counted for impressions.',
        prior_alpha DOUBLE NOT NULL,
        prior_beta DOUBLE NOT NULL,
        alpha_n DOUBLE NOT NULL,
        beta_n DOUBLE NOT NULL,
        signed_kl_divergence DOUBLE,
        signed_js_divergence_shifted DOUBLE,
        CONSTRAINT `ltr_abb_ctr_pk` PRIMARY KEY (`lagged_feature_date_est`, `search_term`, `ecode`))
    USING delta
    PARTITIONED BY (lagged_feature_date_est)
    COMMENT 'Aggregates the clicks and impressions for the last `lookback_days` days and computes the posterior predictive estimates for the CTR for each date/search term/ecode combo. Dates are ET-derived.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.columnMapping' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_abb_impressions_agg (
        tran_date DATE NOT NULL COMMENT 'The date associated with the data in the row.',
        search_term STRING NOT NULL COMMENT 'The search string being counted.',
        ecode STRING NOT NULL COMMENT 'The ecode being counted for impressions.',
        search_count BIGINT NOT NULL COMMENT 'The number of times the search term ecode combination appeared on each date.',
        impression_count BIGINT NOT NULL COMMENT 'The number of impressions the ecode had for the given search term.')
    USING delta
    COMMENT 'Counts the number of searches and impressions for a given search term and ecode on each particular date using rolling windows.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_abb_linked_searches (
        visit_start_tran_date_est DATE NOT NULL,
        mc_visitor_id STRING NOT NULL,
        visit_id STRING NOT NULL COMMENT 'Visit when the event occured.',
        start_time BIGINT NOT NULL COMMENT 'Start of the visit.',
        end_time BIGINT NOT NULL COMMENT 'End of the visit.',
        search_date_utc STRING NOT NULL COMMENT 'The short date the event occurred in the format of YYYY-MM-DD.',
        time BIGINT NOT NULL,
        id STRING NOT NULL COMMENT 'The ML Event UUID, this is a unique identifier for every record.',
        search_term STRING NOT NULL COMMENT 'Search term associated with the id.',
        impression_items ARRAY<STRING> COMMENT 'List of ecodes that got impressions during the search events.',
        impressions ARRAY<STRUCT<event_id: STRING, id: STRING, time: BIGINT>> COMMENT 'Impression generated during the event assosicated with the ML Event UUID.')
    USING delta
    PARTITIONED BY (visit_start_tran_date_est)
    COMMENT 'This query pulls all of the clickstream data we need for identifying searches and links it to search events in the ML events table.  The ML events adds on impression data by aggregating impression events under the parent search event.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_abb_order_rate (
        lagged_feature_date_est DATE NOT NULL COMMENT 'The date associated with the data in each row.',
        search_term STRING NOT NULL COMMENT 'The search string being counted.',
        ecode STRING NOT NULL COMMENT 'The ecode being counted for impressions.',
        prior_alpha DOUBLE NOT NULL,
        prior_beta DOUBLE NOT NULL,
        alpha_n DOUBLE NOT NULL,
        beta_n DOUBLE NOT NULL,
        signed_kl_divergence DOUBLE,
        signed_js_divergence_shifted DOUBLE,
        CONSTRAINT `ltr_abb_order_rate_pk` PRIMARY KEY (`lagged_feature_date_est`, `search_term`, `ecode`))
    USING delta
    PARTITIONED BY (lagged_feature_date_est)
    COMMENT 'Aggregates order events and impressions over `lookback_days` day rolling windows and computes posterior predictive estimates for the order rate for each date/search_term/ecode combo. Dates are ET-derived.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.columnMapping' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_atc_base (
        date_est DATE NOT NULL COMMENT 'Rolling window end date.',
        search_term STRING NOT NULL COMMENT 'Query associated with the add to cart event.',
        tran_date DATE NOT NULL COMMENT 'EST-derived date of the add-to-cart event.',
        hitid STRING NOT NULL COMMENT 'ID associated with hit in clickstream.',
        web_price DOUBLE NOT NULL COMMENT 'Web price associated with the SKU that was carted.')
    USING delta
    PARTITIONED BY (date_est)
    COMMENT 'This table provides price and event information for a specified lookback window across all search terms'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_atc_bod_price_data (
        bod_inv_date DATE NOT NULL COMMENT 'Date associated with the data in the row.',
        ecode STRING NOT NULL COMMENT 'eCode associated with price at the beginning of the day.',
        avg_web_price DOUBLE NOT NULL COMMENT 'Average web price of eCode at the beginning of the specified date.',
        CONSTRAINT `ltr_bod_price_data_pk` PRIMARY KEY (`bod_inv_date`, `ecode`))
    USING delta
    COMMENT 'Provides the average web price at the beginning of the specified day for a given ecode.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_atc_global_agg (
        lagged_feature_date_est DATE NOT NULL COMMENT 'Date associated with information in the table.',
        global_avg_atc_web_price DOUBLE NOT NULL COMMENT 'Average web price at a global level on a specific day.',
        global_std_samp_atc_web_price DOUBLE NOT NULL COMMENT 'Standard deviation of web price at a global level on a specific day.',
        global_atc_event_count BIGINT NOT NULL COMMENT 'Number of unique ID associated hits included in under this lagged feature date.',
        global_atc_day_count BIGINT NOT NULL COMMENT 'Number of days of ATC events included in this row.',
        CONSTRAINT `atc_global_agg_pk` PRIMARY KEY (`lagged_feature_date_est`))
    USING delta
    COMMENT 'Provides price and event information for a specified lookback window across all search terms.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_atc_prices_intermediary (
        tran_date DATE NOT NULL COMMENT 'EST-derived date of the add-to-cart event.',
        bod_inv_date_est DATE NOT NULL COMMENT 'Date associated with the data in the row.',
        search_term STRING NOT NULL COMMENT 'Query associated with the add to cart event.',
        ecode STRING NOT NULL COMMENT 'eCode associated with price at the beginning of the day.',
        _sku BIGINT NOT NULL,
        hitid STRING NOT NULL COMMENT 'ID associated with hit in clickstream.',
        web_price DOUBLE NOT NULL COMMENT 'Web price associated with the SKU that was carted.',
        log1p_web_price DOUBLE NOT NULL COMMENT 'Log of web price.')
    USING delta
    COMMENT ' Intermediary table used to make the base table and merge the base table with the BOD price table.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_atc_search_lvl_agg (
        lagged_feature_date_est DATE NOT NULL COMMENT 'Date associated with information in the table.',
        search_term STRING NOT NULL COMMENT 'Query associated with the add to cart event.',
        avg_atc_web_price DOUBLE NOT NULL COMMENT 'Average web price at a search term level on a specific day.',
        std_samp_atc_web_price DOUBLE NOT NULL COMMENT 'Standard deviation of web price at a search term level on a specific day.',
        atc_event_count BIGINT NOT NULL COMMENT 'Number of unique ID associated hits included in under this lagged feature date and search term combination.',
        atc_day_count BIGINT NOT NULL COMMENT 'Number of days of ATC events included in this row.',
        CONSTRAINT `atc_search_lvl_agg_pk` PRIMARY KEY (`lagged_feature_date_est`, `search_term`))
    USING delta
    COMMENT 'Provides price and event information for a specific look back window and search term combination.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_atc_term_dates_intermediary (
        date_est DATE NOT NULL COMMENT 'Rolling window end date.',
        search_term STRING NOT NULL COMMENT 'Query associated with the add to cart event.',
        ecode STRING NOT NULL COMMENT 'eCode associated with price at the beginning of the day.')
    USING delta
    COMMENT ' Intermediary table used to merge the BOD table with the search level agg and global agg tables.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_atc_zscore (
        bod_inv_date DATE NOT NULL COMMENT 'Date associated with the data in the row.',
        search_term STRING NOT NULL COMMENT 'Query associated with the add to cart event.',
        ecode STRING NOT NULL COMMENT 'eCode associated with price at the beginning of the day.',
        avg_atc_web_price DOUBLE NOT NULL COMMENT 'Average web price at a search term level on a specific day.',
        atc_event_count BIGINT NOT NULL COMMENT 'Number of unique ID associated hits included in under this lagged feature date and search term combination.',
        global_avg_atc_web_price DOUBLE NOT NULL COMMENT 'Average web price at a global level on a specific day.',
        std_samp_atc_web_price DOUBLE NOT NULL COMMENT 'Standard deviation of web price at a search term level on a specific day.',
        global_std_samp_atc_web_price DOUBLE NOT NULL COMMENT 'Standard deviation of web price at a global level on a specific day.',
        avg_web_price DOUBLE NOT NULL COMMENT 'Average web price of eCode at the beginning of the specified date.',
        mu DOUBLE NOT NULL COMMENT '(avg_atc_web_price * atc_event_count + global_avg_atc_web_price) / (atc_event_count + 1).',
        sigma DOUBLE NOT NULL COMMENT '(std_samp_atc_web_price * atc_event_count + global_std_samp_atc_web_price) / (atc_event_count + 1).',
        z_score DOUBLE NOT NULL COMMENT '(avg_atc_web_price - mu) / sigma.',
        shifted_z_score DOUBLE,
        CONSTRAINT `ltr_atc_zscore_pk` PRIMARY KEY (`bod_inv_date`, `search_term`, `ecode`))
    USING delta
    COMMENT ' Provides the necessary data to perform the calculation of the z-score for each search_term/eCode combination.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_feature_agg (
        ecode STRING NOT NULL COMMENT 'eCode associated with the features in the table',
        avg_web_price_atc_z_score_shifted MAP<STRING, DOUBLE> NOT NULL COMMENT 'Indicates how many standard deviations the average price for this search term is away from the mean price of this ecode.',
        ctr_signed_js_divergence_shifted MAP<STRING, DOUBLE>,
        atc_rate_signed_js_divergence_shifted MAP<STRING, DOUBLE>,
        order_rate_signed_js_divergence_shifted MAP<STRING, DOUBLE>)
    USING delta
    COMMENT ' Combines all of the necessary LTR features in one table.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.columnMapping' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_agg_atc (
        tran_date DATE,
        search_term STRING,
        ecode STRING,
        daily_atc_count BIGINT)
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_agg_impressions (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        impression_count BIGINT,
        click_count BIGINT,
        total_clicks_last_x_days BIGINT,
        total_impressions_last_x_days BIGINT)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_all_dates (date_est DATE)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_alphas_and_betas (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        total_clicks_last_x_days BIGINT,
        total_impressions_last_x_days BIGINT,
        prior_ctr DOUBLE,
        prior_alpha DOUBLE,
        prior_beta DOUBLE,
        alpha_n DOUBLE,
        beta_n DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_agg_impressions (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        impression_count BIGINT,
        atc_count BIGINT,
        total_atc_last_x_days BIGINT,
        total_impressions_last_x_days BIGINT)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_alphas_and_betas (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        total_atc_last_x_days BIGINT,
        total_impressions_last_x_days BIGINT,
        prior_atc_rate DOUBLE,
        prior_alpha DOUBLE,
        prior_beta DOUBLE,
        alpha_n DOUBLE,
        beta_n DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_click_counts (
        tran_date DATE,
        search_term STRING,
        ecode STRING,
        daily_clicks BIGINT)
    USING delta
    COMMENT 'Staging table for ltr_abb_ctr_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_global_priors (
        date_est DATE,
        avg_atc_last_x_days_global DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_init_priors (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        total_atc_last_x_days BIGINT,
        total_impressions_last_x_days BIGINT,
        prior_atc_rate DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_joined_impressions (
        tran_date DATE COMMENT 'The date associated with the data in the row.',
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        impression_count BIGINT COMMENT 'The number of impressions the ecode had for the given search term.',
        atc_count BIGINT)
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_posteriors (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        total_atc_last_x_days BIGINT,
        total_impressions_last_x_days BIGINT,
        prior_atc_rate DOUBLE,
        prior_alpha DOUBLE,
        prior_beta DOUBLE,
        alpha_n DOUBLE,
        beta_n DOUBLE,
        atc_rate_posterior DOUBLE,
        prior_atc_rate_std DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_query_priors (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        total_atc_last_x_days_query BIGINT,
        total_impressions_last_x_days_query BIGINT,
        total_daily_atc BIGINT,
        total_daily_impressions BIGINT,
        avg_atc_last_x_days_query DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_search_ecodes (
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.')
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_atc_search_ecodes_with_all_dates (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        impression_count BIGINT,
        atc_count BIGINT)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_click_counts (
        tran_date DATE,
        search_term STRING,
        ecode STRING,
        daily_clicks BIGINT)
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.deletionVectors' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_counted_impressions (
        parent_id STRING COMMENT 'The ML Event parent UUID, this is the parent identifier for a group of related ML Events created during a single user session.',
        impression_count BIGINT)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_dates (
        start_date_est DATE,
        end_date_est DATE,
        lookback_days_minus_one INT,
        start_ts_utc TIMESTAMP,
        end_ts_utc TIMESTAMP)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_deduplicate_linked_orders (
        tran_date DATE,
        mcvisid STRING,
        _visit_id STRING,
        order_time BIGINT,
        ecode STRING,
        search_term STRING,
        _order_id BIGINT,
        _revenue DOUBLE,
        _sku BIGINT,
        _units DOUBLE,
        id STRING,
        search_time BIGINT,
        impression_items ARRAY<STRING>,
        row_num INT)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_global_priors (
        date_est DATE,
        total_clicks_last_x_days_global BIGINT,
        total_impressions_last_x_days_global BIGINT,
        avg_ctr_last_x_days_global DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_grouped_impressions (
        parent_id STRING COMMENT 'The ML Event parent UUID, this is the parent identifier for a group of related ML Events created during a single user session.',
        date_utc STRING,
        mcvisid STRING,
        impressions ARRAY<STRUCT<event_id: STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.', id: STRING, time: BIGINT>>)
    USING delta
    PARTITIONED BY (date_utc)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_atc (
        tran_date DATE,
        tran_date_utc DATE,
        atc_time BIGINT,
        mcvisid STRING,
        _visit_id STRING,
        ecode STRING,
        hitid STRING,
        search_term STRING)
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_atc_agg (
        mcvisid STRING,
        _visit_id STRING,
        search_term STRING,
        ecode STRING,
        atc_time BIGINT,
        tran_date DATE,
        atc_count BIGINT,
        hitid STRING)
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_clicks (
        tran_date DATE,
        tran_date_utc DATE,
        mcvisid STRING,
        _visit_id STRING,
        hitid STRING,
        clk_time BIGINT,
        ecode STRING,
        search_term STRING,
        _evar2 STRING,
        _evar58 STRING,
        next_clk_time BIGINT,
        prev_clk_time BIGINT)
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_clk (
        _visit_id STRING,
        mcvisid STRING,
        visit_start_tran_date_est DATE,
        start_time BIGINT,
        end_time BIGINT)
    USING delta
    PARTITIONED BY (visit_start_tran_date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_orders (
        tran_date DATE,
        tran_date_utc DATE,
        mcvisid STRING,
        _visit_id STRING,
        _order_id BIGINT,
        _revenue DOUBLE,
        _sku BIGINT,
        _units DOUBLE,
        ecode STRING,
        search_term STRING,
        order_time BIGINT)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_priors (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        total_clicks_last_x_days BIGINT,
        total_impressions_last_x_days BIGINT,
        prior_ctr DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_searches (
        id STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.',
        date_utc STRING COMMENT 'The short date the event occurred in the format of YYYY-MM-DD.',
        time BIGINT,
        page_size INT,
        mcvisid STRING,
        search_term STRING)
    USING delta
    PARTITIONED BY (date_utc)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_init_searches_and_impressions (
        id STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.',
        date_utc STRING COMMENT 'The short date the event occurred in the format of YYYY-MM-DD.',
        time BIGINT,
        mcvisid STRING,
        search_term STRING,
        impressions ARRAY<STRUCT<event_id: STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.', id: STRING, time: BIGINT>>)
    USING delta
    PARTITIONED BY (date_utc)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_joined_impressions (
        tran_date DATE COMMENT 'The date associated with the data in the row.',
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        impression_count BIGINT COMMENT 'The number of impressions the ecode had for the given search term.',
        click_count BIGINT)
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_joined_seq_orders (
        tran_date DATE,
        mcvisid STRING,
        _visit_id STRING,
        _order_id BIGINT,
        ecode STRING,
        search_term STRING,
        _revenue DOUBLE,
        _sku BIGINT,
        _units DOUBLE,
        order_time BIGINT,
        prev_order_time BIGINT)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_linked_atc (
        tran_date DATE,
        mcvisid STRING,
        _visit_id STRING,
        atc_time BIGINT,
        ecode STRING,
        atc_event INT,
        search_term STRING,
        id STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.',
        search_time BIGINT,
        impression_items ARRAY<STRING> COMMENT 'List of ecodes that got impressions during the search events.')
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_linked_clicks (
        tran_date DATE,
        mcvisid STRING,
        _visit_id STRING,
        hitid STRING,
        search_time BIGINT,
        search_term STRING,
        ecode STRING)
    USING delta
    PARTITIONED BY (tran_date)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_linked_orders (
        tran_date DATE,
        mcvisid STRING,
        _visit_id STRING,
        order_time BIGINT,
        ecode STRING,
        search_term STRING,
        _order_id BIGINT,
        _revenue DOUBLE,
        _sku BIGINT,
        _units DOUBLE,
        id STRING,
        search_time BIGINT,
        impression_items ARRAY<STRING>)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_linked_searches (
        visit_start_tran_date_est DATE,
        mc_visitor_id STRING,
        visit_id STRING,
        start_time BIGINT,
        end_time BIGINT,
        search_date_utc STRING COMMENT 'The short date the event occurred in the format of YYYY-MM-DD.',
        time BIGINT,
        id STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.',
        search_term STRING,
        impression_items ARRAY<STRING>,
        impressions ARRAY<STRUCT<event_id: STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.', id: STRING, time: BIGINT>>,
        violation_size BIGINT)
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_aggs_level_1 (
        tran_date DATE,
        search_term STRING,
        ecode STRING,
        _order_id BIGINT,
        total_order_revenue DOUBLE,
        total_order_units DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_aggs_level_2 (
        tran_date DATE,
        search_term STRING,
        ecode STRING,
        order_count BIGINT,
        total_revenue DOUBLE,
        total_units DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_agg_impressions (
        date_est DATE,
        search_term STRING,
        ecode STRING,
        impression_count BIGINT,
        order_count BIGINT,
        total_revenue DOUBLE,
        total_units DOUBLE,
        total_impressions_last_x_days BIGINT,
        total_orders_last_x_days BIGINT,
        total_revenue_last_x_days DOUBLE,
        total_units_last_x_days DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_alphas_and_betas (
        date_est DATE,
        search_term STRING,
        ecode STRING,
        total_revenue DOUBLE,
        total_units DOUBLE,
        total_impressions_last_x_days BIGINT,
        total_orders_last_x_days BIGINT,
        total_revenue_last_x_days DOUBLE,
        total_units_last_x_days DOUBLE,
        prior_order_rate DOUBLE,
        prior_alpha DOUBLE,
        prior_beta DOUBLE,
        alpha_n DOUBLE,
        beta_n DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_global_priors (
        date_est DATE,
        total_orders_last_x_days_global BIGINT,
        total_impressions_last_x_days_global BIGINT,
        avg_order_rate_last_x_days_global DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_init_priors (
        date_est DATE,
        search_term STRING,
        ecode STRING,
        total_revenue DOUBLE,
        total_units DOUBLE,
        total_impressions_last_x_days BIGINT,
        total_orders_last_x_days BIGINT,
        total_revenue_last_x_days DOUBLE,
        total_units_last_x_days DOUBLE,
        prior_order_rate DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_joined_impressions (
        tran_date DATE,
        search_term STRING,
        ecode STRING,
        impression_count BIGINT,
        order_count BIGINT,
        total_revenue DOUBLE,
        total_units DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_posteriors (
        date_est DATE,
        search_term STRING,
        ecode STRING,
        total_revenue DOUBLE,
        total_units DOUBLE,
        total_impressions_last_x_days BIGINT,
        total_orders_last_x_days BIGINT,
        total_revenue_last_x_days DOUBLE,
        total_units_last_x_days DOUBLE,
        prior_order_rate DOUBLE,
        prior_alpha DOUBLE,
        prior_beta DOUBLE,
        alpha_n DOUBLE,
        beta_n DOUBLE,
        order_rate_posterior DOUBLE,
        prior_order_rate_std DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_query_priors (
        date_est DATE,
        search_term STRING,
        total_impressions_last_x_days_query BIGINT,
        total_orders_last_x_days_query BIGINT,
        avg_order_rate_last_x_days_query DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_search_ecodes (
        search_term STRING,
        ecode STRING)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_order_rate_search_ecodes_with_all_dates (
        date_est DATE,
        search_term STRING,
        ecode STRING,
        impression_count BIGINT,
        order_count BIGINT,
        total_revenue DOUBLE,
        total_units DOUBLE)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_posteriors (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        total_clicks_last_x_days BIGINT,
        total_impressions_last_x_days BIGINT,
        prior_ctr DOUBLE,
        prior_alpha DOUBLE,
        prior_beta DOUBLE,
        alpha_n DOUBLE,
        beta_n DOUBLE,
        ctr_posterior DOUBLE,
        prior_ctr_std DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_qualified_searches (id STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.')
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_query_priors (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        total_clicks_last_x_days_query BIGINT,
        total_impressions_last_x_days_query BIGINT,
        avg_ctr_last_x_days_query DOUBLE)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_search_ecodes (
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.')
    USING delta
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_search_ecodes_with_all_dates (
        date_est DATE,
        search_term STRING COMMENT 'The search string being counted.',
        ecode STRING COMMENT 'The ecode being counted for impressions.',
        impression_count BIGINT,
        click_count BIGINT)
    USING delta
    PARTITIONED BY (date_est)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_seq_orders (
        tran_date DATE,
        mcvisid STRING,
        _visit_id STRING,
        _order_id BIGINT,
        order_time BIGINT,
        prev_order_time BIGINT)
    USING delta
    COMMENT 'Staging table for ltr_abb_or_table_load workflow.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE ltr_stg_unrolled_impressions (
        impression_event_id STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.',
        parent_id STRING COMMENT 'The ML Event parent UUID, this is the parent identifier for a group of related ML Events created during a single user session.',
        date_utc STRING COMMENT 'The short date the event occurred in the format of YYYY-MM-DD.',
        impression_time BIGINT,
        mcvisid STRING,
        impression_id STRING)
    USING delta
    PARTITIONED BY (date_utc)
    TBLPROPERTIES (
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE VIEW order_line_abandoned_units (
        customer_order_number COMMENT 'Customer order number as defined by Athlete Order.',
        fr_number,
        line_number COMMENT 'Customer order line number as defined by Athlete Order. A unique line number is given to each unique sku on the order, starting from 1.',
        line_seq_number COMMENT 'Customer order line sequence number as defined by Athlete Order. A unique line sequence number is given to each unique item on a given line, starting from 0.',
        item_updated_dttm_etc,
        fulfillment_mode COMMENT 'Fulfillment mode chosen by Athlete at checkout.',
        pickup_facility_number COMMENT 'Facility number where item will be picked up.',
        decline_origin,
        fr_status COMMENT 'Final fulfillment status code attained by this item on this fulfillment request. See prod_oso_db.oso_ref.item_status for mapping of code to name.',
        fr_status_reason,
        fr_action_code COMMENT 'Final fulfillment action code if applicable. See prod_oso_db.oso_ref.decline_reason or .cancel_reason for mapping of code to name.',
        fr_action_code_reason)
WITH SCHEMA COMPENSATION
AS SELECT
    frh.order_id  AS customer_order_number,
    CONCAT(frh.order_id, '.', LPAD(frh.fr_sequence_number, 3, '0'))  AS fr_number,
    item.line_number  AS line_number,
    item.line_seq_number  AS line_seq_number,
    CONVERT_TIMEZONE('UTC', 'America/New_York', item.nmn_updated_timestamp)  AS item_updated_dttm_etc,
    item.ordered_fulfillment_mode  AS fulfillment_mode,
    item.pickup_facility_number  AS pickup_facility_number,
    CASE
        WHEN frh.system = 'FIT' THEN 'FIT'
        WHEN frh.system = 'OSO' THEN 'OSO'
        ELSE 'UNKNOWN'
    END AS decline_origin,
    frh.final_status AS fr_status,
    item_status_codes.name AS fr_status_reason,
    frh.action_code AS fr_action_code,
    decline_codes.name AS fr_action_code_reason
FROM ${oso_silver_catalog}.oso.newman_fr_history frh
JOIN ${oso_silver_catalog}.oso.newman_item item
    ON frh.order_id = item.order_id
    AND frh.line_number = item.line_number
    AND frh.line_seq_number = item.line_seq_number
JOIN prod_oso_db.oso_ref.decline_code decline_codes
    ON frh.action_code = decline_codes.code
JOIN prod_oso_db.oso_ref.item_status item_status_codes
    ON frh.final_status = item_status_codes.code
WHERE frh.managed_by = 'newman'
    AND item.ordered_fulfillment_mode = 'Bopis'
    AND frh.final_status = 790
    AND frh.action_code = 510
ORDER BY frh.completed_timestamp DESC;

CREATE VIEW order_line_allocations (
        capture_timestamp_eastern,
        released_timestamp_eastern,
        last_updated_timestamp_eastern,
        fulfillment_mode COMMENT 'Fulfillment mode chosen by Athlete at checkout.',
        order_id COMMENT 'Customer order number as defined by Athlete Order.',
        line_number COMMENT 'Customer order line number as defined by Athlete Order. A unique line number is given to each unique sku on the order, starting from 1.',
        line_seq_number COMMENT 'Customer order line sequence number as defined by Athlete Order. A unique line sequence number is given to each unique item on a given line, starting from 0.',
        fr_number,
        demand_unit,
        sku COMMENT 'Item sku as defined by Athlete Order.',
        facility_number COMMENT 'Facility number where item will be picked up.')
WITH SCHEMA COMPENSATION
AS WITH completed_frs (
        SELECT convert_timezone('UTC', 'America/New_York', o.order_capture_timestamp)   capture_timestamp_eastern,
               convert_timezone('UTC', 'America/New_York', fr.released_timestamp)       released_timestamp_eastern,
               convert_timezone('UTC', 'America/New_York', i.nmn_updated_timestamp)     last_updated_timestamp_eastern,
               i.ordered_fulfillment_mode                                               fulfillment_mode,
               fr.order_id                                                              order_id,
               fr.line_number                                                           line_number,
               fr.line_seq_number                                                       line_seq_number,
               CONCAT(fr.order_id, '.', LPAD(fr.fr_sequence_number, 3, 0))              fr_number,
               1                                                                        demand_unit,
               fr.sku                                                                   sku,
               fr.pickup_facility_number                                                facility_number
          FROM ${oso_silver_catalog}.oso.newman_fr_history fr
            LEFT JOIN ${oso_silver_catalog}.oso.newman_order o
                ON o.order_id = fr.order_id
            LEFT JOIN ${oso_silver_catalog}.oso.newman_item i
                ON i.order_id = fr.order_id
               AND i.line_number = fr.line_number
               AND i.line_seq_number = fr.line_seq_number
         WHERE fr.managed_by = 'newman'
           AND fr.system = 'FIT'
           AND fr.source_facility_type <> 'DC'
    ),
    open_frs (
        SELECT convert_timezone('UTC', 'America/New_York', o.order_capture_timestamp)   capture_timestamp_eastern,
               convert_timezone('UTC', 'America/New_York', i.released_timestamp)        released_timestamp_eastern,
               convert_timezone('UTC', 'America/New_York', i.nmn_updated_timestamp)     last_updated_timestamp_eastern,
               i.ordered_fulfillment_mode                                               fulfillment_mode,
               i.order_id                                                               order_id,
               i.line_number                                                            line_number,
               i.line_seq_number                                                        line_seq_number,
               CONCAT(i.order_id, '.', LPAD(i.fr_sequence_number, 3, 0))                fr_number,
               1                                                                        demand_unit,
               i.sku                                                                    sku,
               i.pickup_facility_number                                                 facility_number
          FROM ${oso_silver_catalog}.oso.newman_item i
            LEFT JOIN ${oso_silver_catalog}.oso.newman_order o
                ON o.order_id = i.order_id
       WHERE i.managed_by = 'newman' and nvl(i.routing_partner, 'fit') = 'fit'
         AND i.item_status >= 300 AND i.item_status < 790
         AND i.source_facility_type <> 'DC'
         AND NOT EXISTS (
            SELECT 1
            FROM completed_frs c
            WHERE c.order_id = i.order_id
              AND c.line_number = i.line_number
              AND c.line_seq_number = i.line_seq_number
              AND c.fr_number= CONCAT(i.order_id, '.', LPAD(i.fr_sequence_number, 3, 0))
        )
    ),
    all_frs (
        SELECT * FROM completed_frs
        UNION ALL
        SELECT * FROM open_frs
    )

    SELECT *
      FROM all_frs
    ORDER BY released_timestamp_eastern;

CREATE TABLE sku_inventory (
        sku INT NOT NULL COMMENT 'A product sku code.',
        location_id INT NOT NULL COMMENT 'The location ID associated to the sku inventory record.',
        atp_qty INT NOT NULL COMMENT 'The Available to Promise quantity available for the sku at this store location.',
        isa_qty INT NOT NULL COMMENT 'The ISA Quantity is the on-hand inventory that is available for in-store purchase for the specified store location.',
        bopl_qty INT NOT NULL COMMENT 'The Buy Online Pickup Later quantity on-hand inventory that is available at this store location.',
        inventory_change_event_utc TIMESTAMP NOT NULL COMMENT 'The timestamp of when the inventory was updated for this sku and location.',
        silver_created_on_utc TIMESTAMP NOT NULL COMMENT 'The timestamp of when this record was entered into the datalake.',
        silver_created_by STRING NOT NULL COMMENT 'The ID of the user that ran the job/pipeline that created this record.',
        silver_updated_on_utc TIMESTAMP NOT NULL COMMENT 'The timestamp of when this record was updated.',
        silver_updated_by STRING NOT NULL COMMENT 'The ID of the user that ran the job/pipeline that updated this record.',
        CONSTRAINT `sku_inventory_pk` PRIMARY KEY (`sku`, `location_id`, `inventory_change_event_utc`))
    USING delta
    COMMENT 'This table records inventory across multiple programs (atp, isa, and bopl) at the sku level for every location that product is sold.'
    CLUSTER BY (sku,location_id,inventory_change_event_utc,silver_created_on_utc)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE stg_oso_order_delivery (
        order_delivery_key STRING NOT NULL COMMENT 'Unique identifier for the order delivery record',
        webstore_key BIGINT,
        package_type_id BIGINT,
        sci_lpn_id BIGINT,
        tracking_number STRING COMMENT 'Shipment tracking number',
        delivery_status_cd STRING,
        fulfillment_date_key BIGINT,
        fulfillment_location_cd BIGINT,
        actual_shipped_dttm TIMESTAMP,
        shipped_qty BIGINT,
        package_type_descr STRING,
        ship_via STRING,
        carrier_key BIGINT,
        fulfillment_mode_key BIGINT COMMENT 'Identifier for the fulfillment mode',
        order_fulfill_number BIGINT COMMENT 'Composite order-fulfill number',
        web_ord_num BIGINT COMMENT 'Athlete/web order number',
        lpn_create_dttm TIMESTAMP,
        date_last_modified_utc TIMESTAMP COMMENT 'Last modified in source system',
        modified_by STRING NOT NULL COMMENT 'Hard-coded value indicating update source: "OSOSilver"',
        silver_created_on_utc TIMESTAMP NOT NULL COMMENT 'Ingestion timestamp',
        silver_created_by STRING NOT NULL COMMENT 'Pipeline/user that created this row',
        silver_updated_on_utc TIMESTAMP NOT NULL COMMENT 'Last update timestamp',
        silver_updated_by STRING NOT NULL COMMENT 'Pipeline/user that updated this row',
        CONSTRAINT `order_delivery_pk` PRIMARY KEY (`order_delivery_key`))
    USING delta
    COMMENT 'OSO staging table storing order delivery details.'
    CLUSTER BY (web_ord_num,order_delivery_key)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.columnMapping.mode' = 'name',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.appendOnly' = 'supported',
      'delta.feature.changeDataFeed' = 'supported',
      'delta.feature.checkConstraints' = 'supported',
      'delta.feature.columnMapping' = 'supported',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.generatedColumns' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE stg_oso_order_fulfill (
        order_fulfill_number BIGINT NOT NULL COMMENT 'Composite of order_id, ".00", and fr_sequence_number',
        chain_key STRING,
        fulfillment_status_cd STRING COMMENT 'Fulfillment status code. See prod_oso_db.oso_ref.item_status',
        fulfillment_status_dttm TIMESTAMP COMMENT 'UTC timestamp when OSO last updated this record in Newman (nmn_updated_timestamp)',
        fulfillment_date_key BIGINT COMMENT 'UTC timestamp when package was marked fulfilled (package_ship_timestamp) as YYYYMMDD numeric',
        channel_type_key INT,
        fulfillment_location_cd BIGINT COMMENT 'Facility number where item will be sourced (source_facility_number)',
        store_key BIGINT COMMENT 'Facility number where item will be sourced (source_facility_number)',
        shipment_type_cd STRING,
        carrier_key BIGINT,
        fulfillment_mode_key BIGINT,
        promise_date_key INT COMMENT 'Latest promise date presented to Athlete (promise_end_date) as YYYYMMDD numeric',
        ship_state STRING,
        ship_zip STRING COMMENT 'Destination zip code as defined by Athlete Order (destination_zip)',
        web_ord_num BIGINT COMMENT 'Customer order number as defined by Athlete Order (order_id)',
        vendor_key BIGINT COMMENT 'Facility number where item will be sourced (source_facility_number)',
        ship_method STRING,
        do_create_dttm TIMESTAMP COMMENT 'UTC timestamp when OSO determined a source (sourced_timestamp)',
        po_number STRING COMMENT 'Purchase order number generated by Vendor Fulfillment Technology (po_number)',
        date_last_modified TIMESTAMP,
        modified_by STRING,
        silver_created_on_utc TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the record was first inserted (silver_layer_timestamp)',
        silver_created_by STRING NOT NULL COMMENT 'ID of the user/pipeline that created this record',
        silver_updated_on_utc TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the record was last updated (silver_layer_update_timestamp)',
        silver_updated_by STRING NOT NULL COMMENT 'ID of the user/pipeline that updated this record',
        min_fulfillment_dttm TIMESTAMP COMMENT 'Earliest fulfillment event timestamp per order_fulfill_number',
        CONSTRAINT `stg_oso_order_fulfill_pk1` PRIMARY KEY (`order_fulfill_number`))
    USING delta
    COMMENT 'Table storing athlete order fulfillment details'
    CLUSTER BY (order_fulfill_number)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.appendOnly' = 'supported',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE stg_oso_order_fulfill_quarantine (
        order_fulfill_number BIGINT NOT NULL COMMENT 'Composite of order_id, ".00", and fr_sequence_number',
        chain_key STRING,
        fulfillment_status_cd STRING COMMENT 'Fulfillment status code. See prod_oso_db.oso_ref.item_status',
        fulfillment_status_dttm TIMESTAMP COMMENT 'UTC timestamp when OSO last updated this record in Newman (nmn_updated_timestamp)',
        fulfillment_date_key BIGINT COMMENT 'UTC timestamp when package was marked fulfilled (package_ship_timestamp) as YYYYMMDD numeric',
        channel_type_key INT,
        fulfillment_location_cd BIGINT COMMENT 'Facility number where item will be sourced (source_facility_number)',
        store_key BIGINT COMMENT 'Facility number where item will be sourced (source_facility_number)',
        shipment_type_cd STRING,
        carrier_key BIGINT,
        fulfillment_mode_key BIGINT,
        promise_date_key INT COMMENT 'Latest promise date presented to Athlete (promise_end_date) as YYYYMMDD numeric',
        ship_state STRING,
        ship_zip STRING COMMENT 'Destination zip code as defined by Athlete Order (destination_zip)',
        web_ord_num BIGINT COMMENT 'Customer order number as defined by Athlete Order (order_id)',
        vendor_key BIGINT COMMENT 'Facility number where item will be sourced (source_facility_number)',
        ship_method STRING,
        do_create_dttm TIMESTAMP COMMENT 'UTC timestamp when OSO determined a source (sourced_timestamp)',
        po_number STRING COMMENT 'Purchase order number generated by Vendor Fulfillment Technology (po_number)',
        date_last_modified TIMESTAMP,
        modified_by STRING,
        silver_created_on_utc TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the record was first inserted (silver_layer_timestamp)',
        silver_created_by STRING NOT NULL COMMENT 'ID of the user/pipeline that created this record',
        silver_updated_on_utc TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the record was last updated (silver_layer_update_timestamp)',
        silver_updated_by STRING NOT NULL COMMENT 'ID of the user/pipeline that updated this record',
        min_fulfillment_dttm TIMESTAMP COMMENT 'Earliest fulfillment event timestamp per order_fulfill_number',
        CONSTRAINT `stg_oso_order_fulfill_quarantine_pk` PRIMARY KEY (`order_fulfill_number`))
    USING delta
    COMMENT 'Table storing athlete order fulfillment details'
    CLUSTER BY (order_fulfill_number)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.appendOnly' = 'supported',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE stg_oso_order_header (
        web_ord_num STRING NOT NULL COMMENT 'Customer order number (newman_item.order_id)',
        chain_key STRING,
        order_status_dttm TIMESTAMP NOT NULL,
        order_status_key STRING NOT NULL,
        date_last_modified TIMESTAMP NOT NULL COMMENT 'Source update timestamp, e.g. silver_layer_update_timestamp',
        modified_by STRING NOT NULL COMMENT 'Process or user that last modified this record, e.g. "OSOSilver"',
        silver_created_on_utc TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the record was first inserted (silver_layer_timestamp)',
        silver_created_by STRING NOT NULL COMMENT 'ID of the user/pipeline that created this record',
        silver_updated_on_utc TIMESTAMP NOT NULL COMMENT 'UTC timestamp when the record was last updated (silver_layer_update_timestamp)',
        silver_updated_by STRING NOT NULL COMMENT 'ID of the user/pipeline that updated this record',
        CONSTRAINT `pk_order_header` PRIMARY KEY (`web_ord_num`))
    USING delta
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.appendOnly' = 'supported',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');

CREATE TABLE stg_oso_order_sku (
        web_ord_num BIGINT NOT NULL COMMENT 'Athlete order number',
        sku BIGINT NOT NULL COMMENT 'A product sku code',
        order_sku_status_key BIGINT,
        po_number BIGINT,
        min_do_create_dttm_utc TIMESTAMP,
        chain_key BIGINT,
        order_sku_status_dttm TIMESTAMP,
        date_last_modified_utc TIMESTAMP COMMENT 'Last modified date pulled from oso/newman_item.',
        modified_by STRING COMMENT 'Hard‑coded value indicating update source: "OSOSilver"',
        silver_created_on_utc TIMESTAMP NOT NULL COMMENT 'The timestamp of when this record was entered into the datalake.',
        silver_created_by STRING NOT NULL COMMENT 'The ID of the user that ran the job/pipeline that created this record.',
        silver_updated_on_utc TIMESTAMP NOT NULL COMMENT 'The timestamp of when this record was updated.',
        silver_updated_by STRING NOT NULL COMMENT 'The ID of the user that ran the job/pipeline that updated this record.',
        CONSTRAINT `fulfillment_order_sku_pk` PRIMARY KEY (`web_ord_num`, `sku`))
    USING delta
    COMMENT 'OSO staging table storing line item details about the athlete order.'
    CLUSTER BY (sku,web_ord_num)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.appendOnly' = 'supported',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE stg_oso_order_sku_shipment (
        order_sku_shipment_key BIGINT NOT NULL COMMENT 'Surrogate record key (not  part of PK)',
        fulfillment_date_key BIGINT COMMENT 'YYYYMMDD of the shipment event',
        fulfillment_location_cd BIGINT COMMENT 'Facility number where SKU was shipped (newman_item.source_facility_number)',
        shipped_units BIGINT COMMENT 'Quantity of the SKU shipped',
        webstore_key BIGINT COMMENT 'FK to webstore dimension (if applicable)',
        tracking_number STRING COMMENT 'Carrier tracking number for this shipment',
        dks_sku BIGINT NOT NULL COMMENT 'SKU identifier from Newman item (newman_item.sku)',
        sci_lpn_id BIGINT COMMENT 'SCI LPN identifier (sci_rpt_eom_fedex_tracking.dtl_lpn_id)',
        order_fulfill_number BIGINT COMMENT 'Composite of order_id + zero-padded 4-digit sequence (e.g. 0001 → 123450001)',
        eom_shipped_dttm TIMESTAMP COMMENT 'Timestamp when EOM marked this order shipped',
        web_ord_num BIGINT NOT NULL COMMENT 'Customer order number (newman_item.order_id)',
        date_last_modified TIMESTAMP COMMENT 'Source update timestamp, e.g. silver_layer_update_timestamp',
        modified_by STRING COMMENT 'Process or user that last modified this record, e.g. "OSOSilver"',
        silver_created_on_utc TIMESTAMP NOT NULL COMMENT 'UTC when record was first inserted (silver layer)',
        silver_created_by STRING NOT NULL COMMENT 'ID of pipeline that created this record',
        silver_updated_on_utc TIMESTAMP NOT NULL COMMENT 'UTC when record was last updated (silver layer)',
        silver_updated_by STRING NOT NULL COMMENT 'ID of pipeline that updated this record',
        CONSTRAINT `stg_oso_order_sku_shipment_pk` PRIMARY KEY (`order_sku_shipment_key`))
    USING delta
    COMMENT 'Per-SKU shipment details for each order'
    CLUSTER BY (order_sku_shipment_key)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.appendOnly' = 'supported',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE stg_oso_txn_order_sku (
        trans_type_key BIGINT COMMENT 'Transaction type identifier',
        txn_date_key BIGINT NOT NULL COMMENT 'Date Key of the transaction',
        txn_time_key BIGINT NOT NULL COMMENT 'Time-of-day key for the transaction',
        web_ord_num BIGINT NOT NULL COMMENT 'Athlete/web order number',
        txn_seq_number STRING NOT NULL COMMENT 'Sequence number of this txn within the order',
        dks_sku BIGINT COMMENT 'Product SKU code',
        units BIGINT COMMENT 'Quantity in this transaction',
        tracking_number STRING COMMENT 'Fulfillment/tracking identifier',
        order_fulfill_number BIGINT COMMENT 'Composite order-fulfill number',
        estimated_ship_date TIMESTAMP COMMENT 'Estimated ship date',
        source_reason_cd STRING COMMENT 'Source reason code',
        decline_dttm TIMESTAMP COMMENT 'Decline timestamp (if any)',
        decline_unit BIGINT COMMENT 'Declined unit count',
        chain_key BIGINT NOT NULL COMMENT 'Store chain key',
        source_store_cd STRING COMMENT 'Source store code',
        date_last_modified TIMESTAMP COMMENT 'Last modified in source system',
        modified_by STRING COMMENT 'Hard-coded value indicating update source: "OSOSilver"',
        silver_created_on_utc TIMESTAMP NOT NULL COMMENT 'Ingestion timestamp',
        silver_created_by STRING NOT NULL COMMENT 'Pipeline/user that created this row',
        silver_updated_on_utc TIMESTAMP NOT NULL COMMENT 'Last update timestamp',
        silver_updated_by STRING NOT NULL COMMENT 'Pipeline/user that updated this row',
        CONSTRAINT `txn_order_sku_pk` PRIMARY KEY (`txn_date_key`, `txn_time_key`, `web_ord_num`, `txn_seq_number`, `chain_key`))
    USING delta
    COMMENT 'Transaction-level detail per order-SKU, including decline/audit info'
    CLUSTER BY (web_ord_num,txn_seq_number,chain_key)
    TBLPROPERTIES (
      'delta.checkpoint.writeStatsAsJson' = 'false',
      'delta.checkpoint.writeStatsAsStruct' = 'true',
      'delta.checkpointPolicy' = 'v2',
      'delta.enableDeletionVectors' = 'true',
      'delta.enableRowTracking' = 'true',
      'delta.feature.appendOnly' = 'supported',
      'delta.feature.deletionVectors' = 'supported',
      'delta.feature.invariants' = 'supported',
      'delta.feature.rowTracking' = 'supported',
      'delta.feature.v2Checkpoint' = 'supported');

CREATE TABLE watermarks (
        catalog_name STRING NOT NULL COMMENT 'The catalog of the destination table that is being incrementally updated.',
        schema_name STRING NOT NULL COMMENT 'The schema name of the destination table that is being incrementally updated.',
        table_name STRING NOT NULL COMMENT 'The destination table name that is being incrementally updated.',
        watermark_epoch_sec_utc BIGINT COMMENT 'The watermark timestamp in UTC tz that is the last time we fetched data for the destination table',
        CONSTRAINT `watermark_pk` PRIMARY KEY (`catalog_name`, `schema_name`, `table_name`))
    USING delta
    COMMENT 'This table acts as a watermark indicator for incrementally fetching data from one or more source tables into a destination table. This table is used internally by the E-Commerce Data Engineering team.'
    TBLPROPERTIES (
        'delta.checkpoint.writeStatsAsJson' = 'false',
        'delta.checkpoint.writeStatsAsStruct' = 'true',
        'delta.enableDeletionVectors' = 'true',
        'delta.feature.deletionVectors' = 'supported',
        'delta.feature.invariants' = 'supported',
        'delta.minReaderVersion' = '3',
        'delta.minWriterVersion' = '7');
