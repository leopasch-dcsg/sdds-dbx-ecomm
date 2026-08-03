ALTER TABLE ltr_stg_linked_atc ADD COLUMN atc_event INT AFTER ecode;

ALTER TABLE ltr_stg_agg_atc ADD COLUMN daily_atc_count BIGINT AFTER ecode;

ALTER TABLE ltr_stg_atc_joined_impressions ADD COLUMN atc_count BIGINT after time_decay_impression_count;

ALTER TABLE ltr_stg_atc_search_ecodes_with_all_dates ADD COLUMN atc_count BIGINT after time_decay_impression_count;

ALTER TABLE ltr_stg_atc_agg_impressions ADD COLUMN total_atc_last_x_days BIGINT after total_impressions_last_x_days;

ALTER TABLE ltr_stg_click_counts ADD COLUMN daily_clicks BIGINT AFTER ecode;

ALTER TABLE ltr_stg_joined_impressions ADD COLUMN click_count BIGINT after time_decay_impression_count;

ALTER TABLE ltr_stg_search_ecodes_with_all_dates ADD COLUMN click_count BIGINT after time_decay_impression_count;

ALTER TABLE ltr_stg_agg_impressions ADD COLUMN total_clicks_last_x_days BIGINT after total_impressions_last_x_days;

ALTER TABLE ltr_stg_order_aggs_level_2 ADD COLUMN order_count BIGINT after ecode;

ALTER TABLE ltr_stg_order_rate_joined_impressions ADD COLUMN order_count BIGINT after time_decay_impression_count;

ALTER TABLE ltr_stg_order_rate_search_ecodes_with_all_dates ADD COLUMN order_count BIGINT after time_decay_impression_count;

ALTER TABLE ltr_stg_order_rate_agg_impressions ADD COLUMN total_orders_last_x_days BIGINT after time_decay_total_orders_last_x_days;