ALTER TABLE ltr_stg_init_searches ADD COLUMN search_age BIGINT AFTER time;

ALTER TABLE ltr_stg_unrolled_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_unrolled_impressions rename column impression_time to impression_time_utc;
ALTER TABLE ltr_stg_unrolled_impressions ADD COLUMN impression_age BIGINT AFTER impression_time_utc;
ALTER TABLE ltr_stg_unrolled_impressions ADD COLUMN impression_type STRING AFTER impression_id;

ALTER TABLE ltr_stg_grouped_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_grouped_impressions DROP COLUMN impressions;
ALTER TABLE ltr_stg_grouped_impressions ADD COLUMN impressions ARRAY<STRUCT<event_id: STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.', id: STRING, time: BIGINT, age: BIGINT, time_decay_impression: DOUBLE>>;

ALTER TABLE ltr_stg_init_searches_and_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_init_searches_and_impressions ADD COLUMN search_age BIGINT AFTER time;
ALTER TABLE ltr_stg_init_searches_and_impressions DROP COLUMN impressions;
ALTER TABLE ltr_stg_init_searches_and_impressions ADD COLUMN impressions ARRAY<STRUCT<event_id: STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.', id: STRING, time: BIGINT, age: BIGINT, time_decay_impression: DOUBLE>>;

ALTER TABLE ltr_stg_linked_searches SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_linked_searches ADD COLUMN time_decay_search DOUBLE AFTER search_term;
ALTER TABLE ltr_stg_linked_searches DROP COLUMN impressions;
ALTER TABLE ltr_stg_linked_searches ADD COLUMN impressions ARRAY<STRUCT<event_id: STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.', id: STRING, time: BIGINT, age: BIGINT, time_decay_impression: DOUBLE>>;

ALTER TABLE ltr_abb_linked_searches SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_abb_linked_searches ADD COLUMN time_decay_search DOUBLE AFTER search_term;
ALTER TABLE ltr_abb_linked_searches DROP COLUMN impressions;
ALTER TABLE ltr_abb_linked_searches ADD COLUMN impressions ARRAY<STRUCT<event_id: STRING COMMENT 'The ML Event UUID, this is a unique identifier for every record.', id: STRING, time: BIGINT, age: BIGINT, time_decay_impression: DOUBLE>>;

ALTER TABLE ltr_abb_impressions_agg SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_abb_impressions_agg ADD COLUMN time_decay_search_count DOUBLE AFTER search_count;
ALTER TABLE ltr_abb_impressions_agg ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_abb_impressions_agg ADD COLUMN impression_times ARRAY<BIGINT> AFTER time_decay_impression_count;
ALTER TABLE ltr_abb_impressions_agg ADD COLUMN impression_signal_age ARRAY<BIGINT> AFTER impression_times;
ALTER TABLE ltr_abb_impressions_agg DROP COLUMN search_count;
ALTER TABLE ltr_abb_impressions_agg DROP COLUMN impression_count;

ALTER TABLE ltr_stg_init_atc ADD COLUMN dym_search_term STRING AFTER search_term;

ALTER TABLE ltr_stg_init_atc_agg ADD COLUMN dym_search_term STRING AFTER search_term;
ALTER TABLE ltr_stg_init_atc_agg ADD COLUMN atc_age BIGINT AFTER hitid;

ALTER TABLE ltr_stg_linked_atc SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_linked_atc ADD COLUMN time_decay_atc DOUBLE AFTER atc_time;
ALTER TABLE ltr_stg_linked_atc ADD COLUMN atc_age BIGINT AFTER time_decay_atc;
ALTER TABLE ltr_stg_linked_atc DROP COLUMN atc_event;

ALTER TABLE ltr_stg_agg_atc SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_agg_atc ADD COLUMN time_decay_atc_count DOUBLE AFTER daily_atc_count;
ALTER TABLE ltr_stg_agg_atc ADD COLUMN atc_ages ARRAY<BIGINT> AFTER time_decay_atc_count;
ALTER TABLE ltr_stg_agg_atc DROP COLUMN daily_atc_count;

ALTER TABLE ltr_stg_atc_joined_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_atc_joined_impressions ADD COLUMN time_decay_atc_count DOUBLE AFTER atc_count;
ALTER TABLE ltr_stg_atc_joined_impressions ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_atc_joined_impressions ADD COLUMN atc_ages ARRAY<BIGINT> AFTER time_decay_atc_count;
ALTER TABLE ltr_stg_atc_joined_impressions DROP COLUMN impression_count;
ALTER TABLE ltr_stg_atc_joined_impressions DROP COLUMN atc_count;

ALTER TABLE ltr_stg_atc_search_ecodes_with_all_dates SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_atc_search_ecodes_with_all_dates ADD COLUMN time_decay_atc_count DOUBLE AFTER atc_count;
ALTER TABLE ltr_stg_atc_search_ecodes_with_all_dates ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_atc_search_ecodes_with_all_dates DROP COLUMN impression_count;
ALTER TABLE ltr_stg_atc_search_ecodes_with_all_dates DROP COLUMN atc_count;

ALTER TABLE ltr_stg_atc_agg_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_atc_agg_impressions ADD COLUMN time_decay_atc_count DOUBLE AFTER atc_count;
ALTER TABLE ltr_stg_atc_agg_impressions ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_atc_agg_impressions ADD COLUMN time_decay_total_atc_last_x_days DOUBLE AFTER total_atc_last_x_days;
ALTER TABLE ltr_stg_atc_agg_impressions ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_atc_agg_impressions DROP COLUMN impression_count;
ALTER TABLE ltr_stg_atc_agg_impressions DROP COLUMN atc_count;
ALTER TABLE ltr_stg_atc_agg_impressions DROP COLUMN total_atc_last_x_days;
ALTER TABLE ltr_stg_atc_agg_impressions DROP COLUMN total_impressions_last_x_days;

ALTER TABLE ltr_stg_atc_global_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_atc_global_priors DROP COLUMN avg_atc_last_x_days_global;
ALTER TABLE ltr_stg_atc_global_priors ADD COLUMN time_decay_total_atc_last_x_days_global DOUBLE AFTER date_est;
ALTER TABLE ltr_stg_atc_global_priors ADD COLUMN time_decay_total_impressions_last_x_days_global DOUBLE AFTER time_decay_total_atc_last_x_days_global;
ALTER TABLE ltr_stg_atc_global_priors ADD COLUMN time_decay_atc_last_x_days_global DOUBLE AFTER time_decay_total_impressions_last_x_days_global;

ALTER TABLE ltr_stg_atc_query_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_atc_query_priors ADD COLUMN time_decay_total_atc_last_x_days_query DOUBLE AFTER total_atc_last_x_days_query;
ALTER TABLE ltr_stg_atc_query_priors ADD COLUMN time_decay_total_impressions_last_x_days_query DOUBLE AFTER total_impressions_last_x_days_query;
ALTER TABLE ltr_stg_atc_query_priors ADD COLUMN time_decay_atc_last_x_days_query DOUBLE AFTER total_atc_last_x_days_query;
ALTER TABLE ltr_stg_atc_query_priors DROP COLUMN total_daily_atc;
ALTER TABLE ltr_stg_atc_query_priors DROP COLUMN total_daily_impressions;
ALTER TABLE ltr_stg_atc_query_priors DROP COLUMN avg_atc_last_x_days_query;
ALTER TABLE ltr_stg_atc_query_priors DROP COLUMN total_atc_last_x_days_query;
ALTER TABLE ltr_stg_atc_query_priors DROP COLUMN total_impressions_last_x_days_query;

ALTER TABLE ltr_stg_atc_init_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_atc_init_priors ADD COLUMN time_decay_total_atc_last_x_days DOUBLE AFTER total_atc_last_x_days;
ALTER TABLE ltr_stg_atc_init_priors ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_atc_init_priors ADD COLUMN time_decay_prior_atc_rate DOUBLE AFTER prior_atc_rate;
ALTER TABLE ltr_stg_atc_init_priors DROP COLUMN total_atc_last_x_days;
ALTER TABLE ltr_stg_atc_init_priors DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_atc_init_priors DROP COLUMN prior_atc_rate;

ALTER TABLE ltr_stg_atc_alphas_and_betas SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_atc_alphas_and_betas ADD COLUMN time_decay_total_atc_last_x_days DOUBLE AFTER total_atc_last_x_days;
ALTER TABLE ltr_stg_atc_alphas_and_betas ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_atc_alphas_and_betas ADD COLUMN time_decay_prior_atc_rate DOUBLE AFTER prior_atc_rate;
ALTER TABLE ltr_stg_atc_alphas_and_betas ADD COLUMN time_decay_prior_alpha DOUBLE AFTER prior_alpha;
ALTER TABLE ltr_stg_atc_alphas_and_betas ADD COLUMN time_decay_prior_beta DOUBLE AFTER prior_beta;
ALTER TABLE ltr_stg_atc_alphas_and_betas ADD COLUMN time_decay_alpha_n DOUBLE AFTER alpha_n;
ALTER TABLE ltr_stg_atc_alphas_and_betas ADD COLUMN time_decay_beta_n DOUBLE AFTER beta_n;
ALTER TABLE ltr_stg_atc_alphas_and_betas DROP COLUMN total_atc_last_x_days;
ALTER TABLE ltr_stg_atc_alphas_and_betas DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_atc_alphas_and_betas DROP COLUMN prior_atc_rate;
ALTER TABLE ltr_stg_atc_alphas_and_betas DROP COLUMN prior_alpha;
ALTER TABLE ltr_stg_atc_alphas_and_betas DROP COLUMN prior_beta;
ALTER TABLE ltr_stg_atc_alphas_and_betas DROP COLUMN alpha_n;
ALTER TABLE ltr_stg_atc_alphas_and_betas DROP COLUMN beta_n;

ALTER TABLE ltr_stg_atc_posteriors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_total_atc_last_x_days DOUBLE AFTER total_atc_last_x_days;
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_prior_atc_rate DOUBLE AFTER prior_atc_rate;
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_prior_alpha DOUBLE AFTER prior_alpha;
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_prior_beta DOUBLE AFTER prior_beta;
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_alpha_n DOUBLE AFTER alpha_n;
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_beta_n DOUBLE AFTER beta_n;
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_atc_rate_posterior DOUBLE AFTER atc_rate_posterior;
ALTER TABLE ltr_stg_atc_posteriors ADD COLUMN time_decay_prior_atc_rate_std DOUBLE AFTER prior_atc_rate_std;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN atc_rate_posterior;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN prior_atc_rate_std;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN total_atc_last_x_days;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN prior_atc_rate;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN prior_alpha;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN prior_beta;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN alpha_n;
ALTER TABLE ltr_stg_atc_posteriors DROP COLUMN beta_n;

ALTER TABLE ltr_abb_atc_rate SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_total_atc_last_x_days DOUBLE AFTER ecode;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER time_decay_total_atc_last_x_days;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_prior_atc_rate DOUBLE AFTER time_decay_total_impressions_last_x_days;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_prior_alpha DOUBLE AFTER time_decay_prior_atc_rate;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_prior_beta DOUBLE AFTER time_decay_prior_alpha;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_alpha_n DOUBLE AFTER time_decay_prior_beta;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_beta_n DOUBLE AFTER time_decay_alpha_n;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_atc_rate_posterior DOUBLE AFTER time_decay_beta_n;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_prior_atc_rate_std DOUBLE AFTER time_decay_atc_rate_posterior;
ALTER TABLE ltr_abb_atc_rate ADD COLUMN time_decay_atc_rate_posterior_z_score DOUBLE AFTER time_decay_prior_atc_rate_std;
ALTER TABLE ltr_abb_atc_rate DROP COLUMN prior_alpha;
ALTER TABLE ltr_abb_atc_rate DROP COLUMN prior_beta;
ALTER TABLE ltr_abb_atc_rate DROP COLUMN alpha_n;
ALTER TABLE ltr_abb_atc_rate DROP COLUMN beta_n;
ALTER TABLE ltr_abb_atc_rate RENAME column signed_kl_divergence to time_decay_signed_kl_divergence;
ALTER TABLE ltr_abb_atc_rate RENAME column signed_js_divergence_shifted to time_decay_signed_js_divergence_shifted;

ALTER TABLE ltr_stg_init_clicks SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_init_clicks DROP COLUMN tran_date_utc;
ALTER TABLE ltr_stg_init_clicks DROP COLUMN _evar2;
ALTER TABLE ltr_stg_init_clicks ADD COLUMN dym_search_term STRING AFTER search_term;

ALTER TABLE ltr_stg_linked_clicks ADD COLUMN time_decay_click DOUBLE AFTER ecode;
ALTER TABLE ltr_stg_linked_clicks ADD COLUMN signal_age BIGINT AFTER time_decay_click;

ALTER TABLE ltr_stg_click_counts SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_click_counts ADD COLUMN time_decay_clicks DOUBLE AFTER daily_clicks;
ALTER TABLE ltr_stg_click_counts ADD COLUMN click_search_times ARRAY<BIGINT> AFTER time_decay_clicks;
ALTER TABLE ltr_stg_click_counts ADD COLUMN click_signal_age ARRAY<BIGINT> AFTER click_search_times;
ALTER TABLE ltr_stg_click_counts DROP COLUMN daily_clicks;

ALTER TABLE ltr_stg_joined_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_joined_impressions ADD COLUMN time_decay_click_count DOUBLE AFTER click_count;
ALTER TABLE ltr_stg_joined_impressions ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_joined_impressions DROP COLUMN impression_count;
ALTER TABLE ltr_stg_joined_impressions DROP COLUMN click_count;

ALTER TABLE ltr_stg_search_ecodes_with_all_dates SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_search_ecodes_with_all_dates ADD COLUMN time_decay_click_count DOUBLE AFTER click_count;
ALTER TABLE ltr_stg_search_ecodes_with_all_dates ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_search_ecodes_with_all_dates DROP COLUMN impression_count;
ALTER TABLE ltr_stg_search_ecodes_with_all_dates DROP COLUMN click_count;

ALTER TABLE ltr_stg_agg_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_agg_impressions ADD COLUMN time_decay_click_count DOUBLE AFTER click_count;
ALTER TABLE ltr_stg_agg_impressions ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_agg_impressions ADD COLUMN time_decay_total_clicks_last_x_days DOUBLE AFTER total_clicks_last_x_days;
ALTER TABLE ltr_stg_agg_impressions ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_agg_impressions DROP COLUMN impression_count;
ALTER TABLE ltr_stg_agg_impressions DROP COLUMN click_count;
ALTER TABLE ltr_stg_agg_impressions DROP COLUMN total_clicks_last_x_days;
ALTER TABLE ltr_stg_agg_impressions DROP COLUMN total_impressions_last_x_days;

ALTER TABLE ltr_stg_global_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_global_priors ADD COLUMN time_decay_total_clicks_last_x_days_global DOUBLE AFTER total_clicks_last_x_days_global;
ALTER TABLE ltr_stg_global_priors ADD COLUMN time_decay_total_impressions_last_x_days_global DOUBLE AFTER total_impressions_last_x_days_global;
ALTER TABLE ltr_stg_global_priors ADD COLUMN time_decay_ctr_last_x_days_global DOUBLE AFTER avg_ctr_last_x_days_global;
ALTER TABLE ltr_stg_global_priors DROP COLUMN total_clicks_last_x_days_global;
ALTER TABLE ltr_stg_global_priors DROP COLUMN total_impressions_last_x_days_global;
ALTER TABLE ltr_stg_global_priors DROP COLUMN avg_ctr_last_x_days_global;

ALTER TABLE ltr_stg_query_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_query_priors ADD COLUMN time_decay_total_clicks_last_x_days_query DOUBLE AFTER total_clicks_last_x_days_query;
ALTER TABLE ltr_stg_query_priors ADD COLUMN time_decay_total_impressions_last_x_days_query DOUBLE AFTER total_impressions_last_x_days_query;
ALTER TABLE ltr_stg_query_priors ADD COLUMN time_decay_ctr_last_x_days_query DOUBLE AFTER avg_ctr_last_x_days_query;
ALTER TABLE ltr_stg_query_priors DROP COLUMN total_clicks_last_x_days_query;
ALTER TABLE ltr_stg_query_priors DROP COLUMN total_impressions_last_x_days_query;
ALTER TABLE ltr_stg_query_priors DROP COLUMN avg_ctr_last_x_days_query;

ALTER TABLE ltr_stg_init_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_init_priors ADD COLUMN time_decay_total_clicks_last_x_days DOUBLE AFTER total_clicks_last_x_days;
ALTER TABLE ltr_stg_init_priors ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_init_priors ADD COLUMN time_decay_prior_ctr DOUBLE AFTER prior_ctr;
ALTER TABLE ltr_stg_init_priors DROP COLUMN total_clicks_last_x_days;
ALTER TABLE ltr_stg_init_priors DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_init_priors DROP COLUMN prior_ctr;

ALTER TABLE ltr_stg_alphas_and_betas SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_alphas_and_betas ADD COLUMN time_decay_total_clicks_last_x_days DOUBLE AFTER total_clicks_last_x_days;
ALTER TABLE ltr_stg_alphas_and_betas ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_alphas_and_betas ADD COLUMN time_decay_prior_ctr DOUBLE AFTER prior_ctr;
ALTER TABLE ltr_stg_alphas_and_betas ADD COLUMN time_decay_prior_alpha DOUBLE AFTER prior_alpha;
ALTER TABLE ltr_stg_alphas_and_betas ADD COLUMN time_decay_prior_beta DOUBLE AFTER prior_beta;
ALTER TABLE ltr_stg_alphas_and_betas ADD COLUMN time_decay_alpha_n DOUBLE AFTER alpha_n;
ALTER TABLE ltr_stg_alphas_and_betas ADD COLUMN time_decay_beta_n DOUBLE AFTER beta_n;
ALTER TABLE ltr_stg_alphas_and_betas DROP COLUMN total_clicks_last_x_days;
ALTER TABLE ltr_stg_alphas_and_betas DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_alphas_and_betas DROP COLUMN prior_ctr;
ALTER TABLE ltr_stg_alphas_and_betas DROP COLUMN prior_alpha;
ALTER TABLE ltr_stg_alphas_and_betas DROP COLUMN prior_beta;
ALTER TABLE ltr_stg_alphas_and_betas DROP COLUMN alpha_n;
ALTER TABLE ltr_stg_alphas_and_betas DROP COLUMN beta_n;

ALTER TABLE ltr_stg_posteriors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_total_clicks_last_x_days DOUBLE AFTER total_clicks_last_x_days;
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_prior_ctr DOUBLE AFTER prior_ctr;
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_prior_alpha DOUBLE AFTER prior_alpha;
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_prior_beta DOUBLE AFTER prior_beta;
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_alpha_n DOUBLE AFTER alpha_n;
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_beta_n DOUBLE AFTER beta_n;
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_ctr_posterior DOUBLE AFTER ctr_posterior;
ALTER TABLE ltr_stg_posteriors ADD COLUMN time_decay_prior_ctr_std DOUBLE AFTER prior_ctr_std;
ALTER TABLE ltr_stg_posteriors DROP COLUMN total_clicks_last_x_days;
ALTER TABLE ltr_stg_posteriors DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_posteriors DROP COLUMN prior_ctr;
ALTER TABLE ltr_stg_posteriors DROP COLUMN prior_alpha;
ALTER TABLE ltr_stg_posteriors DROP COLUMN prior_beta;
ALTER TABLE ltr_stg_posteriors DROP COLUMN alpha_n;
ALTER TABLE ltr_stg_posteriors DROP COLUMN beta_n;
ALTER TABLE ltr_stg_posteriors DROP COLUMN ctr_posterior;
ALTER TABLE ltr_stg_posteriors DROP COLUMN prior_ctr_std;

ALTER TABLE ltr_abb_ctr SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_total_clicks_last_x_days DOUBLE AFTER ecode;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER time_decay_total_clicks_last_x_days;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_prior_ctr DOUBLE AFTER time_decay_total_impressions_last_x_days;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_prior_alpha DOUBLE AFTER time_decay_prior_ctr;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_prior_beta DOUBLE AFTER time_decay_prior_alpha;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_alpha_n DOUBLE AFTER time_decay_prior_beta;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_beta_n DOUBLE AFTER time_decay_alpha_n;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_ctr_posterior DOUBLE AFTER time_decay_beta_n;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_prior_ctr_std DOUBLE AFTER time_decay_ctr_posterior;
ALTER TABLE ltr_abb_ctr ADD COLUMN time_decay_ctr_posterior_z_score DOUBLE AFTER time_decay_prior_ctr_std;
ALTER TABLE ltr_abb_ctr DROP COLUMN prior_alpha;
ALTER TABLE ltr_abb_ctr DROP COLUMN prior_beta;
ALTER TABLE ltr_abb_ctr DROP COLUMN alpha_n;
ALTER TABLE ltr_abb_ctr DROP COLUMN beta_n;
ALTER TABLE ltr_abb_ctr RENAME column signed_kl_divergence to time_decay_signed_kl_divergence;
ALTER TABLE ltr_abb_ctr RENAME column signed_js_divergence_shifted to time_decay_signed_js_divergence_shifted;

ALTER TABLE ltr_stg_init_orders SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_init_orders DROP COLUMN tran_date_utc;
ALTER TABLE ltr_stg_init_orders ADD COLUMN dym_search_term STRING AFTER search_term;

ALTER TABLE ltr_stg_deduplicate_linked_orders SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_deduplicate_linked_orders DROP COLUMN row_num;

ALTER TABLE ltr_stg_order_aggs_level_1 ADD COLUMN time_decay_order DOUBLE AFTER total_order_units;
ALTER TABLE ltr_stg_order_aggs_level_1 ADD COLUMN order_age BIGINT AFTER time_decay_order;

ALTER TABLE ltr_stg_order_aggs_level_2 SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_aggs_level_2 ADD COLUMN time_decay_order_count DOUBLE AFTER order_count;
ALTER TABLE ltr_stg_order_aggs_level_2 DROP COLUMN order_count;

ALTER TABLE ltr_stg_order_rate_joined_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_rate_joined_impressions ADD COLUMN time_decay_order_count DOUBLE AFTER order_count;
ALTER TABLE ltr_stg_order_rate_joined_impressions ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_order_rate_joined_impressions DROP COLUMN order_count;
ALTER TABLE ltr_stg_order_rate_joined_impressions DROP COLUMN impression_count;

ALTER TABLE ltr_stg_order_rate_search_ecodes_with_all_dates SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_rate_search_ecodes_with_all_dates ADD COLUMN time_decay_order_count DOUBLE AFTER order_count;
ALTER TABLE ltr_stg_order_rate_search_ecodes_with_all_dates ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_order_rate_search_ecodes_with_all_dates DROP COLUMN order_count;
ALTER TABLE ltr_stg_order_rate_search_ecodes_with_all_dates DROP COLUMN impression_count;

ALTER TABLE ltr_stg_order_rate_agg_impressions SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_rate_agg_impressions ADD COLUMN time_decay_order_count DOUBLE AFTER order_count;
ALTER TABLE ltr_stg_order_rate_agg_impressions ADD COLUMN time_decay_impression_count DOUBLE AFTER impression_count;
ALTER TABLE ltr_stg_order_rate_agg_impressions ADD COLUMN time_decay_total_orders_last_x_days DOUBLE AFTER total_orders_last_x_days;
ALTER TABLE ltr_stg_order_rate_agg_impressions ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_order_rate_agg_impressions DROP COLUMN order_count;
ALTER TABLE ltr_stg_order_rate_agg_impressions DROP COLUMN impression_count;
ALTER TABLE ltr_stg_order_rate_agg_impressions DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_order_rate_agg_impressions DROP COLUMN total_orders_last_x_days;

ALTER TABLE ltr_stg_order_rate_global_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_rate_global_priors ADD COLUMN time_decay_total_orders_last_x_days_global DOUBLE AFTER total_orders_last_x_days_global;
ALTER TABLE ltr_stg_order_rate_global_priors ADD COLUMN time_decay_total_impressions_last_x_days_global DOUBLE AFTER total_impressions_last_x_days_global;
ALTER TABLE ltr_stg_order_rate_global_priors ADD COLUMN time_decay_order_rate_last_x_days_global DOUBLE AFTER avg_order_rate_last_x_days_global;
ALTER TABLE ltr_stg_order_rate_global_priors DROP COLUMN total_orders_last_x_days_global;
ALTER TABLE ltr_stg_order_rate_global_priors DROP COLUMN total_impressions_last_x_days_global;
ALTER TABLE ltr_stg_order_rate_global_priors DROP COLUMN avg_order_rate_last_x_days_global;

ALTER TABLE ltr_stg_order_rate_query_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_rate_query_priors ADD COLUMN time_decay_total_orders_last_x_days_query DOUBLE AFTER total_orders_last_x_days_query;
ALTER TABLE ltr_stg_order_rate_query_priors ADD COLUMN time_decay_total_impressions_last_x_days_query DOUBLE AFTER total_impressions_last_x_days_query;
ALTER TABLE ltr_stg_order_rate_query_priors ADD COLUMN time_decay_order_rate_last_x_days_query DOUBLE AFTER avg_order_rate_last_x_days_query;
ALTER TABLE ltr_stg_order_rate_query_priors DROP COLUMN total_orders_last_x_days_query;
ALTER TABLE ltr_stg_order_rate_query_priors DROP COLUMN total_impressions_last_x_days_query;
ALTER TABLE ltr_stg_order_rate_query_priors DROP COLUMN avg_order_rate_last_x_days_query;

ALTER TABLE ltr_stg_order_rate_init_priors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_rate_init_priors ADD COLUMN time_decay_total_orders_last_x_days DOUBLE AFTER total_orders_last_x_days;
ALTER TABLE ltr_stg_order_rate_init_priors ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_order_rate_init_priors ADD COLUMN time_decay_prior_order_rate DOUBLE AFTER prior_order_rate;
ALTER TABLE ltr_stg_order_rate_init_priors DROP COLUMN total_orders_last_x_days;
ALTER TABLE ltr_stg_order_rate_init_priors DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_order_rate_init_priors DROP COLUMN prior_order_rate;

ALTER TABLE ltr_stg_order_rate_alphas_and_betas SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_rate_alphas_and_betas ADD COLUMN time_decay_total_orders_last_x_days DOUBLE AFTER total_orders_last_x_days;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas ADD COLUMN time_decay_prior_order_rate DOUBLE AFTER prior_order_rate;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas ADD COLUMN time_decay_prior_alpha DOUBLE AFTER prior_alpha;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas ADD COLUMN time_decay_prior_beta DOUBLE AFTER prior_beta;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas ADD COLUMN time_decay_alpha_n DOUBLE AFTER alpha_n;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas ADD COLUMN time_decay_beta_n DOUBLE AFTER beta_n;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas DROP COLUMN total_orders_last_x_days;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas DROP COLUMN prior_order_rate;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas DROP COLUMN prior_alpha;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas DROP COLUMN prior_beta;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas DROP COLUMN alpha_n;
ALTER TABLE ltr_stg_order_rate_alphas_and_betas DROP COLUMN beta_n;

ALTER TABLE ltr_stg_order_rate_posteriors SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_total_orders_last_x_days DOUBLE AFTER total_orders_last_x_days;
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER total_impressions_last_x_days;
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_prior_order_rate DOUBLE AFTER prior_order_rate;
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_prior_alpha DOUBLE AFTER prior_alpha;
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_prior_beta DOUBLE AFTER prior_beta;
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_alpha_n DOUBLE AFTER alpha_n;
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_beta_n DOUBLE AFTER beta_n;
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_order_rate_posterior DOUBLE AFTER order_rate_posterior;
ALTER TABLE ltr_stg_order_rate_posteriors ADD COLUMN time_decay_prior_order_rate_std DOUBLE AFTER prior_order_rate_std;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN total_orders_last_x_days;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN total_impressions_last_x_days;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN prior_order_rate;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN prior_alpha;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN prior_beta;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN alpha_n;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN beta_n;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN order_rate_posterior;
ALTER TABLE ltr_stg_order_rate_posteriors DROP COLUMN prior_order_rate_std;

ALTER TABLE ltr_abb_order_rate SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_abb_order_rate ADD COLUMN total_revenue DOUBLE AFTER ecode;
ALTER TABLE ltr_abb_order_rate ADD COLUMN total_units DOUBLE AFTER total_revenue;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_total_orders_last_x_days DOUBLE AFTER total_units;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_total_impressions_last_x_days DOUBLE AFTER time_decay_total_orders_last_x_days;
ALTER TABLE ltr_abb_order_rate ADD COLUMN total_revenue_last_x_days DOUBLE AFTER time_decay_total_impressions_last_x_days;
ALTER TABLE ltr_abb_order_rate ADD COLUMN total_units_last_x_days DOUBLE AFTER total_revenue_last_x_days;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_prior_order_rate DOUBLE AFTER total_units_last_x_days;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_prior_alpha DOUBLE AFTER prior_alpha;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_prior_beta DOUBLE AFTER prior_beta;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_alpha_n DOUBLE AFTER alpha_n;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_beta_n DOUBLE AFTER beta_n;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_order_rate_posterior DOUBLE AFTER time_decay_beta_n;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_prior_order_rate_std DOUBLE AFTER time_decay_order_rate_posterior;
ALTER TABLE ltr_abb_order_rate ADD COLUMN time_decay_order_rate_posterior_z_score DOUBLE AFTER time_decay_prior_order_rate_std;
ALTER TABLE ltr_abb_order_rate DROP COLUMN prior_alpha;
ALTER TABLE ltr_abb_order_rate DROP COLUMN prior_beta;
ALTER TABLE ltr_abb_order_rate DROP COLUMN alpha_n;
ALTER TABLE ltr_abb_order_rate DROP COLUMN beta_n;
ALTER TABLE ltr_abb_order_rate RENAME column signed_kl_divergence to time_decay_signed_kl_divergence;
ALTER TABLE ltr_abb_order_rate RENAME column signed_js_divergence_shifted to time_decay_signed_js_divergence_shifted;

ALTER TABLE ltr_feature_agg SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE ltr_feature_agg RENAME column ctr_signed_js_divergence_shifted to ctr_time_decay_signed_js_divergence_shifted;
ALTER TABLE ltr_feature_agg RENAME column atc_rate_signed_js_divergence_shifted to atc_rate_time_decay_signed_js_divergence_shifted;
ALTER TABLE ltr_feature_agg RENAME column order_rate_signed_js_divergence_shifted to order_rate_time_decay_signed_js_divergence_shifted;