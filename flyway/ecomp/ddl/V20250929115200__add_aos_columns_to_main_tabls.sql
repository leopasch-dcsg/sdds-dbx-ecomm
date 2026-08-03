ALTER TABLE ${ecom_dim_schema}.order_header ADD COLUMN aos_associate_id string AFTER aos_store_key;

ALTER TABLE ${web_stage_schema}.stg_ddw_ready_to_fulfill ADD COLUMN aos_associate_id string AFTER aosstorenumber;

ALTER TABLE ${web_stage_schema}.stg_demand_line_unit_in_rvw ADD COLUMN aos_associate_id string AFTER aos_store;