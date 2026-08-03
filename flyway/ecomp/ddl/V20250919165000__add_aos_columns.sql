ALTER TABLE ${co_stage_schema}.order_message_cancel ADD COLUMN aosassociateid string AFTER aosstorenumber;

ALTER TABLE ${co_stage_schema}.order_message_placed ADD COLUMN aosassociateid string AFTER aosstorenumber;

ALTER TABLE ${co_stage_schema}.order_message_fulfill ADD COLUMN aosstorenumber string AFTER order_type;
ALTER TABLE ${co_stage_schema}.order_message_fulfill ADD COLUMN aosassociateid string AFTER aosstorenumber;