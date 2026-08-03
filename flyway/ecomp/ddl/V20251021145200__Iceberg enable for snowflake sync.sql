--  Disable Deletion Vectors
ALTER TABLE ${web_ops_schema}.order_tender
  SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');

--  Purge existing Deletion Vectors
REORG TABLE ${web_ops_schema}.order_tender APPLY (PURGE);

--  Enable Iceberg compatibility
ALTER TABLE ${web_ops_schema}.order_tender SET TBLPROPERTIES(
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg');


-------------------------------------------------------------------------



ALTER TABLE ${ecom_dim_schema}.order_trans_type
  SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');


REORG TABLE ${ecom_dim_schema}.order_trans_type APPLY (PURGE);


ALTER TABLE ${ecom_dim_schema}.order_trans_type SET TBLPROPERTIES(
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg');


  -------------------------------------------------------------------------



ALTER TABLE ${ecom_dim_schema}.snp_web_product_assortment
  SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');


REORG TABLE ${ecom_dim_schema}.snp_web_product_assortment APPLY (PURGE);


ALTER TABLE ${ecom_dim_schema}.snp_web_product_assortment SET TBLPROPERTIES(
  'delta.columnMapping.mode' = 'name',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg');