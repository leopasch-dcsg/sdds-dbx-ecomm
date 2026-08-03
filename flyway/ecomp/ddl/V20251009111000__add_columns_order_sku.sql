ALTER TABLE ${ecom_dim_schema}.order_sku
  ADD COLUMNS (
    warranty_sku   STRING,
    warranty_units DOUBLE,
    warranty_price DOUBLE,
    warranty_tax   DOUBLE
  );
