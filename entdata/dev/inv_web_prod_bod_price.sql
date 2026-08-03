CREATE VIEW dev_entdata.web.inv_web_prod_bod_price (
    bod_inv_date comment 'The beginning of day date for the inventory product record, zoned for America/New_York.',
    webstore_key comment 'The entdata.web.webstore key this record maps to.',
    product_id,
    web_eligibility_ind,
    web_price,
    clearance_type_key,
    web_sku_clr_color_code,
    web_sku_clr_color_desc,
    web_perm_price,
    base_price,
    map_price,
    product_number,
    style_id
)
WITH SCHEMA BINDING
AS SELECT
  to_date(snp.date_key, 'yyyyMMdd') AS bod_inv_date,
  try_cast(snp.chain_key as bigint) AS webstore_key,
  try_cast(dks_sku_key as bigint) AS product_id,
  snp.web_eligibility_ind,
  snp.web_price,
  try_cast(
      CASE
        WHEN snp.date_key > 20200420 THEN COALESCE(snp.clearance_type_key, -1)
        WHEN snp.date_key > 20191204 THEN
            CASE
              WHEN NOT snp.ddw_clr_color_code IS NULL THEN 3
              WHEN snp.web_price < snp.web_perm_price THEN 2
              WHEN COALESCE(snp.web_perm_price, snp.web_price) < snp.base_price - 0.01 THEN 6
              WHEN snp.web_price IS NULL THEN -1
              ELSE 1
            END
        ELSE
            CASE
              WHEN NOT cc.clr_color_code IS NULL THEN 3
              WHEN ENDSWITH(CAST(round(snp.web_price, 2) AS STRING), '.93') IS TRUE THEN 3
              WHEN ENDSWITH(CAST(round(snp.web_price, 2) AS STRING), '.97') IS TRUE THEN 3
              WHEN ENDSWITH(CAST(round(snp.web_price, 2) AS STRING), '.98') IS TRUE THEN 2
              WHEN snp.web_price IS NULL THEN -1
              WHEN snp.web_price < snp.base_price - 0.01 THEN 6
              ELSE 1
            END
      END as bigint
  ) AS clearance_type_key,
  snp.ddw_clr_color_code AS web_sku_clr_color_code,
  cc.clr_color_desc AS web_sku_clr_color_desc,
  snp.web_perm_price,
  snp.base_price,
  NULL AS map_price,
  try_cast(snp.dks_sku as bigint) AS product_number,
  try_cast(snp.style_key as bigint) AS style_id
FROM prod_ecmde_db.ecom_dim.snp_inventory AS snp
LEFT JOIN entdata.prd.clr_color_dim AS cc
  ON snp.ddw_clr_color_code = cc.clr_color_code
WHERE
  snp.record_status = 'A'
UNION ALL
SELECT
  date(from_utc_timestamp(CURRENT_TIMESTAMP(), 'America/New_York')) AS bod_inv_date,
  chain_key AS webstore_key,
  dks_sku_key AS product_id,
  CASE WHEN discontinued_flg = 'A' THEN 1 ELSE 0 END AS web_eligibility_ind,
  web_price,
  clearance_type_key,
  wh.clr_color_code AS web_sku_clr_color_code,
  cc.clr_color_desc AS web_sku_clr_color_desc,
  web_perm_price,
  list_price AS base_price,
  map_price,
  dks_sku AS product_number,
  style_key AS style_id
FROM prod_ecmde_db.ecom_dim.web_sku_header AS wh
LEFT JOIN entdata.prd.clr_color_dim AS cc
  ON wh.clr_color_code = cc.clr_color_code
WHERE
  wh.record_status = 'A' AND wh.chain_key IN (2, 4, 6, 7, 8)