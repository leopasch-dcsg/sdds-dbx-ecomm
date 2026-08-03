CREATE or replace VIEW entdata.web.inv_web_prod_bod_assortment (
    bod_inv_date comment 'The beginning of day date for the inventory product record, zoned for America/New_York.',
    webstore_key comment 'The entdata.web.webstore key this record maps to.',
    web_product_key,
    eproduct_webstore_key,
    web_sku_key,
    product_number,
    product_id,
    style_id,
    product_status_group,
    product_status,
    product_status_detail,
    web_atp_qty,
    vdc_atp_qty,
    sfs_atp_qty,
    bopis_atp_qty,
    webstore_bopis_atp_qty,
    isa_atp_qty,
    webstore_isa_oh_qty,
    dc_atp_qty,
    dks_dc_atp_qty,
    backstock_atp_qty,
    bopl_backstock_atp_qty,
    presale_atp_qty,
    web_price,
    list_price,
    clearance_type_key,
    web_eligibility_ind,
    web_sku_status,
    web_sku_clr_color_code,
    web_sku_clr_color_desc,
    web_perm_price,
    network_safety_stock,
    web_sku_promotion_exclusion_grp_id
)
AS SELECT
       to_date(snp.date_key, 'yyyyMMdd') AS bod_inv_date,
       TRY_CAST(snp.chain_key AS BIGINT) AS webstore_key,
       TRY_CAST(snp.product_key AS BIGINT) AS web_product_key,
       TRY_CAST(snp.product_key AS BIGINT) AS eproduct_webstore_key,
       TRY_CAST(snp.web_sku_key AS BIGINT) AS web_sku_key,

       TRY_CAST(pd.product_number AS BIGINT) AS product_number,
       TRY_CAST(COALESCE(snp.dks_sku_key, -1) AS BIGINT) AS product_id,
       TRY_CAST(COALESCE(snp.style_key, -1) AS BIGINT) AS style_id,

       snp.product_status_group,

       CASE snp.product_status_group
           WHEN 'A' THEN 'Active'
           WHEN 'I' THEN 'Inactive'
           WHEN 'H' THEN 'Hidden'
           END AS product_status,

       snp.product_status AS product_status_detail,

       TRY_CAST(COALESCE(snp.web_atp_qty, 0) AS BIGINT) AS web_atp_qty,
       TRY_CAST(COALESCE(snp.vdc_atp_qty, 0) AS BIGINT) AS vdc_atp_qty,
       TRY_CAST(COALESCE(snp.sfs_atp_qty, 0) AS BIGINT) AS sfs_atp_qty,
       TRY_CAST(COALESCE(snp.bopis_atp_qty, 0) AS BIGINT) AS bopis_atp_qty,
       TRY_CAST(COALESCE(snp.webstore_bopis_atp_qty, 0) AS BIGINT) AS webstore_bopis_atp_qty,
       TRY_CAST(COALESCE(snp.isa_atp_qty, 0) AS BIGINT) AS isa_atp_qty,
       TRY_CAST(COALESCE(snp.webstore_isa_oh_qty, 0) AS BIGINT) AS webstore_isa_oh_qty,
       TRY_CAST(COALESCE(snp.dc_atp_qty, 0) AS BIGINT) AS dc_atp_qty,
       TRY_CAST(COALESCE(snp.dks_dc_atp_qty, 0) AS BIGINT) AS dks_dc_atp_qty,
       TRY_CAST(COALESCE(snp.backstock_atp_qty, 0) AS BIGINT) AS backstock_atp_qty,
       TRY_CAST(COALESCE(snp.bopl_atp_qty, 0) AS BIGINT) AS bopl_backstock_atp_qty,
       TRY_CAST(COALESCE(snp.presale_atp_qty, 0) AS BIGINT) AS presale_atp_qty,

       TRY_CAST(snp.web_price AS DECIMAL(18,2)) AS web_price,
       TRY_CAST(snp.list_price AS DECIMAL(18,2)) AS list_price,

       TRY_CAST(
           CASE
               WHEN snp.date_key > 20200420 THEN COALESCE(snp.clearance_type_key, -1)
               WHEN snp.date_key > 20191208 THEN
                   CASE
                       WHEN NOT cc.clr_color_code IS NULL THEN 3
                       WHEN snp.web_price < snp.web_perm_price THEN 2
                       WHEN COALESCE(snp.web_perm_price, snp.web_price) < snp.list_price - 0.01 THEN 6
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
                       WHEN snp.web_price < snp.list_price - 0.01 THEN 6
                       ELSE 1
                       END
               END AS BIGINT
       ) AS clearance_type_key,
       snp.web_eligibility_ind AS web_eligibility_ind,
       CASE
           WHEN snp.dks_sku_key = -999 THEN 'N/A'
           WHEN snp.web_eligibility_ind = 1 THEN 'A'
           ELSE 'I'
           END AS web_sku_status,
       snp.ddw_clr_color_code AS web_sku_clr_color_code,
       cc.clr_color_desc AS web_sku_clr_color_desc,
       TRY_CAST(snp.web_perm_price AS DECIMAL(18,2)) AS web_perm_price,

       TRY_CAST(ns.nss AS BIGINT) AS network_safety_stock,

       COALESCE(TRY_CAST(snp.promo_excl_grp AS BIGINT), -1) AS web_sku_promotion_exclusion_grp_id
   FROM prod_ecmde_db.ecom_dim.snp_web_product_assortment AS snp
            LEFT JOIN entdata.prd.clr_color_dim AS cc
                      ON snp.ddw_clr_color_code = cc.clr_color_code
            LEFT JOIN entdata.prd.product_dim AS pd
                      ON snp.dks_sku_key = pd.product_id
            LEFT JOIN prod_gc_ddw_db.eom.ds_pilot_network_safety_stock AS ns
                      ON DATE_ADD(to_date(snp.date_key, 'yyyyMMdd'), -1) = ns.inv_date
                          AND pd.product_number = ns.product_number
   WHERE
       snp.record_status = 'A';