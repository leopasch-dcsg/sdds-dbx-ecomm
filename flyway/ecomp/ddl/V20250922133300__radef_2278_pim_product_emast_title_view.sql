create or replace view ${ecom_dim_schema}.vw_pim_product_emast_title_only as
    SELECT PIM_PRODUCT_EMAST_KEY,
           regexp_replace(REPLACE(PIM_PRODUCT_EMAST_CODE, ',', '_'), '[[:cntrl:]]', '-') AS pim_product_emast_code,
           REPLACE(em_product_title, '|', '\\\\') AS em_product_title
    from ${ecom_dim_schema}.pim_product_emast;