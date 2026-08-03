CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_dim_search_saint
(
  SEARCH_KEY,
  SEARCH_CATEGORY,
  SEARCH_TYPE,
  SEARCH_ROOT,
  SEARCH_BRAND,
  OPTIMIZED_FLAG,
  DATE_ADDED
) AS
SELECT
  search_key,
  search_category,
  search_type,
  search_root,
  search_brand,
  NVL(optimized_flag, 'N') AS optimized_flag,
  date_added AS date_added
FROM
  prod_ecmde_db.ecom.stg_duh_search_saint
WHERE
  imex_log_reference_id = (
    SELECT
      MAX(ss.imex_log_reference_id)
    FROM
      prod_ecmde_db.ecom.stg_duh_search_saint AS ss
        LEFT JOIN prod_ecmde_db.ecom.imex_log_header AS ilh
          ON ilh.imex_log_header_id = ss.imex_log_reference_id
    WHERE
      ilh.log_status IN ('C', 'E')
  )
  AND NVL(search_key, '~') <> '~empty~'
  AND NVL(search_category, '~') <> '~empty~'
  AND NVL(search_type, '~') <> '~empty~'
  AND NVL(search_root, '~') <> '~empty~'
  AND NOT search_root IS NULL
  /* 20191220 JMF Added to exclude search terms that are not "tagged" but are "optimized" from the Internal Search Report */
  AND NVL(search_brand, '~') <> '~empty~'
  AND NVL(optimized_flag, '~') <> '~empty~'
