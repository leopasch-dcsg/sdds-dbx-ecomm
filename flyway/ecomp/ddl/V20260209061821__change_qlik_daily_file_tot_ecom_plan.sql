CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_daily_file_tot_ecom_plan
(
    DATE_KEY,
    WEBSTORE_KEY,
    OMNI,
    DMD_BUDGET_AMT,
    DMD_GM_AMT,
    FF_BUDGET_AMT,
    FF_GM_AMT
)
AS
SELECT
  `DATE_KEY`,
  `WEBSTORE_KEY`,
  `OMNI`,
  `DMD_BUDGET_AMT`,
  `DMD_GM_AMT`,
  `FF_BUDGET_AMT`,
  `FF_GM_AMT`
FROM
  (
    SELECT
      date_id AS date_key,
      1 AS webstore_key,
      omni,
      0 AS dmd_budget_amt,
      0 AS dmd_gm_amt,
      0 AS ff_budget_amt,
      0 AS ff_gm_amt
    FROM
      prod_ecmde_db.ecom.ecom_merch_plan
    UNION ALL
    SELECT
      date_id,
      chain_key,
      omni,
      0 AS dmd_budget_amt,
      0 AS dmd_gm_amt,
      0 AS ff_budget_amt,
      0 AS ff_gm_amt
    FROM
      prod_ecmde_db.ecom.ecom_day_plan_wcs
    WHERE
      chain_key <> -9
    UNION ALL
    SELECT
      date_id,
      1 AS chain_key,
      0 AS omni,
      dmd_budget_amt,
      dmd_gm_amt,
      ff_budget_amt,
      ff_gm_amt
    FROM
      prod_ecmde_db.ecom.ecom_day_plan_wcs
    WHERE
      chain_key = -9
    ORDER BY
      1 DESC NULLS FIRST,
      2 NULLS LAST
  )
WHERE
  date_key BETWEEN 20250202 AND 20270130