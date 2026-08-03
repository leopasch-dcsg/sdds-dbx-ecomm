from datetime import date
from pyspark.sql import DataFrame
from ecmde_ecomm.common.dbx.elastic.trunc_and_load import LoadOperation


class LTRATCBODPriceTableLoad(LoadOperation):
    def batch_dataframe(self) -> DataFrame:
        if not self.start_date_est:
            raise RuntimeError("start_date_est is not set")
        if not self.end_date_est:
            raise RuntimeError("end_date_est is not set")
        if not self.config.lookback_days:
            raise RuntimeError("lookback_days is not set")

        return self.spark.sql(
            f"""
                WITH base_asst AS (
                  SELECT
                    inv.bod_inv_date,
                    ew.ecode,
                    inv.product_number AS sku,
                    CAST(inv.web_atp_qty AS LONG) AS web_atp_qty,
                    CAST(inv.web_price AS DOUBLE) AS web_price,
                    CAST(inv.list_price AS DOUBLE) AS list_price,
                    CAST(IFNULL(p.map_price, 0) AS DOUBLE) AS map_price
                  FROM entdata.ecm.inv_web_prod_bod_assortment inv
                  JOIN entdata.ecm.eproduct_webstore ew
                  USING (webstore_key, eproduct_webstore_key)
                  LEFT JOIN entdata.ecm.inv_web_prod_bod_price p
                  USING (bod_inv_date, webstore_key, product_id)
                  WHERE bod_inv_date BETWEEN DATE("{self.start_date_est}") AND DATE("{self.end_date_est}")
                  AND inv.webstore_key = {self.config.webstore_key}
                  and inv.web_price is not null
                  AND inv.product_status_group = "A"
                  AND inv.web_eligibility_ind = 1
                ),
                asst_aggs AS (
                  SELECT
                    bod_inv_date,
                    ecode,
                    COUNT(DISTINCT sku) AS active_web_eligible_sku_count,
                    COUNT(CASE WHEN web_atp_qty > 0 THEN 1 END) AS active_web_eligible_is_sku_count,
                    COUNT(CASE WHEN web_price < list_price THEN 1 END) AS discount_sku_count,
                    COUNT(CASE WHEN web_price < list_price AND web_atp_qty > 0 THEN 1 END) AS discount_is_sku_count,
                    COUNT(CASE WHEN web_price < map_price THEN 1 END) AS below_map_price_sku_count,
                    MAX((list_price - web_price) / list_price) AS max_list_price_discount_frac,
                    MIN((list_price - web_price) / list_price) AS min_list_price_discount_frac,
                    AVG((list_price - web_price) / list_price) AS avg_list_price_discount_frac,
                    MIN(web_price) AS min_web_price,
                    MAX(web_price) AS max_web_price,
                    AVG(web_price) AS avg_web_price,
                    MIN(list_price) AS min_list_price,
                    MAX(list_price) AS max_list_price,
                    AVG(list_price) AS avg_list_price,
                    MIN(map_price) AS min_map_price,
                    MAX(map_price) AS max_map_price,
                    AVG(map_price) AS avg_map_price
                  FROM base_asst
                  GROUP BY bod_inv_date, ecode
                )
                SELECT bod_inv_date, ecode, avg_web_price FROM asst_aggs;
                """
        )