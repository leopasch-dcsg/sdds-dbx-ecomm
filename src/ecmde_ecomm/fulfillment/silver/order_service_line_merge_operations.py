from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation, MergeConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark

UNKNOWN_DKS_SKU = 0
SERVICE_LINE_TYPES = "'warranty', 'subscription'"


class WarrantyLineLoader(MergeOperation):

    def __init__(
        self,
        ecmde_silver_catalog: str,
        ecmde_silver_schema: str,
        destination_catalog: str,
        destination_schema: str,
        config: MergeConfig,
        watermark: Watermark,
        spark: SparkSession,
        batch_date_utc: datetime = datetime.now(timezone.utc),
    ):
        super().__init__(config, watermark, spark, batch_date_utc)
        self.ecmde_silver_catalog = ecmde_silver_catalog
        self.ecmde_silver_schema = ecmde_silver_schema
        self.destination_catalog = destination_catalog
        self.destination_schema = destination_schema

        self.dest_table_name = "order_service_line"
        self.dest_fqn = f"{self.destination_catalog}.{self.destination_schema}.{self.dest_table_name}"

    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:

        placed = f"{self.ecmde_silver_catalog}.{self.ecmde_silver_schema}.co_stage_order_message_placed"
        cancel = f"{self.ecmde_silver_catalog}.{self.ecmde_silver_schema}.co_stage_order_message_cancel"
        fulfill = f"{self.ecmde_silver_catalog}.{self.ecmde_silver_schema}.co_stage_order_message_fulfill"

        return f"""

        WITH base_placed AS (
          SELECT
              TRY_CAST(order_number AS BIGINT)         AS web_ord_num,
              TRY_CAST(order_line_num AS INT)          AS order_line_num,
              TRY_CAST(order_line_unit_seq AS INT)     AS order_line_unit_seq,
              sku,
              line_item_type,
              order_source,
              aosassociateid,
              purchase_price,
              est_unit_tax,
              silver_created_on_utc,
              order_placed_dttm,
              TRY_CAST(
                 element_at(
                   transform(
                     filter(
                       associations,
                       x -> lower(x.associationtype) = 'product'
                     ),
                     y -> element_at(y.associationdetails, 1).linenumber
                   ),
                   1
                 ) AS INT
              ) AS assoc_line_num,
              TRY_CAST(
                 element_at(
                   transform(
                     filter(
                       associations,
                       x -> lower(x.associationtype) = 'product'
                     ),
                     y -> element_at(y.associationdetails, 1).sequence
                   ),
                   1
                 ) AS INT
              ) AS assoc_seq
          FROM {placed}
          WHERE silver_created_on_utc >= '{last_watermark_utc}'
        ),

        service_units AS (
            SELECT
                bp.web_ord_num,
                bp.order_line_num,
                bp.order_line_unit_seq,
                bp.sku                         AS service_sku,
                bp.line_item_type,
                bp.order_source,
                bp.aosassociateid              AS aos_associate_id,
                TRY_CAST(bp.purchase_price AS DECIMAL(18,2)) AS purchase_price,
                TRY_CAST(bp.est_unit_tax   AS DECIMAL(18,2)) AS est_unit_tax,
                bp.assoc_line_num,
                bp.assoc_seq,
                bp.silver_created_on_utc,
                bp.order_placed_dttm,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        bp.web_ord_num,
                        bp.order_line_num,
                        bp.order_line_unit_seq,
                        bp.sku
                    ORDER BY
                        bp.silver_created_on_utc DESC
                ) AS rn
            FROM base_placed bp
            WHERE LOWER(COALESCE(bp.line_item_type,'')) IN ({SERVICE_LINE_TYPES})
            AND bp.web_ord_num IS NOT NULL
            QUALIFY rn = 1
        ),

        service_rollup AS (
          SELECT
              su.web_ord_num AS web_ord_num,

              CASE
                WHEN UPPER(su.order_source) = 'DICKSSPORTINGGOODS' THEN 6
                WHEN UPPER(su.order_source) = 'GOLFGALAXY'         THEN 2
                WHEN UPPER(su.order_source) = 'PUBLICLANDS'        THEN 7
                WHEN UPPER(su.order_source) = 'MOOSEJAW'           THEN 9
                WHEN UPPER(su.order_source) = 'G3'                 THEN 8
                ELSE -1
              END AS webstore_key,

              su.service_sku AS service_sku,
              LOWER(COALESCE(su.line_item_type, '')) AS line_item_type,
              MAX(su.aos_associate_id) AS aos_associate_id,

              COUNT(*) AS units,
              CAST(SUM(su.purchase_price) AS DECIMAL(18,2)) AS service_price,
              CAST(SUM(su.est_unit_tax)   AS DECIMAL(18,2)) AS service_tax,
              MIN(su.order_placed_dttm) AS transaction_date

          FROM service_units su
          WHERE LOWER(COALESCE(su.line_item_type, '')) IN ({SERVICE_LINE_TYPES})
          GROUP BY
              su.web_ord_num,
              CASE
                WHEN UPPER(su.order_source) = 'DICKSSPORTINGGOODS' THEN 6
                WHEN UPPER(su.order_source) = 'GOLFGALAXY'         THEN 2
                WHEN UPPER(su.order_source) = 'PUBLICLANDS'        THEN 7
                WHEN UPPER(su.order_source) = 'MOOSEJAW'           THEN 9
                WHEN UPPER(su.order_source) = 'G3'                 THEN 8
                ELSE -1
              END,
              su.service_sku,
              LOWER(COALESCE(su.line_item_type, ''))
        ),


        dks_sku_lookup AS (
          SELECT DISTINCT 
              su.web_ord_num AS web_ord_num,
              su.service_sku AS service_sku,
              LOWER(COALESCE(su.line_item_type, '')) AS line_item_type,

              COALESCE(
                TRY_CAST(bpp_by_assoc.sku AS BIGINT),
                TRY_CAST(bpp_by_line.sku  AS BIGINT),
                CAST({UNKNOWN_DKS_SKU} AS BIGINT)
              ) AS dks_sku

          FROM service_units su

          LEFT JOIN base_placed bpp_by_assoc
            ON  bpp_by_assoc.web_ord_num         = su.web_ord_num
            AND bpp_by_assoc.order_line_num      = su.assoc_line_num
            AND bpp_by_assoc.order_line_unit_seq = su.assoc_seq
            AND LOWER(COALESCE(bpp_by_assoc.line_item_type, '')) = 'product'

          LEFT JOIN base_placed bpp_by_line
            ON  bpp_by_line.web_ord_num    = su.web_ord_num
            AND bpp_by_line.order_line_num = su.order_line_num
            AND LOWER(COALESCE(bpp_by_line.line_item_type, '')) = 'product'

          WHERE LOWER(COALESCE(su.line_item_type, '')) IN ({SERVICE_LINE_TYPES})
        )
        ,

        placed_service AS (
          SELECT
              sr.web_ord_num,
              dl.dks_sku,
              sr.webstore_key,
              sr.service_sku,
              sr.line_item_type,
              sr.aos_associate_id,
              sr.units,
              sr.service_price,
              sr.service_tax,
              sr.transaction_date
          FROM service_rollup sr
          LEFT JOIN dks_sku_lookup dl
            ON  dl.web_ord_num   = sr.web_ord_num
            AND dl.service_sku   = sr.service_sku
            AND dl.line_item_type = sr.line_item_type
        ),

        base_cancel AS (
          SELECT
              TRY_CAST(order_number AS BIGINT) AS web_ord_num,
              TRY_CAST(order_line_num AS INT)  AS order_line_num,
              TRY_CAST(order_line_unit_seq AS INT) AS order_line_unit_seq,
              sku,
              line_item_type,
              order_source,
              purchase_price,
              silver_created_on_utc
          FROM {cancel}
          WHERE silver_created_on_utc >= '{last_watermark_utc}'
        ),

        cancel_service AS (
          SELECT
              bc.web_ord_num AS web_ord_num,
              CASE
                WHEN UPPER(bc.order_source) = 'DICKSSPORTINGGOODS' THEN 6
                WHEN UPPER(bc.order_source) = 'GOLFGALAXY'         THEN 2
                WHEN UPPER(bc.order_source) = 'PUBLICLANDS'        THEN 7
                WHEN UPPER(bc.order_source) = 'MOOSEJAW'           THEN 9
                WHEN UPPER(bc.order_source) = 'G3'                 THEN 8
                ELSE -1
              END AS webstore_key,
              bc.sku AS service_sku,
              LOWER(COALESCE(bc.line_item_type, '')) AS line_item_type,
              COUNT(*) AS cancel_units,
              CAST(SUM(TRY_CAST(bc.purchase_price AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS cancel_price
          FROM base_cancel bc
          WHERE LOWER(COALESCE(bc.line_item_type, '')) IN ({SERVICE_LINE_TYPES})
          AND bc.web_ord_num IS NOT NULL  
          GROUP BY
              bc.web_ord_num,
              CASE
                WHEN UPPER(bc.order_source) = 'DICKSSPORTINGGOODS' THEN 6
                WHEN UPPER(bc.order_source) = 'GOLFGALAXY'         THEN 2
                WHEN UPPER(bc.order_source) = 'PUBLICLANDS'        THEN 7
                WHEN UPPER(bc.order_source) = 'MOOSEJAW'           THEN 9
                WHEN UPPER(bc.order_source) = 'G3'                 THEN 8
                ELSE -1
              END,
              bc.sku,
              LOWER(COALESCE(bc.line_item_type, ''))
        ),

        base_fulfill AS (
          SELECT
              TRY_CAST(order_number AS BIGINT) AS web_ord_num,
              sku,
              line_item_type,
              order_source,
              return_price,
              silver_created_on_utc
          FROM {fulfill}
          WHERE silver_created_on_utc >= '{last_watermark_utc}'
            AND LOWER(COALESCE(line_item_type, '')) = 'warranty'
            AND LOWER(COALESCE(order_line_state, '')) = 'returned'
        ),

        return_service AS (
          SELECT
              bf.web_ord_num AS web_ord_num,
              CASE
                WHEN UPPER(bf.order_source) = 'DICKSSPORTINGGOODS' THEN 6
                WHEN UPPER(bf.order_source) = 'GOLFGALAXY'         THEN 2
                WHEN UPPER(bf.order_source) = 'PUBLICLANDS'        THEN 7
                WHEN UPPER(bf.order_source) = 'MOOSEJAW'           THEN 9
                WHEN UPPER(bf.order_source) = 'G3'                 THEN 8
                ELSE -1
              END AS webstore_key,
              bf.sku AS service_sku,
              LOWER(COALESCE(bf.line_item_type, '')) AS line_item_type,
              COUNT(*) AS return_units,
              CAST(SUM(TRY_CAST(bf.return_price AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS return_price
          FROM base_fulfill bf
          WHERE bf.web_ord_num IS NOT NULL
          GROUP BY
              bf.web_ord_num,
              CASE
                WHEN UPPER(bf.order_source) = 'DICKSSPORTINGGOODS' THEN 6
                WHEN UPPER(bf.order_source) = 'GOLFGALAXY'         THEN 2
                WHEN UPPER(bf.order_source) = 'PUBLICLANDS'        THEN 7
                WHEN UPPER(bf.order_source) = 'MOOSEJAW'           THEN 9
                WHEN UPPER(bf.order_source) = 'G3'                 THEN 8
                ELSE -1
              END,
              bf.sku,
              LOWER(COALESCE(bf.line_item_type, ''))
        ),

        stg AS (
          SELECT
              ps.web_ord_num,
              ps.dks_sku AS product_sku,                        
              ps.webstore_key,
              ps.service_sku AS service_sku,
              ps.aos_associate_id,
              CAST(ps.units AS INT)               AS units,
              CAST(ps.service_price AS DECIMAL(18,2)) AS service_price,
              CAST(ps.service_tax   AS DECIMAL(18,2)) AS service_tax,
              CAST(COALESCE(rs.return_units, 0) AS INT)           AS return_units,
              CAST(COALESCE(rs.return_price, 0) AS DECIMAL(18,2)) AS return_price,
              CAST(COALESCE(cs.cancel_units, 0) AS INT)           AS cancel_units,
              CAST(COALESCE(cs.cancel_price, 0) AS DECIMAL(18,2)) AS cancel_price,
              CASE
                WHEN ps.line_item_type = 'warranty' THEN 'Warranty'
                WHEN ps.line_item_type = 'subscription' THEN 'Subscription'
                ELSE INITCAP(ps.line_item_type)
              END AS service_type, 
              ps.transaction_date                                  AS transaction_date,
              CURRENT_TIMESTAMP()                 AS date_added,
              '{self.config.dbx_user_id}'          AS added_by,
              CURRENT_TIMESTAMP()                 AS date_last_modified,
              '{self.config.dbx_user_id}'          AS modified_by
          FROM placed_service ps
          LEFT JOIN cancel_service cs
            ON  cs.web_ord_num   = ps.web_ord_num
            AND cs.webstore_key  = ps.webstore_key
            AND cs.service_sku   = ps.service_sku
            AND cs.line_item_type = ps.line_item_type
          LEFT JOIN return_service rs
            ON  rs.web_ord_num   = ps.web_ord_num
            AND rs.webstore_key  = ps.webstore_key
            AND rs.service_sku   = ps.service_sku
            AND rs.line_item_type = ps.line_item_type
        )

        SELECT
            web_ord_num,
            product_sku,
            webstore_key,
            service_sku,
            aos_associate_id,
            units,
            service_price,
            service_tax,
            return_units,
            return_price,
            cancel_units,
            cancel_price,
            service_type,
            transaction_date,
            date_added,
            added_by,
            date_last_modified,
            modified_by
        FROM stg
        """

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:
        return (
            incremental_changeset.withColumn(
                "silver_created_on_utc", lit(self.batch_date_utc)
            )
            .withColumn("silver_created_by", lit(self.config.dbx_user_id))
            .withColumn("silver_updated_on_utc", lit(self.batch_date_utc))
            .withColumn("silver_updated_by", lit(self.config.dbx_user_id))
        )

    def merge_updates(self, updates: DataFrame) -> None:
        dt = DeltaTable.forName(self.spark, self.dest_fqn)
        (
            dt.alias("target")
            .merge(
                updates.alias("source"),
                """
                target.web_ord_num  = source.web_ord_num AND
                target.product_sku  = source.product_sku  AND
                target.webstore_key     = source.webstore_key   AND
                target.service_sku  = source.service_sku
                """,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
