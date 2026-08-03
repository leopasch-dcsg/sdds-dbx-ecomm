from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation, MergeConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.fulfillment.model.chain_code import ChainCode
from ecmde_ecomm.fulfillment.model.fulfillmentstatus_code import FulfillmentStatus


DEFAULT_ZONE = "America/New_York"
CANCELLED = 999
DECLINED = 790
Order_Status_InProcess = 2
Order_Status_Completed = 3


class SilverOrderHeader(MergeOperation):
    def __init__(
        self,
        oso_catalog: str,
        fit_catalog: str,
        ecomp_catalog: str,
        config: MergeConfig,
        watermark: Watermark,
        spark: SparkSession,
        batch_date_utc: datetime = datetime.now(timezone.utc),
    ):
        super().__init__(config, watermark, spark, batch_date_utc)
        self.oso_catalog = oso_catalog
        self.fit_catalog = fit_catalog
        self.ecomp_catalog = ecomp_catalog

    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        return f"""
        

        WITH odh AS (
          SELECT
            ni.order_id,
            greatest(ni.item_status, nfh.final_status) AS current_item_status,
            ni.nmn_updated_timestamp,
            nfh.completed_timestamp,
            ni.silver_layer_update_timestamp,
            'OSO-Silver' AS modified_by
          FROM {self.config.source_table_qualified()} ni
          LEFT JOIN {self.oso_catalog}.oso.newman_fr_history nfh
            ON ni.order_id           = nfh.order_id
           AND ni.fr_sequence_number = nfh.fr_sequence_number
           AND ni.line_number        = nfh.line_number
           AND ni.line_seq_number    = nfh.line_seq_number
           AND nfh.final_status <> {DECLINED}
          WHERE ni.nmn_updated_timestamp >= '{last_watermark_utc}'
            AND lower(ni.managed_by) = 'newman'
            AND lower(ni.routing_partner) in ('fit','vft')
            AND ni.item_status <> {CANCELLED}
        )

        SELECT
          io.order_id AS web_ord_num,
          CASE any_value(io.order_input_source)
            WHEN '{ChainCode.G3.code}'              THEN {ChainCode.G3.key}
            WHEN '{ChainCode.GolfGalaxy.code}'      THEN {ChainCode.GolfGalaxy.key}
            WHEN '{ChainCode.DicksSportingGoods.code}' THEN {ChainCode.DicksSportingGoods.key}
            WHEN '{ChainCode.Moosejaw.code}'        THEN {ChainCode.Moosejaw.key}
            WHEN '{ChainCode.PublicLands.code}'     THEN {ChainCode.PublicLands.key}
            ELSE {ChainCode.UNK.key}
          END AS chain_key,
          CASE
            WHEN MIN(odh.current_item_status) < {FulfillmentStatus.FULFILLED.status_id} THEN {Order_Status_InProcess}
            WHEN MIN(odh.current_item_status) = {FulfillmentStatus.FULFILLED.status_id} THEN {Order_Status_Completed}
            ELSE 0
            END AS order_status_key,
          MAX(COALESCE(odh.nmn_updated_timestamp, odh.completed_timestamp))
            AS order_status_dttm,
          MAX(odh.silver_layer_update_timestamp
          ) AS date_last_modified,
          MAX(odh.modified_by) AS modified_by
        FROM {self.oso_catalog}.oso.newman_order io
        INNER JOIN odh
          ON io.order_id = odh.order_id
        GROUP BY 1
        HAVING order_status_key IS NOT NULL
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
        dt = DeltaTable.forName(self.spark, self.config.destination_table_qualified())
        (
            dt.alias("target")
            .merge(updates.alias("source"), "target.web_ord_num = source.web_ord_num")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
