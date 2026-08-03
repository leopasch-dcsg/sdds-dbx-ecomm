from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr, lit, concat_ws
from ecmde_ecomm.common.dbx.etl.merge import MergeOperation, MergeConfig
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.fulfillment.model.channel_code import ChannelCode
from ecmde_ecomm.fulfillment.model.chain_code import ChainCode
from ecmde_ecomm.fulfillment.model.fulfillmentstatus_code import FulfillmentStatus
from ecmde_ecomm.fulfillment.model.fulfillmentmode_code import FulfillmentModeCode

DEFAULT_ZONE = "America/New_York"
VDC = 6


class SilverFulfillmentOrder(MergeOperation):
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
        WITH fr_src AS (
          SELECT order_id, fr_sequence_number, line_number, line_seq_number
          FROM {self.oso_catalog}.oso.newman_item
          WHERE lower(managed_by) = 'newman' AND lower(routing_partner) in ('fit','vft','otv','wm')
          AND silver_layer_update_timestamp >= '{last_watermark_utc}'
          and fr_sequence_number is not null
          UNION
          SELECT order_id, fr_sequence_number, line_number, line_seq_number
          FROM {self.oso_catalog}.oso.newman_fr_history
          WHERE lower(managed_by) = 'newman' AND lower(system) in ('fit','vft','wm','otv','ao-consumer','oso')
          AND silver_layer_update_timestamp >= '{last_watermark_utc}'
          and fr_sequence_number is not null
        ),
        sub AS (
          SELECT
            coalesce(concat(os.order_id, lpad(cast(os.fr_sequence_number as string), 4, '0')), concat(ni.order_id, lpad(cast(ni.fr_sequence_number as string), 4, '0')))
            AS order_fulfill_number,
            CASE nor.order_input_source
              WHEN '{ChainCode.G3.code}'              THEN {ChainCode.G3.key}
              WHEN '{ChainCode.GolfGalaxy.code}'      THEN {ChainCode.GolfGalaxy.key}
              WHEN '{ChainCode.DicksSportingGoods.code}' THEN {ChainCode.DicksSportingGoods.key}
              WHEN '{ChainCode.Moosejaw.code}'        THEN {ChainCode.Moosejaw.key}
              WHEN '{ChainCode.PublicLands.code}'     THEN {ChainCode.PublicLands.key}
              ELSE {ChainCode.UNK.key}
            END AS chain_key,
            CASE MAX(os.final_status)
              WHEN {FulfillmentStatus.FULFILLED.status_id} THEN '{FulfillmentStatus.FULFILLED.code}'
              WHEN {FulfillmentStatus.CANCELLED.status_id} THEN '{FulfillmentStatus.CANCELLED.code}'
              WHEN {FulfillmentStatus.DECLINED.status_id}  THEN '{FulfillmentStatus.DECLINED.code}'
              WHEN {FulfillmentStatus.RECEIVED.status_id}  THEN '{FulfillmentStatus.RECEIVED.code}'
              WHEN {FulfillmentStatus.STSACCEPTED.status_id} THEN '{FulfillmentStatus.STSACCEPTED.code}'
              WHEN {FulfillmentStatus.STSREJECTED.status_id}  THEN '{FulfillmentStatus.STSREJECTED.code}'
              WHEN {FulfillmentStatus.STSEXPIRED.status_id}  THEN '{FulfillmentStatus.STSEXPIRED.code}'             
              ELSE '{FulfillmentStatus.OTHER.code}'
            END AS fulfillment_status_cd,
            MAX(coalesce( os.completed_timestamp, ni.nmn_updated_timestamp)
            ) AS fulfillment_status_dttm,
            MAX(
              CAST(
                date_format(
                  convert_timezone(
                    'UTC',
                    '{DEFAULT_ZONE}',
                    coalesce(os.completed_timestamp, ni.nmn_updated_timestamp)
                  ),
                  'yyyyMMdd'
                ) AS integer
              )
            ) AS fulfillment_date_key,
           MAX(
              CASE
                WHEN UPPER(coalesce(os.source_facility_type,ni.source_facility_type)) = 'VENDOR' 
                AND UPPER(coalesce(os.source_facility_type,ni.pickup_facility_type)) = 'VENDOR'
                THEN {ChannelCode.VDC.key}
                ELSE
                  CASE COALESCE(os.executed_fulfillment_mode,ni.ordered_fulfillment_mode, '-1')
                    WHEN '{ChannelCode.BOPIS.code}' THEN {ChannelCode.BOPIS.key}
                    WHEN '{ChannelCode.BOPL.code}'  THEN {ChannelCode.BOPL.key}
                    WHEN '-1'                       THEN {ChannelCode.UNKNOWN.key}
                    ELSE {ChannelCode.SFS.key}
                  END
              END
            ) AS channel_type_key,
            MAX(st.store_key)   AS store_key,
            MAX(vd.vendor_key)  AS vendor_key,
            MAX(coalesce(os.source_facility_number, ni.source_facility_number)) AS fulfillment_location_cd,
            MAX(ca.carrier_key) AS carrier_key,
            MAX(
              CASE
                WHEN COALESCE(fm1.fulfillment_mode_code, fm2.fulfillment_mode_code) = 'SAMEDAY'
                THEN {FulfillmentModeCode.DDSD.key}
                ELSE COALESCE(fm1.fulfillment_mode_key, fm2.fulfillment_mode_key)
              END
            ) AS fulfillment_mode_key,
            MAX(
              CAST(
                  date_format(
                    ni.promise_end_date,
                    'yyyyMMdd'
                  ) AS integer
                )
            ) AS promise_date_key,
            MAX(ge.state_code) AS ship_state,
            MAX(substr(ni.destination_zip,1,5)) AS ship_zip,
            MAX(coalesce(os.order_id, ni.order_id)) AS web_ord_num,
            MAX(coalesce(os.executed_fulfillment_mode, ni.executed_fulfillment_mode)) AS ship_method,
            MIN(coalesce(os.sourced_timestamp, ni.sourced_timestamp)
            ) AS do_create_dttm,
            MIN(ni.package_ship_timestamp) AS min_fulfillment_dttm,
            MAX(ni.po_number) AS po_number,
            MAX(coalesce(os.silver_layer_update_timestamp, ni.silver_layer_update_timestamp)
            ) AS date_last_modified,
            MAX('OSO-Silver') AS modified_by
          FROM fr_src
          LEFT JOIN {self.oso_catalog}.oso.newman_fr_history os
            ON os.order_id = fr_src.order_id
            AND os.fr_sequence_number = fr_src.fr_sequence_number          
          LEFT JOIN {self.oso_catalog}.oso.newman_item ni
            ON ni.order_id = fr_src.order_id
           AND ni.line_number = fr_src.line_number
           AND ni.line_seq_number = fr_src.line_seq_number
          LEFT JOIN {self.oso_catalog}.oso.newman_order nor
            ON nor.order_id = fr_src.order_id
          LEFT JOIN {self.fit_catalog}.fit.fit_package_events_silver fit
            ON fit.tracking_id = ni.package_tracking_id AND fit.event_type = 'LABEL_CREATED'
          LEFT JOIN {self.ecomp_catalog}.ecom_dim.carrier ca
            ON UPPER(ca.carrier_code) = CASE 
            WHEN UPPER(COALESCE(os.package_carrier, ni.package_carrier)) IN ("FDXEXPR", "FDXGRND", "FDXHOME", "FDE", "FDEG", "FEDG") THEN 'FEDEX' 
            WHEN UPPER(COALESCE(os.package_carrier, ni.package_carrier)) = 'SEFL' THEN 'GENERIC'
            ELSE UPPER(COALESCE(os.package_carrier, ni.package_carrier))
          END 
          LEFT JOIN {self.ecomp_catalog}.ecom_dim.store st
            ON st.store_number = coalesce(os.source_facility_number, ni.source_facility_number)
          LEFT JOIN {self.ecomp_catalog}.ecom_dim.geo ge
            ON ge.zip_code = substr(ni.destination_zip,1,5)
          LEFT JOIN {self.ecomp_catalog}.ecom_dim.vendor vd
            ON vd.vendor_number = ni.source_facility_number
          LEFT JOIN {self.ecomp_catalog}.ecom_dim.fulfillment_mode fm1
            ON UPPER(fm1.fulfillment_mode_code) = UPPER(CONCAT_WS('-', 
                          TRIM(ca.carrier_code), 
                          TRIM(COALESCE(os.executed_fulfillment_mode, ni.executed_fulfillment_mode))))
          LEFT JOIN {self.ecomp_catalog}.ecom_dim.fulfillment_mode fm2
            ON UPPER(fm2.fulfillment_mode_code) = UPPER(TRIM(COALESCE(os.executed_fulfillment_mode, ni.executed_fulfillment_mode)))
            AND fm1.fulfillment_mode_key IS NULL
          GROUP BY order_fulfill_number, nor.order_input_source
        )
        SELECT
          order_fulfill_number,
          chain_key,
          fulfillment_status_cd,
          fulfillment_status_dttm,
          COALESCE(CASE
            WHEN fulfillment_status_cd = '{FulfillmentStatus.FULFILLED.code}'
            THEN fulfillment_date_key
            ELSE NULL
          END, -1) AS fulfillment_date_key,  
          COALESCE(channel_type_key, -1) AS channel_type_key,
          CASE WHEN channel_type_key IN ({ChannelCode.BOPIS.key}, {ChannelCode.BOPL.key}) THEN 'P' ELSE 'S' END AS shipment_type_cd,
          CASE WHEN channel_type_key <> {VDC} THEN coalesce(store_key, -1) ELSE NULL END AS store_key,
          CASE WHEN channel_type_key = {VDC} THEN coalesce(vendor_key, -1) ELSE NULL END AS vendor_key,
          fulfillment_location_cd,
          COALESCE(carrier_key, -1) AS carrier_key,
          COALESCE(fulfillment_mode_key, -1) AS fulfillment_mode_key,
          promise_date_key,
          ship_state,
          ship_zip,
          web_ord_num,
          ship_method,
          do_create_dttm,
          min_fulfillment_dttm,
          po_number,
          date_last_modified,
          modified_by
        FROM sub
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
            .merge(
                updates.alias("source"),
                "target.order_fulfill_number = source.order_fulfill_number",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
