import zoneinfo
from datetime import datetime, timezone, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from ecmde_ecomm.shipping.oracle_merge import OracleMergeOperation
from ecmde_ecomm.shipping.db_connection import DBConnection
from ecmde_ecomm.common.util import DateUtil


DEFAULT_ORACLE_TZ = "America/New_York"


def perform_fedex_tracking_transforms(
    df: DataFrame, dbx_user_id: str, batch_date_utc: datetime
) -> DataFrame:
    partition_keys = ["tracking_number", "tcn"]

    window_spec = Window.partitionBy(*partition_keys).orderBy(
        col("date_last_modified").desc()
    )

    transformed_df = (
        df.withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
        .withColumn("total_pieces_qty", col("total_pieces_qty").cast(IntegerType()))
        .withColumn("package_length", col("package_length").cast("decimal(13,2)"))
        .withColumn("package_width", col("package_width").cast("decimal(13,2)"))
        .withColumn("package_height", col("package_height").cast("decimal(13,2)"))
        .withColumn("lb_package_weight", col("lb_package_weight").cast("decimal(13,2)"))
        .withColumn("kg_package_weight", col("kg_package_weight").cast("decimal(13,2)"))
        .withColumn("tracking_number", col("tracking_number").cast(StringType()))
        .withColumn(
            "estimated_delivery_time", col("estimated_delivery_time").cast(StringType())
        )
        .withColumn("delivery_time", col("delivery_time").cast(StringType()))
        .withColumn("last_status_time", col("last_status_time").cast(StringType()))
        .withColumn("zone1_flag", col("zone1_flag").cast(StringType()))
        .withColumn("shipper_city", col("shipper_city").cast(StringType()))
        .withColumn("shipper_state", col("shipper_state").cast(StringType()))
        .withColumn(
            "shipper_country_code", col("shipper_country_code").cast(StringType())
        )
        .withColumn(
            "shipper_postal_code", col("shipper_postal_code").cast(StringType())
        )
        .withColumn("shipper_reference", col("shipper_reference").cast(StringType()))
        .withColumn(
            "recipient_city_name", col("recipient_city_name").cast(StringType())
        )
        .withColumn(
            "recipient_state_code", col("recipient_state_code").cast(StringType())
        )
        .withColumn(
            "recipient_postal_code", col("recipient_postal_code").cast(StringType())
        )
        .withColumn(
            "recipient_country_code", col("recipient_country_code").cast(StringType())
        )
        .withColumn("service_code", col("service_code").cast(StringType()))
        .withColumn("package_code", col("package_code").cast(StringType()))
        .withColumn(
            "transportation_payor", col("transportation_payor").cast(StringType())
        )
        .withColumn("track_type_cd", col("track_type_cd").cast(StringType()))
        .withColumn("weight_uom", col("weight_uom").cast(StringType()))
        .withColumn("dim_uom", col("dim_uom").cast(StringType()))
        .withColumn("purchase_order_nbr", col("purchase_order_nbr").cast(StringType()))
        .withColumn("invoice_nbr", col("invoice_nbr").cast(StringType()))
        .withColumn("dept_nbr", col("dept_nbr").cast(StringType()))
        .withColumn("shipment_id", col("shipment_id").cast(StringType()))
        .withColumn(
            "delivery_attempt_exception",
            col("delivery_attempt_exception").cast(StringType()),
        )
        .withColumn("last_status_code", col("last_status_code").cast(StringType()))
        .withColumn("event_city", col("event_city").cast(StringType()))
        .withColumn("event_state", col("event_state").cast(StringType()))
        .withColumn("event_country", col("event_country").cast(StringType()))
        .withColumn("add_status_info", col("add_status_info").cast(StringType()))
        .withColumn("company_code", col("company_code").cast(StringType()))
        .withColumn(
            "partner_carrier_nbr1", col("partner_carrier_nbr1").cast(StringType())
        )
        .withColumn("tcn", col("tcn").cast(StringType()))
        .withColumn("ship_date", col("ship_date").cast("date"))
        .withColumn(
            "estimated_delivery_date", col("estimated_delivery_date").cast("date")
        )
        .withColumn("delivery_date", col("delivery_date").cast("date"))
        .withColumn("last_status_date", col("last_status_date").cast("date"))
        .withColumn(
            "date_added",
            DateUtil.make_timestamp_with_zone(col("date_added"), DEFAULT_ORACLE_TZ),
        )
        .withColumn("run_date", col("run_date").cast("date"))
        .withColumn(
            "date_last_modified",
            DateUtil.make_timestamp_with_zone(
                col("date_last_modified"), DEFAULT_ORACLE_TZ
            ),
        )
        .withColumn("commit_date", col("commit_date").cast("date"))
        .withColumn("original_commit_date", col("original_commit_date").cast("date"))
        .withColumn("dtl_pickup_dttm", col("dtl_pickup_dttm").cast("timestamp_ntz"))
        .withColumn(
            "dtl_estimated_delivery_dttm",
            col("dtl_estimated_delivery_dttm").cast("timestamp_ntz"),
        )
        .withColumn(
            "dtl_delivery_attempt_dttm",
            col("dtl_delivery_attempt_dttm").cast("timestamp_ntz"),
        )
        .withColumn("dtl_delivery_dttm", col("dtl_delivery_dttm").cast("timestamp_ntz"))
        .withColumn("silver_created_by", lit(dbx_user_id))
        .withColumn("silver_created_on_utc", lit(batch_date_utc))
        .withColumn("silver_updated_by", lit(dbx_user_id))
        .withColumn("silver_updated_on_utc", lit(batch_date_utc))
    )

    return transformed_df


class SilverFedexTracking(OracleMergeOperation):

    def __init__(self, config, watermark, spark):
        super().__init__(config, watermark, spark)
        self.db_conn = DBConnection()

    def get_sql_statement_for_incremental_changes(
        self, last_watermark_utc: datetime
    ) -> str:
        import zoneinfo

        etc_tzinfo = zoneinfo.ZoneInfo("America/New_York")

        batch_date_etc = last_watermark_utc.astimezone(etc_tzinfo).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        query = f""" (
        
                SELECT *
                 FROM sci_rpt.eom_fedex_tracking
                 WHERE date_added >= TO_TIMESTAMP('{batch_date_etc}', 'YYYY-MM-DD HH24:MI:SS')
                 OR date_last_modified >= TO_TIMESTAMP('{batch_date_etc}', 'YYYY-MM-DD HH24:MI:SS')
            )
        """

        return query

    def get_incremental_changeset(self, last_watermark_utc: datetime) -> DataFrame:

        query = self.get_sql_statement_for_incremental_changes(last_watermark_utc)
        return self.db_conn.read_from_oracle(query)

    def perform_changeset_transforms(
        self, incremental_changeset: DataFrame
    ) -> DataFrame:

        return perform_fedex_tracking_transforms(
            incremental_changeset, self.config.dbx_user_id, self.batch_date_utc
        )

    def merge_updates(self, updates: DataFrame) -> None:

        table_qualified = self.config.destination_table_qualified

        (
            DeltaTable.forName(self.spark, table_qualified)
            .alias("target")
            .merge(
                source=updates.alias("source"),
                condition="""
                    target.tracking_number = source.tracking_number and
                    target.tcn = source.tcn
                    
                """,
            )
            .whenMatchedUpdate(
                set={
                    "silver_created_on_utc": "target.silver_created_on_utc",
                    "silver_created_by": "target.silver_created_by",
                    "ship_date": "source.ship_date",
                    "estimated_delivery_date": "source.estimated_delivery_date",
                    "estimated_delivery_time": "source.estimated_delivery_time",
                    "delivery_date": "source.delivery_date",
                    "delivery_time": "source.delivery_time",
                    "last_status_date": "source.last_status_date",
                    "last_status_time": "source.last_status_time",
                    "shipper_city": "source.shipper_city",
                    "shipper_state": "source.shipper_state",
                    "shipper_country_code": "source.shipper_country_code",
                    "shipper_postal_code": "source.shipper_postal_code",
                    "shipper_reference": "source.shipper_reference",
                    "recipient_city_name": "source.recipient_city_name",
                    "recipient_state_code": "source.recipient_state_code",
                    "recipient_postal_code": "source.recipient_postal_code",
                    "recipient_country_code": "source.recipient_country_code",
                    "service_code": "source.service_code",
                    "package_code": "source.package_code",
                    "transportation_payor": "source.transportation_payor",
                    "track_type_cd": "source.track_type_cd",
                    "total_pieces_qty": "source.total_pieces_qty",
                    "weight_uom": "source.weight_uom",
                    "dim_uom": "source.dim_uom",
                    "package_length": "source.package_length",
                    "package_width": "source.package_width",
                    "package_height": "source.package_height",
                    "purchase_order_nbr": "source.purchase_order_nbr",
                    "invoice_nbr": "source.invoice_nbr",
                    "dept_nbr": "source.dept_nbr",
                    "lb_package_weight": "source.lb_package_weight",
                    "kg_package_weight": "source.kg_package_weight",
                    "delivery_attempt_exception": "source.delivery_attempt_exception",
                    "last_status_code": "source.last_status_code",
                    "event_city": "source.event_city",
                    "event_state": "source.event_state",
                    "event_country": "source.event_country",
                    "add_status_info": "source.add_status_info",
                    "date_added": "source.date_added",
                    "run_date": "source.run_date",
                    "date_last_modified": "source.date_last_modified",
                    "dtl_pickup_dttm": "source.dtl_pickup_dttm",
                    "dtl_estimated_delivery_dttm": "source.dtl_estimated_delivery_dttm",
                    "dtl_delivery_attempt_dttm": "source.dtl_delivery_attempt_dttm",
                    "dtl_delivery_dttm": "source.dtl_delivery_dttm",
                    "company_code": "source.company_code",
                    "partner_carrier_nbr1": "source.partner_carrier_nbr1",
                    "zone1_flag": "source.zone1_flag",
                    "tcn": "source.tcn",
                    "commit_date": "source.commit_date",
                    "original_commit_date": "source.original_commit_date",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "tracking_number": "source.tracking_number",
                    "shipment_id": "source.shipment_id",
                    "ship_date": "source.ship_date",
                    "estimated_delivery_date": "source.estimated_delivery_date",
                    "estimated_delivery_time": "source.estimated_delivery_time",
                    "delivery_date": "source.delivery_date",
                    "delivery_time": "source.delivery_time",
                    "last_status_date": "source.last_status_date",
                    "last_status_time": "source.last_status_time",
                    "shipper_city": "source.shipper_city",
                    "shipper_state": "source.shipper_state",
                    "shipper_country_code": "source.shipper_country_code",
                    "shipper_postal_code": "source.shipper_postal_code",
                    "shipper_reference": "source.shipper_reference",
                    "recipient_city_name": "source.recipient_city_name",
                    "recipient_state_code": "source.recipient_state_code",
                    "recipient_postal_code": "source.recipient_postal_code",
                    "recipient_country_code": "source.recipient_country_code",
                    "service_code": "source.service_code",
                    "package_code": "source.package_code",
                    "transportation_payor": "source.transportation_payor",
                    "track_type_cd": "source.track_type_cd",
                    "total_pieces_qty": "source.total_pieces_qty",
                    "weight_uom": "source.weight_uom",
                    "dim_uom": "source.dim_uom",
                    "package_length": "source.package_length",
                    "package_width": "source.package_width",
                    "package_height": "source.package_height",
                    "purchase_order_nbr": "source.purchase_order_nbr",
                    "invoice_nbr": "source.invoice_nbr",
                    "dept_nbr": "source.dept_nbr",
                    "lb_package_weight": "source.lb_package_weight",
                    "kg_package_weight": "source.kg_package_weight",
                    "delivery_attempt_exception": "source.delivery_attempt_exception",
                    "last_status_code": "source.last_status_code",
                    "event_city": "source.event_city",
                    "event_state": "source.event_state",
                    "event_country": "source.event_country",
                    "add_status_info": "source.add_status_info",
                    "date_added": "source.date_added",
                    "run_date": "source.run_date",
                    "date_last_modified": "source.date_last_modified",
                    "dtl_pickup_dttm": "source.dtl_pickup_dttm",
                    "dtl_estimated_delivery_dttm": "source.dtl_estimated_delivery_dttm",
                    "dtl_delivery_attempt_dttm": "source.dtl_delivery_attempt_dttm",
                    "dtl_delivery_dttm": "source.dtl_delivery_dttm",
                    "company_code": "source.company_code",
                    "partner_carrier_nbr1": "source.partner_carrier_nbr1",
                    "zone1_flag": "source.zone1_flag",
                    "tcn": "source.tcn",
                    "commit_date": "source.commit_date",
                    "original_commit_date": "source.original_commit_date",
                    "silver_created_on_utc": "source.silver_created_on_utc",
                    "silver_created_by": "source.silver_created_by",
                    "silver_updated_on_utc": "source.silver_updated_on_utc",
                    "silver_updated_by": "source.silver_updated_by",
                }
            )
            .execute()
        )
