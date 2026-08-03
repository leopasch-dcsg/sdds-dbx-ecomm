from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    to_timestamp,
    from_utc_timestamp,
    convert_timezone,
    lit,
)
from datetime import datetime
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from pyspark.sql.types import DecimalType, StringType
from ecmde_ecomm.common import Logger
from ecmde_ecomm.common.bq.egress import (
    BigQueryEgressOperation,
    DatabricksSource,
    BigQueryCredentials,
    BigQueryDestination,
    LoadMode,
)


class DksSkuShipEgressOperation(BigQueryEgressOperation):

    def __init__(
        self,
        spark: SparkSession,
        bigquery_credentials: BigQueryCredentials,
        source: DatabricksSource,
        destination: BigQueryDestination,
        watermark: Watermark | None = None,
        load_mode: LoadMode = LoadMode.TRUNCATE_LOAD,
    ):
        super().__init__(
            spark=spark,
            credentials=bigquery_credentials,
            source=source,
            destination=destination,
            watermark=watermark,
            load_mode=load_mode,
        )

        self._logger = Logger.logger(__class__.__name__)

    def get_source_dataframe(
        self, last_batch_date_utc: datetime | None = None
    ) -> DataFrame:
        self._logger.info(
            f"Getting source dataframe for {self.source.table} and casting column types to be more explicit."
        )
        df = self.spark.sql(
            f"""
                select * from {self.source.fully_qualified_table()}
                """
        )

        timestamp_format = "yyyy-MM-dd'T'HH:mm:ss"
        timezone = "America/New_York"

        return df.select(
            col("DKS_SKU_KEY").cast(DecimalType(38, 0)).alias("dks_sku_key"),
            col("SHIP_CLASS").cast("string").alias("ship_class"),
            col("SKU_WEIGHT").cast(DecimalType(38, 9)).alias("sku_weight"),
            col("SKU_LENGTH").cast(DecimalType(38, 9)).alias("sku_length"),
            col("SKU_WIDTH").cast(DecimalType(38, 9)).alias("sku_width"),
            col("SKU_HEIGHT").cast(DecimalType(38, 9)).alias("sku_height"),
            col("DIM_TYPE").cast("string").alias("dim_type"),
            col("DIM_VALUE").cast(DecimalType(38, 9)).alias("dim_value"),
            col("HAZMAT_FLAG").cast("string").alias("hazmat_flag"),
            col("SKU_VDC_FLAG").cast("string").alias("sku_vdc_flag"),
            col("DKS_SKU").cast(DecimalType(38, 0)).alias("dks_sku"),
            col("GROUND_SHP_WINDOW")
            .cast(DecimalType(38, 0))
            .alias("ground_shp_window"),
            col("GROUND_TM_TO_DELIVER")
            .cast(DecimalType(38, 0))
            .alias("ground_tm_to_deliver"),
            col("GROUND_SHIP_CHARGE")
            .cast(DecimalType(38, 9))
            .alias("ground_ship_charge"),
            col("1DAY_SHP_WINDOW").cast(DecimalType(38, 0)).alias("one_day_shp_window"),
            col("1DAY_TM_TO_DELIVER")
            .cast(DecimalType(38, 0))
            .alias("one_day_tm_to_deliver"),
            col("1DAY_SHIP_CHARGE")
            .cast(DecimalType(38, 9))
            .alias("one_day_ship_charge"),
            col("2DAY_SHP_WINDOW").cast(DecimalType(38, 0)).alias("two_day_shp_window"),
            col("2DAY_TM_TO_DELIVER")
            .cast(DecimalType(38, 0))
            .alias("two_day_tm_to_deliver"),
            col("2DAY_SHIP_CHARGE")
            .cast(DecimalType(38, 9))
            .alias("two_day_ship_charge"),
            col("CURBSIDE_SHP_WINDOW")
            .cast(DecimalType(38, 0))
            .alias("curbside_shp_window"),
            col("CURBSIDE_TM_TO_DELIVER")
            .cast(DecimalType(38, 0))
            .alias("curbside_tm_to_deliver"),
            col("CURBSIDE_SHIP_CHARGE")
            .cast(DecimalType(38, 9))
            .alias("curbside_ship_charge"),
            col("THRESHOLD_SHP_WINDOW")
            .cast(DecimalType(38, 0))
            .alias("threshold_shp_window"),
            col("THRESHOLD_TM_TO_DELIVER")
            .cast(DecimalType(38, 0))
            .alias("threshold_tm_to_deliver"),
            col("THRESHOLD_SHIP_CHARGE")
            .cast(DecimalType(38, 9))
            .alias("threshold_ship_charge"),
            col("ROC_SHP_WINDOW").cast(DecimalType(38, 0)).alias("roc_shp_window"),
            col("ROC_TM_TO_DELIVER")
            .cast(DecimalType(38, 0))
            .alias("roc_tm_to_deliver"),
            col("ROC_SHIP_CHARGE").cast(DecimalType(38, 9)).alias("roc_ship_charge"),
            col("ASSEMBLY_SHP_WINDOW")
            .cast(DecimalType(38, 0))
            .alias("assembly_shp_window"),
            col("ASSEMBLY_TM_TO_DELIVER")
            .cast(DecimalType(38, 0))
            .alias("assembly_tm_to_deliver"),
            col("ASSEMBLY_SHIP_CHARGE")
            .cast(DecimalType(38, 9))
            .alias("assembly_ship_charge"),
            col("DATA_SOURCE_KEY").cast(DecimalType(38, 0)).alias("data_source_key"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_ADDED_SOURCE")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_added_source"),
            date_format(
                convert_timezone(
                    lit("UTC"), lit(timezone), col("DATE_MODIFIED_SOURCE")
                ),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_modified_source"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_ADDED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_added"),
            col("ADDED_BY").cast("string").alias("added_by"),
            date_format(
                convert_timezone(lit("UTC"), lit(timezone), col("DATE_LAST_MODIFIED")),
                timestamp_format,
            )
            .cast(StringType())
            .alias("date_last_modified"),
            col("MODIFIED_BY").cast("string").alias("modified_by"),
            col("RECORD_STATUS").cast("string").alias("record_status"),
            col("REFERENCE_ID").cast(DecimalType(38, 0)).alias("reference_id"),
            col("SEA_RECORD_STATUS").cast("string").alias("sea_record_status"),
            col("BILLABLE_WEIGHT").cast(DecimalType(38, 9)).alias("billable_weight"),
            col("VN_ACTIVE").cast("string").alias("vn_active"),
            col("VDC_UPC").cast("string").alias("vdc_upc"),
            col("VDC_DATE_LAST_MODIFIED")
            .cast(DecimalType(38, 0))
            .alias("vdc_date_last_modified"),
            col("VDC_LEAD_DAYS").cast(DecimalType(38, 0)).alias("vdc_lead_days"),
            col("FACILITY_NUMBER").cast("long").alias("facility_number"),
            col("FACILITY_NAME").cast("string").alias("facility_name"),
            col("IS_SHIP_RESTRICTED").cast("string").alias("is_ship_restricted"),
            col("SFS_ELIGIBLE_FLG").cast("string").alias("sfs_eligible_flg"),
            col("RDC_ELIGIBLE_FLG").cast("string").alias("rdc_eligible_flg"),
            col("EXPEDITE_ELIGIBLE_FLG").cast("string").alias("expedite_eligible_flg"),
            col("LOCATION_ID").cast("string").alias("location_id"),
            col("GTGT_ELIGIBLE").cast("string").alias("gtgt_eligible"),
            col("PO_ELIGIBLE").cast("string").alias("po_eligible"),
            col("APO_FPO_ELIGIBLE").cast("string").alias("apo_fpo_eligible"),
            col("UST_ELIGIBLE").cast("string").alias("ust_eligible"),
            col("AGE_RESTRICTION").cast("string").alias("age_restriction"),
        )
