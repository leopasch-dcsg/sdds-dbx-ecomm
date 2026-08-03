# Databricks notebook source
# COMMAND ----------

dbutils.widgets.text("watermark_table_name", "", "Watermark Table Name")
dbutils.widgets.text("fmodegress", "DBX", "Egress Mode (DBX, ORACLE, NONE)")
dbutils.widgets.text("dbx_env", "", "Databricks Environment")
dbutils.widgets.text(
    "dbx_data_quality", "silver", "Data Quality (bronze|silver|gold|ecomp)"
)


dbutils.widgets.text("source_catalog", "", "Source Catalog")
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table Name")
dbutils.widgets.text("destination_catalog", "", "Destination Catalog (for DBX)")
dbutils.widgets.text("destination_schema", "", "Destination Schema (for DBX)")
dbutils.widgets.text("destination_table", "", "Destination Table Name (for DBX)")


dbutils.widgets.text("azure_kv_scope", "", "Azure Key-Vault Scope")
dbutils.widgets.text("oracle_jdbc_url", "", "Oracle JDBC URL")
dbutils.widgets.text("oracle_user_name", "", "Oracle Username")
dbutils.widgets.text(
    "oracle_password_key", "", "Azure Key-Vault Secret Key for Oracle Password"
)
dbutils.widgets.text("dbx_source_table", "", "DBX Source Table FQN (for Oracle read)")
dbutils.widgets.text("oracle_destination_table", "", "Oracle Destination Table FQN")

# COMMAND ----------

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

from datetime import datetime, timezone
from ecmde_ecomm.common.errors import ElementNotFoundError, NotSupportedError
from ecmde_ecomm.common.dbx.env import FMODEgressFlag
from ecmde_ecomm.common.dbx.etl.watermark import Watermark
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.oracle.oracle_egress import OracleConfig
from ecmde_ecomm.fulfillment.oracle.oso_stg_oracle_egress import OSOStgOracleEgress
from ecmde_ecomm.fulfillment.ecomp.oso_stg_egress import OSOStgDbxEgress

batch_date_utc = datetime.now(timezone.utc)

wm_table = NotebookUtil.notebook_param("watermark_table_name")
if not wm_table:
    raise ElementNotFoundError("'watermark_table_name' is required.")

fmode_flag = FMODEgressFlag.from_notebook_params()

dbx_count = ora_count = 0
final_src = None


if fmode_flag == FMODEgressFlag.DBX:

    src_catalog = NotebookUtil.notebook_param("source_catalog")
    src_schema = NotebookUtil.notebook_param("source_schema")
    src_table = NotebookUtil.notebook_param("source_table")
    if not (src_catalog and src_schema and src_table):
        raise ElementNotFoundError(
            "For DBX egress, 'source_catalog', 'source_schema', and 'source_table' are required."
        )
    src_fqn = f"{src_catalog}.{src_schema}.{src_table}"

    dest_catalog = NotebookUtil.notebook_param("destination_catalog")
    dest_schema = NotebookUtil.notebook_param("destination_schema")
    dest_table = NotebookUtil.notebook_param("destination_table")
    if not (dest_catalog and dest_schema and dest_table):
        raise ElementNotFoundError(
            "For DBX egress, 'destination_catalog', 'destination_schema', and 'destination_table' are required."
        )
    dest_fqn = f"{dest_catalog}.{dest_schema}.{dest_table}"

    wm_dbx = Watermark(
        wm_table, dest_catalog.lower(), dest_schema.lower(), dest_table.lower(), spark
    )
    dbx_op = OSOStgDbxEgress(src_fqn, dest_fqn, wm_dbx, spark)
    dbx_count = dbx_op.run()
    wm_dbx.update_watermark_timestamp(batch_date_utc)

    final_src = src_fqn


elif fmode_flag == FMODEgressFlag.ORACLE:

    oracle_destination_table = NotebookUtil.notebook_param("oracle_destination_table")
    if not oracle_destination_table:
        raise ElementNotFoundError(
            "'oracle_destination_table' parameter is required for Oracle egress."
        )
    oracle_dest_schema, oracle_dest_table = oracle_destination_table.split(".")

    config = OracleConfig.from_notebook_params()

    wm_oracle = Watermark(
        wm_table, "oracle", oracle_dest_schema.lower(), oracle_dest_table.lower(), spark
    )
    ora_op = OSOStgOracleEgress(config, wm_oracle, spark)
    ora_count = ora_op.execute()
    wm_oracle.update_watermark_timestamp(batch_date_utc)

    final_src = NotebookUtil.notebook_param("dbx_source_table")


elif fmode_flag == FMODEgressFlag.NONE:
    print("Egress mode=NONE; skipping all egress and not updating watermark.")

else:
    raise NotSupportedError(f"Unsupported egress mode: {fmode_flag}")


print(
    f"""
Next Watermark           : {batch_date_utc.isoformat()}
DBX Rows Written         : {dbx_count}
Oracle Rows Written      : {ora_count}
Source                   : {final_src or 'n/a'}
"""
)
