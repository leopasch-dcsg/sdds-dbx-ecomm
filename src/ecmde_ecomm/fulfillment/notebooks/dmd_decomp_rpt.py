# Databricks notebook source

# COMMAND ----------
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import dbutils

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.fulfillment.demand.repository.dmd_decomp_rpt import (
    DemandDecompReportRepository,
    DemandDecompReportRepositoryConf,
    DemandDecompReportDataSources,
)
from ecmde_ecomm.fulfillment.demand.model.dmd_decomp_rpt import FiscalDates

azure_secret_scope = NotebookUtil.notebook_param("azure_kv_scope")
ecmde_catalog = NotebookUtil.notebook_param("ecmde_silver_catalog")
ecmde_silver_schema = NotebookUtil.notebook_param("ecmde_silver_schema")
ecomp_catalog = NotebookUtil.notebook_param("ecomp_catalog", "prod_ecmde_db")
entdata_catalog = NotebookUtil.notebook_param("entdata_catalog", "entdata")
ecom_dim_schema = NotebookUtil.notebook_param("ecom_dim_schema", "ecom_dim")
user_principal = NotebookUtil.notebook_param("dbx_user_id")
ddw_catalog = NotebookUtil.notebook_param("ddw_catalog")
# ecomp_ora_password = dbutils.secrets.get(azure_secret_scope, "az-ecomp-ronly")

conf = DemandDecompReportRepositoryConf(
    ecmde_silver_catalog=ecmde_catalog,
    ecmde_silver_schema=ecmde_silver_schema,
    entdata_catalog=entdata_catalog,
    user_principal=user_principal,
    ddw_catalog=ddw_catalog,
    ecomp_catalog=ecomp_catalog,
    ecom_dim_schema=ecom_dim_schema,
)

repository = DemandDecompReportRepository(conf, spark)

# COMMAND ----------
etc_tz = ZoneInfo("America/New_York")
yesterday: date = (datetime.now(tz=etc_tz) - timedelta(days=1)).date()
display(f"Yesterday: {yesterday.strftime('%Y-%m-%d %H:%M:%S')}")

fiscal_dates: FiscalDates = repository.date_range(yesterday)

print(
    f"""
    Fiscal Date ID LY Start: {fiscal_dates.date_id_ly_start},
    Fiscal Date ID LY End  : {fiscal_dates.date_id_ly_end},
    Fiscal Date ID TY Start: {fiscal_dates.date_id_ty_start},
    Fiscal Date ID TY End  : {fiscal_dates.date_id_ty_end},
    """
)

# COMMAND ----------
open_dmd_agg = repository.open_demand_aggregate(fiscal_dates)
print(f"Open DMD Count: {open_dmd_agg.count()}")

# COMMAND ----------
ff_agg = repository.fulfilled_aggregate(fiscal_dates)
print(f"Fulfilled Amount Agg Count: {ff_agg.count()}")

# COMMAND ----------
cancelled_agg = repository.cancels_aggregate(fiscal_dates)
print(f"Cancelled Amount Agg Count: {cancelled_agg.count()}")

# COMMAND ----------
designated_dmd_agg = repository.designated_aggregate(fiscal_dates)
print(f"Designated DMD Count: {designated_dmd_agg.count()}")

# COMMAND ----------
print("Building Demand Decomp Report Data Sources.\r\n")
data_sources = DemandDecompReportDataSources(
    open_dmd_agg, ff_agg, cancelled_agg, designated_dmd_agg
)

demand_decomp_report = repository.demand_decomp_report(
    fiscal_dates, datetime.now(timezone.utc), data_sources
)

print(f"Demand Decomp Report Count: {demand_decomp_report.count()}")

# COMMAND ----------
repository.save(demand_decomp_report)
print(
    f"Truncate and load of demand decom report is complete. Total Records Processed: {demand_decomp_report.count()}"
)
