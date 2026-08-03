# Databricks notebook source
from natural_key_mapping import NATURAL_KEY_MAPPING
from primary_key_mapping import AUTO_GENERATED_PRIMARY_KEYS
from pyspark.sql import DataFrame

natural_key_mapping = {k:v for k,v in NATURAL_KEY_MAPPING.items() if v[0] != 'IGNORE'}
auto_generated_primary_keys = {k:v for k,v in AUTO_GENERATED_PRIMARY_KEYS.items() if k in natural_key_mapping.keys()}

# COMMAND ----------

date_cutoff = "2025-03-01"
workspace_id = "3759157064858225"

# COMMAND ----------

base_lineage_table = spark.sql(
    f"""
    select distinct
        source_table_full_name,
        upper(source_column_name) as source_column_name,
        target_table_full_name, 
        upper(target_column_name) as target_column_name
    from system.access.column_lineage
    where event_date >= '2025-03-01' --only applies recently
      and workspace_id = '3759157064858225' --only prod
      and target_type = "TABLE" --only matters for tables, not views
      and ( --ignore self-referential
          lower(source_table_full_name), upper(source_column_name)) != 
          (lower(target_table_full_name), upper(target_column_name)
      ) 
      and target_table_full_name is not Null --only matters for tables
      and target_table_schema not in ('reconcile','test','bodi_test','rekeyer') --ignore subset of leftover schemas that can be cleaned up
      and target_table_catalog like '%prod%' --ignore non-prod targets
    order by 1,2,3,4
    """
)

base_lineage_table.cache()
base_lineage_table.createOrReplaceTempView("base_lineage_table")

# COMMAND ----------

table_definitions = [
    ('prod_ecmde_db.ecom_dim.order_fulfill','ORDER_FULFILL_KEY'),
    ('prod_ecmde_db.ecom_dim.order_header','ORDER_HEADER_KEY'),
    ('prod_ecmde_db.ecom.cp_campaign_event','CP_CAMPAIGN_EVENT_ID'),
    ('prod_ecmde_db.ecom_dim.order_sku','ORDER_SKU_KEY'),
    ('prod_ecmde_db.ecom_dim.size_code','SIZE_CODE_KEY'),
    ('prod_ecmde_db.ecom_dim.color_code','COLOR_CODE_KEY'),
    ('prod_ecmde_db.ecom_dim.web_category','WEB_CATEGORY_KEY'),
    ('prod_ecmde_db.ecom_dim.order_delivery','ORDER_DELIVERY_KEY'),
    ('prod_ecmde_db.ecom_dim.order_sku_dmd_adjustment','ORDER_SKU_DMD_ADJUSTMENT_KEY'),
    ('prod_ecmde_db.ecom_dim.txn_web_sku_offer_price','TXN_WEB_SKU_OFFER_PRICE_KEY'),
    ('prod_ecmde_db.ecom_dim.web_product','PRODUCT_KEY'),
    ('prod_ecmde_db.ecom_dim.mdm_attribute_code','MDM_ATTRIBUTE_CODE_KEY'),
    ('prod_ecmde_db.ecom_dim.return_header','RETURN_HEADER_KEY'),
    ('prod_ecmde_db.ecom_dim.order_sku_init_alloc','INIT_ALLOC_KEY'),
    ('prod_ecmde_db.ecom_dim.promotion_event','PROMOTION_EVENT_KEY'),
    ('prod_ecmde_db.ecom_dim.return_delivery','RETURN_DELIVERY_KEY'),
    ('prod_ecmde_db.ecom_dim.pim_product','PIM_PRODUCT_KEY'),
    ('prod_ecmde_db.ecom_dim.order_sku_shipment','ORDER_SKU_SHIPMENT_KEY'),
    ('prod_ecmde_db.ecom.cwl_hierarchy','CWL_HIERARCHY_ID'),
    ('prod_ecmde_db.ecom_dim.promotion_header','PROMOTION_KEY'),
    ('prod_ecmde_db.ecom_dim.attribute_code','ATTRIBUTE_CODE_KEY'),
    ('prod_ecmde_db.ecom_dim.pim_product_emast_color','PIM_PRODUCT_EMAST_COLOR_KEY'),
    ('prod_ecmde_db.ecom_dim.attribute_value','ATTRIBUTE_VALUE_KEY'),
    ('prod_ecmde_db.ecom_dim.brand','BRAND_KEY'),
    ('prod_ecmde_db.ecom.cwl_pmms_style_task','CWL_PMMS_STYLE_TASK_ID'),
    ('prod_ecmde_db.ecom_dim.bridge_pim_sku_product','BRIDGE_PIM_SKU_PRODUCT_KEY'),
    ('prod_ecmde_db.ecom_dim.bridge_product_style','BRIDGE_PRODUCT_STYLE_KEY'),
    ('prod_ecmde_db.ecom_dim.bridge_pim_product_style','BRIDGE_PIM_PRODUCT_STYLE_KEY'),
    ('prod_ecmde_db.ecom_dim.bridge_web_sku_product','BRIDGE_WEB_SKU_PRODUCT_KEY'),
    ('prod_ecmde_db.ecom.stg_wcs_catalog_job_log','STG_WCS_CATALOG_JOB_LOG_ID'),
    ('prod_ecmde_db.ecom.stg_nrt_item_inventory','STG_NRT_ITEM_INVENTORY_ID'),
    ('prod_ecmde_db.ecom.stg_mdm_api_attr_multi','STG_MDM_API_ATTR_MULTI_ID'),
    ('prod_ecmde_db.ecom.stg_mdm_api_attr_clob','STG_MDM_API_ATTR_CLOB_ID'),
    ('prod_ecmde_db.ecom.stg_mdm_entity','STG_MDM_ENTITY_ID'),
    ('prod_ecmde_db.ecom.stg_store_locator_dks','STG_STORE_LOCATOR_DKS_ID'),
    ('prod_ecmde_db.ecom.stg_nrt_item_inventory_solr','STG_NRT_ITEM_INVENTORY_SOLR_ID'),
    ('prod_ecmde_db.ecom.stg_mdm_api_attr_value','STG_MDM_API_ATTR_VALUE_ID'),
]

# COMMAND ----------

table_column_filter = ",".join(
    [
        f"('{table_name.lower()}','{column_name.upper()}')"
        for table_name, column_name in table_definitions #auto_generated_primary_keys.items()
    ]
)

# COMMAND ----------

first_lineage_table = spark.sql(
    f"""
    select distinct 
        source_table_full_name,
        source_column_name as source_column_name,
        source_table_full_name as intermediate_table_full_name,
        source_column_name as intermediate_column_name,
        target_table_full_name,
        target_column_name
    from base_lineage_table
    where (source_table_full_name, source_column_name) in ({table_column_filter})
    order by 1,2,3 nulls first,4 nulls first,5,6
    """
)
first_lineage_table.display()

# COMMAND ----------

from collections import deque

rows_out = set([row for row in first_lineage_table.toPandas().itertuples(index=False, name=None)])
queue = deque()
queue.extend(rows_out)



while queue:
    row = queue.pop()
    source_table_full_name = row[0]
    source_column_name = row[1]
    intermediate_table_full_name = row[4]
    intermediate_column_name = row[5]

    downstream_columns = spark.sql(
        f"""
        select distinct 
            '{source_table_full_name}',
            '{source_column_name}',
            '{intermediate_table_full_name}',
            '{intermediate_column_name}',
            target_table_full_name,
            target_column_name
        from base_lineage_table
        where source_table_full_name = '{intermediate_table_full_name}'
          and source_column_name = '{intermediate_column_name}'
        """
    )

    #no downstream found
    if downstream_columns.isEmpty():
        continue

    downstream_columns = downstream_columns.toPandas().itertuples(index=False, name=None)
    
    for new_row in downstream_columns:
        # already computed, skip
        if new_row in rows_out:
            continue
        queue.append(new_row)
        rows_out.add(new_row)

# COMMAND ----------

spark.createDataFrame(
    rows_out,
    schema="source_table_full_name string,source_column_name string,intermediate_table_full_name string,intermediate_column_name string,target_table_full_name string,target_column_name string"
).display()

# COMMAND ----------

# (
#     spark.createDataFrame(
#         rows_out,
#         schema="source_table_full_name string,source_column_name string,intermediate_table_full_name string,"+
#                "intermediate_column_name string,target_table_full_name string,target_column_name string"
#     )
#     .write
#     .mode("overwrite")
#     .saveAsTable("dev_ecmde_db.rekeyer.source_target_mapping_rerun")
# )

# # apparently cannot read from csv on serverless?
# # with open("source_target_mapping.csv","w") as f:
# #     f.write("source_table_full_name,source_column_name,intermediate_table_full_name,intermediate_column_name,target_table_full_name,target_column_name\n")
# #     for row in rows_out:
# #         for idx,elem in enumerate(row):
# #             if idx == len(row)-1:
# #                 f.write(f"{elem}")
# #             else:
# #                 f.write(f"{elem},")
# #         f.write("\n")

# COMMAND ----------

[
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_delivery',
        'source_column_name':'ORDER_DELIVERY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_delivery',
        'target_column':'ORDER_DELIVERY_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.pim_product',
        'source_column_name':'PIM_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.pim_product',
        'target_column':'PIM_PRODUCT_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'target_column':'PRODUCT_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'source_column_name':'ORDER_FULFILL_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'target_column':'ORDER_FULFILL_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom.cp_campaign_event',
        'source_column_name':'CP_CAMPAIGN_EVENT_ID',
        'target_tabel_full_name':'prod_ecmde_db.ecom.cp_campaign_event',
        'target_column':'CP_CAMPAIGN_EVENT_ID',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.size_code',
        'source_column_name':'SIZE_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.size_code',
        'target_column':'SIZE_CODE_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.color_code',
        'source_column_name':'COLOR_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.color_code',
        'target_column':'COLOR_CODE_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_category',
        'source_column_name':'WEB_CATEGORY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.web_category',
        'target_column':'WEB_CATEGORY_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku_dmd_adjustment',
        'source_column_name':'ORDER_SKU_DMD_ADJUSTMENT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_dmd_adjustment',
        'target_column':'ORDER_SKU_DMD_ADJUSTMENT_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.txn_web_sku_offer_price',
        'source_column_name':'TXN_WEB_SKU_OFFER_PRICE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_web_sku_offer_price',
        'target_column':'TXN_WEB_SKU_OFFER_PRICE_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.mdm_attribute_code',
        'source_column_name':'MDM_ATTRIBUTE_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.mdm_attribute_code',
        'target_column':'MDM_ATTRIBUTE_CODE_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.return_header',
        'source_column_name':'RETURN_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.return_header',
        'target_column':'RETURN_HEADER_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku_init_alloc',
        'source_column_name':'INIT_ALLOC_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_init_alloc',
        'target_column':'INIT_ALLOC_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.promotion_event',
        'source_column_name':'PROMOTION_EVENT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.promotion_event',
        'target_column':'PROMOTION_EVENT_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.return_delivery',
        'source_column_name':'RETURN_DELIVERY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.return_delivery',
        'target_column':'RETURN_DELIVERY_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku_shipment',
        'source_column_name':'ORDER_SKU_SHIPMENT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_shipment',
        'target_column':'ORDER_SKU_SHIPMENT_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom.cwl_hierarchy',
        'source_column_name':'CWL_HIERARCHY_ID',
        'target_tabel_full_name':'prod_ecmde_db.ecom.cwl_hierarchy',
        'target_column':'CWL_HIERARCHY_ID',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.promotion_header',
        'source_column_name':'PROMOTION_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.promotion_header',
        'target_column':'PROMOTION_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.attribute_code',
        'source_column_name':'ATTRIBUTE_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.attribute_code',
        'target_column':'ATTRIBUTE_CODE_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.pim_product_emast_color',
        'source_column_name':'PIM_PRODUCT_EMAST_COLOR_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.pim_product_emast_color',
        'target_column':'PIM_PRODUCT_EMAST_COLOR_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.attribute_value',
        'source_column_name':'ATTRIBUTE_VALUE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.attribute_value',
        'target_column':'ATTRIBUTE_VALUE_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.brand',
        'source_column_name':'BRAND_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.brand',
        'target_column':'BRAND_KEY',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom.cwl_pmms_style_task',
        'source_column_name':'CWL_PMMS_STYLE_TASK_ID',
        'target_tabel_full_name':'prod_ecmde_db.ecom.cwl_pmms_style_task',
        'target_column':'CWL_PMMS_STYLE_TASK_ID',
        'is_identity':True,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.bridge_web_sku_product',
        'source_column_name':'BRIDGE_WEB_SKU_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.snp_web_product_assortment',
        'target_column':'MAX_BRIDGE_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom.cp_campaign_event',
        'source_column_name':'CP_CAMPAIGN_EVENT_ID',
        'target_tabel_full_name':'prod_ecmde_db.ecom.cp_campaign_event_pps',
        'target_column':'CP_CAMPAIGN_EVENT_ID',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom.cp_campaign_event',
        'source_column_name':'CP_CAMPAIGN_EVENT_ID',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_flash_event',
        'target_column':'CP_CAMPAIGN_EVENT_ID',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom.cwl_hierarchy',
        'source_column_name':'CWL_HIERARCHY_ID',
        'target_tabel_full_name':'prod_ecmde_db.ecom.cwl_pmms_style_task',
        'target_column':'CWL_HIERARCHY_ID',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.attribute_code',
        'source_column_name':'ATTRIBUTE_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.attribute_value',
        'target_column':'ATTRIBUTE_CODE_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.brand',
        'source_column_name':'BRAND_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.style',
        'target_column':'BRAND_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.brand',
        'source_column_name':'BRAND_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.style',
        'target_column':'STYLE_BRAND_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.color_code',
        'source_column_name':'COLOR_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.dks_sku',
        'target_column':'DKS_SKU_COLOR_CODE_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.color_code',
        'source_column_name':'COLOR_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.web_sku_attr',
        'target_column':'WEB_SKU_COLOR_CODE_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_delivery',
        'source_column_name':'ORDER_DELIVERY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'ORIG_ORDER_DELIVERY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_delivery',
        'source_column_name':'ORDER_DELIVERY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'ORDER_DELIVERY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_delivery',
        'source_column_name':'ORDER_DELIVERY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_shipment',
        'target_column':'ORDER_DELIVERY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_delivery',
        'source_column_name':'ORDER_DELIVERY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku',
        'target_column':'ORDER_DELIVERY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'source_column_name':'ORDER_FULFILL_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_curr_alloc',
        'target_column':'ORDER_FULFILL_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'source_column_name':'ORDER_FULFILL_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_init_alloc',
        'target_column':'ORDER_FULFILL_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'source_column_name':'ORDER_FULFILL_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'ORDER_FULFILL_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'source_column_name':'ORDER_FULFILL_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'ORIG_ORDER_FULFILL_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'source_column_name':'ORDER_FULFILL_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_shipment',
        'target_column':'ORDER_FULFILL_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'source_column_name':'ORDER_FULFILL_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku',
        'target_column':'ORDER_FULFILL_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_fulfill',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_curr_alloc',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_dmd_adjustment',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_init_alloc',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'RS_ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'PREV_ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'ORIG_ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_shipment',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_header',
        'source_column_name':'ORDER_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku_adjustment',
        'target_column':'ORDER_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_curr_alloc',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_dmd_adjustment',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_flash_event',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_init_alloc',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'ORIG_ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'RS_ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'PREV_ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_reship',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_shipment',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'source_column_name':'ORDER_SKU_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku_adjustment',
        'target_column':'ORDER_SKU_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.pim_product',
        'source_column_name':'PIM_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom.pim_product_attr_lob',
        'target_column':'PIM_PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.pim_product',
        'source_column_name':'PIM_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.bridge_pim_product_style',
        'target_column':'PIM_PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.pim_product',
        'source_column_name':'PIM_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.bridge_pim_sku_product',
        'target_column':'PIM_PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.pim_product',
        'source_column_name':'PIM_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.tmp_pim_product_attr',
        'target_column':'PIM_PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.pim_product',
        'source_column_name':'PIM_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.tmp_pim_product_attr_clob',
        'target_column':'PIM_PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.pim_product_emast_color',
        'source_column_name':'PIM_PRODUCT_EMAST_COLOR_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.dks_sku_pim',
        'target_column':'PIM_PRODUCT_EMAST_COLOR_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.promotion_header',
        'source_column_name':'PROMOTION_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_dmd_adjustment',
        'target_column':'PROMOTION_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.promotion_header',
        'source_column_name':'PROMOTION_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_dmd_adjustment',
        'target_column':'TAG_PROMOTION_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.promotion_header',
        'source_column_name':'PROMOTION_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku_adjustment',
        'target_column':'PROMOTION_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.return_delivery',
        'source_column_name':'RETURN_DELIVERY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku',
        'target_column':'RETURN_DELIVERY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.return_header',
        'source_column_name':'RETURN_HEADER_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku',
        'target_column':'RETURN_HEADER_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.size_code',
        'source_column_name':'SIZE_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.dks_sku',
        'target_column':'DKS_SKU_SIZE_CODE_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.size_code',
        'source_column_name':'SIZE_CODE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.web_sku_attr',
        'target_column':'WEB_SKU_SIZE_CODE_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_category',
        'source_column_name':'WEB_CATEGORY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom.flash_event_style',
        'target_column':'WEB_CATEGORY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_category',
        'source_column_name':'WEB_CATEGORY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.snp_web_product_category',
        'target_column':'WEB_CATEGORY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_category',
        'source_column_name':'WEB_CATEGORY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'target_column':'WEB_CATEGORY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_category',
        'source_column_name':'WEB_CATEGORY_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'target_column':'GLOBAL_CATEGORY_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.bridge_product_style',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.bridge_web_sku_product',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_curr_alloc',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_dmd_adjustment',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_init_alloc',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.order_sku_shipment',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.snp_web_product_assortment',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.snp_web_product_category',
        'target_column':'WEB_PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.snp_web_product_category',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.snp_web_product_category',
        'target_column':'PRODUCT_STATUS',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.tmp_product_attr',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.txn_order_sku_adjustment',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'target_column':'WEB_PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.web_product',
        'source_column_name':'PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.web_product_attr',
        'target_column':'PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':False,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.bridge_pim_sku_product',
        'source_column_name':'BRIDGE_PIM_SKU_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.bridge_pim_sku_product',
        'target_column':'BRIDGE_PIM_SKU_PRODUCT_KEY',
        'is_identity':True,
        'is_bridge':True,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.bridge_product_style',
        'source_column_name':'BRIDGE_PRODUCT_STYLE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.bridge_product_style',
        'target_column':'BRIDGE_PRODUCT_STYLE_KEY',
        'is_identity':True,
        'is_bridge':True,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.bridge_pim_product_style',
        'source_column_name':'BRIDGE_PIM_PRODUCT_STYLE_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.bridge_pim_product_style',
        'target_column':'BRIDGE_PIM_PRODUCT_STYLE_KEY',
        'is_identity':True,
        'is_bridge':True,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.bridge_web_sku_product',
        'source_column_name':'BRIDGE_WEB_SKU_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.bridge_web_sku_product',
        'target_column':'BRIDGE_WEB_SKU_PRODUCT_KEY',
        'is_identity':True,
        'is_bridge':True,
    },
    {
        'source_table_full_name':'prod_ecmde_db.ecom_dim.bridge_web_sku_product',
        'source_column_name':'BRIDGE_WEB_SKU_PRODUCT_KEY',
        'target_tabel_full_name':'prod_ecmde_db.ecom_dim.snp_web_product_assortment',
        'target_column':'BRIDGE_WEB_SKU_PRODUCT_KEY',
        'is_identity':False,
        'is_bridge':True,
    }
]