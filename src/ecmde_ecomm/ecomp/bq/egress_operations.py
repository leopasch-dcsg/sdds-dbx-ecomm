from enum import Enum

from ecmde_ecomm.common import Logger, NotSupportedError
from ecmde_ecomm.common.bq.egress import (
    BigQueryEgressOperation,
    BigQueryEgressOperationBuilder,
)

from .manual_discount_lkup import ManualDiscountLkupEgressOperation
from .snp_web_product_assortment import SnpWebProductAssortmentEgressOperation
from .order_header import OrderHeaderEgressOperation
from .promotion_header import PromotionHeaderEgressOperation
from .promotion_hierarchy_sub import PromotionHierarchySubEgressOperation
from .attribute_code import AttributeCodeEgressOperation
from .pim_product_emast import PimProductEmastEgressOperation
from .vw_style import VwStyleEgressOperation
from .mdm_attribute_code import MDMAttributeCodeEgressOperation
from .dks_sku_ship import DksSkuShipEgressOperation
from .nrt_atp_bopis_vw import NrtAtpBopisEgressOperation
from .flash_sale_styles_dly_vw import FlashSaleStylesDlyEgressOperation
from .tmp_store_productivity_bopis import TmpStoreProductivityBopisEgressOperation
from .tmp_store_productivity_sfs import TmpStoreProductivitySfsEgressOperation
from .tmp_store_productivity_hrs_vw import TmpStoreProductivityHrsVwEgressOperation
from .vw_stg_mdm_master_cat_attr_mu import VwStgMdmMasterCatAttrMuEgressOperation
from .web_sku_header import WebSkuHeaderEgressOperation
from .px_description import PxDescriptionEgressOperation
from .vw_stg_dks_master_cat_attr_mu import VwStgDksMasterCatAttrMuEgressOperation
from .px_promoauth import PxPromoauthEgressOperation
from .pim_product_emast_title_only import PimProductEmastTitleOnlyEgressOperation
from .clr_color_lkup import ClrColorLkupEgressOperation
from .promotion_event import PromotionEventEgressOperation
from .promotion_hierarchy_main import PromotionHierarchyMainEgressOperation
from .order_sku import OrderSkuEgressOperation
from .order_sku_flash_event import OrderSkuFlashEventEgressOperation
from .order_delivery import OrderDeliveryEgressOperation
from .snp_inventory import SnpInventoryEgressOperation
from .order_fulfill import OrderFulfillEgressOperation
from .txn_order_sku_adjustment import TxnOrderSkuAdjustmentEgressOperation
from .txn_order_sku import TxnOrderSkuEgressOperation
from .txn_web_sku_offer_price import TxnWebSkuOfferPriceEgressOperation
from .stg_mdm_master_catalog_attr import StgMdmMasterCatalogAttrEgressOperation
from .pim_product_desc_vw import PimProductDescVwEgressOperation
from .web_product import WebProductEgressOperation
from .web_sku_attr import WebSkuAttrEgressOperation
from .dks_sku_pim import DksSkuPimEgressOperation
from .pim_product import PimProductEgressOperation
from .pim_product_emast_color import PimProductEmastColorEgressOperation
from .bridge_pim_sku_product import BridgePimSkuProductEgressOperation
from .order_sku_shipment import OrderSkuShipmentEgressOperation
from .order_tender import OrderTenderEgressOperation
from .order_sku_init_alloc import OrderSkuInitAllocEgressOperation
from .pim_sku import PimSkuEgressOperation
from .manual_discount_lkup import ManualDiscountLkupEgressOperation


class EgressTableNames(Enum):
    ATTRIBUTE_CODE = "attribute_code"
    PIM_PRODUCT_EMAST = "pim_product_emast"
    VW_STYLE = "vw_style"
    MDM_ATTRIBUTE_CODE = "mdm_attribute_code"
    DKS_SKU_SHIP = "dks_sku_ship"
    NRT_ATP_BOPIS_VW = "nrt_atp_bopis_vw"
    FLASH_SALE_STYLES_DLY_VW = "flash_sale_styles_dly_vw"
    TMP_STORE_PRODUCTIVITY_BOPIS = "tmp_store_productivity_bopis"
    TMP_STORE_PRODUCTIVITY_SFS = "tmp_store_productivity_sfs"
    TMP_STORE_PRODUCTIVITY_HRS_VW = "tmp_store_productivity_hrs_vw"
    VW_STG_MDM_MASTER_CAT_ATTR_MU = "vw_stg_mdm_master_cat_attr_mu"
    WEB_SKU_HEADER = "web_sku_header"
    PX_DESCRIPTION = "px_description"
    VW_STG_DKS_MASTER_CAT_ATTR_MU = "vw_stg_dks_master_cat_attr_mu"
    PX_PROMOAUTH = "px_promoauth"
    VW_PIM_PRODUCT_EMAST_TITLE_ONLY = "vw_pim_product_emast_title_only"
    CLR_COLOR_LKUP = "clr_color_lkup"
    PROMOTION_EVENT = "promotion_event"
    PROMOTION_HIERARCHY_MAIN = "promotion_hierarchy_main"
    PROMOTION_HIERARCHY_SUB = "promotion_hierarchy_sub"
    ORDER_HEADER = "order_header"
    ORDER_SKU = "order_sku"
    ORDER_SKU_FLASH_EVENT = "order_sku_flash_event"
    ORDER_DELIVERY = "order_delivery"
    SNP_INVENTORY = "snp_inventory"
    ORDER_FULFILL = "order_fulfill"
    PROMOTION_HEADER = "promotion_header"
    SNP_WEB_PRODUCT_ASSORTMENT = "snp_web_product_assortment"
    TXN_ORDER_SKU_ADJUSTMENT = "txn_order_sku_adjustment"
    TXN_ORDER_SKU = "txn_order_sku"
    TXN_WEB_SKU_OFFER_PRICE = "txn_web_sku_offer_price"
    STG_MDM_MASTER_CATALOG_ATTR = "stg_mdm_master_catalog_attr"
    PIM_PRODUCT_DESC_VW = "pim_product_desc_vw"
    WEB_PRODUCT = "web_product"
    WEB_SKU_ATTRIBUTE = "web_sku_attr"
    DKS_SKU_PIM = "dks_sku_pim"
    PIM_PRODUCT = "pim_product"
    PIM_PRODUCT_EMAST_COLOR = "pim_product_emast_color"
    BRIDGE_PIM_SKU_PRODUCT = "bridge_pim_sku_product"
    ORDER_SKU_SHIPMENT = "order_sku_shipment"
    ORDER_TENDER = "order_tender"
    ORDER_SKU_INIT_ALLOC = "order_sku_init_alloc"
    PIM_SKU = "pim_sku"
    MANUAL_DISCOUNT_LKUP = "manual_discount_lkup"

    def __init__(self, table_name: str):
        self.table_name = table_name


class ECOMPBigQueryEgressOperationBuilder(BigQueryEgressOperationBuilder):
    def __init__(self, spark):
        super().__init__(spark)
        self._logger = Logger.logger(__class__.__name__)

    @staticmethod
    def builder(spark):
        return ECOMPBigQueryEgressOperationBuilder(spark)

    def operation(self) -> BigQueryEgressOperation:
        self._logger.info(
            f"Building ECOMP BigQuery Egress Operation for Databricks Source Table: {self.databricks_source.table}"
        )

        match self.databricks_source.table:
            case EgressTableNames.ATTRIBUTE_CODE.table_name:
                return AttributeCodeEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.PIM_PRODUCT_EMAST.table_name:
                return PimProductEmastEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.VW_STYLE.table_name:
                return VwStyleEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.MDM_ATTRIBUTE_CODE.table_name:
                return MDMAttributeCodeEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.DKS_SKU_SHIP.table_name:
                return DksSkuShipEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.NRT_ATP_BOPIS_VW.table_name:
                return NrtAtpBopisEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.FLASH_SALE_STYLES_DLY_VW.table_name:
                return FlashSaleStylesDlyEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.TMP_STORE_PRODUCTIVITY_BOPIS.table_name:
                return TmpStoreProductivityBopisEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.TMP_STORE_PRODUCTIVITY_SFS.table_name:
                return TmpStoreProductivitySfsEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.TMP_STORE_PRODUCTIVITY_HRS_VW.table_name:
                return TmpStoreProductivityHrsVwEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.VW_STG_MDM_MASTER_CAT_ATTR_MU.table_name:
                return VwStgMdmMasterCatAttrMuEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.WEB_SKU_HEADER.table_name:
                return WebSkuHeaderEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.WEB_SKU_ATTRIBUTE.table_name:
                return WebSkuAttrEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.PX_DESCRIPTION.table_name:
                return PxDescriptionEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.VW_STG_DKS_MASTER_CAT_ATTR_MU.table_name:
                return VwStgDksMasterCatAttrMuEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.PX_PROMOAUTH.table_name:
                return PxPromoauthEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.VW_PIM_PRODUCT_EMAST_TITLE_ONLY.table_name:
                return PimProductEmastTitleOnlyEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.CLR_COLOR_LKUP.table_name:
                return ClrColorLkupEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.PROMOTION_EVENT.table_name:
                return PromotionEventEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                    load_mode=self.load_mode,
                )
            case EgressTableNames.ORDER_SKU.table_name:
                return OrderSkuEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                )
            case EgressTableNames.ORDER_SKU_FLASH_EVENT.table_name:
                return OrderSkuFlashEventEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                )
            case EgressTableNames.ORDER_DELIVERY.table_name:
                return OrderDeliveryEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.PROMOTION_HIERARCHY_MAIN.table_name:
                return PromotionHierarchyMainEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                )
            case EgressTableNames.PROMOTION_HIERARCHY_SUB.table_name:
                return PromotionHierarchySubEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                )
            case EgressTableNames.SNP_INVENTORY.table_name:
                return SnpInventoryEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    watermark=self.watermark,
                )
            case EgressTableNames.ORDER_HEADER.table_name:
                return OrderHeaderEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.ORDER_FULFILL.table_name:
                return OrderFulfillEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.PROMOTION_HEADER.table_name:
                return PromotionHeaderEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.SNP_WEB_PRODUCT_ASSORTMENT.table_name:
                return SnpWebProductAssortmentEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.TXN_ORDER_SKU_ADJUSTMENT.table_name:
                return TxnOrderSkuAdjustmentEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.TXN_WEB_SKU_OFFER_PRICE.table_name:
                return TxnWebSkuOfferPriceEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.TXN_ORDER_SKU.table_name:
                return TxnOrderSkuEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.STG_MDM_MASTER_CATALOG_ATTR.table_name:
                return StgMdmMasterCatalogAttrEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.PIM_PRODUCT_DESC_VW.table_name:
                return PimProductDescVwEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.WEB_PRODUCT.table_name:
                return WebProductEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.DKS_SKU_PIM.table_name:
                return DksSkuPimEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.PIM_PRODUCT.table_name:
                return PimProductEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.PIM_PRODUCT_EMAST_COLOR.table_name:
                return PimProductEmastColorEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.BRIDGE_PIM_SKU_PRODUCT.table_name:
                return BridgePimSkuProductEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.ORDER_SKU_SHIPMENT.table_name:
                return OrderSkuShipmentEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.ORDER_TENDER.table_name:
                return OrderTenderEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.ORDER_SKU_INIT_ALLOC.table_name:
                return OrderSkuInitAllocEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.PIM_SKU.table_name:
                return PimSkuEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case EgressTableNames.MANUAL_DISCOUNT_LKUP.table_name:
                return ManualDiscountLkupEgressOperation(
                    self.spark,
                    self.bigquery_credentials,
                    self.databricks_source,
                    self.bigquery_destination,
                    self.watermark,
                )
            case _:
                raise NotSupportedError(
                    f"The specified databricks table {self.databricks_source.table} is not supported."
                )
