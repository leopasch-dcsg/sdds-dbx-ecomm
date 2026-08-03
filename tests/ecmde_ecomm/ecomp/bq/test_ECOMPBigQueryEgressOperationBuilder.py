import pytest

from src.ecmde_ecomm.ecomp.bq import (
    OrderSkuShipmentEgressOperation,
    OrderSkuInitAllocEgressOperation,
)
from tests.ecmde_ecomm.fixtures import spark

from ecmde_ecomm.common import NotSupportedError
from ecmde_ecomm.common.bq import (
    BigQueryCredentials,
    DatabricksSource,
    BigQueryDestination,
)

from ecmde_ecomm.common.dbx.etl import Watermark

from ecmde_ecomm.ecomp.bq import (
    ECOMPBigQueryEgressOperationBuilder,
    EgressTableNames,
    AttributeCodeEgressOperation,
    PimProductEmastEgressOperation,
    VwStyleEgressOperation,
    MDMAttributeCodeEgressOperation,
    DksSkuShipEgressOperation,
    NrtAtpBopisEgressOperation,
    FlashSaleStylesDlyEgressOperation,
    TmpStoreProductivityBopisEgressOperation,
    TmpStoreProductivitySfsEgressOperation,
    TmpStoreProductivityHrsVwEgressOperation,
    VwStgMdmMasterCatAttrMuEgressOperation,
    WebSkuHeaderEgressOperation,
    PxDescriptionEgressOperation,
    VwStgDksMasterCatAttrMuEgressOperation,
    PxPromoauthEgressOperation,
    PimProductEmastTitleOnlyEgressOperation,
    ClrColorLkupEgressOperation,
    PromotionEventEgressOperation,
    PromotionHierarchyMainEgressOperation,
    PromotionHierarchySubEgressOperation,
    OrderHeaderEgressOperation,
    OrderSkuEgressOperation,
    OrderSkuFlashEventEgressOperation,
    OrderDeliveryEgressOperation,
    SnpInventoryEgressOperation,
    OrderFulfillEgressOperation,
    PromotionHeaderEgressOperation,
    SnpWebProductAssortmentEgressOperation,
    TxnOrderSkuAdjustmentEgressOperation,
    TxnOrderSkuEgressOperation,
    TxnWebSkuOfferPriceEgressOperation,
    StgMdmMasterCatalogAttrEgressOperation,
    PimProductDescVwEgressOperation,
    WebProductEgressOperation,
    WebSkuAttrEgressOperation,
    DksSkuPimEgressOperation,
    PimProductEgressOperation,
    PimProductEmastColorEgressOperation,
    BridgePimSkuProductEgressOperation,
    OrderSkuShipmentEgressOperation,
    OrderTenderEgressOperation,
    OrderSkuInitAllocEgressOperation,
    PimSkuEgressOperation,
    ManualDiscountLkupEgressOperation,
)


@pytest.fixture
def big_query_credentials() -> BigQueryCredentials:
    return BigQueryCredentials("{'json_toke': 'some token'")


def databricks_source(table: str) -> DatabricksSource:
    return DatabricksSource("dev_ecmde_db", "ecom_dim", table)


@pytest.fixture
def bigquery_destination() -> BigQueryDestination:
    return BigQueryDestination(
        "dbx-gcp-ecom-ddw-bucket",
        "gcp-dks-ecmde-sbox",
        "gn-ddw-project01",
        "ecm_incoming",
        "some_table_name",
    )


@pytest.mark.unit
class TestECOMPBigQueryEgressOperationBuilder:
    def test_builder(self, spark):
        builder = ECOMPBigQueryEgressOperationBuilder.builder(spark)
        assert builder is not None
        assert builder.spark == spark

    @pytest.mark.parametrize(
        "source_table",
        [member for member in EgressTableNames],
    )
    def test_operation(
        self,
        source_table: EgressTableNames,
        spark,
        big_query_credentials,
        bigquery_destination,
    ):
        operation = (
            ECOMPBigQueryEgressOperationBuilder.builder(spark)
            .with_databricks_source(databricks_source(source_table.table_name))
            .with_bigquery_destination(bigquery_destination)
            .with_bigquery_credentials(big_query_credentials)
            .with_watermark(
                Watermark(
                    "watermark_table_name",
                    "catalog_name",
                    "schema_name",
                    "table_name",
                    spark,
                )
            )
            .operation()
        )

        match source_table:
            case EgressTableNames.ATTRIBUTE_CODE:
                assert isinstance(operation, AttributeCodeEgressOperation)
            case EgressTableNames.PIM_PRODUCT_EMAST:
                assert isinstance(operation, PimProductEmastEgressOperation)
            case EgressTableNames.VW_STYLE:
                assert isinstance(operation, VwStyleEgressOperation)
            case EgressTableNames.MDM_ATTRIBUTE_CODE:
                assert isinstance(operation, MDMAttributeCodeEgressOperation)
            case EgressTableNames.DKS_SKU_SHIP:
                assert isinstance(operation, DksSkuShipEgressOperation)
            case EgressTableNames.NRT_ATP_BOPIS_VW:
                assert isinstance(operation, NrtAtpBopisEgressOperation)
            case EgressTableNames.FLASH_SALE_STYLES_DLY_VW:
                assert isinstance(operation, FlashSaleStylesDlyEgressOperation)
            case EgressTableNames.TMP_STORE_PRODUCTIVITY_BOPIS:
                assert isinstance(operation, TmpStoreProductivityBopisEgressOperation)
            case EgressTableNames.TMP_STORE_PRODUCTIVITY_SFS:
                assert isinstance(operation, TmpStoreProductivitySfsEgressOperation)
            case EgressTableNames.TMP_STORE_PRODUCTIVITY_HRS_VW:
                assert isinstance(operation, TmpStoreProductivityHrsVwEgressOperation)
            case EgressTableNames.VW_STG_MDM_MASTER_CAT_ATTR_MU:
                assert isinstance(operation, VwStgMdmMasterCatAttrMuEgressOperation)
            case EgressTableNames.WEB_SKU_HEADER:
                assert isinstance(operation, WebSkuHeaderEgressOperation)
            case EgressTableNames.PX_DESCRIPTION:
                assert isinstance(operation, PxDescriptionEgressOperation)
            case EgressTableNames.VW_STG_DKS_MASTER_CAT_ATTR_MU:
                assert isinstance(operation, VwStgDksMasterCatAttrMuEgressOperation)
            case EgressTableNames.PX_PROMOAUTH:
                assert isinstance(operation, PxPromoauthEgressOperation)
            case EgressTableNames.VW_PIM_PRODUCT_EMAST_TITLE_ONLY:
                assert isinstance(operation, PimProductEmastTitleOnlyEgressOperation)
            case EgressTableNames.CLR_COLOR_LKUP:
                assert isinstance(operation, ClrColorLkupEgressOperation)
            case EgressTableNames.PROMOTION_EVENT:
                assert isinstance(operation, PromotionEventEgressOperation)
            case EgressTableNames.PROMOTION_HIERARCHY_MAIN:
                assert isinstance(operation, PromotionHierarchyMainEgressOperation)
            case EgressTableNames.PROMOTION_HIERARCHY_SUB:
                assert isinstance(operation, PromotionHierarchySubEgressOperation)
            case EgressTableNames.ORDER_HEADER:
                assert isinstance(operation, OrderHeaderEgressOperation)
            case EgressTableNames.ORDER_SKU:
                assert isinstance(operation, OrderSkuEgressOperation)
            case EgressTableNames.ORDER_SKU_FLASH_EVENT:
                assert isinstance(operation, OrderSkuFlashEventEgressOperation)
            case EgressTableNames.SNP_INVENTORY:
                assert isinstance(operation, SnpInventoryEgressOperation)
            case EgressTableNames.ORDER_FULFILL:
                assert isinstance(operation, OrderFulfillEgressOperation)
            case EgressTableNames.ORDER_DELIVERY:
                assert isinstance(operation, OrderDeliveryEgressOperation)
            case EgressTableNames.PROMOTION_HEADER:
                assert isinstance(operation, PromotionHeaderEgressOperation)
            case EgressTableNames.SNP_WEB_PRODUCT_ASSORTMENT:
                assert isinstance(operation, SnpWebProductAssortmentEgressOperation)
            case EgressTableNames.TXN_ORDER_SKU_ADJUSTMENT:
                assert isinstance(operation, TxnOrderSkuAdjustmentEgressOperation)
            case EgressTableNames.TXN_ORDER_SKU:
                assert isinstance(operation, TxnOrderSkuEgressOperation)
            case EgressTableNames.TXN_WEB_SKU_OFFER_PRICE:
                assert isinstance(operation, TxnWebSkuOfferPriceEgressOperation)
            case EgressTableNames.STG_MDM_MASTER_CATALOG_ATTR:
                assert isinstance(operation, StgMdmMasterCatalogAttrEgressOperation)
            case EgressTableNames.PIM_PRODUCT_DESC_VW:
                assert isinstance(operation, PimProductDescVwEgressOperation)
            case EgressTableNames.WEB_PRODUCT:
                assert isinstance(operation, WebProductEgressOperation)
            case EgressTableNames.WEB_SKU_ATTRIBUTE:
                assert isinstance(operation, WebSkuAttrEgressOperation)
            case EgressTableNames.DKS_SKU_PIM:
                assert isinstance(operation, DksSkuPimEgressOperation)
            case EgressTableNames.PIM_PRODUCT:
                assert isinstance(operation, PimProductEgressOperation)
            case EgressTableNames.PIM_PRODUCT_EMAST_COLOR:
                assert isinstance(operation, PimProductEmastColorEgressOperation)
            case EgressTableNames.BRIDGE_PIM_SKU_PRODUCT:
                assert isinstance(operation, BridgePimSkuProductEgressOperation)
            case EgressTableNames.ORDER_SKU_SHIPMENT:
                assert isinstance(operation, OrderSkuShipmentEgressOperation)
            case EgressTableNames.ORDER_TENDER:
                assert isinstance(operation, OrderTenderEgressOperation)
            case EgressTableNames.ORDER_SKU_INIT_ALLOC:
                assert isinstance(operation, OrderSkuInitAllocEgressOperation)
            case EgressTableNames.PIM_SKU:
                assert isinstance(operation, PimSkuEgressOperation)
            case EgressTableNames.MANUAL_DISCOUNT_LKUP:
                assert isinstance(operation, ManualDiscountLkupEgressOperation)
            case _:
                raise NotSupportedError(
                    "Test failed and did not return the expected operation instance."
                )

    def test_operation_with_unsupported_source_table(
        self, spark, big_query_credentials, bigquery_destination
    ):
        with pytest.raises(NotSupportedError):
            (
                ECOMPBigQueryEgressOperationBuilder.builder(spark)
                .with_bigquery_destination(bigquery_destination)
                .with_bigquery_credentials(big_query_credentials)
                .with_databricks_source(databricks_source("table_not_supported"))
                .with_watermark(
                    Watermark(
                        "watermark_table_name",
                        "catalog_name",
                        "schema_name",
                        "table_name",
                        spark,
                    )
                )
                .operation()
            )
