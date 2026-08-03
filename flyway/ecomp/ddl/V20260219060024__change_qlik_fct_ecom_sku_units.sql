CREATE OR REPLACE VIEW ${ecom_apps_schema}.qlik_fct_ecom_sku_units
(
  PRODUCT_ID,
  ECOM_SKU_STORE_ONHAND_QTY,
  ECOM_SKU_BACKSTOCK_QTY,
  ECOM_SKU_DKS_DC_OH_QTY,
  ECOM_SKU_ECOM_DC_OH_QTY,
  ECOM_SKU_SFS_OH_QTY,
  ECOM_SKU_MIA_QTY,
  ECOM_SKU_VDC_OH_QTY,
  ECOM_SKU_STORE_OO,
  ECOM_SKU_DC_OO,
  ECOM_SKU_ECOM_OO,
  ECOM_SKU_LAST_MIN_PO_ERD,
  ECOM_SKU_LAST_RECEIPT_DATE,
  ECOM_SKU_WEB_ATP_QTY,
  ECOM_SKU_SFS_STORE_CNT,
  ECOM_SKU_SFS_ATP_QTY,
  ECOM_SKU_DKS_DC_ATP_QTY,
  ECOM_SKU_BOPIS_STORE_CNT,
  ECOM_SKU_BOPIS_ATP_QTY,
  ECOM_SKU_BACKSTOCK_DC_CNT,
  ECOM_SKU_BACKSTOCK_ATP_QTY,
  ECOM_SKU_RADIAL_ATP_QTY,
  ECOM_SKU_VDC_ATP_QTY,
  ECOM_SKU_ATP_DATE_LAST_MOD,
  ECOM_SKU_PRESALE_ATP_QTY,
  ECOM_SKU_RDC_OH_QTY,
  ECOM_SKU_MFC_ATP_QTY,
  ECOM_SKU_MFC_OH_QTY,
  ECOM_SKU_ATP_BOPS_DATE_MOD,
  ECOM_SKU_VDC_ALLOC_QTY,
  ECOM_SKU_MFC_ALLOC_QTY,
  ECOM_SKU_RDC_ALLOC_QTY,
  ECOM_SKU_STORE_ALLOC_QTY,
  ECOM_SKU_STORE_MIA_QTY,
  ECOM_SKU_VDC_UNAVAIL_QTY,
  ECOM_SKU_MFC_UNAVAIL_QTY,
  ECOM_SKU_RDC_UNAVAIL_QTY,
  ECOM_SKU_SFS_UNAVAIL_QTY,
  ECOM_SKU_BOPIS_UNAVAIL_QTY,
  ECOM_SKU_COMP_DC_OH_QTY,
  ECOM_SKU_COMP_DC_IT_QTY,
  ECOM_SKU_SFS_SS_QTY,
  ECOM_SKU_BOPIS_SS_QTY,
  ECOM_SKU_STHMINUSVDC_QTY,
  ECOM_SKU_PRESALE_PROJ_STH_QTY,
  ECOM_SKU_RDC_EXCL_QTY,
  ECOM_SKU_VDC_WATERMARK_QTY,
  ECOM_SKU_NONMFC_QTY,
  ECOM_SKU_BOPL_QTY
) AS
SELECT
  dks_Sku_key AS product_id,
  NVL(STORE_ONHAND_QTY, 0) AS DKS_SKU_STORE_ONHAND_QTY,
  NVL(NONPACK_BACKSTOCK_OH_QTY, 0) AS DKS_SKU_BACKSTOCK_QTY,
  NVL(ECOM_DC_OH_QTY, 0) - NVL(RADIAL_DC_OH_QTY, 0) AS DKS_DC_OH_QTY,
  NVL(RADIAL_DC_OH_QTY, 0) AS ECOM_SKU_ECOM_DC_OH_QTY,
  NVL(SFS_OH_QTY, 0) AS ECOM_SKU_SFS_OH_QTY,
  NVL(MIA_QTY, 0) AS ECOM_SKU_MIA_QTY,
  NVL(VDC_OH_QTY, 0) AS ECOM_SKU_VDC_OH_QTY,
  NVL(STORE_OO, 0) AS ECOM_SKU_STORE_OO,
  NVL(DC_OO, 0) AS ECOM_SKU_DC_OO,
  NVL(ECOM_OO, 0) AS ECOM_SKU_ECOM_OO,
  NVL(LAST_MIN_PO_ERD, -1) AS ECOM_SKU_LAST_MIN_PO_ERD,
  NVL(LAST_RECEIPT_DATE, -1) AS ECOM_SKU_LAST_RECEIPT_DATE,
  NVL(WEB_ATP_QTY, 0) AS ECOM_SKU_WEB_ATP_QTY,
  NVL(SFS_STORE_CNT, 0) AS ECOM_SKU_SFS_STORE_CNT,
  NVL(SFS_ATP_QTY, 0) AS ECOM_SKU_SFS_ATP_QTY,
  NVL(DC_ATP_QTY, 0) - NVL(RADIAL_ATP_QTY, 0) AS DKS_DC_ATP_QTY,
  NVL(BOPIS_STORE_CNT, 0) AS ECOM_SKU_BOPIS_STORE_CNT,
  NVL(BOPIS_ATP_QTY, 0) AS ECOM_SKU_BOPIS_ATP_QTY,
  NVL(BACKSTOCK_DC_CNT, 0) AS BACKSTOCK_DC_CNT,
  NVL(BACKSTOCK_ATP_QTY, 0) AS BACKSTOCK_ATP_QTY,
  NVL(RADIAL_ATP_QTY, 0) AS ECOM_SKU_RADIAL_ATP_QTY,
  NVL(VDC_ATP_QTY, 0) AS ECOM_SKU_VDC_ATP_QTY,
  ATP_DATE_LAST_MODIFIED,
  NVL(presale_ATP_QTY, 0) AS ECOM_SKU_PRESALE_ATP_QTY,
  NVL(backstock_oh_qty, 0) AS ECOM_SKU_RDC_OH_QTY,
  NVL(DC_ATP_QTY, 0) AS ECOM_SKU_MFC_ATP_QTY,
  NVL(ECOM_DC_OH_QTY, 0) AS ECOM_SKU_MFC_OH_QTY,
  bopis_atp_date_last_modified,
  NVL(vdc_alloc_qty, 0) AS ECOM_SKU_VDC_ALLOC_QTY,
  NVL(dc_alloc_qty, 0) AS ECOM_SKU_MFC_ALLOC_QTY,
  NVL(backstock_alloc_qty, 0) AS ECOM_SKU_RDC_ALLOC_QTY,
  NVL(store_alloc_qty, 0) AS ECOM_SKU_STORE_ALLOC_QTY,
  NVL(store_mia_qty, 0) AS ECOM_SKU_STORE_MIA_QTY,
  NVL(vdc_unavail_qty, 0) AS ECOM_SKU_VDC_UNAVAIL_QTY,
  NVL(dc_unavail_qty, 0) AS ECOM_SKU_MFC_UNAVAIL_QTY,
  NVL(backstock_unavail_qty, 0) AS ECOM_SKU_RDC_UNAVAIL_QTY,
  NVL(sfs_unavail_qty, 0) AS ECOM_SKU_SFS_UNAVAIL_QTY,
  NVL(bopis_unavail_qty, 0) AS ECOM_SKU_BOPIS_UNAVAIL_QTY,
  NVL(dc_oh_qty, 0) AS ECOM_SKU_RDC_OH_QTY,
  NVL(in_transit_qty, 0) AS ECOM_SKU_RDC_EXCL_QTY,
  NVL(sfs_ss_qty, 0) AS ECOM_SKU_SFS_SS_QTY,
  NVL(bopis_ss_qty, 0) AS ECOM_SKU_BOPIS_SS_QTY,
  (
    NVL(DC_ATP_QTY, 0) + NVL(BACKSTOCK_ATP_QTY, 0) + NVL(SFS_ATP_QTY, 0)
  ) AS ECOM_SKU_ATP_DATE_LAST_MOD,
  NVL(presale_proj_sth_qty, 0) AS ECOM_SKU_PRESALE_PROJ_STH_QTY,
  NVL(rdc_excl_qty, 0) AS ECOM_SKU_RDC_EXCL_QTY,
  NVL(vendor_ss_qty, 0) AS ECOM_SKU_VDC_WATERMARK_QTY,
  NVL(STORE_ONHAND_QTY, 0) + NVL(BACKSTOCK_QTY, 0) + NVL(IN_TRANSIT_QTY, 0) + NVL(DC_OH_QTY, 0)
  + NVL(STORE_OO, 0)
  + NVL(DC_OO, 0)
  + NVL(ECOM_OO, 0) AS non_mfc_qty,
  NVL(bopl_atp_qty, 0) AS ECOM_SKU_BOPL_QTY
FROM
  prod_ecmde_db.ecom_dim.dks_Sku_units AS un
    LEFT JOIN (
      SELECT
        prod_ecmde_db.ecom_dim.GET_NUMBER_FROM_STRING(sku) AS sku,
        SUM(
          CASE
            WHEN fulfillment_location_type = 'Vendor' THEN allocated_quantity
            ELSE 0
          END
        ) AS vdc_alloc_qty,
        SUM(
          CASE
            WHEN
              fulfillment_location_type = 'Distribution Center'
              AND NVL(fulfillment_loc_code, 'x') <> 'Backstock'
            THEN
              allocated_quantity
            ELSE 0
          END
        ) AS dc_alloc_qty,
        SUM(
          CASE
            WHEN
              fulfillment_location_type = 'Distribution Center'
              AND fulfillment_loc_code = 'Backstock'
            THEN
              allocated_quantity
            ELSE 0
          END
        ) AS backstock_alloc_qty,
        SUM(
          CASE
            WHEN fulfillment_location_type = 'Customer Store' THEN allocated_quantity
            ELSE 0
          END
        ) AS store_alloc_qty,
        SUM(
          CASE
            WHEN fulfillment_location_type = 'Customer Store' THEN mia_qty
            ELSE 0
          END
        ) AS store_mia_qty,
        SUM(
          CASE
            WHEN fulfillment_location_type = 'Vendor' THEN NVL(unavailable_qty, 0)
            ELSE 0
          END
        ) AS vdc_unavail_qty,
        SUM(
          CASE
            WHEN
              fulfillment_location_type = 'Distribution Center'
              AND NVL(fulfillment_loc_code, 'x') <> 'Backstock'
            THEN
              NVL(unavailable_qty, 0) + NVL(sfs_ss_qty, 0)
              + CASE
                WHEN
                  is_excluded = 1
                  OR fulfillment_eligible = 'F'
                THEN
                  NVL(calc_sth_atp, 0)
                ELSE 0
              END
            ELSE 0
          END
        ) AS dc_unavail_qty,
        SUM(
          CASE
            WHEN
              fulfillment_location_type = 'Distribution Center'
              AND fulfillment_loc_code = 'Backstock'
            THEN
              NVL(unavailable_qty, 0) + NVL(sfs_ss_qty, 0)
            ELSE 0
          END
        ) AS backstock_unavail_qty,
        SUM(
          CASE
            WHEN
              fulfillment_location_type = 'Customer Store'
            THEN
              NVL(unavailable_qty, 0)
              + CASE
                WHEN
                  is_excluded = 1
                  OR fulfillment_eligible = 'F'
                  OR NOT sth_watermark IS NULL
                THEN
                  NVL(calc_sth_atp, 0)
                  + CASE
                    WHEN NOT sth_watermark IS NULL THEN NVL(available_qty, 0)
                    ELSE 0
                  END
                ELSE 0
              END
            ELSE 0
          END
        ) AS sfs_unavail_qty,
        SUM(
          CASE
            WHEN
              fulfillment_location_type = 'Customer Store'
            THEN
              NVL(unavailable_qty, 0)
              + CASE
                WHEN NOT bopis_watermark IS NULL THEN NVL(available_qty, 0)
                ELSE 0
              END
            ELSE 0
          END
        ) AS bopis_unavail_qty,
        SUM(
          CASE
            WHEN fulfillment_location_type = 'Customer Store' THEN bopis_ss_qty
            ELSE 0
          END
        ) AS bopis_ss_qty,
        SUM(
          CASE
            WHEN fulfillment_location_type = 'Customer Store' THEN sfs_ss_qty
            ELSE 0
          END
        ) AS sfs_ss_qty,
        SUM(
          CASE
            WHEN
              fulfillment_location_type = 'Distribution Center'
              AND fulfillment_loc_code = 'Backstock'
              AND (
                is_excluded = 1
                OR fulfillment_eligible = 'F'
              )
            THEN
              NVL(calc_sth_atp, 0)
            ELSE 0
          END
        ) AS rdc_excl_qty,
        SUM(
          CASE
            WHEN
              fulfillment_location_type = 'Vendor'
            THEN
              CASE
                WHEN
                  sfs_ss_qty > NVL(available_qty, 0)
                  AND available_qty > 0
                THEN
                  NVL(available_qty, 0)
                WHEN NVL(available_qty, 0) > sfs_ss_qty THEN NVL(sfs_ss_qty, 0)
                ELSE 0
              END
            ELSE 0
          END
        ) AS vendor_ss_qty
      FROM
        prod_ecmde_db.ECOM.STG_eom_INVENTORY
      GROUP BY
        prod_ecmde_db.ecom_dim.GET_NUMBER_FROM_STRING(sku)
    ) AS alloc
      ON UN.DKS_SKU_CODE = alloc.sku
