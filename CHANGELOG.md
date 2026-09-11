# eComm Release v3.10.0 (FY26 Q3)
The v3.10.0 release productionizes the SDDS index comparison job and reworks the bundle to build under the `sdds` package name.

## Enhancements and Features
* SDDS Index Comparison job: productionize `sdds_index_comparison` job (bronze → silver → gold pipeline for catalog load, catalog stream, inventory, and gold compare/report tasks). Job runs daily at 04:00:21 America/New_York on the prod workspace.
* Environment-driven catalog/schema: notebooks now read `sdds_catalog`, `sdds_bronze_schema`, `sdds_silver_schema`, and `sdds_gold_schema` from job `base_parameters` via `NotebookUtil.notebook_param(...)`. Values resolve per deploy target (`dev_sdsc_db` in development, `prod_sdsc_db` in production).
* `NotebookUtil` helper (`sdds.common.util`): mirrors the mature `ecmde_ecomm` pattern with safe widget reads, defaults, and `text_widget` / `spark_param` helpers.
* Wheel repackaged under `sdds` name (was `ecmde_ecomm`); notebooks and job tasks reference `sdds.common.util`.

## Fixes
* Job tasks previously pointing at dev cluster IDs now target the prod clusters (`0812-152951-77vhlhwi` for catalog-load tasks, `0811-215511-4bazq276` for catalog-stream tasks).
* `flat_blended_comparison_gold` uses the serverless `catalog_load_silver_environment`; its wheel is installed via the environment `dependencies` block instead of task-level `libraries`.
* `pyproject.toml` cleaned up — dropped unused `ecmde_ecomm`-era dependencies (`confluent-kafka`, `elasticsearch`, `PyMySQL`, `mysql-connector-python`, `google-cloud-bigquery`) and stale `[project.scripts]` entries.

# eComm Release v3.9.3 (FY26 Q2 - Sprint 5)
The v3.9.3 contains an update to the txn_order_sku watermark timestamp.

## Fixes
* [RADEF-3060](https://dcsgcloud.atlassian.net/browse/RADEF-3060): Allocation fix at eaches level.

# eComm Release v3.9.2 (FY26 Q2 - Sprint 2)
The v3.9.2 contains an update to the txn_order_sku watermark timestamp.

## Enhancements and Features
* [RADEF-2938](https://dcsgcloud.atlassian.net/browse/RADEF-2938): Adding a recovery window for txn_order_sku watermark timestamp.

# eComm Release v3.9.1 (FY26 Q2 - Sprint 2)
The v3.9.1 contains an update to the txn_order_sku data quality check.

## Enhancements and Features
* [RADEF-2930](https://dcsgcloud.atlassian.net/browse/RADEF-2930): txn_order_sku data quality check

# eComm Release v3.9.0 (FY26 Q2 - Sprint 2)
The v3.9.0 contains changes to the order service line pipeline, LTR pipeline & new data quality checks for fulfillment.

## Enhancements and Features
* [RADEF-2930](https://dcsgcloud.atlassian.net/browse/RADEF-2930): txn_order_sku data quality check
* [RADEF-2733](https://dcsgcloud.atlassian.net/browse/RADEF-2733): LTR Iteration v3 - Positive Profits
* [RADEF-2850](https://dcsgcloud.atlassian.net/browse/RADEF-2850): Order line service table enhancements for reporting

# eComm Release v3.8.11 (FY26 Q2 - Sprint 0)
The v3.8.11 contains updates to the demand fulfill report to pull from Databricks source and fulfillment changes to include DC6 records. 

## Enhancements and Features
* [RADEF-2890](https://dcsgcloud.atlassian.net/browse/RADEF-2890): Demand decomp source changes
* [RADEF-2903](https://dcsgcloud.atlassian.net/browse/RADEF-2903): DC6 addition in fulfillment

# eComm Release v3.8.10 (FY26 Q1 - Sprint 6)
The v3.8.10 release contains a fix to the LineUp VDC processes. 

## Fixes
* [RADEF-2107](https://dcsgcloud.atlassian.net/browse/RADEF-2107): VDC fixes

# eComm Release v3.8.9 (FY26 Q1 - Sprint 6)
The v3.8.9 release contains a fix to prevent overwrite of egress data if fulfillment dq check fails. 

## Fixes
* [RADEF-2861](https://dcsgcloud.atlassian.net/browse/RADEF-2861): Data Quality check fix

# eComm Release v3.8.8 (FY26 Q1 - Sprint 5)
The v3.8.8 contains updates to the LTR aggregation logic and new DQ check tasks for fulfillment egress tables.

## Enhancements and Features
* [RADEF-2853](https://dcsgcloud.atlassian.net/browse/RADEF-2853): Update  feature aggregation logic for LTR
* [RADEF-2842](https://dcsgcloud.atlassian.net/browse/RADEF-2842): DQ Check for fulfillment egress

# eComm Release v3.8.7 (FY26 Q1 - Sprint 5)
The v3.8.7 contains enhancements to the LTR workflows by filtering out search term ecode combos without a signal in the ABB feature tables.

## Enhancements and Features
* [RADEF-2732](https://dcsgcloud.atlassian.net/browse/RADEF-2732): ABB feature table updates for LTR

# eComm Release v3.8.6 (FY26 Q1 - Sprint 3)
The v3.8.6 contains changes needed for the Scorecard+ program and fixes to the order_tender egress process.

## Enhancements and Features
* [RADEF-2504](https://dcsgcloud.atlassian.net/browse/RADEF-2504): Order_service_line table updates for Scorecard+
* [RADEF-2767](https://dcsgcloud.atlassian.net/browse/RADEF-2767): Split the fulfillment egress workflow into two separate workflows.

## Fixes
* [RADEF-2810](https://dcsgcloud.atlassian.net/browse/RADEF-2810): Update chain_key filter in Order tender egress process


# eComm Release v3.8.5 (FY26 Q1 - Sprint 3)
The v3.8.5 contains changes to the fulfillment order delivery data pull process from OSO.

## Fixes
* [RADEF-2809](https://dcsgcloud.atlassian.net/browse/RADEF-2809): Order delivery load failure


# eComm Release v3.8.4 (FY26 Q1 - Sprint 3)
The v3.8.4 contains a changes to the VDC upload process for LineUp and updatss to the Sonar Host name.

## Enhancements and Features
* [RADEF-2730](https://dcsgcloud.atlassian.net/browse/RADEF-2730): Update VDC loads for LineUp
* [RADEF-2471](https://dcsgcloud.atlassian.net/browse/RADEF-2471): Update Sonar host name

# eComm Release v3.8.3 (FY26 Q1 - Sprint 2)
The v3.8.3 contains a change in Save the Sale process to add offer accepted timeline to txn_order_sku.

## Enhancements and Features
* [RADEF-2737](https://dcsgcloud.atlassian.net/browse/RADEF-2737): Add offer accepted timeline to txn_order_sku

# eComm Release v3.8.2 (FY26 Q1 - Sprint 2)
The v3.8.2 contains a fix for carrier mapping in order_delivery and oso order line allocations view

## Fixes
* [RADEF-2737](https://dcsgcloud.atlassian.net/browse/RADEF-2737): Stores reporting fix
* [RADEF-2650](https://dcsgcloud.atlassian.net/browse/RADEF-2728): SEFL to Generic Carrier Mapping

# eComm Release v3.8.1 (FY26 Q1 - Sprint 1)
The v3.8.1 contains a fix for carrier mapping in fulfillment and lineup ownership changes

## Fixes
* [RADEF-2730](https://dcsgcloud.atlassian.net/browse/RADEF-2730): Lineup ownership changes
* [RADEF-2650](https://dcsgcloud.atlassian.net/browse/RADEF-2650): Carrier mapping to fulfillment mode

# eComm Release v3.7.8 (FY26 Q1 - Sprint 1)
The v3.7.8 contains a fix for ECOM_APPS view definitions to revert timestamp to UTC.

## Fixes
* [RADEF-2689](https://dcsgcloud.atlassian.net/browse/RADEF-2689): Revert changes to UTC

# eComm Release v3.7.7 (FY26 Q1 - Sprint 1)
The v3.7.7 contains a fix for ECOM_APPS view definitions to report timestamp in ET and changes to mapping of fulfillment_mode

## Fixes
* [RADEF-2650](https://dcsgcloud.atlassian.net/browse/RADEF-2650): Carrier mapping to fulfillment mode
* [RADEF-2689](https://dcsgcloud.atlassian.net/browse/RADEF-2689): Update flyway for DTTM for DBX format to match ECOMP format

# eComm Release v3.7.6 (FY26 Q1 - Sprint 1)
The v3.7.6 contains a fix for our LTR workflows to filter out search results items that have a null e-code value.

## Fixes
* Filter our ML Events search result items with null e-code value.

# eComm Release v3.7.5 (FY26 Q1 - Sprint 1)
The v3.7.5 release includes creating history tables for LTR feature tables

## Enhancements and Features
* [RADEF-2545](https://dcsgcloud.atlassian.net/browse/RADEF-2545): Creates clone process to build history tables for LTR feature tables

# eComm Release v3.7.4 (FY26 Q1 - Sprint 1)
The v3.7.4 release makes a single adjustment to ensure that the DBX to BQ egress will output to the correct dataset by
environment.

# eComm Release v3.7.3 (FY26 Q1 - Sprint 1)
The v3.7.3 release includes LineUp changes for ECOMP Lift/Shift

## Enhancements and Features
* [RADEF-2528](https://dcsgcloud.atlassian.net/browse/RADEF-2528): Parameter changes and CWL Loads for LineUp

# eComm Release v3.7.2 (FY26 Q1 - Sprint 0)
The v3.7.2 release includes LineUp changes for ECOMP Lift/Shift and changes in Demand vs Fulfill report.

## Enhancements and Features
* [RADEF-2528](https://dcsgcloud.atlassian.net/browse/RADEF-2528): Parameter changes for LineUp
* [RADEF-2601](https://dcsgcloud.atlassian.net/browse/RADEF-2629): Demand vs Fulfill report update for ECOMP L/S

# eComm Release v3.7.1 (FY26 Q1 - Sprint 0)
The v3.7.1 release includes LineUp changes for ECOMP Lift/Shift, fix for Save for the Sale, QLik view change.

## Fixes
* Save the sale fix by removing filter on PO_Number in PySpark.

## Enhancements and Features
* [RADEF-2629](https://dcsgcloud.atlassian.net/browse/RADEF-2629): Qlik View updates
* [RADEF-2528](https://dcsgcloud.atlassian.net/browse/RADEF-2528): Parameter changes for LineUp


# eComm Release v3.7.0 (FY26 Q1 - Sprint 0)
The v3.7.0 release includes LineUp changes for ECOMP Lift/Shift and a bug fix for FMod.

## Fixes
* This was to remove Whitespaces in the order_delivery_key

## Enhancements and Features
* [RADEF-2528](https://dcsgcloud.atlassian.net/browse/RADEF-2528): ECOMP Lift/Shift LineUp changes

# eComm Release v3.6.4 (FY25 Q4 - Sprint 6)
The v3.6.2 release includes Save the Sale changes for FMod orders and FMod prod fix.

## Fixes
* [RADEF-2613](https://dcsgcloud.atlassian.net/browse/RADEF-2607): Databricks View definition fixes

# eComm Release v3.6.3 (FY25 Q4 - Sprint 6)
The v3.6.3 release includes Save the Sale changes for FMod orders and FMod prod fix.

## Fixes
* [RADEF-2607](https://dcsgcloud.atlassian.net/browse/RADEF-2607): Save the Sale flag view change
* Prod_Fix: Add new ship method in mapping

# eComm Release v3.6.2 (FY25 Q4 - Sprint 6)
The v3.6.2 release includes Save the Sale changes for FMod orders and lineup data refresh timing.

## Fixes
* [RADEF-2544](https://dcsgcloud.atlassian.net/browse/RADEF-2544): Lineup data refresh timing
* [RADEF-2589](https://dcsgcloud.atlassian.net/browse/RADEF-2589): Add additional system values & update save the sale logic.

# eComm Release v3.6.1 (FY25 Q4 - Sprint 5)
The v3.6.1 release includes updating the feature table column names for LTR time decay.

## Enhancements and Features
* [RADEF-2545](https://dcsgcloud.atlassian.net/browse/RADEF-2545): LTR Time Decay


# eComm Release v3.6.0 (FY25 Q4 - Sprint 5)
The v3.6.0 release contains features that includes save the sale support for modern orders and warranty data iterations.

## Enhancements and Features
* [RADEF-2532](https://dcsgcloud.atlassian.net/browse/RADEF-2532): Save the Sale changes
* [RADEF-2537](https://dcsgcloud.atlassian.net/browse/RADEF-2537): Warranty data iterations

# eComm Release v3.5.8 (FY25 Q4 - Sprint 3)
The v3.5.8 release contains several bug fixes in support of ECOMP and the initial pass for time decay with LTR.

## Features
* [RADEF-2498](https://dcsgcloud.atlassian.net/browse/RADEF-2498): Add time decay to LTR jobs.

## Fixes
* [RADEF-2524](https://dcsgcloud.atlassian.net/browse/RADEF-2524): Correct date outbound for compatibility in BigQuery.
* Map invalid cancel reason from Athlete Order to supported value.

# eComm Release v3.5.7 (FY25 Q4 - Sprint 3)
The v3.5.7 release contains bug fixes for the rekeyer in support of ECOMP L/S.

## Fixes
* [RADEF-2514](https://dcsgcloud.atlassian.net/browse/RADEF-2514): Fix web_product flags being set to whitespace characters, should be NULL.
* [RADEF-2529](https://dcsgcloud.atlassian.net/browse/RADEF-2529): Add missing columns to pim_product_emast_color egress to Big Query.
* [RADEF-2524](https://dcsgcloud.atlassian.net/browse/RADEF-2524): Correct column data types and zone-less timestamps for dks_sku_pim and promotion_header legacy tables.

# eComm Release v3.5.6 (FY25 Q4 - Sprint 2)
The v3.5.6 release fixes the estimated ship date bug for FMod records.

## Fixes
* [RADEF-2500](https://dcsgcloud.atlassian.net/browse/RADEF-2500): Fix the timezone discrepancy issue with estimated_ship_date column.

# eComm Release v3.5.5 (FY25 Q4 - Sprint 1)
The v3.5.5 release refactors the DBX -> BQ egress logic to remove the join on the rekeyer mapping tables. Previously we
wanted to filter out records in DBX that had no Oracle equivalent yet, but with our updated plan for the cutover we no
longer need this added filter.

# Enhancements and Features
* [RADEF-2466](https://dcsgcloud.atlassian.net/browse/RADEF-2466): Remove joins into the rekeyer mapping tables for BQ egress.

# eComm Release v3.5.4 (FY25 Q4 - Sprint 1)
The v3.5.4 release contains bug fixes to the rekeyer in support of ECOMP L/S.

## Fixes
* Skip rekey of tables without mappings
* Handle null max ORA_KEY

# eComm Release v3.5.3 (FY25 Q4 - Sprint 1)
The v3.5.3 release move 3 LTR jobs to L16sv3 series instance pool.

## Performance Enhancements
[RADEF-2482](https://dcsgcloud.atlassian.net/browse/RADEF-2482): Move to new L16sv3 series vm for 3 of our LTR jobs.

# eComm Release v3.5.2 (FY25 Q4 - Sprint 1)
The v3.5.2 release contains ECOMP L/S enhancements to support egress to BQ and a roll back to non-pool node types.

## Performance Enhancements
[RADEF-2482](https://dcsgcloud.atlassian.net/browse/RADEF-2482): Roll back to Lsv3 series vm for 3 of our LTR jobs.

## Fixes
[RADEF-2466](https://dcsgcloud.atlassian.net/browse/RADEF-2466): Update join logic to correctly send matching Oracle records to BQ from DBX.

# eComm Release v3.5.1 (FY25 Q4 - Sprint 1)
The v3.5.1 release contains minor adjustments to the LTR workflows for job compute and instance pools, as well as a fix
for the rekeyer in support of ECOMP L/S.

## Performance Enhancements
[RADEF-2482](https://dcsgcloud.atlassian.net/browse/RADEF-2482): Increase job compute to E20ads_v5 series for 3 of our LTR jobs.

## Fixes
[RADEF-2466](https://dcsgcloud.atlassian.net/browse/RADEF-2466): Handle null checks on natural keys, and ensure mapped_key gets persisted on source records

# eComm Release v3.5.0 (FY25 Q4 - Sprint 1)
The v3.5.0 release contains the final changes for instance pools and the initial release of our integration work for
Vector Search in support of the Supercharged Search program.

## Enhancements and Features
[RADEF-2480](https://dcsgcloud.atlassian.net/browse/RADEF-2480): Initial implementation for Vector Search integrations

## Performance Enhancements
[RADEF-2482](https://dcsgcloud.atlassian.net/browse/RADEF-2482): Final changes for instance pools for holiday prep

# eComm Release v3.4.4 (FY25 Q4 - Sprint 1)
The v3.4.4 release contains changes to address Databricks to Big Query egress in support of ECOMP L/S, and the final
touches on Holiday Preparedness for FY25.

## Fixes
[RADEF-2467](https://dcsgcloud.atlassian.net/browse/RADEF-2470): Final enhancements for Databricks to BQ egress

## Performance Enhancements
[RADEF-2441](https://dcsgcloud.atlassian.net/browse/RADEF-2441): Swap all compute to instance pools due to Azure East constraints

# eComm Release v3.4.3 (FY25 Q4 - Sprint 0)
The v3.4.3 release contains the below fixes for Rekeyer logic & FMod Order fulfill.

## Fixes
* [RADEF-2471](https://dcsgcloud.atlassian.net/browse/RADEF-2471): Pull source/pickup facility type & executed fulfillment mode from history table to handle orders with mutiple fulfillment modes.
* [RADEF-2470](https://dcsgcloud.atlassian.net/browse/RADEF-2470): Modifying the rekeyer logic Egressing DBX to BQ.

# eComm Release v3.4.2 (FY25 Q3 - Sprint 6)
The v3.4.2 release contains the final changes for the Q3 quarterly commitments and holiday prep. 

## Enhancements and Features
* [RADEF-2441](https://dcsgcloud.atlassian.net/browse/RADEF-2441): Swapping out to DBX instance pools for holiday prep
* [RADEF-2440](https://dcsgcloud.atlassian.net/browse/RADEF-2440): Enabling CO Stage tables to be orchestrated from ctrl-m
* [RADEF-2414](https://dcsgcloud.atlassian.net/browse/RADEF-2414): Finalize egress to Kafka for Intelligent Filtering work
* [RADEF-2456](https://dcsgcloud.atlassian.net/browse/RADEF-2456): Enabling EAT Availability tables to be orchestrated from ctrl-m
* [RADEF-2425](https://dcsgcloud.atlassian.net/browse/RADEF-2425): New data models for reporting on orders with non-merch SKUs

## Fixes
* [RADEF-2451](https://dcsgcloud.atlassian.net/browse/RADEF-2451): Fix for FMOD: incorrect join was assigning unknown to the channel type key
* [RADEF-2459](https://dcsgcloud.atlassian.net/browse/RADEF-2459): Fix for FMOD: adjust FMOD logic to filter on both carrier code and service level when processing VFT orders

# eComm Release v3.4.1 (FY25 Q3 - Sprint 6)
The v3.4.1 release contains the code changes to send Intelligent Filtering tables to elasticearch and Kafka along with 
continued work on Ecomp L/S project.

## Enhancements and Features
* [RADEF-2414](https://dcsgcloud.atlassian.net/browse/RADEF-2414): Send Intelligent Filtering Agg tables to ElasticSearch
* [RADEF-2413](https://dcsgcloud.atlassian.net/browse/RADEF-2413): Send Intelligent Filtering Agg tables to Kafka probabilities
* [RADEF-2443](https://dcsgcloud.atlassian.net/browse/RADEF-2443): Call BQ stored procedures from DBX
* [RADEF-2443](https://dcsgcloud.atlassian.net/browse/RADEF-2443): Egress manual_discount_lkup to BQ

## Fixes
* Kafka Fix - Resolve NPE on tombstone operation

# eComm Release v3.4.0 (FY25 Q3 - Sprint 6)
The v3.4.0 release contains the initial support for Intelligent Filtering data model for Supercharged Search, Warranties 
reporting, support for Snowflake egress, and bug fixes for BOPL Demand in support of Fulfillment Modernization.

## Enhancements and Features
* [RADEF-2404](https://dcsgcloud.atlassian.net/browse/RADEF-2404): Intelligent Filtering search term to applied facets probabilities
* [RADEF-2405](https://dcsgcloud.atlassian.net/browse/RADEF-2405): Intelligent Filtering search term to applied facets probabilities
* [RADEF-2406](https://dcsgcloud.atlassian.net/browse/RADEF-2406): Intelligent Filtering search term to applied facets financials
* [RADEF-2298](https://dcsgcloud.atlassian.net/browse/RADEF-2298): Add support for Snowflake egress out of DBX for several ECOMP tables
* [RADEF-2402](https://dcsgcloud.atlassian.net/browse/RADEF-2402): Add warranties unit data to order_sku
* [RADEF-2403](https://dcsgcloud.atlassian.net/browse/RADEF-2403): Add warranties price data to order_sku

## Fixes
* [RADEF-526](https://dcsgcloud.atlassian.net/browse/RADEF-526): Support BOPL Demand reporting for fulfillment modernization
* [TSD-2209286](https://dcsgcloud.atlassian.net/browse/TSD-2209286): Swap out VMs for LTR clusters from Es_v5 to the Eads_v5 due to Microsoft stockout errors in Azure East

# eComm Release v3.3.3 (FY25 Q3 - Sprint 5)

The release v3.3.3 contains changes to the order_line_allocations view to handle the switch from system to 
routing_partner as a filter.
x
## Enhancements and Features
* [RADEF-2422](https://dcsgcloud.atlassian.net/browse/RADEF-2422): Update order_line_allocations view 

# eComm Release v3.3.2 (FY25 Q3 - Sprint 5)

The release v3.3.2 contains additional DBX to Big Query egress flows to support ECOMP Lift and Shift cutover to Databricks
with additional enhancements to lineup changes and support the new decline codes in fulfillment modernization.

## Enhancements and Features
* [RADEF-2257](https://dcsgcloud.atlassian.net/browse/RADEF-2257): dbx to mysql egress in support of LineUp and ECOMP L/S
* [RADEF_2352](https://dcsgcloud.atlassian.net/browse/RADEF-2352): Egress promotion_event from DBX to BQ update 
* [RADEF_2351](https://dcsgcloud.atlassian.net/browse/RADEF-2351): Egress order_sku_init_alloc from DBX to BQ update 
* [RADEF_2350](https://dcsgcloud.atlassian.net/browse/RADEF-2350): Egress mdm_attribute_code from DBX to BQ update 
* [RADEF-2300](https://dcsgcloud.atlassian.net/browse/RADEF-2300): new entdata.web.inv_web_prod_bod_price view
* [RADEF_2356](https://dcsgcloud.atlassian.net/browse/RADEF-2356): Egress pim_product_emast_color rekeyer from DBX to BQ update 
* [RADEF_2355](https://dcsgcloud.atlassian.net/browse/RADEF-2355): Egress attribute_code rekeyer from DBX to BQ update 
* [RADEF_2353](https://dcsgcloud.atlassian.net/browse/RADEF-2353): Egress order_sku_shipment rekeyer from DBX to BQ update 
* [RADEF_2354](https://dcsgcloud.atlassian.net/browse/RADEF-2354): Egress promotion_header rekeyer from DBX to BQ update 
* [RADEF-2107](https://dcsgcloud.atlassian.net/browse/RADEF-2107): mysql to DBX ingest for lineup reporting
* [RADEF-2420](https://dcsgcloud.atlassian.net/browse/RADEF-2420): Remove absolute values for decline code 

# eComm Release v3.3.1 (FY25 Q3 - Sprint 4)

The release v3.3.1 contains additional DBX to Big Query egress flows to support ECOMP Lift and Shift cutover to Databricks
with additional enhancements to support the Warranties program.

## Enhancements and Features
* [RADEF-2320](https://dcsgcloud.atlassian.net/browse/RADEF-2320): DBX to BQ egress for order_delivery (Delta Load).
* [RADEF-2322](https://dcsgcloud.atlassian.net/browse/RADEF-2322): DBX to BQ egress for web_product (Truncate Load).
* [RADEF-2323](https://dcsgcloud.atlassian.net/browse/RADEF-2323): DBX to BQ egress for dks_sku_pim (Truncate Load).
* [RADEF-2324](https://dcsgcloud.atlassian.net/browse/RADEF-2324): DBX to BQ egress for pim_product (Truncate Load).
* [RADEF-2326](https://dcsgcloud.atlassian.net/browse/RADEF-2326): DBX to BQ egress for pim_product_emast_color (Truncate Load).
* [RADEF-2332](https://dcsgcloud.atlassian.net/browse/RADEF-2332): DBX to BQ egress for order_sku_flash_event (Delta Load).
* [RADEF-2331](https://dcsgcloud.atlassian.net/browse/RADEF-2331): DBX to BQ egress for web_sku_attr (Delta Load).
* [RADEF-2325](https://dcsgcloud.atlassian.net/browse/RADEF-2325): DBX to BQ egress for bridge_pim_sku_product (Delta Load).
* [RADEF-2327](https://dcsgcloud.atlassian.net/browse/RADEF-2327): DBX to BQ egress for order_tender (Delta Load).
* [RADEF-2328](https://dcsgcloud.atlassian.net/browse/RADEF-2328): DBX to BQ egress for order_sku_shipment (Delta Load).
* [RADEF-2329](https://dcsgcloud.atlassian.net/browse/RADEF-2329): DBX to BQ egress for order_sku_init_alloc (Delta Load).
* [RADEF-2339](https://dcsgcloud.atlassian.net/browse/RADEF-2339): DBX to BQ egress for pim_sku (Truncate Load).
* [RADEF-2338](https://dcsgcloud.atlassian.net/browse/RADEF-2338): The first phase in Control-M orchestration for triggering the DBX to BQ egress jobs.
* [RADEF-2187](https://dcsgcloud.atlassian.net/browse/RADEF-2187): Support for the Warranties program by ingesting AOS associate ID from AO data. This will support being able to identify associates that assist with purchase of a Warranty through AOS when an Athlete calls in for support.
* [RADEF-2343](https://dcsgcloud.atlassian.net/browse/RADEF-2343): DBX to BQ egress for Order_delivery with Rekeyer
* [RADEF-2345](https://dcsgcloud.atlassian.net/browse/RADEF-2345): DBX to BQ egress for Order_sku with Rekeyer
* [RADEF-2346](https://dcsgcloud.atlassian.net/browse/RADEF-2346): DBX to BQ egress for pim_product with Rekeyer
* [RADEF-2347](https://dcsgcloud.atlassian.net/browse/RADEF-2347): DBX to BQ egress for web_product with Rekeyer
* [RADEF-2348](https://dcsgcloud.atlassian.net/browse/RADEF-2348): DBX to BQ egress for Order_fulfill with Rekeyer
* [RADEF-2349](https://dcsgcloud.atlassian.net/browse/RADEF-2349): DBX to BQ Rekeyer Egress for txn_web_sku_offer_price (Delta Load)
* [RADEF-2335](https://dcsgcloud.atlassian.net/browse/RADEF-2335): VDC Carrier Changes


# eComm Release v3.2.2 (FY25 Q3 - Sprint 4)

The release of v3.2.2 is primarily focused on our changes for the VDC Modernization carrier changes along with the work of getting the ECOMP L/S finalized for a Q3
cutover.

## Enhancements and Features
* [RADEF - 2309](https://dcsgcloud.atlassian.net/browse/RADEF-2309): DBX -> BQ Egress for promotion_hierarchy_main
* [RADEF - 2310](https://dcsgcloud.atlassian.net/browse/RADEF-2310): DBX -> BQ Egress for promotion_hierarchy_sub
* [RADEF - 2304](https://dcsgcloud.atlassian.net/browse/RADEF-2304): DBX -> BQ Egress for vw_stg_dks_master_cat_attr_mu
* [RADEF - 2313](https://dcsgcloud.atlassian.net/browse/RADEF-2313): BQ Egress for snp_inventory (Delta Load)
* [RADEF - 2319](https://dcsgcloud.atlassian.net/browse/RADEF-2319): BQ Egress for order_fulfill (Delta Load)
* [RADEF - 2314](https://dcsgcloud.atlassian.net/browse/RADEF-2314): BQ Egress for promotion_header
* [RADEF - 2317](https://dcsgcloud.atlassian.net/browse/RADEF-2317): BQ Egress for snp_web_product_assortment (Delta Load)
* [RADEF - 2315](https://dcsgcloud.atlassian.net/browse/RADEF-2315): BQ Egress for txn_order_sku_adjustment (Delta Load)
* [RADEF - 2335](https://dcsgcloud.atlassian.net/browse/RADEF-2335): VDC Modernization
* [RADEF - 2321](https://dcsgcloud.atlassian.net/browse/RADEF-2318): Egress pim_product_desc_vw from DBX to BQ
* [RADEF - 2318](https://dcsgcloud.atlassian.net/browse/RADEF-2318): txn_order_sku BQ egress

# eComm Release v3.2.0 (FY25 Q3 - Sprint 3)

The release of v3.2.0 is primarily focused on our primary initiative of getting the ECOMP L/S work finalized for a Q3
cutover. Additionally, we targeted work for Semantic Filtering in support of Supercharged Search, and updated ingestion
pipelines to support E2E testing of VDC and Warranties programs.

## Enhancements and Features
* [RADEF-2185](https://dcsgcloud.atlassian.net/browse/RADEF-2185): Semantic Filtering egress to Kafka in support of the Super Charged Search program
* [RADEF-2299](https://dcsgcloud.atlassian.net/browse/RADEF-2299): Kafka schema updates to Semantic Filtering output in support of the Super Charged Search program 
* [RADEF-2276](https://dcsgcloud.atlassian.net/browse/RADEF-2276): DBX to BQ egress of mdm_attribute_code in support of ECOMP L/S
* [RADEF-2277](https://dcsgcloud.atlassian.net/browse/RADEF-2277): DBX to BQ egress of vw_style in support of ECOMP L/S
* [RADEF-2278](https://dcsgcloud.atlassian.net/browse/RADEF-2278): DBX to BQ egress of pim_product_emast_title_only in support of ECOMP L/S
* [RADEF-2279](https://dcsgcloud.atlassian.net/browse/RADEF-2279): DBX to BQ egress of pim_product_emast in support of ECOMP L/S
* [RADEF-2280](https://dcsgcloud.atlassian.net/browse/RADEF-2280): DBX to BQ egress of dks_sku_ship_vw in support of ECOMP L/S
* [RADEF-2281](https://dcsgcloud.atlassian.net/browse/RADEF-2281): DBX to BQ egress of flash_sale_styles_dly_vw in support of ECOMP L/S
* [RADEF-2282](https://dcsgcloud.atlassian.net/browse/RADEF-2282): DBX to BQ egress of nrt_atp_bopis_vw in support of ECOMP L/S
* [RADEF-2283](https://dcsgcloud.atlassian.net/browse/RADEF-2283): DBX to BQ egress of tmp_store_productivity_bopis in support of ECOMP L/S
* [RADEF-2284](https://dcsgcloud.atlassian.net/browse/RADEF-2284): DBX to BQ egress of tmp_store_productivity_sfs in support of ECOMP L/S
* [RADEF-2285](https://dcsgcloud.atlassian.net/browse/RADEF-2285): DBX to BQ egress of tmp_store_productivity_hrs in support of ECOMP L/S
* [RADEF-2305](https://dcsgcloud.atlassian.net/browse/RADEF-2305): DBX to BQ egress of web_stage.px_description in support of ECOMP L/S
* [RADEF-2302](https://dcsgcloud.atlassian.net/browse/RADEF-2303): DBX to BQ egress of web_sku_header in support of ECOMP L/S
* [RADEF-2303](https://dcsgcloud.atlassian.net/browse/RADEF-2303): DBX to BQ egress of vw_stg_mdm_master_cat_attr_mu in support of ECOMP L/S
* [RADEF-2304](https://dcsgcloud.atlassian.net/browse/RADEF-2304): DBX to BQ egress of vw_stg_dks_master_cat_attr_mu in support of ECOMP L/S
* [RADEF-2306](https://dcsgcloud.atlassian.net/browse/RADEF-2306): DBX to BQ egress of web_stage.px_promoauth in support of ECOMP L/S
* [RADEF-2307](https://dcsgcloud.atlassian.net/browse/RADEF-2307): DBX to BQ egress of clr_color_lkup in support of ECOMP L/S
* [RADEF-2308](https://dcsgcloud.atlassian.net/browse/RADEF-2308): DBX to BQ egress of promotion_event in support of ECOMP L/S
* [RADEF-2277](https://dcsgcloud.atlassian.net/browse/RADEF-2277): DBX to BQ egress of vw_style in support of ECOMP L/S
* [RADEF-2266](https://dcsgcloud.atlassian.net/browse/RADEF-2266): DBX ingestion of new DDW topics for CO_STAGE data in support of Warranties and VDC programs
