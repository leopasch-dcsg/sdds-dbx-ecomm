# del_unpub_cats — Delete Unpublished Categories Pipeline

## Overview

`del_unpub_cats` is a Databricks pipeline that identifies e-commerce categories
that have been unpublished long enough to be safely deleted. It follows the
**medallion architecture** (bronze → silver → gold) with the raw zone reading
from **MySQL** via JDBC.

- **Location:** `src/sdds_ecomm/del_unpub_cats/`
- **Asset bundle jobs:** `config/ecomp/del-unpub-cats-jobs.yml`
- **Asset bundle variables:** `config/ecomp/del-unpub-cats-variables.yml`
- **Job name:** `ecmde-sdds-del-unpub-cats`

## Architecture

```
MySQL (unpublished_categories)
        │  (watermarked incremental read)
        ▼
   BRONZE  <sdsc_catalog>.sdds.unpublished_categories
        │  (dedupe → latest state per category_id)
        ▼
   SILVER  <sdsc_catalog>.sdds.unpublished_categories_latest
        │  (filter unpublished + stale ≥ N days)
        ▼
    GOLD   <sdsc_catalog>.sdds.deletable_unpublished_categories
```

All bronze/silver/gold tables land in the **same sdsc catalog**, in the `sdds`
schema. This is the standard for every project under `src/sdds_ecomm/`:

| Target | Catalog |
|---|---|
| `local` / `development` | `dev_sdsc_db` |
| `qa` | `qa_sdsc_db` |
| `production` | `prod_sdsc_db` |

The catalog is overridden per target in `databricks.yml`; the schema is always
`sdds`.

The pipeline runs as a **single Databricks Job** with three sequential tasks
(`bronze_ingest_unpublished_categories` → `silver_latest_unpublished_categories`
→ `gold_deletable_unpublished_categories`), each backed by a Python notebook.

## Modules used from `ecmde_ecomm`

The bronze layer reuses the shared MySQL/ETL infrastructure — no MySQL client
plumbing is redefined in this project:

| Module | Role |
|---|---|
| `ecmde_ecomm.common.mysql.mysql_ingestion.MySqlIngestConfig` | Reads notebook widgets and Azure Key Vault secrets to build a typed connection + source/destination config. |
| `ecmde_ecomm.common.mysql.mysql_ingestion.MySqlIngestion` | Abstract base with the JDBC read + watermark update workflow; concrete bronze classes implement `perform_transforms` / `write_to_destination`. |
| `ecmde_ecomm.common.dbx.etl.watermark.Watermark` | Delta-backed watermark table used to drive incremental reads. |
| `ecmde_ecomm.common.util.NotebookUtil` | Widget/param helpers used by every notebook in the repo. |
| `ecmde_ecomm.common.errors` | `ElementNotFoundError`, `NotSupportedError` used for parameter/table dispatch. |

The two base modules the user called out are used as follows:

- **`ecmde_ecomm.ecomp.lineup.lineup_dbx_egress.LineupDBXEgress`** — reference
  implementation for parallel JDBC reads from MySQL and MySQL stored-procedure
  invocation. The `del_unpub_cats` bronze layer uses the watermarked
  `MySqlIngestion` path (same MySQL driver, credentials, JDBC options) rather
  than a full-table parallel read, but `LineupDBXEgress` is the go-to helper if
  a table needs the parallel/partitioned read pattern.
- **`ecmde_ecomm.ecomp.notebooks.lineup.mysql_dbx_egress_dly`** — reference
  notebook showing how to wire `NotebookUtil` widgets + `CredentialUtil` +
  `LineupDBXEgressConfig` for MySQL reads into a catalog. The `del_unpub_cats`
  bronze notebook mirrors the same widget-driven, secret-scoped pattern.

## Project layout

```
src/sdds_ecomm/
├── __init__.py
├── del_unpub_cats/
│   ├── __init__.py
│   ├── del_unpub_cats_ingestion_provider.py     # dispatch: table → bronze op
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── del_unpub_cats_ingestion_operations.py   # BronzeUnpublishedCategoriesIngestion
│   ├── silver/
│   │   ├── __init__.py
│   │   └── del_unpub_cats_silver_operations.py      # SilverUnpublishedCategoriesTransform
│   └── gold/
│       ├── __init__.py
│       └── del_unpub_cats_gold_operations.py        # GoldDeletableUnpublishedCategoriesTransform
└── notebooks/
    └── del_unpub_cats/
        ├── __init__.py
        ├── del_unpub_cats_bronze_notebook.py
        ├── del_unpub_cats_silver_notebook.py
        └── del_unpub_cats_gold_notebook.py
```

## Bronze — MySQL → Delta

`BronzeUnpublishedCategoriesIngestion` extends `MySqlIngestion`. It:

1. **Discovery** — Before ingesting, the bronze notebook queries
   `information_schema.TABLES` on the configured MySQL connection and prints
   the first **30** `(TABLE_SCHEMA, TABLE_NAME)` pairs (excluding
   `mysql`, `information_schema`, `performance_schema`, `sys`) to the job log.
   This is a diagnostic step — it writes nothing — and gives the operator a
   quick reachability check every run.
2. Reads the `Watermark` for the destination table (`unpublished_categories`).
3. Pulls rows from MySQL where `updated_at > last_watermark`.
4. Appends `ingested_on_utc` and `ingested_by` audit columns.
5. Appends into the bronze Delta table with `mergeSchema=true`.
6. Updates the watermark to the current batch timestamp.

`DelUnpubCatsIngestionProvider` dispatches on `dbx_destination_table`, matching
the `MissingImageIngestionProvider` shape. New source tables ⇒ add a new bronze
op + a new `case` arm.

## Silver — dedupe to latest state

`SilverUnpublishedCategoriesTransform` reads the full bronze table and keeps
the newest row per `category_id` (ordered by `updated_at DESC`), then overwrites
the silver table. Audit columns: `processed_on_utc`, `processed_by`.

## Gold — deletable categories

`GoldDeletableUnpublishedCategoriesTransform` filters silver to rows where
`is_published = false` and `updated_at ≤ now() - stale_days` and overwrites the
gold table. `stale_days` is a job parameter (default `30`). Audit columns:
`stale_days_threshold`, `flagged_on_utc`, `flagged_by`.

## Configuration

All defaults live in `config/ecomp/del-unpub-cats-variables.yml`. Per-target
overrides go in the matching `targets.<env>.variables` block of
`databricks.yml`, exactly the way `missing_image_*` variables are overridden
today.

Key variables:

| Variable | Purpose |
|---|---|
| `del_unpub_cats_mysql_jdbc_url` | Source MySQL JDBC URL. |
| `del_unpub_cats_mysql_username_key` / `_password_key` | Key Vault secret names for MySQL creds. |
| `del_unpub_cats_mysql_watermark_column` | Source column driving incremental reads (default `updated_at`). |
| `del_unpub_cats_source_table` | MySQL source table. |
| `del_unpub_cats_bronze_catalog` / `_schema` / `_table` | Bronze Delta destination. |
| `del_unpub_cats_silver_catalog` / `_schema` / `_table` | Silver Delta destination. |
| `del_unpub_cats_gold_catalog` / `_schema` / `_table` | Gold Delta destination. |
| `del_unpub_cats_watermark_table_name` | Delta table backing `Watermark`. |
| `del_unpub_cats_stale_days` | Days a category must stay unpublished before it is flagged (default `30`). |

## Prerequisites

- Watermark Delta table exists at `del_unpub_cats_watermark_table_name`
  (defaults to `<sdsc_catalog>.sdds.watermarks`).
- MySQL credentials exist in the Key Vault scope referenced by
  `${var.azure_kv_scope}` under the keys declared in the variables file.
- The `sdds` schema exists in the target sdsc catalog and the running principal
  has `CREATE TABLE` on it.

## Running

Deploy and run through the same asset-bundle CLI used for every other job in
this repo:

```bash
# Deploy to dev
databricks bundle deploy -t development

# Trigger the job on demand
databricks bundle run ecmde-sdds-del-unpub-cats -t development
```

For QA / prod, swap `-t development` with `-t qa` or `-t production`.
