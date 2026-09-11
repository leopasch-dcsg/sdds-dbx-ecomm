# sdds-dbx-ecomm

**SDDS Index Comparison — a Databricks Asset Bundle for the eCommerce Data Engineering team.**

This repository packages the `sdds_index_comparison` job: a bronze → silver → gold pipeline on Databricks that ingests catalog and inventory feeds from the Search Data Distribution System (SDDS), compares indexed content across sources at both SKU and style level, and publishes daily email reports on any drift.

---

## What this project does

The `sdds_index_comparison` job runs daily at **04:00:21 America/New_York** on the production workspace and executes the following medallion pipeline:

| Layer  | Purpose                                                                                               | Notebooks                                                                                                              |
| ------ | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Bronze | Land raw catalog, catalog-stream, inventory, and unpub/deleted events into Delta tables               | `catalog-load-bronze`, `catalog-stream-bronze`, `catalog-load-inventory-bronze`, `unpub_and_deleted_events_mysq_bronze` |
| Silver | Cleanse, deduplicate, and conform bronze events into query-ready Delta tables                         | `catalog-load-silver`, `catalog-stream-silver`, `catalog-load-inventory-silver`                                         |
| Gold   | Flatten and compare indexed values across sources, then dispatch SKU- and style-level drift reports   | `flat_values`, `catalog-compare-run-id-sku-optimized`, `catalog-compare-run-id-style-optimized`, `catalog-email-report-sku`, `catalog-email-report-style` |

Catalog, schemas, and cluster targets are resolved per deploy target (`dev_sdsc_db` for development, `prod_sdsc_db` for production) through `sdds.common.util.NotebookUtil` and Databricks bundle variables.

---

## Repository layout

```
sdds-dbx-ecomm/
├── config/                 # Databricks bundle job & variable YAML
│   ├── common/             #   Shared bundle variables
│   └── sdds_index_comparison/  # sdds_index_comparison.job.yml
├── src/sdds/               # Python package published as the `sdds` wheel
│   ├── common/util.py      #   NotebookUtil: safe widget/param reads
│   └── sdds_comparison/    #   Notebooks (bronze / silver / gold)
├── flyway/                 # SQL DDL migrations for bronze, silver, gold, ecomp, devops
├── entdata/                # Environment-specific data configs (dev / qa / prod)
├── tests/                  # pytest suites (unit + integration)
├── documentation/          # Team-facing setup, contributing, and release docs
├── databricks.yml          # Bundle entry point
├── pyproject.toml          # Package metadata (name = "sdds")
└── CHANGELOG.md            # Release history
```

---

## Getting started

### Prerequisites

Install the following before contributing. For the complete team baseline see the [Required Software](https://dcsgcloud.atlassian.net/wiki/x/UQDrDw) page on Confluence.

1. An IDE (PyCharm recommended)
2. **Java SDK 11 or 21** — required to run PySpark unit tests locally (install via self-service)
3. **Homebrew** (self-service)
4. **Python 3.12.4+** (`brew install python@3.12`) — project requires Python ≥ 3.10
5. **Databricks CLI** (`brew install databricks`) — used to deploy your local `target`
6. **Docker** (self-service) — used to run Flyway migrations locally

### Additional setup guides

Please review these documents before your first contribution:

* [Configure Environment Variables](documentation/environment-variables.md)
* [Configure Databricks CLI](documentation/databricks.md)
* [Configure PyCharm](documentation/pycharm.md)
* [Contributing Code](documentation/contributing.md)
* [Developing Notebooks](documentation/notebooks.md)
* [Developing and Testing Migrations](documentation/migrations.md)
* [Developing Automated Tests](documentation/testing.md)
* [Local Dev & Testing](documentation/dev-local-testing.md)
* [Data Quality Framework](documentation/data-quality-framework.md)

---

## Deploying and running the bundle

The bundle is defined in `databricks.yml`; the SDDS job is included from `config/sdds_index_comparison/sdds_index_comparison.job.yml`.

```bash
# Validate the bundle for a target
databricks bundle validate --target dev

# Deploy to your development target
databricks bundle deploy --target dev

# Trigger the job on demand
databricks bundle run sdds_index_comparison_job --target dev
```

The `extraction_date` job parameter defaults to `{{job.start_time.iso_date}}`, so scheduled runs are self-partitioning; override it when backfilling.

---

## Release to production

See [Release to Production](documentation/release_to_production.md) for the end-to-end promotion process. Version and release notes live in [`CHANGELOG.md`](CHANGELOG.md); the current release is tracked in `pyproject.toml`.
