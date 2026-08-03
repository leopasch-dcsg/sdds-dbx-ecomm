# Overview
The environment variables outline in this file need to be added to your `.zshrc` file in order to contribute changes to
this project.

These environment variables support deploying your local bundle to your sandbox environment, to applying migrations in
your sandbox schemas within Databricks.

**Add the following lines to your .zshrc file in your home folder:**
```bash
export SP_CLIENT_ID={Find this value in your Vault Path: concourse/data-e-commerce/databricks/non-prod clientId}
export SP_CLIENT_SECRET={Find this value in your Vault Path: concourse/data-e-commerce/databricks/non-prod clientSecret}
export SP_TENANT_ID={Find this value in your Vault Path: concourse/data-e-commerce/databricks/non-prod tenantId}
export SP_OAUTH_HOST=https://login.microsoftonline.com
export SP_OAUTH_SCOPE=2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default

export FLYWAY_ECOMP_SANDBOX_SCHEMA=<first intial+last name>_ecomp
export FLYWAY_BRONZE_SANDBOX_SCHEMA=<first intial+last name>_bronze
export FLYWAY_SILVER_SANDBOX_SCHEMA=<first intial+last name>_silver
export FLYWAY_GOLD_SANDBOX_SCHEMA=<first intial+last name>_gold
```