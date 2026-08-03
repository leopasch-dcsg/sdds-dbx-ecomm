# Prerequisite Setup
* [Environment Variables](environment-variables.md) for our project.
* [Required Software](https://dcsgcloud.atlassian.net/wiki/x/UQDrDw) confluence page.

## Flyway Migrations
Flyway is being used to managed the schema across our medallion layers. Adding new tables or making alterations to
existing tables should be managed through Flyway migrations.

There are limitations to what Flyway can manage in Databricks today. The biggest limitation is managing DLT streaming
tables. Databricks handles these table types differently, and they are created automatically by the DBX runtime.

For more information on why we use migrations, please see the [Flyway Documentation](https://documentation.red-gate.com/fd/why-database-migrations-184127574.html).

### Testing Flyway Migrations
There will be times when you need to test one or more migrations. Every team should have a team based catalog across the
Databricks environments. Within that catalog (preferably dev_{team_code}_db) I would create a sandbox schema that you
can use as your testing grounds. This sandbox schema will act as your "local" environment for testing migrations as well
as running your jobs/pipelines from the "local" Databricks target.

### Creating Migrations
Migrations are nothing more than a DDL or a DML SQL script, however, Flyway migrations must be named using a specific
naming convention. There are several different types of migrations, but we will focus on versioned migrations, see [here](https://documentation.red-gate.com/fd/versioned-migrations-273973333.html) for more details.

In simple single engineer scenarios, you will see versioned migrations take an ever incrementing sequence (1, 2, 3, ...) however, in a 
team environment this increases the risk of two engineers creating a migration with the same version. To reduce the chances
of that occurring we opt for using a timestamp as the version down to the second. 

`VYYYYMMDDHHMMSS__some_migration_description`

### Use our service principal to get an OAUTH2 token locally.
Our Databricks environment requires JDBC access authentication using OAuth2. There two python scripts in the `flyway/devops`
folder that will fetch an OAuth2 token using your team's Service Principal. `set_jdbc_access_token` is only used with the
migrations Github Action so that your migrations can run in a CI/CD pipeline. The second script,
`set_jdbc_access_token_devlocal` is designed for you to use locally. This devlocal requires having several pieces of
information about your service principal set as environment variables. If you are on a Mac, I recommend setting them in
your `~/.zshrc` file. See [Environment Variables](environment-variables.md) for details.

After modifying your `.zshrc` file, you will need to restart your terminal or IDE for the changes to be picked up.

### Create Sandbox Schemas
The steps below will allow you to create your sandbox schemas in our dev_ecmde_db team catalog. This is where you will
test out all your migrations locally before submitting a PR. By the time you submit a PR, you should know that your
migrations are correct. These sandbox schemas also allow you to deploy your DAB (Databricks Asset Bundle) using the local
target and run all your jobs and pipelines for validation.

1. Log into DEV Databricks
2. Open a SQL Editor, start and attach the ecmde-serverless-dev SQL Warehouse
3. Set the Catalog to dev_ecmde_db and execute the following SQL statements
   * `create schema dev_ecmde_db.{first_initial}{last_name}_ecomp;`
   * `create schema dev_ecmde_db.{first_initial}{last_name}_bronze;`
   * `create schema dev_ecmde_db.{first_initial}{last_name}_silver;`
   * `create schema dev_ecmde_db.{first_initial}{last_name}_gold;`

### Set JDBC Access Token
Running the following command will authenticate our service principal, return an OAuth2 token, and set it as an
environment variable that will be picked up when you run your migrations locally.

```shell
  export JDBC_ACCESS_TOKEN=$(echo $(python flyway/devops/set_jdbc_access_token_devlocal.py))
```

### Apply Migrations
Running this command will apply all migrations in flyway/{medallion-layer}/ddl into your sandbox schema in dev_ecmde_db.

#### ECOMP DDL Migrations
Run your sandbox DDL migrations for ECOMP using the command below.

```shell

  docker run --rm --volume ./flyway/ecomp/ddl:/flyway/sql:ro \
  --volume ./flyway/conf:/flyway/conf:ro \
  -e JDBC_ACCESS_TOKEN=$JDBC_ACCESS_TOKEN \
  -e FLYWAY_PLACEHOLDERS_CO_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_COMMON_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_SB_LOAD_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_DIM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_APPS_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_WEB_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  redgate/flyway:11.11.2 migrate -environment="local" -cleanDisabled=false -baselineVersion=1 -defaultSchema=$FLYWAY_ECOMP_SANDBOX_SCHEMA
```

#### Bronze DDL Migrations
Run your sandbox DDL migrations for bronze using the command below.

```shell

docker run --rm --volume ./flyway/bronze/ddl:/flyway/sql:ro \
  --volume ./flyway/conf:/flyway/conf:ro \
  -e JDBC_ACCESS_TOKEN=$JDBC_ACCESS_TOKEN \
  -e FLYWAY_PLACEHOLDERS_CO_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_COMMON_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_SB_LOAD_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_DIM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_APPS_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_WEB_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  redgate/flyway:11.11.2 migrate -environment="local" -cleanDisabled=false -baselineVersion=1 -defaultSchema=$FLYWAY_BRONZE_SANDBOX_SCHEMA
```

#### Silver DDL Migrations
Run your sandbox DDL migrations for silver using the command below.

```shell

docker run --rm --volume ./flyway/silver/ddl:/flyway/sql:ro \
  --volume ./flyway/conf:/flyway/conf:ro \
  -e JDBC_ACCESS_TOKEN=$JDBC_ACCESS_TOKEN \
  -e FLYWAY_PLACEHOLDERS_CO_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_COMMON_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_SB_LOAD_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_DIM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_APPS_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_WEB_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  redgate/flyway:11.11.2 migrate -environment="local" -cleanDisabled=false -baselineVersion=1 -defaultSchema=$FLYWAY_SILVER_SANDBOX_SCHEMA
```

#### Gold DDL Migrations
Run your sandbox DDL migrations for gold using the command below.

```shell

docker run --rm --volume ./flyway/gold/ddl:/flyway/sql:ro \
  --volume ./flyway/conf:/flyway/conf:ro \
  -e JDBC_ACCESS_TOKEN=$JDBC_ACCESS_TOKEN \
  -e FLYWAY_PLACEHOLDERS_CO_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_COMMON_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_SB_LOAD_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_DIM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_APPS_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_WEB_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  redgate/flyway:11.11.2 migrate -environment="local" -cleanDisabled=false -baselineVersion=1 -defaultSchema=$FLYWAY_GOLD_SANDBOX_SCHEMA
```


### Clean Migrations
Running this command will clean your schema, this causes Flyway to drop all tables in the schema allowing you to re-run
all your migrations from scratch.

```shell
  # NOTE: You need to update the following on the command below
  # 1. the path to the sql folder you want to run migrations for.
  # 2. the value of the defaultSchema argument to your sandbox schema name.
  docker run --rm --volume ./relative/path/to/sql/folder:/flyway/sql:ro \
  --volume ./flyway/conf:/flyway/conf:ro \
  -e JDBC_ACCESS_TOKEN=$JDBC_ACCESS_TOKEN \
  -e FLYWAY_PLACEHOLDERS_CO_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_COMMON_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_SB_LOAD_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_DIM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_ECOM_APPS_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  -e FLYWAY_PLACEHOLDERS_WEB_STAGE_SCHEMA=$FLYWAY_ECOMP_SANDBOX_SCHEMA \
  redgate/flyway:11.11.2 clean -environment="local" -cleanDisabled=false -baselineVersion=1 -defaultSchema=your_sandbox
```
