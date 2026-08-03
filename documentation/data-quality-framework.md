# Data Quality Framework Guide
This guide explains how to define and run Data Quality (DQ) checks with the current framework, including:

1. How to create and maintain DQ rules in the `dq_rule_config` table.
2. Which checks are supported today.
3. How to wire DQ as a downstream job task.
4. How persisted results and notifications work.

## Architecture Overview
The current framework executes DQ checks through a dedicated notebook task and writes results to Delta tables.

Core components:

1. Rule and config parsing:
   - `src/ecmde_ecomm/common/dq/dq_config.py`
   - `src/ecmde_ecomm/common/dq/dq_types.py`
2. Execution engine:
   - `src/ecmde_ecomm/common/dq/engine.py`
3. Rule evaluators:
   - `src/ecmde_ecomm/common/dq/rules/`
4. Runner notebook:
   - `src/ecmde_ecomm/common/notebooks/dq_runner_notebook.py`
5. Framework result tables:
   - `flyway/silver/ddl/V20260407101500__create_dq_framework_tables.sql`

Execution flow:

1. Job task passes `dq_*` parameters.
2. Notebook reads params, loads active rules from `dq_rule_config` at runtime.
3. Engine executes checks against source table.
4. Results are stored in DQ result/summary tables.
5. Notifications are sent based on configured channels.

## Required Job Parameters
The DQ runner notebook expects the following parameters:

1. `dq_run_id`
2. `dq_domain`
3. `dq_table_name`
4. `dq_source_table`
5. `dq_result_table`
6. `dq_summary_table`
7. `dq_config_table`

Optional parameters:

1. `dq_fail_on_breach` (default: `false`)
2. `dq_xmatters_webhook_secret` (required only if at least one active rule has `is_xmatter_notify=true`)
3. `azure_kv_scope` (required for xMatters secret lookup)

## Rule Configuration Table Schema
Rules are now stored as rows in `dq_rule_config`. Each row represents one check.

Common fields:

1. `rule_id` (string, required)
2. `domain` (string, required)
3. `table_name` (string, required)
4. `is_active` (boolean, required)
5. `check_type` (string, required)
6. `severity` (string, optional, default `medium`)
7. `threshold_value` (number, optional, default `0`)
8. `comparator` (string, optional, default `lte`)
9. `is_email_notify` (boolean, optional)
10. `is_xmatter_notify` (boolean, optional)
11. `notify_email_recipients` (string, optional, comma-separated recipients)
9. `fail_on_breach` (boolean, optional)

### Allowed `check_type`

1. `freshness`
2. `null`
3. `pattern`
4. `condition`
5. `aggregate`
6. `quarantine_count`

### Allowed `severity`

1. `low`
2. `medium`
3. `high`
4. `critical`

### Allowed `comparator`

1. `lte` (observed <= threshold)
2. `gte` (observed >= threshold)
3. `eq` (observed == threshold)

### Type-specific fields

#### `null`

1. `column_name` required.

#### `pattern`

1. `column_name` required.
2. `pattern` required (regex string).

#### `condition`

1. `condition_expr` required (Spark SQL filter expression).

#### `freshness`

1. `freshness_column` required.
2. `freshness_minutes` required (max lag in minutes).

#### `aggregate`

1. `aggregation_type` required.
2. `aggregation_column` required for all except simple row count.
3. `group_by_columns` optional array for grouped checks.

Allowed `aggregation_type`:

1. `count`
2. `count_distinct`
3. `sum`
4. `avg`
5. `median`
6. `mode`
7. `stddev`
8. `sigma_3`

For `sigma_3` rules:

1. `threshold_value` is interpreted as sigma level and must be `1`, `2`, or `3`.
2. If `threshold_value` is null, the framework defaults to `3`.

#### `quarantine_count`

1. `quarantine_table_fqn` optional.
2. If null/blank, framework defaults to `${dq_source_table}_quarantine`.
3. If `aggregation_type = count`, the rule compares raw quarantine row count.
4. If `aggregation_type` is omitted, the rule compares quarantine-to-source ratio.

## SQL Examples for New Rules

Insert one rule:

```sql
INSERT INTO dq_rule_config (
  rule_id,
  domain,
  table_name,
  check_type,
  severity,
  threshold_value,
  condition_expr,
  comparator,
      is_email_notify,
   notify_email_recipients,
      is_xmatter_notify,
  fail_on_breach,
  is_active,
  updated_on_utc,
  updated_by
)
VALUES (
  'header_web_order_num_null',
  'fulfillment',
  'stg_oso_order_header',
  'condition',
  'high',
  0,
  'web_order_num is null',
  'lte',
   true,
   'jishnu.lekshmidas@dcsg.com',
   true,
  false,
  true,
  current_timestamp(),
  'manual'
);
```

Disable a rule without redeploy:

```sql
UPDATE dq_rule_config
   SET is_active = false,
     updated_on_utc = current_timestamp(),
     updated_by = 'manual'
 WHERE domain = 'fulfillment'
   AND table_name = 'stg_oso_order_header'
   AND rule_id = 'header_web_order_num_null';
```

Update threshold without redeploy:

```sql
UPDATE dq_rule_config
   SET threshold_value = 0.02,
     updated_on_utc = current_timestamp(),
     updated_by = 'manual'
 WHERE domain = 'fulfillment'
   AND table_name = 'stg_oso_order_delivery'
   AND rule_id = 'delivery_quarantine_ratio';
```

## Rule Configuration Examples by Type

### 1) Null ratio check
```sql
INSERT INTO dq_rule_config (rule_id, domain, table_name, check_type, severity, column_name, threshold_value, comparator, is_active, updated_on_utc, updated_by)
VALUES ('order_sku_web_order_null', 'fulfillment', 'stg_oso_order_sku', 'null', 'high', 'web_order_num', 0, 'lte', true, current_timestamp(), 'manual');
```

### 2) Pattern check
```sql
INSERT INTO dq_rule_config (rule_id, domain, table_name, check_type, severity, column_name, pattern, threshold_value, comparator, is_active, updated_on_utc, updated_by)
VALUES ('delivery_tracking_format', 'fulfillment', 'stg_oso_order_delivery', 'pattern', 'medium', 'tracking_number', '^[A-Za-z0-9-]+$', 0, 'lte', true, current_timestamp(), 'manual');
```

### 3) Condition check
```sql
INSERT INTO dq_rule_config (rule_id, domain, table_name, check_type, severity, threshold_value, condition_expr, comparator, is_active, updated_on_utc, updated_by)
VALUES ('delivery_negative_shipped_qty', 'fulfillment', 'stg_oso_order_delivery', 'condition', 'high', 0, 'shipped_qty < 0', 'lte', true, current_timestamp(), 'manual');
```

### 4) Aggregate grouped uniqueness check
Use grouped `count_distinct` with threshold 1 to detect one-to-many mappings.

```sql
INSERT INTO dq_rule_config (rule_id, domain, table_name, check_type, severity, aggregation_type, aggregation_column, group_by_columns, threshold_value, comparator, is_active, updated_on_utc, updated_by)
VALUES ('tracking_to_lpn_unique', 'fulfillment', 'stg_oso_order_delivery', 'aggregate', 'high', 'count_distinct', 'sci_lpn_id', 'tracking_number', 1, 'lte', true, current_timestamp(), 'manual');
```

### 5) Quarantine ratio check
```sql
INSERT INTO dq_rule_config (rule_id, domain, table_name, check_type, severity, quarantine_table_fqn, threshold_value, comparator, is_active, updated_on_utc, updated_by)
VALUES ('order_header_quarantine_ratio', 'fulfillment', 'stg_oso_order_header', 'quarantine_count', 'medium', 'dev_ent_silver_db.ecmde.order_header_quarantine', 0.05, 'lte', true, current_timestamp(), 'manual');
```

### 6) Quarantine absolute count check
```sql
INSERT INTO dq_rule_config (rule_id, domain, table_name, check_type, severity, aggregation_type, quarantine_table_fqn, threshold_value, comparator, is_active, updated_on_utc, updated_by)
VALUES ('order_header_quarantine_count_limit', 'fulfillment', 'stg_oso_order_header', 'quarantine_count', 'high', 'count', 'dev_ent_silver_db.ecmde.order_header_quarantine', 10, 'lte', true, current_timestamp(), 'manual');
```

### 7) Freshness check
```sql
INSERT INTO dq_rule_config (rule_id, domain, table_name, check_type, severity, freshness_column, freshness_minutes, is_active, updated_on_utc, updated_by)
VALUES ('order_delivery_freshness', 'fulfillment', 'stg_oso_order_delivery', 'freshness', 'high', 'silver_updated_on_utc', 90, true, current_timestamp(), 'manual');
```

## Where to Add New DQ Rules
Add and maintain rules directly in `dq_rule_config` (runtime table), not in job YAML variables.

For fulfillment, use:

1. `domain = 'fulfillment'`
2. `table_name = <silver table name>`
3. `is_active = true` for enabled checks

## Configure DQ as Downstream of Egress Job
Add a DQ task in `config/fulfillment/fulfillment-jobs.yml` and set `depends_on` to the egress task.

Example:

```yaml
- task_key: fulfillment-order-header-stg-dq
  depends_on:
    - task_key: fulfillment-order-header-stg-dbx-egress
  environment_key: default
  notebook_task:
    notebook_path: ${var.workspace_path_prefix}/src/ecmde_ecomm/common/notebooks/dq_runner_notebook
    base_parameters:
      azure_kv_scope: ${var.azure_kv_scope}
      dq_run_id: "{{job.run_id}}-fulfillment-order-header-stg-egress"
      dq_domain: fulfillment
      dq_table_name: ${var.oso_order_header_silver_table_name}
      dq_source_table: ${var.ecmde_silver_catalog}.${var.ecmde_silver_schema}.${var.oso_order_header_silver_table_name}
      dq_config_table: ${var.dq_config_table}
      dq_result_table: ${var.dq_result_table}
      dq_summary_table: ${var.dq_summary_table}
      dq_fail_on_breach: ${var.dq_fail_on_breach}
      dq_xmatters_webhook_secret: ${var.dq_xmatters_webhook_secret}
```

   ## Rule Source
   The framework is table-driven at runtime.

   1. Rules are always loaded from `dq_config_table`.
   2. The run fails if table is missing or no active rules are found for `(domain, table_name)`.

## Result Tables and Trend Monitoring
Current framework tables:

1. `dq_rule_config` - active/inactive rule control table.
2. `dq_check_result` - one record per evaluated rule.
3. `dq_run_summary` - run-level aggregate summary.
4. `dq_notification_log` - channel delivery audit.

Typical trend queries:

1. Failed checks by rule over time.
2. Table-level failure rates by day.
3. Critical-only breaches.
4. Quarantine ratio trends.

## Notifications
Supported channels today:

1. `email` (summary logging path)
2. `xmatters` (webhook call using secret lookup)

Current mode:

1. Both channels operate in FAILED_CHECK_NOTIFY mode.
2. Only checks with `status = fail` are included.
3. Payloads/messages include failed `rule_id` values and failure details.

How to configure mail IDs:

1. Configure job-level email recipients in production target job settings under `email_notifications.on_failure` in `databricks.yml`.
2. Set `notify_email_recipients` on DQ rules that should trigger email notifications.
3. If no active rule provides recipients, the framework falls back to `jishnu.lekshmidas@dcsg.com`.
4. When `dq_fail_on_breach=true`, the thrown exception includes failed rule IDs so Databricks failure emails include those IDs.

When using xMatters:

1. Set `is_xmatter_notify=true` in dq_rule_config rows.
2. Pass `azure_kv_scope`.
3. Provide `dq_xmatters_webhook_secret`.

## Capabilities with Current Framework
The current implementation supports:

1. Row-level freshness checks.
2. Column-level null checks.
3. Column-level regex/pattern checks.
4. Expression-based condition checks.
5. Aggregate checks (count, distinct count, sum, avg, median, mode, stddev).
6. Three-sigma outlier checks with built-in small-sample guard.
7. Grouped aggregate checks using `group_by_columns`.
8. Quarantine table count-ratio checks.
9. Configurable thresholds and comparator semantics.
10. Runtime table-driven rule updates without package redeploy.
11. Historical persistence for trend analysis.
12. Downstream notifications (email and xMatters).

## Current Limitations and Operational Notes

1. Rule expressions and column names are not schema-validated before execution; invalid columns fail at runtime.
2. `mode` aggregate uses Spark SQL mode function behavior and may depend on engine support.
3. Notification delivery auditing table exists, but per-channel writeback can be expanded further.
4. Most checks are implemented as full-table scans; very large tables may need partition-aware optimization.
5. Quarantine checks assume quarantine table exists and is maintained by pipeline logic.

## Best Practices for New Rules

1. Start with high-severity rules only for mandatory business keys and referential expectations.
2. Use `condition` rules for business logic checks that combine multiple columns.
3. Use grouped `count_distinct` for one-to-many violation detection patterns.
4. Keep thresholds realistic for initial rollout, then tighten after observing baselines.
5. Pair every critical table with one quarantine ratio rule.
6. Use `dq_fail_on_breach=false` in initial rollout, then enable once stable.

## Validation Checklist
Before enabling a new DQ ruleset:

1. Validate inserted/updated rows in `dq_rule_config` for target `(domain, table_name)`.
2. Confirm all columns in rule definitions exist in source table schema.
3. Confirm quarantine table exists if using `quarantine_count` rules.
4. Run `databricks bundle validate -t local --profile <profile>`.
5. Execute one dev run and inspect `dq_check_result` and `dq_run_summary`.
6. Verify notification behavior for warning/failure scenarios.
