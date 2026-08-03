CREATE TABLE IF NOT EXISTS ${common_schema}.dq_rule_config (
    rule_id STRING COMMENT 'Unique identifier for the DQ rule.',
    domain STRING COMMENT 'Business domain for this rule configuration.',
    table_name STRING COMMENT 'Target table name for this rule configuration.',
    check_type STRING COMMENT 'Check type: freshness, null, pattern, aggregate, quarantine_count.',
    severity STRING COMMENT 'Rule severity level: low, medium, high, critical.',
    column_name STRING COMMENT 'Optional column used by null/pattern/aggregate checks.',
    pattern STRING COMMENT 'Regex pattern for pattern check rules.',
    threshold_value DOUBLE COMMENT 'Configurable threshold for pass/fail evaluation.',
    freshness_column STRING COMMENT 'Timestamp column used by freshness checks.',
    freshness_minutes INT COMMENT 'Maximum allowed freshness lag in minutes.',
    aggregation_type STRING COMMENT 'Aggregate type: count, sum, avg, median, mode, stddev, sigma_3.',
    aggregation_column STRING COMMENT 'Numeric column for aggregate checks.',
    group_by_columns STRING COMMENT 'Comma separated list of columns for grouped aggregate checks.',
    quarantine_table_fqn STRING COMMENT 'Optional quarantine table used for count ratio checks.',
    condition_expr STRING COMMENT 'Spark SQL predicate expression for condition checks.',
    comparator STRING COMMENT 'Comparator for threshold evaluation: lte, gte, eq.',
    notify_email_recipients STRING COMMENT 'Comma-separated email recipients used when is_email_notify is true.',
    is_email_notify BOOLEAN COMMENT 'Whether email notification is enabled for this rule.',
    is_xmatter_notify BOOLEAN COMMENT 'Whether xMatters notification is enabled for this rule.',
    fail_on_breach BOOLEAN COMMENT 'Whether this rule should fail the pipeline on breach.',
    is_active BOOLEAN COMMENT 'Whether this rule is active.',
    updated_on_utc TIMESTAMP COMMENT 'Config update timestamp UTC.',
    updated_by STRING COMMENT 'Config update actor.'
)
USING DELTA
COMMENT 'Control table for configurable data quality rules.';

CREATE TABLE IF NOT EXISTS ${common_schema}.dq_check_result (
    run_id STRING COMMENT 'Execution run identifier.',
    domain STRING COMMENT 'Business domain.',
    table_name STRING COMMENT 'Table evaluated by this check.',
    rule_id STRING COMMENT 'Rule identifier.',
    check_type STRING COMMENT 'Check type executed.',
    status STRING COMMENT 'Check status: pass, warn, fail.',
    severity STRING COMMENT 'Severity assigned to this rule.',
    observed_value STRING COMMENT 'Observed metric value serialized as string.',
    threshold_value STRING COMMENT 'Threshold value serialized as string.',
    dimension_key STRING COMMENT 'Optional group key for aggregate checks.',
    message STRING COMMENT 'Execution detail message for diagnostics.',
    created_on_utc TIMESTAMP COMMENT 'Result creation timestamp UTC.'
)
USING DELTA
COMMENT 'Historical result table for each DQ check execution.';

CREATE TABLE IF NOT EXISTS ${common_schema}.dq_run_summary (
    run_id STRING COMMENT 'Execution run identifier.',
    domain STRING COMMENT 'Business domain.',
    table_name STRING COMMENT 'Table evaluated during this run.',
    total_checks INT COMMENT 'Total number of checks executed.',
    warn_checks INT COMMENT 'Total checks with warn status.',
    failed_checks INT COMMENT 'Total checks with fail status.',
    created_on_utc TIMESTAMP COMMENT 'Summary creation timestamp UTC.'
)
USING DELTA
COMMENT 'Run-level summary of data quality results for trend analysis.';

CREATE TABLE IF NOT EXISTS ${common_schema}.dq_notification_log (
    run_id STRING COMMENT 'Execution run identifier.',
    domain STRING COMMENT 'Business domain.',
    table_name STRING COMMENT 'Table evaluated during this run.',
    channel STRING COMMENT 'Notification channel: email or xmatters.',
    payload_hash STRING COMMENT 'Hash of payload sent to downstream notification channel.',
    notification_status STRING COMMENT 'Delivery status (success/failure/skipped).',
    notification_detail STRING COMMENT 'Response or error details.',
    created_on_utc TIMESTAMP COMMENT 'Notification event timestamp UTC.'
)
USING DELTA
COMMENT 'Audit trail for DQ notification delivery outcomes.';
