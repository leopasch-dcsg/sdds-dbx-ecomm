# Databricks notebook source

# COMMAND ----------
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()


# COMMAND ----------
from ecmde_ecomm.common.errors import IllegalArgumentError
from ecmde_ecomm.common.logger import Logger
from ecmde_ecomm.common.util import NotebookUtil
from ecmde_ecomm.common.dq import DQEngine, DQFrameworkConfig, DQRuleConfig
from ecmde_ecomm.common.dq.notifiers import EmailNotifier, XMattersNotifier

logger = Logger.logger("DQ_Runner_Notebook")
DEFAULT_DQ_EMAIL_RECIPIENTS = "jishnu.lekshmidas@dcsg.com"


def _email_recipients_from_rules(rules: list[DQRuleConfig]) -> str:
    configured = {
        value.strip()
        for rule in rules
        for value in (rule.notify_email_recipients or "").split(",")
        if len(value.strip()) > 0
    }
    if len(configured) == 0:
        return DEFAULT_DQ_EMAIL_RECIPIENTS
    return ",".join(sorted(configured))


framework_config = DQFrameworkConfig.from_notebook_params()
rule_configs = DQRuleConfig.load_rules(spark, framework_config)

if len(rule_configs) == 0:
    raise IllegalArgumentError(
        "No active DQ rules were found for the configured domain and table."
    )

source_dataframe = spark.table(framework_config.source_table_fqn)
if framework_config.source_filter_expr:
    logger.info("Applying source filter: %s", framework_config.source_filter_expr)
    source_dataframe = source_dataframe.filter(framework_config.source_filter_expr)
engine = DQEngine(spark)
email_recipients = _email_recipients_from_rules(rule_configs)

notifiers = []
if any(rule.is_email_notify for rule in rule_configs):
    notifiers.append(EmailNotifier(email_recipients))

if any(rule.is_xmatter_notify for rule in rule_configs):
    secret_scope = NotebookUtil.notebook_param("azure_kv_scope", "")
    webhook_secret = NotebookUtil.notebook_param("dq_xmatters_webhook_secret", "")
    if len(secret_scope) == 0 or len(webhook_secret) == 0:
        logger.warning(
            "xMatters was requested, but secret scope/key is missing. Skipping xMatters notifier."
        )
    else:
        notifiers.append(XMattersNotifier(secret_scope, webhook_secret))

results = engine.run(source_dataframe, framework_config, rule_configs, notifiers)

failed = [result for result in results if result.status.value == "fail"]
warn = [result for result in results if result.status.value == "warn"]

logger.info(
    "DQ run complete. domain=%s table=%s total=%s warn=%s fail=%s",
    framework_config.domain,
    framework_config.table_name,
    len(results),
    len(warn),
    len(failed),
)

if framework_config.fail_on_breach and len(failed) > 0:
    failed_rule_ids = ", ".join([result.rule_id for result in failed])
    raise IllegalArgumentError(
        (
            f"DQ failure threshold reached for {framework_config.table_name}. "
            f"Failed checks: {len(failed)}. Failed rule IDs: [{failed_rule_ids}]"
        )
    )
