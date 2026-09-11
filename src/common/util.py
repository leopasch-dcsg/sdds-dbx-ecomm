from databricks.sdk.runtime import dbutils

def get_param(param_name):
    return dbutils.widgets.get(param_name)

azure_secret_scope = get_param("azure_kv_scope")

def get_secret(param_name):
    key = get_param(param_name)
    return dbutils.secrets.get(azure_secret_scope, key)