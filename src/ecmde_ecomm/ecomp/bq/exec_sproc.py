import argparse, json
from typing import List
from google.cloud import bigquery
from google.oauth2 import service_account
from databricks.sdk.runtime import dbutils
from ecmde_ecomm.common import Logger


def _client(kv_scope: str, kv_key: str, parent_project: str) -> bigquery.Client:
    sa_json = dbutils.secrets.get(kv_scope, kv_key)
    creds = service_account.Credentials.from_service_account_info(json.loads(sa_json))
    return bigquery.Client(project=parent_project, credentials=creds)


def _parse_fqn(fqn: str) -> tuple[str, str, str]:
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"bq_proc_fqn must be project.dataset.routine, got: {fqn}")
    return parts[0], parts[1], parts[2]


def _routine_param_names(client: bigquery.Client, fqn: str) -> List[str]:
    """Return routine parameter names in order (lowercased); empty list if none."""
    proj, ds, name = _parse_fqn(fqn)
    sql = f"""
      SELECT LOWER(parameter_name) AS pname
      FROM `{proj}.{ds}.INFORMATION_SCHEMA.PARAMETERS`
      WHERE specific_name = @routine
      ORDER BY ordinal_position
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("routine", "STRING", name)]
        ),
    )
    return [row["pname"] or "" for row in job.result()]


def _build_script(
    proc_fqn: str, arg_names: List[str], pjob_id: str, table_key: str
) -> str:
    """Builds DECLAREs only for provided inputs; fills the rest with NULL to match arity."""
    decls = []
    provided_expr = {}

    if pjob_id.strip():
        pj = pjob_id.strip().replace("'", "''")
        decls.append(f"DECLARE pjob_id STRING DEFAULT '{pj}';")
        provided_expr["pjob_id"] = "pjob_id"

    if table_key.strip():
        tk = int(table_key.strip())
        decls.append(f"DECLARE table_key INT64 DEFAULT {tk};")
        provided_expr["table_key"] = "table_key"

    if not arg_names:
        call = f"CALL `{proc_fqn}`()"
        return "\n".join([*decls, call + ";"])

    positional = [provided_expr.get(n, "NULL") for n in arg_names]
    call = f"CALL `{proc_fqn}`({', '.join(positional)})"
    return "\n".join([*decls, call + ";"])


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="ecomp-bq-exec-sproc")
    ap.add_argument("--azure_kv_scope", required=True)
    ap.add_argument("--bq_credentials_json_credentials_key", required=True)
    ap.add_argument("--bq_parent_project_id", required=True)
    ap.add_argument("--bq_project_id", required=True)
    ap.add_argument("--bq_dataset", default="")
    ap.add_argument("--bq_proc_fqn", required=True)
    ap.add_argument("--pjob_id", default="")
    ap.add_argument("--table_key", default="")
    return ap


def parse_args(argv: list[str] | None = None):
    return build_arg_parser().parse_args(argv)


def main():
    log = Logger.logger("BQExecSproc")

    args = parse_args()
    log.info(
        "Starting BigQuery stored procedure. "
        f"proc={args.bq_proc_fqn} "
        f"pjob_id_provided={bool(args.pjob_id.strip())} "
        f"table_key_provided={bool(args.table_key.strip())} "
        f"parent_project={args.bq_parent_project_id}"
    )

    client = _client(
        args.azure_kv_scope,
        args.bq_credentials_json_credentials_key,
        args.bq_parent_project_id,
    )
    arg_names = _routine_param_names(client, args.bq_proc_fqn)
    script = _build_script(args.bq_proc_fqn, arg_names, args.pjob_id, args.table_key)

    job = client.query(script)
    job.result()
    log.info(
        f"Stored procedure executed via BigQuery API. job_id={job.job_id} proc={args.bq_proc_fqn}"
    )
