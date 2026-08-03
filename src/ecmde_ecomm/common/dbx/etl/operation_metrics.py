from dataclasses import dataclass
from enum import Enum
from ecmde_ecomm.common.spark import spark_session
from ecmde_ecomm.common.errors import NotFoundError


class OperationMetric(Enum):
    MERGE = "MERGE"

    def __init__(self, operation_name: str):
        self.operation_name = operation_name


@dataclass(frozen=True)
class MergeMetrics:
    version: int
    targetRowsInserted: int
    targetRowsUpdated: int
    targetRowsDeleted: int
    sourceRows: int
    scanTimeMs: int
    rewriteTimeMs: int
    executionTimeMs: int


def merge_metrics(qualified_table_name: str) -> MergeMetrics:
    operation_name = OperationMetric.MERGE.operation_name

    sql = f"""
    WITH history_records AS (
      DESCRIBE HISTORY {qualified_table_name}
    ),
    last_operation_version as (
      select max(version) as last_version from history_records where operation = '{operation_name}'
    )
    SELECT
      history_records.version,
      history_records.operationMetrics.numTargetRowsInserted,
      history_records.operationMetrics.numTargetRowsUpdated,
      history_records.operationMetrics.numTargetRowsDeleted,
      history_records.operationMetrics.numSourceRows,
      history_records.operationMetrics.scanTimeMs,
      history_records.operationMetrics.rewriteTimeMs,
      history_records.operationMetrics.executionTimeMs
    FROM history_records
    join last_operation_version 
      on history_records.version = last_operation_version.last_version
    WHERE history_records.operation = '{operation_name}';
    """

    spark_records = spark_session().sql(sql).collect()
    if len(spark_records) == 0:
        raise NotFoundError(
            f"Table {qualified_table_name} has no history records for '{operation_name}' operation."
        )

    row = spark_records[0]
    return MergeMetrics(
        row.version,
        row.numTargetRowsInserted,
        row.numTargetRowsUpdated,
        row.numTargetRowsDeleted,
        row.numSourceRows,
        row.scanTimeMs,
        row.rewriteTimeMs,
        row.executionTimeMs,
    )
