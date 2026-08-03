from dataclasses import dataclass
from datetime import date


@dataclass()
class DemandDecompReportResult:
    record_count: int


@dataclass(frozen=True)
class FiscalDates:
    date_id_ly_start: int
    date_id_ly_end: int
    date_id_ty_start: int
    date_id_ty_end: int
