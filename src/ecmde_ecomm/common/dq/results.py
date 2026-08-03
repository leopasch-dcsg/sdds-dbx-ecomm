from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone

from ecmde_ecomm.common.dq.dq_types import DQStatus, DQSeverity, DQCheckType


@dataclass(frozen=True)
class DQCheckResult:
    run_id: str
    domain: str
    table_name: str
    rule_id: str
    check_type: DQCheckType
    status: DQStatus
    severity: DQSeverity
    observed_value: float | int | str
    threshold_value: float | int | str | None
    dimension_key: str | None = None
    message: str | None = None
    created_on_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["check_type"] = self.check_type.value
        payload["status"] = self.status.value
        payload["severity"] = self.severity.value
        payload["observed_value"] = str(self.observed_value)
        payload["threshold_value"] = (
            str(self.threshold_value) if self.threshold_value is not None else None
        )
        return payload
