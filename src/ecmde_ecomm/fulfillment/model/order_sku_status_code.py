from enum import Enum


class OrderSkuStatus(Enum):
    PACKED = (
        [100, 111, 112, 113, 114, 200, 290, 300, 400, 410, 500, 600, 700, 710, 790],
        23,
        "PACKED",
        "Packed",
    )
    CANCELLED = ([999], 16, "CANCELLED", "Cancelled")
    FULFILLED = ([800, 810], 32, "FULFILLED", "Fulfilled")
    UNKNOWN = (
        [-1],
        -1,
        "UNKNOWN",
        "Unknown",
    )

    def __init__(
        self, status_ids: list[int], key: int, code: str, description: str
    ) -> None:
        self.status_ids = status_ids
        self.key = key
        self.code = code
        self.description = description
