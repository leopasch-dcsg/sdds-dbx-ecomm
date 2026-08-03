from enum import Enum


class QuarantineReason(Enum):
    UNDEFINED = -1
    FUTURE_ORDER_DATE = 0
    FUTURE_ORDER_CANCEL_DATE = 1
