from enum import Enum


class DQStatus(Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


class DQSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DQCheckType(Enum):
    FRESHNESS = "freshness"
    NULL = "null"
    PATTERN = "pattern"
    CONDITION = "condition"
    AGGREGATE = "aggregate"
    QUARANTINE_COUNT = "quarantine_count"


class DQAggregationType(Enum):
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    AVG = "avg"
    MEDIAN = "median"
    MODE = "mode"
    STDDEV = "stddev"
    SIGMA_3 = "sigma_3"


class DQComparator(Enum):
    LTE = "lte"
    GTE = "gte"
    EQ = "eq"
