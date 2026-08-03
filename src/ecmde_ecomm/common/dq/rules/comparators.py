from ecmde_ecomm.common.dq.dq_types import DQComparator, DQStatus


def evaluate_status(observed_value: float, threshold_value: float, comparator: DQComparator) -> DQStatus:
    if comparator == DQComparator.LTE:
        return DQStatus.PASS if observed_value <= threshold_value else DQStatus.FAIL

    if comparator == DQComparator.GTE:
        return DQStatus.PASS if observed_value >= threshold_value else DQStatus.FAIL

    if comparator == DQComparator.EQ:
        return DQStatus.PASS if observed_value == threshold_value else DQStatus.FAIL

    return DQStatus.FAIL
