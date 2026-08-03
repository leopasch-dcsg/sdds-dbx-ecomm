from abc import ABC, abstractmethod

from ecmde_ecomm.common.dq.results import DQCheckResult


class DQNotifier(ABC):
    @abstractmethod
    def notify(self, results: list[DQCheckResult]) -> None:
        pass
