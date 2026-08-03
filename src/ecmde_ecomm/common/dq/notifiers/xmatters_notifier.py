import json
from urllib.request import Request, urlopen

from ecmde_ecomm.common.errors import ExpectationNotMetError
from ecmde_ecomm.common.logger import Logger
from ecmde_ecomm.common.util import CredentialUtil
from ecmde_ecomm.common.dq.results import DQCheckResult
from ecmde_ecomm.common.dq.notifiers.base import DQNotifier


class XMattersNotifier(DQNotifier):
    def __init__(self, secret_scope: str, webhook_secret_key: str):
        self.logger = Logger.logger("DQ_XMattersNotifier")
        self.secret_scope = secret_scope
        self.webhook_secret_key = webhook_secret_key

    def notify(self, results: list[DQCheckResult]) -> None:
        flagged = [result for result in results if result.status.value == "fail"]

        if len(flagged) == 0:
            return

        webhook_url = CredentialUtil.secret(self.secret_scope, self.webhook_secret_key)
        if webhook_url is None or len(webhook_url.strip()) == 0:
            raise ExpectationNotMetError("xMatters webhook URL is missing.")

        payload = {
            "total_checks": len(results),
            "failed_checks": len(flagged),
            "failed_rule_ids": [result.rule_id for result in flagged],
            "failed_results": [result.as_dict() for result in flagged],
        }

        request = Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urlopen(request, timeout=10) as response:
            self.logger.info("xMatters notification status: %s", response.status)
