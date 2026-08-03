import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from ecmde_ecomm.common.errors import ExpectationNotMetError
from ecmde_ecomm.common.logger import Logger
from ecmde_ecomm.common.dq.results import DQCheckResult
from ecmde_ecomm.common.dq.notifiers.base import DQNotifier

_SMTP_HOST = "smtp.dcsg.com"
_SMTP_PORT = 25
_SMTP_FROM = "dq-alerts@dcsg.com"


class EmailNotifier(DQNotifier):
    def __init__(self, recipients: str | None = None):
        self.logger = Logger.logger("DQ_EmailNotifier")
        self.recipients = recipients

    def notify(self, results: list[DQCheckResult]) -> None:
        failed = [result for result in results if result.status.value == "fail"]
        if len(failed) == 0:
            return

        failed_ids = ", ".join([result.rule_id for result in failed])
        failed_details = "\n".join(
            f"  - {result.rule_id}: {result.message}" for result in failed
        )

        self.logger.error(
            "DQ FAILED_CHECK_NOTIFY. recipients=%s total_checks=%s failed_checks=%s failed_rule_ids=[%s]",
            self.recipients if self.recipients is not None else "",
            len(results),
            len(failed),
            failed_ids,
        )

        if not self.recipients:
            self.logger.warning(
                "EmailNotifier: no recipients configured, skipping send."
            )
            return

        recipient_list = [r.strip() for r in self.recipients.split(",") if r.strip()]
        subject = f"[DQ Alert] {len(failed)} check(s) failed — rule IDs: {failed_ids}"
        body = (
            f"DQ run detected {len(failed)} failure(s) out of {len(results)} total check(s).\n\n"
            f"Failed rule IDs: {failed_ids}\n\n"
            f"Details:\n{failed_details}\n"
        )

        msg = MIMEMultipart()
        msg["From"] = _SMTP_FROM
        msg["To"] = ", ".join(recipient_list)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=15) as server:
                server.ehlo()
                refused = server.sendmail(_SMTP_FROM, recipient_list, msg.as_string())
                if refused:
                    if len(refused) == len(recipient_list):
                        raise ExpectationNotMetError(
                            f"EmailNotifier: all recipients were refused: {refused}"
                        )
                    self.logger.warning(
                        "EmailNotifier: some recipients refused: %s", refused
                    )
                else:
                    self.logger.info(
                        "EmailNotifier: alert sent to %s", ", ".join(recipient_list)
                    )
        except Exception as exc:
            self.logger.error("EmailNotifier: failed to send email: %s", exc)
            raise ExpectationNotMetError(
                f"EmailNotifier: SMTP send failed on {_SMTP_HOST}:{_SMTP_PORT}: {exc}"
            ) from exc
