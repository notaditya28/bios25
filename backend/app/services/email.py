"""Transactional email behind a small channel interface.

Postmark in production, SMTP (Mailpit) in development.
"""

import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Protocol

import httpx

from app.config import get_settings
from app.logging import log


@dataclass(frozen=True)
class OutgoingEmail:
    to: str
    subject: str
    text: str


class EmailBackend(Protocol):
    def send(self, msg: OutgoingEmail) -> None: ...


class SmtpBackend:
    def send(self, msg: OutgoingEmail) -> None:
        s = get_settings()
        em = EmailMessage()
        em["From"] = s.email_from
        em["To"] = msg.to
        em["Subject"] = msg.subject
        em.set_content(msg.text)
        with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=10) as smtp:
            smtp.send_message(em)


class PostmarkBackend:
    URL = "https://api.postmarkapp.com/email"

    def send(self, msg: OutgoingEmail) -> None:
        s = get_settings()
        resp = httpx.post(
            self.URL,
            headers={
                "Accept": "application/json",
                "X-Postmark-Server-Token": s.postmark_server_token,
            },
            json={
                "From": s.email_from,
                "To": msg.to,
                "Subject": msg.subject,
                "TextBody": msg.text,
                "MessageStream": "outbound",
            },
            timeout=15,
        )
        resp.raise_for_status()


def get_backend() -> EmailBackend:
    return PostmarkBackend() if get_settings().email_backend == "postmark" else SmtpBackend()


def send_email(msg: OutgoingEmail) -> None:
    """Called from Celery tasks so the request path never blocks on email delivery."""
    get_backend().send(msg)
    log.info("email.sent", to_domain=msg.to.split("@")[-1], subject=msg.subject)


def verification_email(to: str, link: str) -> OutgoingEmail:
    return OutgoingEmail(
        to=to,
        subject="Verify your TradeRadar email",
        text=(
            "Welcome to TradeRadar.\n\n"
            f"Confirm your email address by opening this link (valid for 48 hours):\n{link}\n\n"
            "If you did not sign up, ignore this email."
        ),
    )


def reset_email(to: str, link: str) -> OutgoingEmail:
    return OutgoingEmail(
        to=to,
        subject="Reset your TradeRadar password",
        text=(
            f"A password reset was requested for this address.\n\n"
            f"Choose a new password here (valid for 60 minutes):\n{link}\n\n"
            "If you did not request this, you can ignore this email; your password is unchanged."
        ),
    )


def invite_email(to: str, org_name: str, inviter: str, link: str) -> OutgoingEmail:
    return OutgoingEmail(
        to=to,
        subject=f"{inviter} invited you to {org_name} on TradeRadar",
        text=(
            f"{inviter} invited you to join {org_name} on TradeRadar.\n\n"
            f"Accept the invitation (valid for 7 days):\n{link}\n"
        ),
    )
