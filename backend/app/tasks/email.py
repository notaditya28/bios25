from app.services.email import OutgoingEmail, send_email
from app.worker import celery_app


@celery_app.task(
    name="app.tasks.email.deliver",
    autoretry_for=(Exception,),
    retry_backoff=30,
    retry_backoff_max=1800,
    max_retries=8,
)
def deliver(to: str, subject: str, text: str) -> None:
    send_email(OutgoingEmail(to=to, subject=subject, text=text))


def enqueue(msg: OutgoingEmail) -> None:
    deliver.delay(msg.to, msg.subject, msg.text)
