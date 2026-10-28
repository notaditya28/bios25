from celery import Celery
from celery.schedules import crontab

from app.config import get_settings
from app.logging import configure_logging

settings = get_settings()
configure_logging(settings.log_level)

celery_app = Celery(
    "traderadar",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["app.tasks.email", "app.tasks.ports", "app.tasks.sanctions"],
)
celery_app.conf.update(
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_time_limit=60 * 30,
    timezone="UTC",
    # Keep our structlog JSON lines as-is instead of Celery re-labelling stdout as WARNING.
    worker_hijack_root_logger=False,
    worker_redirect_stdouts=False,
    beat_schedule={
        # Port reference data changes slowly; weekly is plenty and polite to the publishers.
        "refresh-ports-weekly": {
            "task": "app.tasks.ports.refresh_ports",
            "schedule": crontab(hour=2, minute=17, day_of_week="sun"),
        },
        # Lists change on publisher schedules (OFAC can publish any business day).
        "refresh-sanctions-6h": {
            "task": "app.tasks.sanctions.refresh_sanctions",
            "schedule": crontab(minute=23, hour="*/6"),
        },
        # Safety net: catches subjects whose on-save screening could not be queued.
        "rescreen-all-daily": {
            "task": "app.tasks.sanctions.rescreen_all",
            "schedule": crontab(hour=3, minute=41),
        },
    },
    # Tests run tasks inline so API → screening flows can be asserted synchronously.
    task_always_eager=settings.app_env == "test",
)

app = celery_app  # `celery -A app.worker` looks for `app`
