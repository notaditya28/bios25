import uuid

from app.db import SessionLocal
from app.logging import log
from app.models import SubjectType
from app.screening import service
from app.sources.base import source_enabled, sync_registry
from app.sources.sanctions import LISTS, refresh_list
from app.worker import celery_app


def refresh_sanctions_now() -> dict:
    """Refresh every enabled list; re-screen everyone if any list content changed.

    One failing list never blocks the others: its failure is recorded on its source run and
    the previous version stays in force.
    """
    with SessionLocal() as db:
        sync_registry(db)
    changed, failed = [], []
    for code, spec in LISTS.items():
        if not source_enabled(spec.source.code):
            continue
        try:
            if refresh_list(code):
                changed.append(code)
        except Exception:
            failed.append(code)
    if changed:
        with SessionLocal() as db:
            service.deactivate_for_inactive_entries(db)
            db.commit()
        rescreen_all.delay()
    log.info("sanctions.refreshed", changed=changed, failed=failed)
    return {"changed": changed, "failed": failed}


@celery_app.task(name="app.tasks.sanctions.refresh_sanctions")
def refresh_sanctions() -> dict:
    return refresh_sanctions_now()


@celery_app.task(name="app.tasks.sanctions.rescreen_all")
def rescreen_all() -> int:
    with SessionLocal() as db:
        n = service.screen_many(db)
    log.info("screening.rescreened_all", subjects=n)
    return n


@celery_app.task(name="app.tasks.sanctions.rescreen_org")
def rescreen_org(org_id: str) -> int:
    with SessionLocal() as db:
        return service.screen_many(db, uuid.UUID(org_id))


@celery_app.task(
    name="app.tasks.sanctions.screen_subject",
    autoretry_for=(Exception,),
    retry_backoff=30,
    max_retries=5,
)
def screen_subject(subject_type: str, subject_id: str) -> None:
    with SessionLocal() as db:
        service.screen(db, SubjectType(subject_type), uuid.UUID(subject_id))


def request_screening(subject_type: SubjectType, subject_id: uuid.UUID) -> None:
    """Call after the change is committed. A broker outage must not fail the user's save:
    the daily full re-screen picks up anything that could not be queued."""
    try:
        screen_subject.delay(subject_type.value, str(subject_id))
    except Exception as e:
        log.warning(
            "screening.enqueue_failed",
            subject_type=subject_type.value,
            subject_id=str(subject_id),
            error=str(e)[:200],
        )
