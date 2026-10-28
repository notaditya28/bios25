from app.db import SessionLocal
from app.logging import log
from app.sources import ports as src
from app.sources.base import fetch, source_enabled, sync_registry, track_run
from app.worker import celery_app


def refresh_ports_now() -> dict:
    """Download, merge and upsert port reference data. Each source is tracked separately;
    the enrichment sources (WPI, Natural Earth) may fail without blocking the main list."""
    with SessionLocal() as db:
        sync_registry(db)

    summary: dict = {}
    with track_run(src.UNLOCODE.code) as run:
        path, sha = fetch(src.UNLOCODE.url, cache_name="unlocode-latest.zip")
        records = src.parse_unlocode_zip(path)
        if len(records) < 10_000:
            # Guard against a truncated or changed file wiping out the reference list.
            raise ValueError(f"UN/LOCODE parse produced only {len(records)} ports; aborting")
        run.content_hash = sha
        summary["unlocode_records"] = len(records)

        for spec, apply in (
            (src.WPI, lambda p: src.merge_wpi(records, src.parse_wpi_csv(p))),
            (
                src.NATURAL_EARTH,
                lambda p: src.merge_natural_earth(records, src.parse_natural_earth(p)),
            ),
        ):
            if not source_enabled(spec.code):
                continue
            try:
                with track_run(spec.code) as sub:
                    p, h = fetch(spec.url, cache_name=spec.url.rsplit("/", 1)[-1].split("?")[0])
                    sub.rows = apply(p)
                    sub.content_hash = h
                    summary[spec.code] = sub.rows
            except Exception:  # noqa: S112 - already recorded by track_run; keep going
                continue

        with SessionLocal() as db:
            run.rows = src.upsert_ports(db, records)
        summary["upserted"] = run.rows
        summary["with_coords"] = sum(1 for r in records.values() if r.coords)
        run.details = summary
    log.info("ports.refreshed", **summary)
    return summary


@celery_app.task(
    name="app.tasks.ports.refresh_ports",
    autoretry_for=(Exception,),
    retry_backoff=600,
    max_retries=3,
)
def refresh_ports() -> dict:
    return refresh_ports_now()
