"""Shared plumbing for external data sources: registry, run tracking, polite HTTP with retries."""

import hashlib
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.config import get_settings
from app.db import SessionLocal
from app.logging import log
from app.models import DataSource, SourceRun


@dataclass(frozen=True)
class SourceSpec:
    code: str
    name: str
    url: str
    licence_note: str
    attribution: str | None
    stale_after_hours: int


REGISTRY: dict[str, SourceSpec] = {}


def register(spec: SourceSpec) -> SourceSpec:
    REGISTRY[spec.code] = spec
    return spec


def sync_registry(db: Session) -> None:
    """Upsert static metadata; never touches run state or the admin's enabled flag."""
    for spec in REGISTRY.values():
        stmt = insert(DataSource).values(
            code=spec.code,
            name=spec.name,
            url=spec.url,
            licence_note=spec.licence_note,
            attribution=spec.attribution,
            stale_after_hours=spec.stale_after_hours,
            enabled=True,
        )
        db.execute(
            stmt.on_conflict_do_update(
                index_elements=[DataSource.code],
                set_={
                    "name": stmt.excluded.name,
                    "url": stmt.excluded.url,
                    "licence_note": stmt.excluded.licence_note,
                    "attribution": stmt.excluded.attribution,
                    "stale_after_hours": stmt.excluded.stale_after_hours,
                },
            )
        )
    db.commit()


class RunHandle:
    def __init__(self, run_id: object, code: str):
        self.run_id = run_id
        self.code = code
        self.rows: int | None = None
        self.content_hash: str | None = None
        self.data_as_of: datetime | None = None
        self.details: dict = {}
        self.skipped = False


def source_enabled(code: str) -> bool:
    with SessionLocal() as db:
        src = db.get(DataSource, code)
        return src is None or src.enabled


@contextmanager
def track_run(code: str) -> Iterator[RunHandle]:
    """Records a source_runs row in its own session so failures are always persisted.

    On failure the previous good data is left untouched and data_sources.last_error is set,
    which is what the staleness badges in the UI read.
    """
    with SessionLocal() as db:
        run = SourceRun(source_code=code, status="running")
        db.add(run)
        db.commit()
        handle = RunHandle(run.id, code)
    try:
        yield handle
    except Exception as e:
        with SessionLocal() as db:
            failed = db.get(SourceRun, handle.run_id)
            assert failed is not None
            run = failed
            run.status = "failed"
            run.finished_at = datetime.now(UTC)
            run.error = f"{type(e).__name__}: {e}"[:4000]
            run.details = handle.details or None
            src = db.get(DataSource, code)
            if src:
                src.last_error = run.error
            db.commit()
        log.error("source.run_failed", source=code, error=str(e)[:500])
        raise
    else:
        with SessionLocal() as db:
            done = db.get(SourceRun, handle.run_id)
            assert done is not None
            run = done
            run.status = "skipped" if handle.skipped else "success"
            run.finished_at = datetime.now(UTC)
            run.rows = handle.rows
            run.content_hash = handle.content_hash
            run.details = handle.details or None
            src = db.get(DataSource, code)
            if src:
                src.last_success_at = run.finished_at
                src.last_data_as_of = handle.data_as_of or run.finished_at
                src.last_error = None
            db.commit()
        log.info("source.run_ok", source=code, rows=handle.rows, skipped=handle.skipped)


def last_content_hash(code: str) -> str | None:
    with SessionLocal() as db:
        return db.scalar(
            select(SourceRun.content_hash)
            .where(SourceRun.source_code == code, SourceRun.status == "success")
            .order_by(SourceRun.started_at.desc())
            .limit(1)
        )


class SourceFetchError(RuntimeError):
    pass


@retry(
    retry=retry_if_exception_type((httpx.TransportError, SourceFetchError)),
    wait=wait_exponential(multiplier=5, max=120),
    stop=stop_after_attempt(4),
    reraise=True,
)
def fetch(url: str, *, cache_name: str, params: dict | None = None) -> tuple[Path, str]:
    """Download to the local cache atomically. Returns (path, sha256)."""
    s = get_settings()
    cache_dir = Path(s.source_cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / cache_name
    tmp = target.with_suffix(target.suffix + ".part")
    digest = hashlib.sha256()
    with (
        httpx.Client(
            timeout=s.source_http_timeout_s,
            follow_redirects=True,
            headers={"User-Agent": s.source_user_agent},
        ) as client,
        client.stream("GET", url, params=params) as resp,
    ):
        if resp.status_code == 429 or resp.status_code >= 500:
            raise SourceFetchError(f"{url} returned HTTP {resp.status_code}")
        if resp.status_code != 200:
            raise RuntimeError(f"{url} returned HTTP {resp.status_code}")
        with open(tmp, "wb") as f:
            for chunk in resp.iter_bytes(1 << 16):
                f.write(chunk)
                digest.update(chunk)
    if tmp.stat().st_size == 0:
        raise SourceFetchError(f"{url} returned an empty body")
    os.replace(tmp, target)
    return target, digest.hexdigest()
