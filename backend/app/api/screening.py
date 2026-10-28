import uuid
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps import OrgContext, get_org, get_owner_org, get_verified_user
from app.models import (
    DataSource,
    MatchStatus,
    SanctionEntry,
    SanctionList,
    SanctionName,
    Screening,
    ScreeningMatch,
    SubjectType,
    User,
)
from app.schemas.trade import Page
from app.screening.service import mark_reviewed
from app.services.audit import audit
from app.sources.sanctions import LISTS
from app.tasks.sanctions import rescreen_org

router = APIRouter(prefix="/api", tags=["screening"])


class ListStatus(BaseModel):
    code: str
    name: str
    publisher: str
    published_at: datetime | None
    fetched_at: datetime | None
    entry_count: int
    stale: bool


class EntryOut(BaseModel):
    id: uuid.UUID
    list_code: str
    list_name: str
    source_uid: str
    entity_type: str
    primary_name: str
    names: list[str]
    programs: list[str]
    countries: list[str]
    birth_dates: list[str]
    imo_numbers: list[str]
    listed_on: str | None
    remarks: str | None
    active: bool
    removed_at: datetime | None
    source_url: str | None


class SubjectOut(BaseModel):
    type: SubjectType
    id: uuid.UUID
    name: str
    country: str | None
    imo: str | None


class MatchOut(BaseModel):
    id: uuid.UUID
    status: MatchStatus
    active: bool
    score: float
    matched_name: str
    breakdown: dict
    subject: SubjectOut
    entry: EntryOut
    screened_at: datetime
    list_versions: dict
    reviewed_by: str | None
    reviewed_at: datetime | None
    review_note: str | None


class DecisionIn(BaseModel):
    status: Literal["confirmed", "dismissed"]
    note: str = Field(min_length=5, max_length=2000)


class ReopenIn(BaseModel):
    note: str | None = Field(default=None, max_length=2000)


class Summary(BaseModel):
    pending: int
    confirmed: int
    dismissed: int
    subjects_screened: int
    last_run_at: datetime | None
    lists: list[ListStatus]
    lists_stale: bool


class SubjectStatus(BaseModel):
    subject_type: SubjectType
    subject_id: uuid.UUID
    run_at: datetime
    pending: int
    confirmed: int


def _source_url(e: SanctionEntry) -> str | None:
    if e.list_code in ("OFAC_SDN", "OFAC_CONS"):
        return f"https://sanctionssearch.ofac.treas.gov/Details.aspx?id={e.source_uid}"
    return None


def _list_statuses(db: Session) -> list[ListStatus]:
    sources = {s.code: s for s in db.scalars(select(DataSource))}
    held = {lst.code: lst for lst in db.scalars(select(SanctionList))}
    now = datetime.now(UTC)
    out = []
    for code, spec in LISTS.items():
        lst = held.get(code)
        src = sources.get(spec.source.code)
        stale = (
            src is None
            or src.last_success_at is None
            or now - src.last_success_at > timedelta(hours=src.stale_after_hours)
        )
        out.append(
            ListStatus(
                code=code,
                name=spec.name,
                publisher=spec.publisher,
                published_at=lst.published_at if lst else None,
                fetched_at=lst.fetched_at if lst else None,
                entry_count=lst.entry_count if lst else 0,
                stale=stale,
            )
        )
    return out


def _match_out(db: Session, m: ScreeningMatch, reviewers: dict) -> MatchOut:
    e, s = m.entry, m.screening
    names = list(
        db.scalars(
            select(SanctionName.name)
            .where(SanctionName.entry_id == e.id)
            .order_by(SanctionName.is_primary.desc(), SanctionName.id)
            .limit(25)
        )
    )
    return MatchOut(
        id=m.id,
        status=m.status,
        active=m.active,
        score=m.score,
        matched_name=m.matched_name,
        breakdown=m.breakdown,
        subject=SubjectOut(
            type=s.subject_type,
            id=s.subject_id,
            name=s.subject_name,
            country=s.subject_country,
            imo=s.subject_imo,
        ),
        entry=EntryOut(
            id=e.id,
            list_code=e.list_code,
            list_name=LISTS[e.list_code].name,
            source_uid=e.source_uid,
            entity_type=e.entity_type.value,
            primary_name=e.primary_name,
            names=names,
            programs=e.programs,
            countries=e.countries,
            birth_dates=e.birth_dates,
            imo_numbers=e.imo_numbers,
            listed_on=e.listed_on,
            remarks=e.remarks,
            active=e.active,
            removed_at=e.removed_at,
            source_url=_source_url(e),
        ),
        screened_at=s.run_at,
        list_versions=s.list_versions,
        reviewed_by=reviewers.get(m.reviewed_by),
        reviewed_at=m.reviewed_at,
        review_note=m.review_note,
    )


def _reviewers(db: Session, matches: list[ScreeningMatch]) -> dict:
    ids = {m.reviewed_by for m in matches if m.reviewed_by}
    if not ids:
        return {}
    rows = db.execute(select(User.id, User.full_name).where(User.id.in_(ids)))
    return {uid: name for uid, name in rows}


def _match(db: Session, ctx: OrgContext, match_id: uuid.UUID) -> ScreeningMatch:
    m = db.get(ScreeningMatch, match_id)
    if m is None or m.organization_id != ctx.org.id:
        raise HTTPException(404, "Match not found")
    return m


@router.get("/sanctions/lists", response_model=list[ListStatus])
def sanctions_lists(
    _: User = Depends(get_verified_user), db: Session = Depends(get_db)
) -> list[ListStatus]:
    return _list_statuses(db)


@router.get("/orgs/{org_id}/screening/summary", response_model=Summary)
def summary(ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)) -> Summary:
    counts: dict[MatchStatus, int] = {
        status: n
        for status, n in db.execute(
            select(ScreeningMatch.status, func.count())
            .where(ScreeningMatch.organization_id == ctx.org.id, ScreeningMatch.active)
            .group_by(ScreeningMatch.status)
        )
    }
    screened, last = db.execute(
        select(func.count(), func.max(Screening.run_at)).where(
            Screening.organization_id == ctx.org.id
        )
    ).one()
    lists = _list_statuses(db)
    return Summary(
        pending=counts.get(MatchStatus.pending, 0),
        confirmed=counts.get(MatchStatus.confirmed, 0),
        dismissed=counts.get(MatchStatus.dismissed, 0),
        subjects_screened=screened,
        last_run_at=last,
        lists=lists,
        lists_stale=any(x.stale for x in lists),
    )


@router.get("/orgs/{org_id}/screening/subjects", response_model=list[SubjectStatus])
def subject_statuses(
    subject_type: SubjectType | None = None,
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> list[SubjectStatus]:
    stmt = select(Screening).where(Screening.organization_id == ctx.org.id)
    if subject_type:
        stmt = stmt.where(Screening.subject_type == subject_type)
    screenings = list(db.scalars(stmt))
    counts: dict[tuple, int] = {}
    for sid, status, n in db.execute(
        select(ScreeningMatch.screening_id, ScreeningMatch.status, func.count())
        .where(ScreeningMatch.organization_id == ctx.org.id, ScreeningMatch.active)
        .group_by(ScreeningMatch.screening_id, ScreeningMatch.status)
    ):
        counts[(sid, status)] = n
    return [
        SubjectStatus(
            subject_type=s.subject_type,
            subject_id=s.subject_id,
            run_at=s.run_at,
            pending=counts.get((s.id, MatchStatus.pending), 0),
            confirmed=counts.get((s.id, MatchStatus.confirmed), 0),
        )
        for s in screenings
    ]


@router.get("/orgs/{org_id}/screening/matches", response_model=Page[MatchOut])
def list_matches(
    status: MatchStatus | None = MatchStatus.pending,
    subject_id: uuid.UUID | None = None,
    include_inactive: bool = False,
    limit: int = Query(default=25, le=100),
    offset: int = Query(default=0, ge=0),
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> Page[MatchOut]:
    stmt = select(ScreeningMatch).where(ScreeningMatch.organization_id == ctx.org.id)
    if status:
        stmt = stmt.where(ScreeningMatch.status == status)
    if not include_inactive:
        stmt = stmt.where(ScreeningMatch.active)
    if subject_id:
        stmt = stmt.join(Screening).where(Screening.subject_id == subject_id)
    total = db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    rows = list(
        db.scalars(
            stmt.order_by(ScreeningMatch.score.desc(), ScreeningMatch.created_at)
            .limit(limit)
            .offset(offset)
        ).unique()
    )
    reviewers = _reviewers(db, rows)
    return Page(
        items=[_match_out(db, m, reviewers) for m in rows], total=total, limit=limit, offset=offset
    )


@router.get("/orgs/{org_id}/screening/matches/{match_id}", response_model=MatchOut)
def get_match(
    match_id: uuid.UUID, ctx: OrgContext = Depends(get_org), db: Session = Depends(get_db)
) -> MatchOut:
    m = _match(db, ctx, match_id)
    return _match_out(db, m, _reviewers(db, [m]))


@router.post("/orgs/{org_id}/screening/matches/{match_id}/decision", response_model=MatchOut)
def decide(
    match_id: uuid.UUID,
    body: DecisionIn,
    ctx: OrgContext = Depends(get_org),
    db: Session = Depends(get_db),
) -> MatchOut:
    m = _match(db, ctx, match_id)
    if m.status != MatchStatus.pending:
        raise HTTPException(409, "Already reviewed; an owner can reopen it first")
    before = {"status": m.status.value}
    mark_reviewed(m, MatchStatus(body.status), ctx.user.id, body.note.strip())
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action=f"screening.match_{body.status}",
        target_type="screening_match",
        target_id=m.id,
        before=before,
        after={
            "status": body.status,
            "score": m.score,
            "entry": m.entry.primary_name,
            "list": m.entry.list_code,
            "subject": m.screening.subject_name,
        },
        note=body.note.strip(),
    )
    db.commit()
    return _match_out(db, m, _reviewers(db, [m]))


@router.post("/orgs/{org_id}/screening/matches/{match_id}/reopen", response_model=MatchOut)
def reopen(
    match_id: uuid.UUID,
    body: ReopenIn | None = None,
    ctx: OrgContext = Depends(get_owner_org),
    db: Session = Depends(get_db),
) -> MatchOut:
    m = _match(db, ctx, match_id)
    if m.status == MatchStatus.pending:
        raise HTTPException(409, "Match is already pending")
    before = {"status": m.status.value, "note": m.review_note}
    m.status = MatchStatus.pending
    audit(
        db,
        actor=ctx.user.id,
        org=ctx.org.id,
        action="screening.match_reopened",
        target_type="screening_match",
        target_id=m.id,
        before=before,
        after={"status": "pending"},
        note=body.note if body else None,
    )
    db.commit()
    return _match_out(db, m, _reviewers(db, [m]))


@router.post("/orgs/{org_id}/screening/rescreen", status_code=202)
def rescreen(ctx: OrgContext = Depends(get_owner_org)) -> dict:
    rescreen_org.delay(str(ctx.org.id))
    return {"message": "Re-screening queued"}
