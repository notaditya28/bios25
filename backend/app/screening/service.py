"""Screening of an organization's counterparties against all active sanctions entries.

Subjects: partners (buyers/suppliers), client companies, and vessels named on shipments.
Each subject has one `screenings` row (re-used on every run) and zero or more
`screening_matches`. Review decisions survive re-screens unless what was reviewed changes:
if the subject's inputs (name/country/IMO) or the list entry's content change, a confirmed or
dismissed match goes back to `pending` and the reopening is written to the audit log.
"""

import hashlib
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import delete, select, text, update
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import (
    ClientCompany,
    MatchStatus,
    Partner,
    SanctionEntry,
    SanctionList,
    Screening,
    ScreeningMatch,
    Shipment,
    SubjectType,
)
from app.screening.match import Candidate, MatchResult, Subject, score_candidate
from app.screening.normalize import looks_like_company, normalize
from app.services.audit import audit

MAX_MATCHES_PER_SUBJECT = 10
CANDIDATES_PER_STRATEGY = 200

_CANDIDATE_SQL = text(
    """
    WITH by_trgm AS (
        SELECT n.entry_id, n.name, n.is_weak
        FROM sanction_names n JOIN sanction_entries e ON e.id = n.entry_id AND e.active
        WHERE n.name_norm % :norm
        ORDER BY similarity(n.name_norm, :norm) DESC
        LIMIT :lim
    ), by_key AS (
        SELECT n.entry_id, n.name, n.is_weak
        FROM sanction_names n JOIN sanction_entries e ON e.id = n.entry_id AND e.active
        WHERE n.token_keys && CAST(:keys AS varchar[])
        ORDER BY similarity(n.name_norm, :norm) DESC
        LIMIT :lim
    )
    SELECT * FROM by_trgm UNION SELECT * FROM by_key
    """
)


@dataclass
class LoadedSubject:
    type: SubjectType
    id: uuid.UUID
    organization_id: uuid.UUID
    subject: Subject

    @property
    def fingerprint(self) -> str:
        s = self.subject
        raw = f"{self.type.value}|{normalize(s.name).norm}|{s.country or ''}|{s.imo or ''}"
        return hashlib.sha256(raw.encode()).hexdigest()


def load_subject(
    db: Session, subject_type: SubjectType, subject_id: uuid.UUID
) -> LoadedSubject | None:
    if subject_type == SubjectType.partner:
        p = db.get(Partner, subject_id)
        if p is None:
            return None
        return LoadedSubject(
            subject_type,
            p.id,
            p.organization_id,
            Subject(
                p.name, "partner", p.country_iso2, looks_like_company=looks_like_company(p.name)
            ),
        )
    if subject_type == SubjectType.company:
        c = db.get(ClientCompany, subject_id)
        if c is None:
            return None
        # The company's own country is not captured; Indian companies are the norm here.
        return LoadedSubject(
            subject_type,
            c.id,
            c.organization_id,
            Subject(c.name, "company", None, looks_like_company=True),
        )
    s = db.get(Shipment, subject_id)
    if s is None or not (s.vessel_name or s.vessel_imo):
        return None
    return LoadedSubject(
        subject_type,
        s.id,
        s.organization_id,
        Subject(s.vessel_name or "", "vessel", None, imo=s.vessel_imo),
    )


def _candidates(db: Session, subj: Subject) -> list[Candidate]:
    nn = normalize(subj.name)
    rows: dict[tuple, tuple[str, bool]] = {}
    if nn.tokens:
        for entry_id, name, weak in db.execute(
            _CANDIDATE_SQL,
            {
                "norm": nn.norm,
                "keys": nn.distinctive_keys or ["\x00"],
                "lim": CANDIDATES_PER_STRATEGY,
            },
        ):
            rows[(entry_id, name)] = (name, weak)
    entry_ids = {eid for eid, _ in rows}
    imo_entries: list[SanctionEntry] = []
    if subj.imo:
        imo_entries = list(
            db.scalars(
                select(SanctionEntry).where(
                    SanctionEntry.active, SanctionEntry.imo_numbers.contains([subj.imo])
                )
            )
        )
        entry_ids |= {e.id for e in imo_entries}
    if not entry_ids:
        return []
    entries = {
        e.id: e for e in db.scalars(select(SanctionEntry).where(SanctionEntry.id.in_(entry_ids)))
    }
    out = [
        Candidate(
            eid,
            name,
            entries[eid].entity_type,
            entries[eid].countries,
            entries[eid].imo_numbers,
            weak,
        )
        for (eid, _), (name, weak) in rows.items()
    ]
    out += [
        Candidate(e.id, e.primary_name, e.entity_type, e.countries, e.imo_numbers)
        for e in imo_entries
    ]
    return out


def best_matches(db: Session, subj: Subject, threshold: float) -> tuple[dict, int]:
    """Best-scoring name per list entry, keeping entries at or above `threshold`."""
    nn = normalize(subj.name)
    cands = _candidates(db, subj)
    best: dict = {}
    for c in cands:
        r: MatchResult = score_candidate(subj, nn, c)
        if r.score >= threshold and (c.entry_id not in best or r.score > best[c.entry_id].score):
            best[c.entry_id] = r
    top = dict(sorted(best.items(), key=lambda kv: -kv[1].score)[:MAX_MATCHES_PER_SUBJECT])
    return top, len(cands)


def _list_versions(db: Session) -> dict:
    return {
        lst.code: {
            "version": (lst.version_hash or "")[:12],
            "published_at": lst.published_at.isoformat() if lst.published_at else None,
        }
        for lst in db.scalars(select(SanctionList))
    }


def screen(db: Session, subject_type: SubjectType, subject_id: uuid.UUID) -> Screening | None:
    loaded = load_subject(db, subject_type, subject_id)
    if loaded is None:
        forget_subject(db, subject_id)
        db.commit()
        return None
    now = datetime.now(UTC)
    fp = loaded.fingerprint
    results, checked = best_matches(db, loaded.subject, get_settings().screening_review_threshold)

    scr = db.scalar(
        select(Screening).where(
            Screening.subject_type == subject_type, Screening.subject_id == subject_id
        )
    )
    if scr is None:
        scr = Screening(
            organization_id=loaded.organization_id, subject_type=subject_type, subject_id=subject_id
        )
        db.add(scr)
    scr.subject_name = loaded.subject.name
    scr.subject_country = loaded.subject.country
    scr.subject_imo = loaded.subject.imo
    scr.fingerprint = fp
    scr.list_versions = _list_versions(db)
    scr.run_at = now
    scr.candidates_checked = checked
    db.flush()

    existing = {
        m.entry_id: m
        for m in db.scalars(select(ScreeningMatch).where(ScreeningMatch.screening_id == scr.id))
    }
    entries = {
        e.id: e
        for e in db.scalars(
            select(SanctionEntry).where(SanctionEntry.id.in_(list(results) or [uuid.uuid4()]))
        )
    }
    for entry_id, r in results.items():
        m = existing.pop(entry_id, None)
        if m is None:
            db.add(
                ScreeningMatch(
                    organization_id=loaded.organization_id,
                    screening_id=scr.id,
                    entry_id=entry_id,
                    score=r.score,
                    matched_name=r.matched_name,
                    breakdown=r.breakdown,
                    status=MatchStatus.pending,
                    last_seen_at=now,
                )
            )
            continue
        m.score, m.matched_name, m.breakdown = r.score, r.matched_name, r.breakdown
        m.active, m.last_seen_at = True, now
        entry_hash = entries[entry_id].content_hash
        if m.status != MatchStatus.pending and (
            m.decided_fingerprint != fp or m.decided_entry_hash != entry_hash
        ):
            before = {"status": m.status.value}
            m.status = MatchStatus.pending
            audit(
                db,
                actor=None,
                org=loaded.organization_id,
                action="screening.match_reopened",
                target_type="screening_match",
                target_id=m.id,
                before=before,
                after={"status": "pending"},
                note="Subject details or the sanctions list entry changed since review",
            )
    for m in existing.values():
        m.active = False  # no longer produced (delisted, renamed, or now below threshold)
    db.commit()
    return scr


def forget_subject(db: Session, subject_id: uuid.UUID) -> None:
    """Called when a partner/company/shipment is deleted (screenings have no FK to subjects)."""
    db.execute(delete(Screening).where(Screening.subject_id == subject_id))


def org_subjects(
    db: Session, org_id: uuid.UUID | None = None
) -> Iterator[tuple[SubjectType, uuid.UUID]]:
    def scoped(stmt, model):
        return stmt.where(model.organization_id == org_id) if org_id else stmt

    for pid in db.scalars(scoped(select(Partner.id), Partner)):
        yield SubjectType.partner, pid
    for cid in db.scalars(scoped(select(ClientCompany.id), ClientCompany)):
        yield SubjectType.company, cid
    vessel_q = select(Shipment.id).where(
        (Shipment.vessel_name.is_not(None)) | (Shipment.vessel_imo.is_not(None))
    )
    for sid in db.scalars(scoped(vessel_q, Shipment)):
        yield SubjectType.vessel, sid


def screen_many(db: Session, org_id: uuid.UUID | None = None) -> int:
    subjects = list(org_subjects(db, org_id))
    for st, sid in subjects:
        screen(db, st, sid)
    return len(subjects)


def mark_reviewed(m: ScreeningMatch, status: MatchStatus, user_id: uuid.UUID, note: str) -> None:
    m.status = status
    m.reviewed_by = user_id
    m.reviewed_at = datetime.now(UTC)
    m.review_note = note
    m.decided_fingerprint = m.screening.fingerprint
    m.decided_entry_hash = m.entry.content_hash


def deactivate_for_inactive_entries(db: Session) -> None:
    db.execute(
        update(ScreeningMatch)
        .where(ScreeningMatch.entry_id.in_(select(SanctionEntry.id).where(~SanctionEntry.active)))
        .values(active=False)
    )
