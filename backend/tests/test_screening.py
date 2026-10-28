"""End-to-end screening: list ingestion → automatic screening on save → review workflow."""

import pytest
from sqlalchemy import select

from app.db import SessionLocal
from app.models import AuditLog, EntityType, SanctionEntry
from app.sources.sanctions import LISTS, apply_list
from app.sources.sanctions_parsers import ParsedEntry, ParsedList, ParsedName
from tests.conftest import User


def _entry(uid: str, name: str, et: EntityType = EntityType.entity, **kw) -> ParsedEntry:  # type: ignore[no-untyped-def]
    return ParsedEntry(
        uid=uid, entity_type=et, names=[ParsedName(name, is_primary=True)], programs=["TEST"], **kw
    ).finalize()


def load(code: str, entries: list[ParsedEntry], version: str = "v1") -> dict:
    with SessionLocal() as db:
        return apply_list(db, LISTS[code], ParsedList(None, entries), version)


@pytest.fixture
def lists() -> None:
    load(
        "OFAC_SDN",
        [
            _entry("1", "AL-RASHID TRADING COMPANY", countries=["AE"]),
            _entry("2", "PETREL 8", EntityType.vessel, imo_numbers=["9562233"]),
            _entry("3", "PRIGOZHIN, Yevgeniy Viktorovich", EntityType.individual, countries=["RU"]),
        ],
    )


def _partner(u: User, name: str, country: str = "AE") -> dict:
    r = u.post(
        f"/api/orgs/{u.org_id}/partners",
        json={"company_id": u.company_id(), "kind": "buyer", "name": name, "country": country},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _matches(u: User, status: str = "pending") -> list[dict]:
    return u.get(f"/api/orgs/{u.org_id}/screening/matches", params={"status": status}).json()[
        "items"
    ]


def test_partner_is_screened_on_save(exporter: User, lists: None) -> None:
    _partner(exporter, "Al Rashid Trading Co LLC")
    _partner(exporter, "Van Dijk Trading B.V.", "NL")
    pending = _matches(exporter)
    assert len(pending) == 1
    m = pending[0]
    assert m["entry"]["list_code"] == "OFAC_SDN" and m["score"] >= 85
    assert m["subject"]["name"] == "Al Rashid Trading Co LLC"
    assert m["breakdown"]["token_pairs"]
    assert m["entry"]["source_url"].startswith("https://sanctionssearch.ofac.treas.gov/")
    s = exporter.get(f"/api/orgs/{exporter.org_id}/screening/summary").json()
    # The org's own company is not screened at sign-up; the next full re-screen includes it.
    assert s["pending"] == 1 and s["subjects_screened"] == 2


def test_vessel_screened_by_imo(exporter: User, lists: None) -> None:
    cid = exporter.company_id()
    lane = exporter.post(
        f"/api/orgs/{exporter.org_id}/lanes",
        json={"company_id": cid, "origin_port": "INNSA", "dest_port": "NLRTM"},
    ).json()
    exporter.post(
        f"/api/orgs/{exporter.org_id}/shipments",
        json={
            "company_id": cid,
            "lane_id": lane["id"],
            "reference": "B1",
            "vessel_name": "Jal Vahini",
            "vessel_imo": "9562233",
        },
    )
    m = _matches(exporter)[0]
    assert m["score"] == 100 and m["breakdown"]["method"] == "imo_exact"
    assert m["subject"]["type"] == "vessel"


def test_review_decision_is_audited_and_sticky(exporter: User, lists: None, db) -> None:  # type: ignore[no-untyped-def]
    p = _partner(exporter, "Al Rashid Trading Co LLC")
    mid = _matches(exporter)[0]["id"]
    url = f"/api/orgs/{exporter.org_id}/screening/matches/{mid}"
    assert (
        exporter.post(f"{url}/decision", json={"status": "dismissed", "note": "x"}).status_code
        == 422
    )
    r = exporter.post(
        f"{url}/decision",
        json={"status": "dismissed", "note": "Different company: Dubai licence 12345"},
    )
    assert r.status_code == 200 and r.json()["reviewed_by"] == "Test User"
    assert db.scalar(select(AuditLog).where(AuditLog.action == "screening.match_dismissed"))
    # Re-screening with nothing changed keeps the decision.
    exporter.post(f"/api/orgs/{exporter.org_id}/screening/rescreen")
    assert _matches(exporter, "dismissed")[0]["id"] == mid
    # Cosmetic edits that normalise to the same identity keep the decision...
    exporter.put(
        f"/api/orgs/{exporter.org_id}/partners/{p['id']}",
        json={"kind": "buyer", "name": "Al-Rashid Trading Company", "country": "AE"},
    )
    assert _matches(exporter) == []
    # ...but a material change (here the country) re-opens the match for review.
    exporter.put(
        f"/api/orgs/{exporter.org_id}/partners/{p['id']}",
        json={"kind": "buyer", "name": "Al-Rashid Trading Company", "country": "IN"},
    )
    assert [m["id"] for m in _matches(exporter)] == [mid]
    assert db.scalar(select(AuditLog).where(AuditLog.action == "screening.match_reopened"))


def test_list_entry_change_reopens_and_delisting_deactivates(exporter: User, lists: None) -> None:
    _partner(exporter, "Al Rashid Trading Co LLC")
    mid = _matches(exporter)[0]["id"]
    exporter.post(
        f"/api/orgs/{exporter.org_id}/screening/matches/{mid}/decision",
        json={"status": "confirmed", "note": "Same entity, confirmed by compliance"},
    )
    # The list entry gains an alias → confirmed match goes back to review.
    load(
        "OFAC_SDN",
        [
            ParsedEntry(
                "1",
                EntityType.entity,
                [ParsedName("AL-RASHID TRADING COMPANY", True), ParsedName("RASHID GROUP")],
                countries=["AE"],
            ).finalize(),
            _entry("2", "PETREL 8", EntityType.vessel, imo_numbers=["9562233"]),
            _entry("3", "PRIGOZHIN, Yevgeniy Viktorovich", EntityType.individual, countries=["RU"]),
        ],
        "v2",
    )
    exporter.post(f"/api/orgs/{exporter.org_id}/screening/rescreen")
    assert [m["id"] for m in _matches(exporter)] == [mid]
    # Delisting: the match becomes inactive and leaves the queue.
    load(
        "OFAC_SDN",
        [
            _entry("2", "PETREL 8", EntityType.vessel, imo_numbers=["9562233"]),
            _entry("3", "PRIGOZHIN, Yevgeniy Viktorovich", EntityType.individual, countries=["RU"]),
        ],
        "v3",
    )
    exporter.post(f"/api/orgs/{exporter.org_id}/screening/rescreen")
    assert _matches(exporter) == []
    with SessionLocal() as s:
        assert (
            s.scalar(select(SanctionEntry.active).where(SanctionEntry.source_uid == "1")) is False
        )


def test_truncated_list_is_refused(lists: None) -> None:
    with pytest.raises(ValueError, match="refusing"):
        load("OFAC_SDN", [_entry("2", "PETREL 8", EntityType.vessel)], "bad")


def test_only_owner_can_reopen_and_tenants_isolated(
    exporter: User, other_exporter: User, lists: None
) -> None:
    _partner(exporter, "Al Rashid Trading Co LLC")
    mid = _matches(exporter)[0]["id"]
    assert (
        other_exporter.get(f"/api/orgs/{other_exporter.org_id}/screening/matches/{mid}").status_code
        == 404
    )
    assert (
        other_exporter.post(
            f"/api/orgs/{other_exporter.org_id}/screening/matches/{mid}/decision",
            json={"status": "dismissed", "note": "not ours at all"},
        ).status_code
        == 404
    )
    url = f"/api/orgs/{exporter.org_id}/screening/matches/{mid}"
    exporter.post(f"{url}/decision", json={"status": "dismissed", "note": "Different company"})
    assert (
        exporter.post(
            f"{url}/decision", json={"status": "confirmed", "note": "changed my mind"}
        ).status_code
        == 409
    )
    assert (
        exporter.post(f"{url}/reopen", json={"note": "second look"}).json()["status"] == "pending"
    )


def test_deleting_partner_removes_its_screening(exporter: User, lists: None) -> None:
    p = _partner(exporter, "Al Rashid Trading Co LLC")
    exporter.delete(f"/api/orgs/{exporter.org_id}/partners/{p['id']}")
    assert _matches(exporter) == []


def test_lists_endpoint_reports_staleness(exporter: User, lists: None) -> None:
    lists_ = {x["code"]: x for x in exporter.get("/api/sanctions/lists").json()}
    assert lists_["OFAC_SDN"]["entry_count"] == 3
    assert lists_["UN"]["stale"] is True  # never fetched in tests
