from tests.conftest import User


def _partner(u: User, company_id: str, **kw) -> dict:  # type: ignore[no-untyped-def]
    body = {
        "company_id": company_id,
        "kind": "buyer",
        "name": "Van Dijk Trading B.V.",
        "country": "Netherlands",
        "city": "Rotterdam",
        "port": "NLRTM",
        **kw,
    }
    r = u.post(f"/api/orgs/{u.org_id}/partners", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def _lane(u: User, company_id: str, **kw) -> dict:  # type: ignore[no-untyped-def]
    body = {"company_id": company_id, "origin_port": "INNSA", "dest_port": "NLRTM", **kw}
    r = u.post(f"/api/orgs/{u.org_id}/lanes", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def test_partner_lane_shipment_crud(exporter: User) -> None:
    cid = exporter.company_id()
    p = _partner(exporter, cid)
    assert p["country"] == "NL" and p["port"]["unlocode"] == "NLRTM"
    lane = _lane(exporter, cid, partner_id=p["id"])
    assert lane["origin_port"]["lat"] == 18.95

    r = exporter.post(
        f"/api/orgs/{exporter.org_id}/shipments",
        json={
            "company_id": cid,
            "lane_id": lane["id"],
            "reference": "BKG-001",
            "container_refs": ["CSQU3054383"],
            "etd": "2026-11-05",
            "eta": "2026-12-01",
            "value_inr": "4500000.00",
            "vessel_imo": "9074729",
        },
    )
    assert r.status_code == 201, r.text
    s = r.json()
    assert s["container_count"] == 1
    assert s["partner_id"] == p["id"]  # inherited from the lane

    listing = exporter.get(f"/api/orgs/{exporter.org_id}/shipments").json()
    assert listing["total"] == 1

    upd = exporter.put(
        f"/api/orgs/{exporter.org_id}/shipments/{s['id']}",
        json={"lane_id": lane["id"], "reference": "BKG-001", "status": "in_transit"},
    )
    assert upd.json()["status"] == "in_transit"

    # Lanes in use cannot be deleted.
    assert exporter.delete(f"/api/orgs/{exporter.org_id}/lanes/{lane['id']}").status_code == 409
    assert exporter.delete(f"/api/orgs/{exporter.org_id}/shipments/{s['id']}").status_code == 204
    assert exporter.delete(f"/api/orgs/{exporter.org_id}/lanes/{lane['id']}").status_code == 204


def test_validation_errors(exporter: User) -> None:
    cid = exporter.company_id()
    org = exporter.org_id
    lane = _lane(exporter, cid)
    bad = [
        {
            "company_id": cid,
            "lane_id": lane["id"],
            "reference": "X",
            "etd": "2026-12-01",
            "eta": "2026-11-01",
        },
        {
            "company_id": cid,
            "lane_id": lane["id"],
            "reference": "X",
            "container_refs": ["CSQU3054384"],
        },
        {"company_id": cid, "lane_id": lane["id"], "reference": "X", "vessel_imo": "1234568"},
        {"company_id": cid, "lane_id": lane["id"], "reference": "X", "value_inr": "-1"},
    ]
    for body in bad:
        assert exporter.post(f"/api/orgs/{org}/shipments", json=body).status_code == 422, body


def test_lane_mode_must_match_port_type(exporter: User) -> None:
    cid = exporter.company_id()
    r = exporter.post(
        f"/api/orgs/{exporter.org_id}/lanes",
        json={"company_id": cid, "origin_port": "INDEL", "dest_port": "NLRTM"},
    )
    assert r.status_code == 422 and "not listed as a seaport" in r.json()["detail"]
    r = exporter.post(
        f"/api/orgs/{exporter.org_id}/lanes",
        json={"company_id": cid, "origin_port": "INDEL", "dest_port": "GBLHR", "mode": "air"},
    )
    assert r.status_code == 201


def test_duplicate_lane_rejected(exporter: User) -> None:
    cid = exporter.company_id()
    _lane(exporter, cid)
    r = exporter.post(
        f"/api/orgs/{exporter.org_id}/lanes",
        json={"company_id": cid, "origin_port": "INNSA", "dest_port": "NLRTM"},
    )
    assert r.status_code == 422


def test_tenant_isolation(exporter: User, other_exporter: User) -> None:
    cid = exporter.company_id()
    p = _partner(exporter, cid)
    lane = _lane(exporter, cid)
    rival_org = other_exporter.org_id
    # Another org cannot read through its own org path...
    assert other_exporter.get(f"/api/orgs/{rival_org}/partners/{p['id']}").status_code == 404
    # ...nor through the victim's org path.
    assert other_exporter.get(f"/api/orgs/{exporter.org_id}/partners").status_code == 404
    # Nor attach the victim's lane or company to its own records.
    rival_cid = other_exporter.company_id()
    r = other_exporter.post(
        f"/api/orgs/{rival_org}/shipments",
        json={"company_id": rival_cid, "lane_id": lane["id"], "reference": "X"},
    )
    assert r.status_code == 422
    r = other_exporter.post(
        f"/api/orgs/{rival_org}/partners",
        json={"company_id": cid, "kind": "buyer", "name": "X", "country": "IN"},
    )
    assert r.status_code == 404


def test_forwarder_manages_client_companies(forwarder: User, exporter: User) -> None:
    org = forwarder.org_id
    assert forwarder.get(f"/api/orgs/{org}/companies").json() == []
    r = forwarder.post(
        f"/api/orgs/{org}/companies", json={"name": "Shree Textiles", "iec_code": "abcde12345"}
    )
    assert r.status_code == 201 and r.json()["iec_code"] == "ABCDE12345"
    assert (
        forwarder.post(f"/api/orgs/{org}/companies", json={"name": "Shree Textiles"}).status_code
        == 409
    )
    assert (
        forwarder.post(
            f"/api/orgs/{org}/companies", json={"name": "Bad", "gstin": "123"}
        ).status_code
        == 422
    )
    # Exporters cannot add client companies.
    assert (
        exporter.post(f"/api/orgs/{exporter.org_id}/companies", json={"name": "Nope"}).status_code
        == 400
    )


def test_port_search(exporter: User) -> None:
    r = exporter.get("/api/ports", params={"q": "nhava", "mode": "sea"})
    assert r.status_code == 200 and r.json()[0]["unlocode"] == "INNSA"
    r = exporter.get("/api/ports", params={"q": "INMUN"})
    assert r.json()[0]["name"] == "Mundra"
    jebel = exporter.get("/api/ports/AEJEA").json()
    assert jebel["lat"] is None and jebel["coord_source"] is None


def test_audit_log_written(exporter: User, db) -> None:  # type: ignore[no-untyped-def]
    from sqlalchemy import select

    from app.models import AuditLog

    _partner(exporter, exporter.company_id())
    actions = set(db.scalars(select(AuditLog.action)))
    assert {"org.created", "partner.created"} <= actions
