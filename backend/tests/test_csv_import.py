from tests.conftest import User


def _upload(u: User, kind: str, content: str, *, dry_run: bool) -> dict:
    r = u.post(
        f"/api/orgs/{u.org_id}/imports/{kind}",
        params={"company_id": u.company_id(), "dry_run": str(dry_run).lower()},
        files={"file": (f"{kind}.csv", content.encode("utf-8"), "text/csv")},
    )
    assert r.status_code == 200, r.text
    return r.json()


PARTNERS = (
    "kind,name,country,city,port\n"
    "buyer,Van Dijk Trading B.V.,Netherlands,Rotterdam,NLRTM\n"
    "supplier,Hamburg Chemie GmbH,DE,Hamburg,DEHAM\n"
)


def test_template_download(client) -> None:  # type: ignore[no-untyped-def]
    r = client.get("/api/imports/templates/shipments.csv")
    assert r.status_code == 200 and r.text.startswith("reference,origin_port")


def test_dry_run_does_not_save(exporter: User) -> None:
    res = _upload(exporter, "partners", PARTNERS, dry_run=True)
    assert res["status"] == "validated" and res["created_count"] == 2
    assert exporter.get(f"/api/orgs/{exporter.org_id}/partners").json()["total"] == 0


def test_import_is_all_or_nothing(exporter: User) -> None:
    bad = PARTNERS + "buyer,Ghost Co,Atlantis,,\nretailer,X,IN,,\n"
    res = _upload(exporter, "partners", bad, dry_run=False)
    assert res["status"] == "rejected" and res["created_count"] == 0
    rows = {e["row"] for e in res["errors"]}
    assert rows == {3, 4}
    assert exporter.get(f"/api/orgs/{exporter.org_id}/partners").json()["total"] == 0


def test_shipments_import_creates_lanes_and_parses_indian_formats(exporter: User) -> None:
    assert _upload(exporter, "partners", PARTNERS, dry_run=False)["status"] == "committed"
    shipments = (
        "reference,origin_port,dest_port,mode,partner_name,etd,eta,container_refs,value_inr,"
        "vessel_imo\n"
        "EXP/26/001,INNSA,NLRTM,sea,van dijk trading b.v.,05/11/2026,01/12/2026,"
        'CSQU3054383,"45,00,000",9074729\n'
        "EXP/26/002,INMUN,NLRTM,,,2026-11-10,,,,\n"
    )
    res = _upload(exporter, "shipments", shipments, dry_run=False)
    assert res["status"] == "committed", res["errors"]
    items = exporter.get(f"/api/orgs/{exporter.org_id}/shipments").json()["items"]
    by_ref = {s["reference"]: s for s in items}
    first = by_ref["EXP/26/001"]
    assert first["etd"] == "2026-11-05" and first["value_inr"] == "4500000.00"
    assert first["partner_name"] == "Van Dijk Trading B.V."
    assert exporter.get(f"/api/orgs/{exporter.org_id}/lanes").json()["total"] == 2


def test_unknown_columns_rejected(exporter: User) -> None:
    r = exporter.post(
        f"/api/orgs/{exporter.org_id}/imports/partners",
        params={"company_id": exporter.company_id()},
        files={"file": ("p.csv", b"kind,name,country,colour\nbuyer,A,IN,red\n", "text/csv")},
    )
    assert r.status_code == 422 and "colour" in r.json()["detail"]
