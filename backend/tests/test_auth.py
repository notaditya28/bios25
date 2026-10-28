from fastapi.testclient import TestClient

from tests.conftest import PASSWORD, Outbox, User, signup


def test_signup_verify_login_me(exporter: User) -> None:
    me = exporter.get("/api/auth/me").json()
    assert me["email"] == "owner@acme.example"
    assert me["email_verified"] is True
    assert me["memberships"][0]["role"] == "owner"
    # Exporters get exactly one implicit "self" company.
    companies = exporter.get(f"/api/orgs/{exporter.org_id}/companies").json()
    assert len(companies) == 1 and companies[0]["is_self"]


def test_session_cookie_flags(client: TestClient, outbox: Outbox) -> None:
    client.post(
        "/api/auth/register",
        json={
            "email": "a@b.example",
            "password": PASSWORD,
            "full_name": "A",
            "org_name": "A Co",
            "org_type": "importer",
        },
    )
    client.post("/api/auth/verify-email", json={"token": outbox.links("/verify-email")[0]})
    r = client.post("/api/auth/login", json={"email": "a@b.example", "password": PASSWORD})
    cookie = r.headers["set-cookie"].lower()
    assert "httponly" in cookie and "samesite=lax" in cookie


def test_unverified_cannot_login(client: TestClient, outbox: Outbox) -> None:
    client.post(
        "/api/auth/register",
        json={
            "email": "new@x.example",
            "password": PASSWORD,
            "full_name": "N",
            "org_name": "N Co",
            "org_type": "exporter",
        },
    )
    r = client.post("/api/auth/login", json={"email": "new@x.example", "password": PASSWORD})
    assert r.status_code == 403 and r.json()["detail"] == "email_unverified"


def test_wrong_password_and_unknown_email_look_the_same(client: TestClient, exporter: User) -> None:
    a = client.post("/api/auth/login", json={"email": exporter.email, "password": "nope-nope-nope"})
    b = client.post(
        "/api/auth/login", json={"email": "ghost@x.example", "password": "whatever-123"}
    )
    assert a.status_code == b.status_code == 401
    assert a.json() == b.json()


def test_register_existing_email_does_not_leak(
    client: TestClient, exporter: User, outbox: Outbox
) -> None:
    before = len(outbox)
    r = client.post(
        "/api/auth/register",
        json={
            "email": exporter.email,
            "password": PASSWORD,
            "full_name": "X",
            "org_name": "Dup",
            "org_type": "exporter",
        },
    )
    assert r.status_code == 202
    # The real owner gets a reset email instead of a second account being made.
    assert len(outbox) == before + 1 and "reset" in outbox[-1].subject.lower()


def test_weak_password_rejected(client: TestClient) -> None:
    r = client.post(
        "/api/auth/register",
        json={
            "email": "w@x.example",
            "password": "short",
            "full_name": "W",
            "org_name": "W",
            "org_type": "exporter",
        },
    )
    assert r.status_code == 422


def test_csrf_required_for_mutations(exporter: User) -> None:
    r = exporter.client.post(f"/api/orgs/{exporter.org_id}/partners", json={})
    assert r.status_code == 403 and "CSRF" in r.json()["detail"]


def test_logout_revokes_session(exporter: User) -> None:
    assert exporter.post("/api/auth/logout").status_code == 200
    assert exporter.get("/api/auth/me").status_code == 401


def test_password_reset_flow_revokes_sessions(exporter: User, outbox: Outbox) -> None:
    anon = TestClient(exporter.client.app)
    assert anon.post("/api/auth/forgot-password", json={"email": exporter.email}).status_code == 202
    token = outbox.links("/reset-password")[-1]
    r = anon.post(
        "/api/auth/reset-password", json={"token": token, "password": "a brand new passphrase"}
    )
    assert r.status_code == 200
    # Old session is gone; token is single-use; new password works.
    assert exporter.get("/api/auth/me").status_code == 401
    assert (
        anon.post(
            "/api/auth/reset-password", json={"token": token, "password": "another passphrase!"}
        ).status_code
        == 400
    )
    assert (
        anon.post(
            "/api/auth/login", json={"email": exporter.email, "password": "a brand new passphrase"}
        ).status_code
        == 200
    )


def test_login_rate_limited(client: TestClient, exporter: User) -> None:
    codes = [
        client.post(
            "/api/auth/login", json={"email": exporter.email, "password": "bad-guess-1"}
        ).status_code
        for _ in range(12)
    ]
    assert codes[-1] == 429


def test_invite_new_user_joins_as_staff(exporter: User, outbox: Outbox) -> None:
    r = exporter.post(
        f"/api/orgs/{exporter.org_id}/invites",
        json={"email": "staff@acme.example", "role": "staff"},
    )
    assert r.status_code == 201
    token = outbox.links("/accept-invite")[-1]
    c = TestClient(exporter.client.app)
    r = c.post(
        "/api/auth/register",
        json={
            "email": "staff@acme.example",
            "password": PASSWORD,
            "full_name": "Staff",
            "invite_token": token,
        },
    )
    assert r.status_code == 202
    me = c.post(
        "/api/auth/login", json={"email": "staff@acme.example", "password": PASSWORD}
    ).json()
    assert me["memberships"][0]["organization_id"] == exporter.org_id
    assert me["memberships"][0]["role"] == "staff"
    staff = User(c, "staff@acme.example", me)
    # Staff cannot invite.
    assert (
        staff.post(
            f"/api/orgs/{exporter.org_id}/invites", json={"email": "x@acme.example"}
        ).status_code
        == 403
    )


def test_last_owner_cannot_leave(exporter: User) -> None:
    r = exporter.delete(f"/api/orgs/{exporter.org_id}/members/{exporter.me['id']}")
    assert r.status_code == 400


def test_invite_for_other_email_rejected(exporter: User, outbox: Outbox) -> None:
    exporter.post(f"/api/orgs/{exporter.org_id}/invites", json={"email": "right@acme.example"})
    token = outbox.links("/accept-invite")[-1]
    other = signup(outbox, "wrong@else.example")
    r = other.post("/api/invites/accept", json={"token": token})
    assert r.status_code == 400
