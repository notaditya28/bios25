"""Tests run against a real PostGIS database (`<db>_test`) and Redis DB 15 inside docker compose.

docker compose run --rm api pytest
"""

import os
import re
from collections.abc import Iterator

import psycopg

_base = os.environ["DATABASE_URL"]
TEST_DB_URL = re.sub(r"/([^/?]+)(\?|$)", r"/\1_test\2", _base, count=1)
os.environ["DATABASE_URL"] = TEST_DB_URL
os.environ["REDIS_URL"] = re.sub(
    r"/\d+$", "/15", os.environ.get("REDIS_URL", "redis://redis:6379/0")
)
os.environ["APP_ENV"] = "test"
os.environ["APP_BASE_URL"] = "http://testserver"

import pytest  # noqa: E402
import redis  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import text  # noqa: E402

from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402


def _create_test_database() -> None:
    admin_url = _base.replace("postgresql+psycopg://", "postgresql://")
    dbname = TEST_DB_URL.rsplit("/", 1)[-1].split("?")[0]
    with psycopg.connect(admin_url, autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)).fetchone()
        if not exists:
            conn.execute(f'CREATE DATABASE "{dbname}"')


_create_test_database()
_cfg = Config(os.path.join(os.path.dirname(__file__), "..", "alembic.ini"))
_cfg.set_main_option("script_location", os.path.join(os.path.dirname(__file__), "..", "alembic"))
_cfg.attributes["database_url"] = TEST_DB_URL
command.upgrade(_cfg, "head")

from app.db import SessionLocal, engine  # noqa: E402
from app.main import app  # noqa: E402

TENANT_TABLES = [
    "shipments",
    "lanes",
    "partners",
    "csv_imports",
    "client_companies",
    "memberships",
    "auth_tokens",
    "user_sessions",
    "oauth_identities",
    "audit_log",
    "users",
    "organizations",
    "source_runs",
    "data_sources",
]

# A few real reference ports (coordinates from UN/LOCODE / NGA WPI), enough for routing tests.
TEST_PORTS = [
    ("INNSA", "Jawaharlal Nehru (Nhava Sheva)", "IN", True, False, 18.95, 72.93),
    ("INMUN", "Mundra", "IN", True, False, 22.728, 69.707),
    ("INMAA", "Chennai (ex Madras)", "IN", True, True, 13.083, 80.283),
    ("NLRTM", "Rotterdam", "NL", True, False, 51.9, 4.483),
    ("DEHAM", "Hamburg", "DE", True, False, 53.55, 9.933),
    ("AEJEA", "Jebel Ali", "AE", True, False, None, None),
    ("INDEL", "Delhi", "IN", False, True, 28.567, 77.1),
    ("GBLHR", "Heathrow Apt/London", "GB", False, True, 51.467, -0.45),
]


@pytest.fixture(scope="session", autouse=True)
def _ports() -> None:
    with engine.begin() as conn:
        for code, name, cc, sea, air, lat, lon in TEST_PORTS:
            conn.execute(
                text(
                    """
                    INSERT INTO ports (unlocode, name, name_ascii, country_iso2, is_seaport,
                                       is_airport, geom, coord_source)
                    VALUES (:c, :n, :n, :cc, :sea, :air,
                            CASE WHEN CAST(:lat AS float) IS NULL THEN NULL ELSE
                              ST_SetSRID(ST_MakePoint(CAST(:lon AS float), CAST(:lat AS float)),
                                         4326)::geography END,
                            CASE WHEN CAST(:lat AS float) IS NULL THEN NULL
                                 ELSE 'unlocode'::coord_source END)
                    ON CONFLICT (unlocode) DO NOTHING
                    """
                ),
                {"c": code, "n": name, "cc": cc, "sea": sea, "air": air, "lat": lat, "lon": lon},
            )


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(TENANT_TABLES)} CASCADE"))
    redis.Redis.from_url(os.environ["REDIS_URL"]).flushdb()
    yield


class Outbox(list):
    def links(self, path: str) -> list[str]:
        out = []
        for m in self:
            out += re.findall(rf"http://testserver{re.escape(path)}\?token=([\w-]+)", m.text)
        return out


@pytest.fixture(autouse=True)
def outbox(monkeypatch: pytest.MonkeyPatch) -> Outbox:
    box = Outbox()
    import app.api.auth
    import app.api.orgs

    monkeypatch.setattr(app.api.auth, "enqueue", box.append)
    monkeypatch.setattr(app.api.orgs, "enqueue", box.append)
    return box


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def db() -> Iterator:
    with SessionLocal() as s:
        yield s


PASSWORD = "correct horse battery"


class User:
    def __init__(self, client: TestClient, email: str, me: dict):
        self.client = client
        self.email = email
        self.me = me

    @property
    def org_id(self) -> str:
        return self.me["memberships"][0]["organization_id"]

    def _h(self) -> dict:
        return {"X-CSRF-Token": self.me["csrf_token"]}

    def get(self, url: str, **kw):  # type: ignore[no-untyped-def]
        return self.client.get(url, **kw)

    def post(self, url: str, **kw):  # type: ignore[no-untyped-def]
        return self.client.post(url, headers=self._h(), **kw)

    def put(self, url: str, **kw):  # type: ignore[no-untyped-def]
        return self.client.put(url, headers=self._h(), **kw)

    def patch(self, url: str, **kw):  # type: ignore[no-untyped-def]
        return self.client.patch(url, headers=self._h(), **kw)

    def delete(self, url: str, **kw):  # type: ignore[no-untyped-def]
        return self.client.delete(url, headers=self._h(), **kw)

    def company_id(self) -> str:
        return self.get(f"/api/orgs/{self.org_id}/companies").json()[0]["id"]


def signup(
    outbox: Outbox, email: str, org_type: str = "exporter", org_name: str = "Acme Exports"
) -> User:
    """Register → verify → log in, through the public API only."""
    c = TestClient(app)
    r = c.post(
        "/api/auth/register",
        json={
            "email": email,
            "password": PASSWORD,
            "full_name": "Test User",
            "org_name": org_name,
            "org_type": org_type,
        },
    )
    assert r.status_code == 202, r.text
    token = outbox.links("/verify-email")[-1]
    assert c.post("/api/auth/verify-email", json={"token": token}).status_code == 200
    r = c.post("/api/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 200, r.text
    return User(c, email, r.json())


@pytest.fixture
def exporter(outbox: Outbox) -> User:
    return signup(outbox, "owner@acme.example")


@pytest.fixture
def other_exporter(outbox: Outbox) -> User:
    return signup(outbox, "owner@rival.example", org_name="Rival Exports")


@pytest.fixture
def forwarder(outbox: Outbox) -> User:
    return signup(outbox, "ops@forwarder.example", org_type="forwarder", org_name="Swift CHA")
