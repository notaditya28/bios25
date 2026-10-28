# TradeRadar

Trade-disruption and sanctions-risk monitoring for Indian SME exporters, importers, and the
freight forwarders / customs house agents (CHAs) who serve them.

**Status: Milestone 2 of 7.** M1 delivered the skeleton, auth, organizations, partner/lane/shipment
CRUD, CSV import and real port reference data. M2 adds sanctions screening against the OFAC, UN, EU
and UK lists, with a human review queue. See [docs/MILESTONES.md](docs/MILESTONES.md) for what is real, what is
pending, and the known limitations.

## Stack

FastAPI · SQLAlchemy 2 + Alembic · PostgreSQL 16 + PostGIS · Celery + Redis · Next.js 15 (TypeScript,
Tailwind) · Docker Compose. Design notes are in [ARCHITECTURE.md](ARCHITECTURE.md), and source
licensing is in [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md).

## Run locally

Prerequisites: Docker Desktop (or Docker Engine + Compose v2). Nothing else is needed on the host.

```bash
cp .env.example .env            # then set SECRET_KEY and POSTGRES_PASSWORD (also inside DATABASE_URL)
docker compose up -d --build    # db, redis, api, worker, beat, web, mailpit
docker compose exec api python -m app.cli refresh-ports       # ~25k real ports/airports (≈30 s)
docker compose exec api python -m app.cli refresh-sanctions   # OFAC/UN/EU/UK lists (≈2 min first time)
```

| URL | What |
|---|---|
| http://localhost:3000 | Web app |
| http://localhost:8000/api/docs | API docs (disabled when `APP_ENV=production`) |
| http://localhost:8025 | Mailpit — catches every email sent in development (verification links etc.) |

First user:

1. Open http://localhost:3000/register, then sign up as an exporter, importer or forwarder.
2. Open the verification email in Mailpit and click the link, then sign in.
3. Optional: make yourself a platform admin to see the data-source health page:
   `docker compose exec api python -m app.cli make-admin you@yourcompany.com`

Celery beat refreshes ports weekly (Sunday 02:17 UTC) and sanctions lists every 6 hours, then
re-screens all counterparties whenever a list changes. A full re-screen also runs daily at 03:41 UTC.

### Demo data (development only)

```bash
docker compose exec -e ALLOW_DEV_SEED=true api python scripts/seed_dev.py
```

The seed script refuses to run unless `APP_ENV=development` and `ALLOW_DEV_SEED=true`. Every seeded
row has `is_demo=true`, and the UI shows a **DEMO DATA** banner wherever such rows appear.

## Tests and linting

```bash
docker compose exec api pytest                 # uses a separate <db>_test database + Redis DB 15
docker compose exec api ruff check .
docker compose exec api mypy app
cd frontend && npm ci && npm run lint && npm run typecheck && npm test
```

## Repository layout

```
backend/   FastAPI app (app/), Alembic migrations, Celery tasks, data-source adapters, tests
frontend/  Next.js app (src/app routes, src/lib API client + session, src/components)
docs/      Data-source licensing, milestone status
```

## Configuration

All configuration comes from environment variables; see [.env.example](.env.example). Secrets never
live in code. In production the app refuses to start with `COOKIE_SECURE=false`, a placeholder
`SECRET_KEY`, or `ALLOW_DEV_SEED=true`.

## Data attribution

Port reference data: UN/LOCODE (UNECE); World Port Index (US National Geospatial-Intelligence
Agency); Made with Natural Earth. Sanctions data: US Treasury OFAC; UN Security Council; European
Commission; UK FCDO (contains public sector information licensed under the Open Government Licence
v3.0).
