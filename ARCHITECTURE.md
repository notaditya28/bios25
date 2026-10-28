# Architecture

## Components

```
Browser ──► Next.js (web, :3000) ──/api/* rewrite──► FastAPI (api, :8000) ──► PostgreSQL + PostGIS
                                                         │                       ▲
                                                         └── enqueue ──► Redis ──► Celery worker ──┘
                                                                           ▲
                                                               Celery beat (schedules)
```

- **web**: Next.js App Router. All API calls go to the same origin (`/api/...`), which Next proxies to
  FastAPI. The session cookie therefore stays first-party, and no CORS is configured at all.
- **api**: FastAPI with synchronous SQLAlchemy 2 (psycopg 3). Sync routes run in FastAPI's
  threadpool, and the same ORM code is shared with Celery tasks. This deliberately departs from the
  "async SQLAlchemy" in the original plan: it keeps one code path for the web and the workers.
- **worker / beat**: Celery on Redis. They run email delivery (retried with backoff) and scheduled
  data ingestion. From M3, a separate long-running `ais-consumer` service will hold the AIS websocket.
- **db**: PostgreSQL 16 with PostGIS (port geometry, and later routes and events), `pg_trgm` (fuzzy
  port and name search) and `citext` (case-insensitive emails).

## Multi-tenancy

- `organizations` → `memberships` (role `owner` | `staff`) → `users`.
- `client_companies` belong to an organization. Exporters and importers get exactly one (`is_self`).
  Forwarders/CHAs create one per client.
- `partners`, `lanes`, `shipments` and `csv_imports` carry **both** `organization_id` and
  `company_id`.
- Every tenant route is `/api/orgs/{org_id}/...`. The `get_org` dependency checks membership and
  returns 404 (not 403), so org ids cannot be probed. Writes go through `app/services/trade.py`,
  which re-checks that every referenced partner/lane belongs to the same company, so a forged id
  from another tenant is rejected.
- Planned for M7: Postgres row-level security keyed on `organization_id` as defence in depth.

## Authentication

- Argon2id password hashing (`argon2-cffi`). Hashes are upgraded on login if the parameters change.
- Opaque random session token in an `httpOnly`, `SameSite=Lax` cookie (`Secure` in production).
  Only the SHA-256 of the token is stored (`user_sessions`), so a DB leak exposes no live sessions.
- CSRF: a per-session token returned by `/api/auth/me` must be echoed in `X-CSRF-Token` on every
  non-GET request.
- Email verification, password reset and invitations use single-use, hashed, expiring tokens
  (`auth_tokens`). A password reset revokes all sessions. A password change revokes all *other*
  sessions.
- No account enumeration: register, forgot-password and resend-verification always return the same
  message. Login hashes against a dummy hash when the email is unknown.
- Redis fixed-window rate limits on login (per email and per IP), register, resend and forgot.
  They fail open if Redis is down, and `/readyz` reports that.
- `oauth_identities` is reserved for Google sign-in.

## Data sources and degradation

Every external source is described by a `SourceSpec` (URL, licence note, attribution, staleness
threshold) and registered in `data_sources`. Each run is wrapped in `track_run()`:

1. A `source_runs` row is written in its own transaction (`running`).
2. Downloads go to a cache file via a temp file plus atomic rename, with tenacity retries on 5xx/429
   and transport errors.
3. Sanity guards abort before touching the DB if the data looks wrong (e.g. UN/LOCODE parsing
   yields fewer than 10k ports).
4. On failure the run is marked `failed`, `data_sources.last_error` is set, and **the previous good
   data is left untouched**. On success `last_success_at` / `last_data_as_of` are updated.
5. `/api/sources` exposes freshness (stale = older than `stale_after_hours`) for UI badges.
   `/api/admin/sources` adds errors, licence notes and recent runs.

### Port reference data (M1)

UN/LOCODE is the master list (seaports, function `1`, and airports, function `4`). It has about
24.8k entries, but many lack coordinates. Coordinates are merged by priority:

| Priority | Source | Notes |
|---|---|---|
| 0 | Manual (platform admin) | Never overwritten by ingestion. Audit-logged with a provenance note. |
| 1 | NGA World Port Index | Precise. Matched by UN/LOCODE (about 2.3k ports). |
| 2 | UN/LOCODE | Minute precision. |
| 3 | Natural Earth ports | Name-matched only when unambiguous and within 500 km of a known port in the same country. |

When a refresh brings no coordinates, previously known ones are kept. Ports with no coordinates
stay selectable and are flagged in the UI. Route analysis (M3) skips them until an admin sets
coordinates.

## CSV import

All-or-nothing. Each row is validated inside a savepoint, and the transaction commits only if every
row passed. `dry_run=true` validates without saving. Shipments CSVs auto-create missing lanes. The
importer accepts ISO and Indian day-first dates and lakh-grouped numbers (`45,00,000`). Container
numbers are checked against the ISO 6346 check digit and vessel IMO numbers against the IMO check
digit.

## Auditability

`audit_log` records who changed what (before/after JSON) for org, membership, company, partner,
lane, shipment, password and port-coordinate changes. From M2 it also records sanctions review
decisions.

## Logging

structlog JSON to stdout, with a request id per request (echoed in `X-Request-ID`). Celery uses the
same configuration.

## Key decisions (and why)

| Decision | Reason |
|---|---|
| UN/LOCODE as port master list, not the World Port Index alone | The current NGA WPI API returns about 2.9k ports and is missing JNPT, Chennai, Kolkata, Jebel Ali and Colombo, which are critical for Indian lanes. |
| Server-side sessions over JWT | Instant revocation (logout, reset, disable), no token-refresh complexity, and the cookie never readable by JS. |
| Same-origin proxy instead of CORS | Simpler and safer cookie handling, with one public origin to put behind HTTPS. |
| All-or-nothing CSV import | Partial imports leave users unsure what was saved; one clear error report is easier to act on. |
