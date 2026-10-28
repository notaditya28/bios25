# Milestone status

## M1 — Skeleton, auth, orgs, CRUD, CSV import ✅

**Real and working end to end**
- Sign-up → email verification (via Mailpit locally) → sign-in → sessions, logout, password reset,
  password change, invitations, roles (owner/staff), and leaving or removing members (the last owner
  is protected).
- Exporter/importer orgs (one implicit company) and forwarder/CHA orgs (many client companies,
  optional IEC/GSTIN validation).
- Partners, lanes (sea/air, port type enforced) and shipments (ISO 6346 container and IMO
  check-digit validation, INR value, ETD/ETA) with full CRUD, tenant isolation and an audit log.
- CSV import for partners/lanes/shipments: dry run, all-or-nothing, row-level error report,
  downloadable templates.
- **Real port reference data**: 24,808 UN/LOCODE seaports and airports. 15,283 have coordinates
  from WPI, UN/LOCODE or Natural Earth. The data refreshes weekly, and runs are tracked with
  staleness.
- Admin data-source health page and manual port-coordinate override.
- Tests: 46 backend (pytest against real PostGIS + Redis), 6 frontend (vitest). ruff, mypy and
  ESLint are clean.

**Known limitations**
- Some major ports have **no coordinates in any free source**, e.g. **Jebel Ali (AEJEA)**. Delhi
  airport (INDEL) is also missing coordinates because UN/LOCODE has none for it. A platform admin
  must enter these once, with a provenance note, before M3 routing can use them.
- Inland container depots (ICDs, UN/LOCODE function 6, e.g. Tughlakabad) are not yet selectable as
  lane origins. This will be decided in M3 together with routing (ICD → gateway port).
- UN/LOCODE has no explicit licence from UNECE (see DATA_SOURCES.md).
- Postgres row-level security is not yet enabled; isolation is enforced in the API layer and tested.
- Email is SMTP/Mailpit locally. The Postmark backend is written but not exercised until there is
  an account.

**Not started (by design):** sanctions (M2), routing/map (M3), events/LLM (M4), scoring (M5),
notifications/admin tuning (M6), hardening/deploy (M7).
