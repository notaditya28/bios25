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

## M2 — Sanctions ingestion, screening, review queue ✅

**Real and working end to end**
- Five official lists are ingested from the publishers' own downloads every 6 hours: **OFAC SDN**
  (19,471 entries), **OFAC Consolidated non-SDN** (486), **UN Security Council** (1,007), **EU FSF**
  (6,269) and the **UK Sanctions List** (6,420). That is about 106k names in Latin, Cyrillic,
  Arabic, CJK and other scripts, plus 2,288 vessel IMO numbers.
- Content-hash change detection: an unchanged file is skipped in seconds. Entries are diff-applied
  (added / updated / delisted). A file that parses to fewer than 50% of the entries held is refused,
  so a truncated download can't delist anyone.
- Transliteration-aware matcher: any script → Latin, legal-form / title / particle stripping, and
  phonetic skeletons (Mohammed = Muhammad = Mukhammad; Yevgeny = Evgeniy). Token-alignment scoring
  down-weights generic business words, and every adjustment is explained in the breakdown.
  Vessels match exactly on IMO.
- Partners, client companies and shipment vessels are screened automatically on save and
  re-screened whenever any list changes, plus a daily safety-net run.
- Review queue: confirm or dismiss with a mandatory note, reviewer and time recorded, and every
  decision in the audit log. Decisions persist across re-screens, **but reopen automatically if the
  subject's details or the list entry change**. Delisted entries leave the queue. Owners can reopen
  a decision manually.
- Partner list badges (Clear / N to review / Sanctioned match). List freshness and a staleness
  warning appear on the screening page.
- Checked against the live lists: Rosneft Trading S.A. → OFAC SDN + CONS (and its parent PJSC
  Rosneft at 85). Vessel "Jal Vahini" IMO 9562233 → UK-listed *Petrel 8*. Ordinary Indian/EU
  company names come back clear.
- Tests: 84 backend (matcher quality set, parsers on real-structure excerpts, full
  ingest → screen → review flows, tenant isolation). ruff, mypy and ESLint are clean.

**Known limitations**
- The EU download currently uses the **public token the Commission publishes**. Register your own
  FSF account for production and set `EU_FSF_TOKEN`.
- The 85-point review threshold is tuned on a curated test set, not on your real counterparties.
  Expect some false positives (deliberately: recall over precision), and tune
  `SCREENING_REVIEW_THRESHOLD` after the first weeks of real reviews.
- An entity that appears on several lists (e.g. OFAC SDN and CONS) shows one card per list. Grouping
  them is a UX improvement for later.
- Individuals are matched on name, country and alias quality only. Dates of birth are shown to the
  reviewer but not scored, because partner records don't capture a DOB.
- An exporter's own company is screened by the daily run, not at sign-up.
- New matches are not yet notified by email (M6).
- This is a screening aid, not legal advice; a human must review every potential match.
- India's MHA list of organisations banned under UAPA is not included (optional addition).

**Not started (by design):** routing/map (M3), events/LLM (M4), scoring (M5),
notifications/admin tuning (M6), hardening/deploy (M7).
