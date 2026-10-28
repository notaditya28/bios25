# Data sources: licensing and access

Verified 28 Oct 2026. Re-check terms before launch and whenever a provider changes them.

## In use (Milestone 1)

| Source | Access | Commercial use | Attribution |
|---|---|---|---|
| UN/LOCODE (UNECE), `unlocode.unece.org/downloads/unlocode-latest.zip` | No key | Free UN standard for trade locations. **UNECE publishes no explicit data licence.** Using it as reference codes inside the product is standard industry practice. Ask UNECE before redistributing the raw list as a dataset. | "UN/LOCODE, UNECE" |
| NGA World Port Index (Pub. 150), `msi.nga.mil/api/publications/world-port-index?output=csv` | No key | US Government work, no copyright claimed | "World Port Index, US NGA" |
| Natural Earth 1:10m ports | No key | Public domain | "Made with Natural Earth" |

## Planned (later milestones) — see the approved plan for full notes

| Source | Milestone | Key / account | Commercial notes |
|---|---|---|---|
| OFAC SDN + Consolidated (Advanced XML) | M2 | none | US Gov public data |
| UN Security Council Consolidated List | M2 | none | public |
| EU Financial Sanctions Files | M2 | `EU_FSF_TOKEN` (free registration) | public |
| UK Sanctions List (FCDO) | M2 | none | OGL. The OFSI list closed on 28 Jan 2026. |
| searoute-py | M3 | none | Apache-2.0 |
| aisstream.io | M3 | `AISSTREAM_API_KEY` | No published ToS or SLA. Behind an adapter so it can be swapped. |
| Open-Meteo | M3 | `OPEN_METEO_API_KEY` | **Free tier is non-commercial. A paid plan is required before launch.** |
| IMF PortWatch | M3 | n/a | **Disabled until IMF grants commercial permission** (copyright@imf.org) |
| GDELT 2.0 | M4 | none | Free incl. commercial. Must cite GDELT and link gdeltproject.org. |
| Anthropic Claude API | M4 | `ANTHROPIC_API_KEY` | paid |
| Frankfurter FX | M5 | none | Blended rates free for commercial use |
| Freight indices (FBX/WCI) | M5 | — | No compliant free feed. Manual admin entry. |
| Postmark | M6 | `POSTMARK_SERVER_TOKEN` | paid |

Excluded: ACLED (needs a corporate licence and restricts AI use), and OpenSanctions data (CC
BY-NC; needs a paid licence).
