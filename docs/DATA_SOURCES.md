# Data sources: licensing and access

Verified 28 Oct 2026. Re-check terms before launch and whenever a provider changes them.

## In use (Milestone 1)

| Source | Access | Commercial use | Attribution |
|---|---|---|---|
| UN/LOCODE (UNECE), `unlocode.unece.org/downloads/unlocode-latest.zip` | No key | Free UN standard for trade locations. **UNECE publishes no explicit data licence.** Using it as reference codes inside the product is standard industry practice. Ask UNECE before redistributing the raw list as a dataset. | "UN/LOCODE, UNECE" |
| NGA World Port Index (Pub. 150), `msi.nga.mil/api/publications/world-port-index?output=csv` | No key | US Government work, no copyright claimed | "World Port Index, US NGA" |
| Natural Earth 1:10m ports | No key | Public domain | "Made with Natural Earth" |

## In use (Milestone 2) — sanctions

| List | URL | Access | Licence |
|---|---|---|---|
| OFAC SDN | `sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ENHANCED.XML` | No key | US Gov public data |
| OFAC Consolidated (non-SDN) | `.../exports/CONS_ENHANCED.XML` | No key | US Gov public data |
| UN Security Council | `scsanctions.un.org/resources/xml/en/consolidated.xml` | No key | Public UN document |
| EU FSF | `webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content?token=…` | `EU_FSF_TOKEN` (the published public token works; register your own) | Public EU data |
| UK Sanctions List | `sanctionslist.fcdo.gov.uk/docs/UK-Sanctions-List.xml` | No key | Open Government Licence v3.0 (attribution shown in the app footer) |

The matching libraries are all permissively licensed: `rapidfuzz` (MIT), `jellyfish` (MIT) and
`anyascii` (ISC). `Unidecode` was avoided because it is GPL.

## Planned (later milestones) — see the approved plan for full notes

| Source | Milestone | Key / account | Commercial notes |
|---|---|---|---|
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
