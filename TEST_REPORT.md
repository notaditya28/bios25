# Hydra Stream — test report (final build, 2026-11-01)

Every check below was run on the final build on one Windows laptop (20 cores, Go 1.27). The command
to repeat each one is in the first column; how to read the output is in [TESTING.md](TESTING.md).

| # | Check | Command | Result |
|---|---|---|---|
| 1 | Unit + cluster tests (fresh, no cache) | `go test ./... -count=1` | **all packages ok** (10 packages with tests) |
| 2 | Race detector | `go test -race ./... -count=1` (gcc on PATH) | **ok, no races** |
| 3 | Static checks | `go vet ./...`, `gofmt -l` | clean |
| 4 | Required failure tests | `go test ./internal/broker -run "Leader\|Controller\|Stale" -v` | controller crash, leader crash, stale leader after a pause: **PASS** |
| 5 | Feature tests | `go test ./internal/broker -run "Jurisdiction\|Checkpoints\|AcksModes\|Dedup\|Simulator" -v` | **PASS**; 1 vs 100 redeliveries; 655,200 bytes deduped; simulator **18/18** agree |
| 6 | Chaos, Hydra (real processes, 7 scenarios) | `.\scripts\hydra.ps1 chaos` | **7/7 PASS**; 0 lost / 0 duplicates / 0 order violations over ~1.07M acked messages; audit PASS before/during/after every fault; surviving-region stall ≤ 33 ms |
| 7 | Hydra vs single global controller | `.\scripts\hydra.ps1 compare` | Hydra **7/7**, baseline **6/7**: cut-off region losing its leader — Hydra back in **0.59 s**, baseline frozen **8.25 s** (3 of 3 runs). `results/compare-20261101-084600.md` |
| 8 | Benchmarks | `.\bin\hydra-bench.exe` | live p95 **57 ms** adaptive vs 633 ms FIFO / 628 ms fixed; urgent misses **0%** vs 100% / 70%; crash duplicates **67** vs 587; policy check 0.1–1.4 µs |
| 9 | Simulator | `.\bin\hydra-sim.exe -preset guide`, `-broker 127.0.0.1:9101` | six what-ifs with findings; live mode sees all 12 demo partitions |
| 10 | Live demo + dashboard | `.\scripts\hydra.ps1 demo` | 3 regions up, audit PASS, policy v installed, per-region decisions (US/IN denials for forbidden classes); kill / restart region, isolate, slow link, cap links, heal all — all work, audit PASS throughout |
| 11 | Audit catches a violation | copy `data\in-1\topics\in-orders` → `data\eu-2\...`, run `residency_audit -online` | PASS (exit 0) → **FAIL, 2 violations (exit 1)**, dashboard badge FAIL → PASS after cleanup |
| 12 | Containers | `docker compose up --build -d` | image builds; 9 containers, a controller per region, policies installed; topics, cross-region reads, `WRONG_REGION` refusal; leader crash → 1.12 s; whole IN region killed → EU/US global writes max stall 16 ms, IN topic `REGION_UNAVAILABLE`; IN rejoins |

## Bugs found by this round of testing (all fixed and re-tested)
- **No connect timeout** in the broker RPC client and the Go client: in containers a killed broker's
  address drops packets, so the first write after a leader crash waited 4.05 s. Now a 1 s dial
  timeout → 1.12 s. (Same situation as a machine vanishing on a real network.)
- **`hydra-sim -broker` missed topics** stored only in other regions (saw 9 of 12 partitions); it now
  asks one broker per region.
- **Simulator summary text** attributed every unavailable partition to residency law; it now
  separates resident data from `acks=all` policy partitions.
- **Comparison claims corrected**: an early compare run showed the baseline 2–9× slower and failing
  kill-broker once; three later runs did not reproduce that, so only the deterministic difference
  (isolated-region failover) is claimed.

## Not covered
- Real WAN latencies and packet loss (regions are processes/containers on one laptop; delays are injected).
- Long soak tests (runs are minutes, not hours); no retention/compaction/auth to test (not built).
