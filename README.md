# Hydra Stream

A region-aware, Kafka-style message broker: data stays inside its legal region, global traffic
routes around slow links, and a regional outage never freezes the surviving regions.
(SDG 9.1 · Resilient infrastructure.) Design decisions and limitations live in [NOTES.md](NOTES.md).

## Status
- [x] Milestone 1 — single broker: segmented on-disk log, topics/partitions/offsets, HTTP/JSON produce & consume
- [x] Milestone 2 — in-region replication (3 zones, 2-of-3 acks, lease-elected controller, epoch-fenced leader failover)
- [x] Milestone 3 — residency enforcement at every data path + signed manifests + `residency_audit`
- [x] Milestone 4 — independent regions; global topics with cross-region quorum and self-elected leaders; latency-aware relay routing
- [x] Milestone 5 — idempotent producer; `hydra-chaos` harness + correctness checker (all 6 scenarios PASS)
- [ ] Milestone 6 — dashboard

## Quick start (Windows PowerShell; Go 1.27+)
```powershell
go test ./...

# build and start the IN region: 3 brokers = 3 zones (ids, zones, addresses from configs/cluster.local.json)
.\scripts\hydra.ps1 start -Region IN
.\scripts\hydra.ps1 status

.\bin\hydra.exe create-topic -name orders -partitions 3 -residency IN
.\bin\hydra.exe produce -topic orders -n 200000 -batch 2000 -size 100
.\bin\hydra.exe consume -topic orders -partition 0 -offset 0 -max 5
.\bin\hydra.exe consume -topic orders -partition 0 -follow      # long-poll for new messages
```

Kill the controller under load (the producer retries through the surviving brokers):
```powershell
Invoke-RestMethod http://127.0.0.1:9101/admin/v1/status        # who is controller?
Start-Job { Set-Location $using:PWD; .\bin\hydra.exe produce -b "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103" -topic orders -n 1500000 -batch 2000 -keys -retry 15s }
.\scripts\hydra.ps1 kill -Id in-3                               # use the controller's id
Get-Job | Receive-Job -Wait                                     # prints throughput and max stall
.\scripts\hydra.ps1 restart -Id in-3                            # comes back as a follower and catches up
.\scripts\hydra.ps1 stop
```

Residency (all 9 brokers: IN, EU, US × 3 zones):
```powershell
.\scripts\hydra.ps1 start
.\bin\hydra.exe create-topic -b 127.0.0.1:9101 -name aadhaar -partitions 3 -residency IN
.\bin\hydra.exe create-topic -b 127.0.0.1:9201 -name aadhaar-copy -partitions 1 -residency IN   # RESIDENCY_VIOLATION
.\bin\hydra.exe produce -b 127.0.0.1:9101 -topic aadhaar -n 200000 -keys
.\bin\residency_audit.exe -online                                   # PASS, manifests verified
Copy-Item -Recurse data\in-1\topics\aadhaar data\eu-2\topics\aadhaar # simulate misplaced data
.\bin\residency_audit.exe -online                                   # FAIL, exit code 1
Remove-Item -Recurse data\eu-2\topics\aadhaar
```

Multi-region (all 9 brokers running):
```powershell
.\bin\hydra.exe create-topic -b 127.0.0.1:9101 -name prices -partitions 3 -residency global   # one replica per region
.\bin\hydra.exe produce -b 127.0.0.1:9201 -topic prices -n 100000 -keys                        # from EU
.\bin\hydra.exe consume -b 127.0.0.1:9301 -topic prices -partition 0 -quiet                    # from US
Invoke-RestMethod http://127.0.0.1:9201/admin/v1/routes | ForEach-Object links | Format-Table # measured RTTs + relay choice
# slow IN<->EU by 150 ms each way (POST the same to eu-1..3 with the in-* ids):
Invoke-RestMethod -Method Post http://127.0.0.1:9101/admin/v1/net -ContentType application/json -Body '{"blocked":[],"delays_ms":{"eu-1":150,"eu-2":150,"eu-3":150}}'
.\scripts\hydra.ps1 kill -Region IN                                                            # whole region down
```

Chaos harness — starts all 9 brokers itself, runs every scenario under load, verifies every
acknowledged message and the residency audit, prints the results table (≈2 minutes):
```powershell
go build -o bin/ ./cmd/...
.\bin\hydra-chaos.exe                                  # all scenarios; exit code 1 if any fails
.\bin\hydra-chaos.exe -only kill-region,isolate-region # a subset
```
Results also land in `results/chaos-<time>.md` and `.json`. Latest run: see NOTES.md
("Chaos results") — 0 lost, 0 duplicates, 0 order violations in all 6 scenarios.

Raw HTTP works too:
```powershell
curl.exe -s -X POST localhost:9101/v1/topics/orders/records -d '{\"records\":[{\"key\":\"u1\",\"value\":\"hello\"}]}'
curl.exe -s "localhost:9101/v1/topics/orders/partitions/0/records?offset=0&max_records=5"
```

## Layout
```
cmd/hydra-broker        broker process
cmd/hydra               CLI (topics, produce, consume)
internal/api            protocol-independent Service interface, wire types, typed errors
internal/seglog         append-only segmented log (plain files, CRC-checked, crash recovery)
internal/broker         api.Service: routing/forwarding to leaders, topic placement, leader elections
internal/coord          Coordinator interface + lease-based controller election and metadata commit
internal/replica        partition replicas: 2-of-3 acks, epoch fencing, follower truncation
internal/meta           region metadata (topics, replicas, leaders, epochs)
internal/rpc            broker-to-broker JSON/HTTP + link blocker for partition simulation
internal/geo            inter-region latency probes, relay path selection, topic gossip
internal/residency      residency guard used at every store / replicate / relay / place decision
internal/layout         on-disk layout of a broker data directory + scanner
internal/manifest       signed (Ed25519) per-broker segment manifests
internal/audit          residency audit rules and compliance report
cmd/residency_audit     audit CLI (exit 1 on violations)
cmd/hydra-chaos         chaos harness: real processes, faults, load, checker, results table
internal/chaos          process control, fault injection, load streams, verification, report
internal/client         also: idempotent Producer (producer id + sequence, retry without duplicates)
scripts/hydra.ps1       start / stop / kill / restart brokers as local processes
internal/transport/httpapi  HTTP/JSON adapter for api.Service
internal/client         Go client used by the CLI, tests and (later) the load generator
internal/config         cluster topology (regions, brokers, zones, addresses)
configs/                cluster configs
```
