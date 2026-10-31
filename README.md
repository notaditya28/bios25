# Hydra Stream

A region-aware, Kafka-style message broker: data stays inside its legal region, global traffic
routes around slow links, and a regional outage never freezes the surviving regions.
(SDG 9.1 · Resilient infrastructure.) Design decisions and limitations live in [NOTES.md](NOTES.md).

## Status
- [x] Milestone 1 — single broker: segmented on-disk log, topics/partitions/offsets, HTTP/JSON produce & consume
- [x] Milestone 2 — in-region replication (3 zones, 2-of-3 acks, lease-elected controller, epoch-fenced leader failover)
- [ ] Milestone 3 — residency enforcement + `residency_audit`
- [ ] Milestone 4 — multi-region, global replication, latency-aware routing
- [ ] Milestone 5 — chaos harness + correctness checker
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
scripts/hydra.ps1       start / stop / kill / restart brokers as local processes
internal/transport/httpapi  HTTP/JSON adapter for api.Service
internal/client         Go client used by the CLI, tests and (later) the load generator
internal/config         cluster topology (regions, brokers, zones, addresses)
configs/                cluster configs
```
