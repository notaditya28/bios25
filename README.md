# Hydra Stream

A region-aware, Kafka-style message broker: data stays inside its legal region, global traffic
routes around slow links, and a regional outage never freezes the surviving regions.
(SDG 9.1 · Resilient infrastructure.) Design decisions and limitations live in [NOTES.md](NOTES.md).

## Status
- [x] Milestone 1 — single broker: segmented on-disk log, topics/partitions/offsets, HTTP/JSON produce & consume
- [ ] Milestone 2 — in-region replication (3 zones, 2-of-3 acks, leader failover)
- [ ] Milestone 3 — residency enforcement + `residency_audit`
- [ ] Milestone 4 — multi-region, global replication, latency-aware routing
- [ ] Milestone 5 — chaos harness + correctness checker
- [ ] Milestone 6 — dashboard

## Quick start (Windows PowerShell; Go 1.27+)
```powershell
go build -o bin/ ./cmd/...
go test ./...

# start one broker (id, region, zone and address come from configs/cluster.local.json)
.\bin\hydra-broker.exe -id in-1

# in another terminal
.\bin\hydra.exe create-topic -name orders -partitions 3 -residency IN
.\bin\hydra.exe produce -topic orders -n 200000 -batch 2000 -size 100
.\bin\hydra.exe consume -topic orders -partition 0 -offset 0 -max 5
.\bin\hydra.exe consume -topic orders -partition 0 -follow      # long-poll for new messages
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
internal/broker         topics, partitioning, produce/fetch on top of seglog
internal/transport/httpapi  HTTP/JSON adapter for api.Service
internal/client         Go client used by the CLI, tests and (later) the load generator
internal/config         cluster topology (regions, brokers, zones, addresses)
configs/                cluster configs
```
