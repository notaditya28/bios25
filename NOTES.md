# Hydra Stream — design notes

Running log of decisions, assumptions and known limitations. Items marked ★ are
pending confirmation from the organisers.

## Ground rules
- Language: Go (stdlib only so far). Solo team.
- Banned: existing brokers/queues (Kafka, Redpanda, Pulsar, NATS, RabbitMQ) and embedded
  databases for the log (RocksDB, SQLite, ...). The log is plain segment files we write ourselves.
- Allowed: stdlib, an HTTP framework, a JSON library, a crypto/hash library, a frontend library.
  Docker for local testing only. No cloud services — everything runs on one laptop.
- ★ Raft libraries (hashicorp/raft, etcd/raft) may or may not be allowed. Coordination is built
  behind a `LeaderElector` / `Coordinator` interface so a library can be swapped in later.

## Definitions (working defaults)
- Region = a group of broker processes on one machine (default 3 regions IN/EU/US × 3 brokers,
  each broker = one zone). Every broker's id, region, zone and address comes from a cluster
  config file, so the same binary can run in docker-compose (one network per region). ★
- Regional outage = two chaos scenarios:
  (a) crash — every broker in the region is killed;
  (b) partition — the region is alive but cut off from the other regions.
- Freezing targets ★: < 1 s stall for clients in surviving regions; < 500 ms to return an
  explicit error for anything genuinely unavailable. Both measured by the chaos harness.

## Protocol
- HTTP/JSON for produce, consume and admin. ★ Organisers may require a different protocol.
- The broker implements `api.Service`; HTTP is just an adapter (`internal/transport/httpapi`),
  so gRPC could be added as a second adapter without touching broker code.
- Produce is batched: one request carries many records (up to 100k).
- Keys and values are JSON strings (UTF-8). Binary payloads would need base64 — not needed for the demo.
- Errors are explicit: `{"error":{"code":"UNKNOWN_TOPIC","message":"..."}}` with a matching HTTP status.

### Endpoints (Milestone 1)
| Method | Path | Purpose |
|---|---|---|
| GET  | `/healthz` | liveness |
| GET  | `/v1/broker` | broker id / region / zone |
| GET  | `/v1/topics` | list topics |
| POST | `/v1/topics` | create topic `{name, partitions, residency}` (idempotent for identical config) |
| GET  | `/v1/topics/{topic}` | topic info incl. per-partition offsets |
| POST | `/v1/topics/{topic}/records` | batched produce `{records:[{key?, value, partition?}]}` |
| GET  | `/v1/topics/{topic}/partitions/{p}/records?offset=&max_records=&max_bytes=&wait_ms=` | fetch (long-poll with `wait_ms`) |

Partitioning: explicit `partition` wins; else keyed records go to `fnv1a(key) % partitions`;
keyless records in one request all go to one partition chosen round-robin per request
("sticky" — keeps batches together for throughput).

## Storage (Milestone 1)
- Each partition is a directory of segment files `<base-offset 20 digits>.log`.
- Record framing: `u32 body length | u32 CRC32-C(body) | body`, body =
  `u64 offset | u64 leader epoch | i64 timestamp ms | u32 keylen | key | u32 vallen | value`.
  The offset is stored in every record so recovery can verify continuity; the leader epoch is
  stored now so Milestone 2 can truncate divergent tails after failover.
- Index: kept in memory (one int64 position per record), rebuilt by scanning on startup.
  Simple and correct; costs 8 bytes/record of RAM (fine for a demo, not for TB-scale).
- Crash recovery: on open, the last segment is scanned; a torn or CRC-failing tail is truncated.
  Any segment after a gap is discarded.
- Segments roll at `-segment-bytes` (default 64 MiB). No retention/compaction yet.
- `TruncateTo(offset)` exists for replication (discard an un-committed divergent tail).
- Durability: by default writes go to the OS page cache (no fsync per batch). Durability of
  acknowledged data comes from 2-of-3 replication (Milestone 2), the same trade-off Kafka makes.
  `-fsync` turns on fsync per append.
- Data dir layout:
  ```
  data/<broker-id>/broker.json            identity: id, region, zone
  data/<broker-id>/topics/<topic>/topic.json   name, partitions, residency
  data/<broker-id>/topics/<topic>/<partition>/00000000000000000000.log
  ```
  A broker refuses to start on a data dir that belongs to a different broker id or region —
  data can't silently "move" regions by misconfiguration. The residency audit (Milestone 3) will
  rely on `broker.json` + `topic.json`.

## Coordination plan (Milestone 2 — no Raft library assumed)
- Per-region controller elected among that region's 3 brokers with majority-granted leases:
  candidate bumps the epoch, asks peers for a lease, becomes controller only with 2 of 3 grants.
  A broker grants at most one lease per epoch and refuses lower epochs.
- Every write, replication request and metadata change carries the epoch; stale epochs are
  rejected (fencing), so an old controller or old partition leader can't cause split-brain.
- Partition leaders are assigned by the controller under the same epoch rules; writes are
  acknowledged at 2-of-3 replicas. Consumers only see records up to the high watermark.
- Leases depend on clocks. On one machine that's safe; across real machines we would need
  bounded clock drift or a proper consensus protocol.

## Global topics under partition (Milestone 4 plan)
- A global partition has replicas in all 3 regions; writes commit at a majority of regions,
  leader carries an epoch. An isolated region can't reach a majority, so it rejects global
  writes fast (explicit error, no hang). No split-brain: the minority side is fenced by epoch.
- The isolated region keeps serving its own resident topics (all replicas are local).
- On heal the region truncates to the point of agreement with the current leader and catches up.

## Competitive extras (priority order; feature freeze at hour 20)
1. **Correctness checker** (top priority) — runs alongside chaos tests, records every acknowledged
   message (topic, partition, offset, unique message id) and verifies: no loss of acked messages,
   per-partition ordering, no duplicates, no residency violations. Prints a results table per scenario.
   Duplicates from client retries will be prevented with an idempotent producer
   (producer id + sequence number per partition, deduplicated by the leader) — planned for Milestone 2.
3. **Network partition tests** (top priority) — region alive but isolated, with epoch fencing visible
   in broker logs (`fenced stale epoch=… current=…`).
2. Naive baseline mode — a single global controller, implemented as another `Coordinator`
   so the same chaos run can compare stall time and loss side by side.
4. Signed segment manifests — each broker publishes a manifest of stored segments (hash per segment,
   signed with a per-broker key); `residency_audit` verifies signatures + placement and emits a
   compliance report.
5. Specific, fast errors — `REGION_UNAVAILABLE` with a `reason` (e.g. `region_crashed`,
   `region_isolated`), `NOT_LEADER` with a leader hint, `STALE_EPOCH`, `NOT_ENOUGH_REPLICAS`,
   `RESIDENCY_VIOLATION`. The codes exist in `internal/api/errors.go` from Milestone 1.
6. One command to start the whole system + dashboard buttons: kill region, isolate region, slow link.

## Measured results
| Milestone | Scenario | Result |
|---|---|---|
| 1 | single broker, 100-byte values, batches of 2000, HTTP on localhost | produce ~670–750k msg/s (~70 MB/s); consume ~850k msg/s |
| 1 | hard kill (`Stop-Process -Force`) + restart | all 400,000 messages and offsets intact |

## Known limitations
- In-memory index (see above). No retention, compaction, consumer groups or auth.
- Consumers read by explicit offset; no committed consumer offsets (yet).
- Produce across multiple partitions in one request is not atomic (each partition's slice is).
