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

Internal broker-to-broker RPCs live under `/internal/v1/...` (same JSON-over-HTTP transport,
`internal/rpc`). Admin: `GET /admin/v1/status`, `GET|POST /admin/v1/net` (blocked peer links,
used to simulate network partitions).

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

Record body also has a `u8 flags` byte (after the timestamp) since Milestone 2; flag 1 marks
control records that consumers never see.

## In-region replication (Milestone 2 — no Raft library)
Built behind `coord.Coordinator` (controller election + replicated metadata) so a Raft library
can replace `coord.Lease` without touching broker code.

**Controller election (`internal/coord/lease.go`)**
- Majority-granted leases. A candidate bumps the epoch and asks all 3 brokers in its region;
  it is controller only with 2 of 3 grants. A broker grants one candidate per epoch, refuses
  lower epochs, and refuses newer epochs from someone else while its granted lease runs.
  The promised epoch is persisted (`coord.json`) before replying; after a restart a broker
  waits a full lease before granting to anyone else.
- The controller renews every 150 ms (lease 1 s). The renewals double as heartbeats: a broker
  that doesn't answer for 500 ms is treated as down.
- Controller timing is measured from when the renewal is *sent*; grants run from when it is
  *received*, so the controller always believes its lease ends no later than the granters do.
  This relies on clocks advancing at the same rate — safe on one machine; across real
  machines it needs bounded clock drift or a proper consensus protocol.
- Bug found by repeated chaos testing: two candidates could livelock because a failed
  candidate kept its self-grant for a whole lease, blocking the other's next attempt. Fixed by
  releasing the self-grant when a campaign fails or a controller steps down.

**Region metadata** (topics, replicas, leader + leader epoch per partition)
- Changed only by the controller; a version is committed once 2 of 3 brokers have persisted it.
  Brokers reject metadata from a controller epoch lower than they promised (fencing).
- A new controller adopts the newest version seen in its grant quorum (any two majorities
  overlap, so that includes every committed version) and re-commits it before acting.
- Brokers that missed a commit catch up by pulling (renewals carry the committed version).
  A restarted broker takes no partition role until it has synced with a live controller.

**Partition replication (`internal/replica`)**
- Replicas = the 3 brokers of the region (one per zone); preferred leaders rotate across zones.
- The leader pushes to each follower (one ordered stream per follower, heartbeat every 100 ms).
  Every request carries the leader epoch plus (offset, epoch) of the previous record; a
  follower rejects older epochs (`fenced stale leader` in its log), and on a mismatch it drops
  its uncommitted tail (everything above its high watermark) and re-fetches.
- A write is acknowledged once 2 of 3 replicas have it (the high watermark passes it).
  Consumers only see records below the high watermark.
- A new leader first writes a control record in its own epoch; the high watermark only moves
  once a majority has that record. This stops the "commit an old-epoch entry and lose it" case
  (Raft's figure 8).
- Fast errors instead of hangs: if the leader hasn't heard from a majority within 1 s, writes
  fail immediately with `NOT_ENOUGH_REPLICAS (replicas_unreachable)`; otherwise a write waits at
  most 2 s (`ack_timeout`). Unreachable leaders give `NOT_LEADER (leader_unreachable)` with the
  leader's id. Both are ambiguous for the client ("may or may not be committed"), so a retry can
  produce a duplicate — see the idempotent-producer item below.

**Leader failover (controller, `internal/broker/controller.go`)** — every 50 ms:
1. find partitions whose leader is down (no lease renewal answer) or "stuck" (fenced past its
   assigned epoch, e.g. a controller died mid-election);
2. **fence first**: send the live replicas a new epoch; they stop accepting the old leader and
   report (last epoch, end offset). Fencing before comparing logs closes the race where a slow
   old leader commits one more write after we looked;
3. pick the most up-to-date fenced replica (needs 2 of 3), commit the new leader + epoch.
A broker coming back after a crash or pause keeps its data, discovers it is stale (followers
answer its appends with a higher epoch → `fenced: stale leader stepping down`), truncates its
unacknowledged tail and follows.

**Routing**: any broker accepts produce/fetch and forwards to the partition leader.

## Global topics under partition (Milestone 4 plan)
- A global partition has replicas in all 3 regions; writes commit at a majority of regions,
  leader carries an epoch. An isolated region can't reach a majority, so it rejects global
  writes fast (explicit error, no hang). No split-brain: the minority side is fenced by epoch.
- The isolated region keeps serving its own resident topics (all replicas are local).
- On heal the region truncates to the point of agreement with the current leader and catches up.

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
   (producer id + sequence number per partition, deduplicated by the leader). **Not built yet** —
   Milestone 2 tests show retries after an ambiguous error create 0–100 duplicates per failover.
   Planned alongside the checker (Milestone 5) since the load generator is its main user.
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
| 2 | 3 broker processes, 2-of-3 acks, 3 partitions, 1 client through one broker | ~185k msg/s (18.5 MB/s) |
| 2 | hard-kill the controller (also a partition leader) under load, real processes, default timings | max client stall 1.14 s, 19 fast errors (all retried), 263k msg/s overall |
| 2 | `TestLeaderCrashUnderLoad` ×8 (test timings: lease 400 ms) | 0 acked messages lost; new leader in 175–606 ms; duplicates 0–50 from retries |
| 2 | `TestControllerCrash` ×8 | new controller in 0.41–1.34 s; metadata intact |
| 2 | `TestStaleLeaderAfterPauseIsFenced` ×8 | isolated leader never acks (first write `ack_timeout` at 0.8 s, then fails in < 200 ms); after heal it is fenced, truncates and converges |

## Known limitations
- In-memory index (see above). No retention, compaction, consumer groups or auth.
- Consumers read by explicit offset; no committed consumer offsets (yet).
- Produce across multiple partitions in one request is not atomic (each partition's slice is).
- No idempotent producer yet: a retry after an ambiguous error can duplicate a batch.
- The high watermark isn't persisted: after a restart a replica may truncate (and re-copy)
  more than strictly necessary. Safe, just extra work.
- Each metadata change ships the whole metadata snapshot (fine for tens of topics).
- Replication is JSON over HTTP (values base64-encoded): simple, not the fastest.
- Forwarding adds one hop when a client talks to a non-leader; a smart client could route
  straight to leaders.
- A region needs 2 of 3 brokers to elect a controller or a leader. With 2 down, the survivor
  keeps serving reads of committed data but rejects writes fast (`NOT_ENOUGH_REPLICAS`).
