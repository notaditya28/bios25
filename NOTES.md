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

## Residency enforcement (Milestone 3)
Rule: a topic tagged `residency=<REGION>` may only be stored on, replicated to, or relayed
through brokers whose **configured** region (cluster config, not self-reported) is that region.
`global` may go anywhere. Enforced in code at every point data could move (`internal/residency`):

| Where | Check |
|---|---|
| Topic creation | a region refuses topics resident elsewhere (`RESIDENCY_VIOLATION wrong_region`), on the receiving broker and again on the controller |
| Placement (controller) | only brokers in the residency region are eligible replicas, even if the region's peer list is misconfigured to include outsiders |
| Replica creation | a broker refuses to create a partition directory for data it may not hold (nothing is written) |
| Replication, sending | a leader never streams a resident partition to an out-of-region follower |
| Replication, receiving | a broker refuses appends it may not store, and refuses resident data *arriving from* an out-of-region sender (checked against the sending broker's id) |
| Relay / forwarding | a broker refuses to accept or forward produce/fetch for a resident topic it may not relay, and won't forward to an out-of-region leader |

Every refusal is logged (`residency violation blocked`) and counted (`residency_rejections` in
`/admin/v1/status`). Topic *names and configs* (metadata) are not treated as resident data.
Trade-off (for the pitch): data that legally can't leave a region can't survive the loss of
that whole region. Resident topics are as available as their region, no more.

**Signed manifests (`internal/manifest`)** — each broker has an Ed25519 key (`manifest.key` in
its data dir, created on first start) and serves `GET /admin/v1/manifest`: every segment it
stores with topic, residency tag, size and SHA-256, signed.

**`residency_audit` (`cmd/residency_audit`)**
- Offline (default): scans every broker data directory under `data/`. Violations: resident data
  on a broker configured in another region; partition data without a residency tag; a residency
  tag naming no region; `broker.json` disagreeing with the cluster config (relabelled dir); data
  directories belonging to no configured broker.
- `-online`: also fetches each live broker's manifest, verifies the signature against a key
  pinned on first use (`data/audit-trust.json`; a changed key is a violation), applies the same
  rules to what the broker *reports*, and checks the manifest against the bytes on disk
  (a sealed segment that doesn't match its signed hash is a violation).
- Output: table + findings + PASS/FAIL; `-json` writes the report; exit code 1 on FAIL.
- Safe to run during chaos: dead brokers are still covered by the disk scan; manifests are
  fetched *before* the disk scan so growing segments don't raise false alarms.
- Limitation: trust on first use. A real deployment would pin keys out of band (or use an HSM).

Bugs found while building it: (1) a non-IN broker could accept an IN produce on its public API
and forward it to the IN leader — destination legal, but the data passed through the EU broker;
now refused at entry. (2) On Windows, directory listings report stale sizes for files another
process has open, so the audit read 0 bytes; it now stats each segment.

## Multi-region (Milestone 4) — design A: cross-region quorum, no global leader
**Independent regions.** Each region has its own controller (Milestone 2) and its own metadata.
Nothing global coordinates regions, so one region's failure can't block another's resident
traffic. Measured: resident traffic in surviving regions never stalled more than 51 ms when a
whole region was killed.

**Global topics.** Each global partition has exactly one replica per region, placed by a pure
function of the cluster config (`globalReplicas`), so every region computes the same placement
without talking to anyone. Writes commit at 2 of 3 regions.
- *Leader election without a controller* (`internal/replica/election.go`): the 3 replicas elect
  their own leader, Raft-style — one vote per epoch (persisted), only for a candidate whose log
  is at least as up to date. Plus **pre-vote** (an isolated replica can't inflate the epoch and
  disrupt the healthy leader when it reconnects) and **check-quorum** (a leader that loses
  contact with a majority of regions steps down, so the isolated side fails fast instead of
  hanging). Log replication, the commit rule (high watermark + leader marker record) and
  follower truncation are the same code as in-region partitions.
- *Creation:* the region that receives the request creates the topic locally, then asks one
  broker in every other region to create it there; it succeeds once a majority of regions has
  it, and waits for leaders to be elected so clients can produce at once. Regions that were
  down or isolated pick up missing global topics from the topic gossip (anti-entropy, every 1 s).
- *Routing:* any broker accepts a global request; a non-replica hands it to its region's replica,
  a follower replica forwards to the leader (at most 2 forwards).

**Regional outage behaviour (the trade-off for the pitch)**
| | crash of region R | R isolated (alive, cut off) |
|---|---|---|
| Resident topics of other regions | unaffected | unaffected |
| Resident topics of R | clients elsewhere get `REGION_UNAVAILABLE (region_unreachable)` in ms; data can't be served elsewhere by law | **keep working inside R** (all replicas local) |
| Global partitions led in R | re-elected in the other 2 regions (~0.3–0.9 s) | same; R's old leader steps down (check-quorum), writes in R fail fast with `NOT_ENOUGH_REPLICAS (region_isolated)` |
| On recovery | R's replicas catch up automatically | R rejoins, truncates unacknowledged writes, catches up; no leader disruption thanks to pre-vote |

A client in region X asking for a topic resident in Y gets `WRONG_REGION` with Y's broker
addresses (relaying it through X would itself break residency). Regions learn which topics
live where from the probe gossip (topic name + residency + partition count; metadata only).

**Latency-aware routing (`internal/geo`)**
- Every broker probes every broker in other regions every 200 ms; RTT is an EWMA (α = 0.3).
  Probe replies carry the responder's RTT table and its region's topic list.
- For global traffic to broker B, the router compares direct RTT with the best two-hop path
  via a broker in the third region (our RTT to it + its RTT to B). It relays when that is ≥30%
  faster *and* saves ≥10 ms, and keeps the relay while it stays ≥10% faster (hysteresis).
  A down direct link uses any working relay.
- Relaying is a generic envelope (`/internal/v1/geo/relay`); the relay re-checks residency for
  itself and the next hop, so resident data can never take a detour. Resident traffic never
  goes through the router's relay path at all.
- Slow links are simulated by per-peer delay injection in the RPC layer (`/admin/v1/net`
  `delays_ms`), added on both request and response. (A separate TCP delay proxy is unnecessary
  for local runs; the effect on RTT is identical.)

Known flake: once in ~40 runs of the multi-region tests under heavy CPU load, a region's
controller lost its lease and creating a topic in that region timed out for 10 s. Not
reproduced since; the create forward now has a timeout and errors are reported. Watch for it
in chaos runs.

## Idempotent producer + chaos harness (Milestone 5)
**Exactly-once writes per partition.** A producer has a random id; each batch for a partition
carries `producer_id` + `base_seq`, and a failed batch is retried with the same numbers. Records
store producer id + seq (and a batch-start flag), so every replica derives the same "last 8 batches
per producer" table from its own log — after a failover the new leader recognises retries of
batches the old leader wrote. A known batch is answered with its original offsets (after it is
committed); a gap or an altered retry is refused. Bug caught by the test: the table first merged
consecutive batches, so after failover a retry of the second batch looked "stale"; fixed with the
batch-start flag.

**`hydra-chaos`** starts the 9 brokers as real processes (fresh `data-chaos/`), creates one
resident topic per region + a global topic, and for each scenario:
1. runs 6 paced streams (≈12k msg/s total; batches of 20 every 10 ms) through idempotent
   producers — one resident stream per region using that region's brokers, one stream per global
   partition that prefers its home region but may use any broker — plus cross-region probes
   that ask other regions for each resident topic every 250 ms;
2. warm-up 3 s → audit → **fault** → audit mid-fault → hold 6 s → **heal** → wait until every
   stream is acknowledged again → 2 s more → stop;
3. reads every partition back and checks each acknowledged message is at its acked offset (loss),
   appears once (duplicates) and in sequence (order); runs the residency audit again.

Scenarios: `baseline`, `kill-broker` (leader of IN's resident partition), `kill-zone` (the zone
hosting IN's controller), `kill-region` (all of IN), `isolate-region` (IN cut off both ways),
`slow-link` (+150 ms each way between the global leader's region and another).

Pass criteria (definition of done): 0 lost, 0 duplicates, 0 order violations; surviving traffic
stalls < 1 s; explicit cross-region errors < 500 ms; no request hangs > 2.5 s; audit PASS before,
during and after; slow-link must reroute.

Tuning found by the harness: global partitions' election timeout lowered from 600 ms to 400 ms
(+ up to 50% jitter). At 600 ms, global stalls in kill-zone/kill-region were 950–980 ms (just under
target); at 400 ms they are 740–780 ms with no spurious elections in 3 full runs.

## Competitive extras (priority order; feature freeze at hour 20)
1. **Correctness checker** — **done in Milestone 5** (`hydra-chaos`), with the idempotent producer.
3. **Network partition tests** — **done**: `isolate-region` chaos scenario + Go tests for an
   isolated leader (in-region) and an isolated region (global); fencing visible in logs
   (`fenced stale leader`, `stepping down`, `lost contact with a majority of regions`).
2. Naive baseline mode — a single global controller, implemented as another `Coordinator`
   so the same chaos run can compare stall time and loss side by side.
4. Signed segment manifests — **done in Milestone 3** (see Residency enforcement).
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

| 3 | 9 brokers, 3 resident topics + 1 global, ~84 MiB per replica set | `residency_audit -online`: PASS, 9/9 manifests verified |
| 3 | IN partition directories copied onto an EU broker | FAIL, 6 violations (3 from disk scan, 3 from the broker's own signed manifest), exit 1 |
| 3 | audit ×3 while producing 120k msg/s | PASS every time (≈1.5 s per audit); producer unaffected (max stall 51 ms) |
| 3 | `TestResidencyEnforcedDespiteMisconfiguredPeers` (EU broker wrongly in IN's group) | IN partitions never placed on, replicated to, relayed through or accepted from it; audit PASS, then FAIL after planting data |

| 4 | 9 processes: create global topic (3 regions, leaders elected) | 396 ms |
| 4 | produce global from EU, consume from US | 84k msg/s produce, 86k msg/s consume (one partition) |
| 4 | IN↔EU slowed 150 ms each way (direct RTT measured 297 ms) | EU→IN global traffic re-routed via US; fetch p50 3.1 ms (vs ~300 ms direct) |
| 4 | kill whole IN region under load (it led the global partition) | EU resident: 0 errors, max stall 51 ms; global: max stall 924 ms, 14 fast errors retried; IN topic via EU → `REGION_UNAVAILABLE` in 55 ms; audit PASS |
| 4 | tests ×5: region crash | global re-elected in 0.32–0.45 s, max stall ≤ 0.47 s; surviving resident stall ≤ 42 ms; 0 acked loss |
| 4 | tests ×5: region isolation | resident writes inside isolated region 1–2 ms; global writes there fail in ~1–2 ms (`region_isolated`); majority re-elects (epoch 1→2); converges after heal, unacked writes dropped |
| 4 | tests ×5: slow link | reroute decided 135–180 ms after the delay starts (≈3 probes) |

| 5 | `hydra-chaos`, all 6 scenarios, real processes (see table below) | all PASS; 0 lost / 0 dups / 0 order violations over ~900k acked messages |

### Chaos results (Milestone 5, `results/chaos-20261031-162802.md`)
| Scenario | Acked | Lost | Dups | Order | Max stall surviving | Max stall global | Max stall affected | msg/s | Slowest error | Slowest probe | Audit b/d/a |
|---|---|---|---|---|---|---|---|---|---|---|---|
| baseline | 148,500 | 0 | 0 | 0 | 38 ms | 22 ms | – | 12,000 | – | 5.8 ms | PASS/PASS/PASS |
| kill-broker | 148,780 | 0 | 0 | 0 | 44 ms | 777 ms | 553 ms | 11,782 | 34 ms | 6.2 ms | PASS/PASS/PASS |
| kill-zone | 149,780 | 0 | 0 | 0 | 12 ms | 737 ms | 17 ms | 11,893 | 13 ms | 3.0 ms | PASS/PASS/PASS |
| kill-region | 148,120 | 0 | 0 | 0 | 15 ms | 198 ms | 7,801 ms* | 10,835 | 46 ms | 5.2 ms | PASS/PASS/PASS |
| isolate-region | 151,120 | 0 | 0 | 0 | 15 ms | 154 ms | – | 11,983 | 1.6 ms | 5.5 ms | PASS/PASS/PASS |
| slow-link | 152,800 | 0 | 0 | 0 | 13 ms | 311 ms | – | 11,916 | – | 3.0 ms | PASS/PASS/PASS |

\* By design: IN-resident data legally can't fail over to another region, so its stream waits
for IN to come back (6 s fault + restart). Its clients got fast errors the whole time (slowest 46 ms),
and other regions answered `REGION_UNAVAILABLE` for it within milliseconds. In `isolate-region` the
IN resident stream kept flowing inside IN (counted under "surviving": 15 ms max stall).
"Max stall global" is per global partition from a client's view; across runs it was 200–980 ms
(kill-zone/kill-region, depending on whether the killed broker led a global partition).
Slow-link: EU↔IN direct RTT measured 301 ms; EU-led global traffic from IN went via us-3;
fetch p50 1.1 ms before vs 1.4 ms during the fault.

## Known limitations
- In-memory index (see above). No retention, compaction, consumer groups or auth.
- Consumers read by explicit offset; no committed consumer offsets (yet).
- Produce across multiple partitions in one request is not atomic (each partition's slice is).
- Idempotence needs the client to use producer id + sequence numbers (the Go `client.Producer`
  and the chaos load do); plain produce requests (curl, CLI) can still duplicate on retry.
  The leader remembers the last 8 batches per producer per partition.
- The high watermark isn't persisted: after a restart a replica may truncate (and re-copy)
  more than strictly necessary. Safe, just extra work.
- Each metadata change ships the whole metadata snapshot (fine for tens of topics).
- Replication is JSON over HTTP (values base64-encoded): simple, not the fastest.
- Forwarding adds one hop when a client talks to a non-leader; a smart client could route
  straight to leaders.
- A global partition has one replica per region (3 total), so losing a region *and* a broker
  holding another region's replica makes that partition unavailable until one returns.
- Global writes pay a cross-region round trip (leader + fastest other region); reads go to the leader.
- Global topic creation is not atomic across regions (majority + anti-entropy); conflicting
  concurrent creates of the same name with different configs are rejected where detected.
- A region needs 2 of 3 brokers to elect a controller or a leader. With 2 down, the survivor
  keeps serving reads of committed data but rejects writes fast (`NOT_ENOUGH_REPLICAS`).
