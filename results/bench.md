# Hydra Stream benchmarks

2026-10-31 18:26 · 3 runs per configuration · medians unless noted

## 1. Replication bandwidth under a degrading link

One directed link, 4 MB/s, falls to 60% after 2s. Live traffic 1.6 MB/s (16 KB / 10 ms), urgent control 1 KB / 50 ms with a 25ms deadline, and a 10 MB recovery backlog sent by 4 concurrent senders. Same scheduler code; only the policy differs.

| mode | live p50 ms | live p95 ms | live p99 ms | urgent over deadline | backlog cleared in | link utilization |
|---|---:|---:|---:|---:|---:|---:|
| FIFO (one shared queue) | 544.5 | 632.3 | 644.0 | 100.0% | 6.9 s | 101% |
| fixed 50% recovery share | 459.9 | 627.3 | 640.2 | 69.9% | 7.9 s | 97% |
| **adaptive (Hydra)** | 29.9 | 57.3 | 94.6 | 0.0% | 8.0 s | 101% |

Live delay is the time a send waits for link budget (queueing), not network latency.

## 2. Consumer failover: what gets processed twice

20000 messages, 16 parallel workers, processing time log-normal (median 5 ms) with 2% of messages 50x slower; checkpoint every 100ms; the consumer crashes at a random moment (400 trials). Both modes commit at the same instants; the only difference is what a checkpoint can say. Granular uses the real client.RangeSet.

| checkpoint | duplicates after crash (median) | p95 | checkpoint size (ranges, median) |
|---|---:|---:|---:|
| offset-only (Kafka-style committed offset) | 587 | 973 | 1 offset |
| **granular ranges (Hydra)** | 67 | 145 | 9 |

Pulsar can track individual acks on the broker (ack holes) for shared subscriptions; the comparison here is against a single committed offset, the Kafka default. The end-to-end cluster test (TestGranularCheckpointsAndFencing) shows the same effect on the real system: 1 redelivery vs 100.

## 3. Cost of a per-hop jurisdiction check

| check | ns per batch | allocs |
|---|---:|---:|
| store check, built-in residency (hot path, allow not logged) | 97 | 2 |
| store check, signed policy class (allow not logged) | 277 | 6 |
| peer check with a per-message class narrowing | 825 | 12 |
| entry check with an audit record written | 1402 | 9 |

Checks run once per batch per hop, not per message. A produce batch crosses 2-4 hops, so the whole policy path costs a few microseconds against a cross-region round trip of 80-220 ms.
