# Conduit

A multithreaded Java service that ingests security telemetry from several sources at once, then
filters, transforms, routes and publishes it to several sinks. It is built to be correct under load:
every queue is bounded, backpressure reaches the sources, one broken sink cannot stall the others,
shutdown drains everything, and every run ends with an accounting check that proves no event was
lost.

![Throughput vs processing workers](docs/throughput.svg)

**Measured on an 8-core M1 MacBook Air** (4 performance + 4 efficiency cores, 8 GB, JDK 21). Full
tables in [`bench/results/summary.md`](bench/results/summary.md); raw JSON next to it.

| Claim | Result |
|---|---|
| Sustained throughput | **1.33M events/s** for 60 s, unthrottled, full transform chain, heap flat (278 → 326 MB average, 644 MB peak of 1 GB) |
| End-to-end, file sink | 639k events/s with 1 worker, **917k with 8 (1.43×)**; see [why scaling stops](#why-scaling-stops) |
| Zero loss | **10,000,000 events**, 5% injected write failures plus a 2 s outage: invariant holds, dead-letter file matches the counters exactly |
| Latency (ingest → sink ack) | p50 3.7 ms, p95 6.8 ms, **p99 18.9 ms at 100k events/s**; p99 130 ms at half of peak (target < 50 ms **not met** there) |
| Failure isolation | Healthy sink averaged 95% of its throughput while another sink hung for 6 s, with a 1.5 s dip explained [below](#failure-isolation) |


## Quick start

Needs Java 21. Docker is only needed for the database end-to-end tests and the full stack.

```bash
./gradlew test
```

```bash
./gradlew installDist && build/install/conduit/bin/conduit config/local.yaml
```

`config/local.yaml` runs all four sources into file sinks with no databases. While it runs:

```bash
curl -s -X POST localhost:8088/ingest -d '[{"type":"auth","user":"erin","src_ip":"13.70.1.1","host":"app-3","outcome":"failure","severity":"high"}]'
```

```bash
printf '<134>1 2026-10-25T12:00:00Z web-1 sshd - auth [m user="carol" src_ip="34.1.2.3" host="web-1"] ok\n' | nc -w1 localhost 5140
```

```bash
curl -s localhost:9400/health
```

The full stack with PostgreSQL and Neo4j (`config/conduit.yaml`):

```bash
docker compose up --build
```

Neo4j's browser is at http://localhost:7474 (neo4j / conduit-pass). Try
`MATCH p=(:User)-[:AUTHENTICATED_FROM]->(:IP)-[:ACCESSED]->(:Host) RETURN p LIMIT 50`.

## Architecture

```
 sources (1 thread each)       processing (N workers)             sinks (own lanes each)
 ┌───────────────┐                                              ┌─────────────────────┐
 │ file tail     │──┐   key hash    ┌──────────────────────┐    │ postgres  lane 0..k │
 │ tcp syslog    │──┼──▶ lane 0..N ─▶│ filter → transforms → │──▶│ neo4j     lane 0    │
 │ http batch    │──┤  (bounded)     │ route (fan-out)      │    │ file      lane 0..k │
 │ synthetic     │──┘               └──────────┬───────────┘    └──────────┬──────────┘
 └───────────────┘                             │                           │ retries exhausted,
        │ malformed                            │ transform error           │ circuit open,
        └──────────────────────────────────────┴──────────▶ dead-letter ◀──┘ queue overflow
```

| Stage | What it does | Code |
|---|---|---|
| Ingestion | Tailed JSON-lines file (follows rotation and truncation), TCP syslog (RFC 5424, `<PRI>`+JSON, or free text with `key=value`), HTTP batch POST (JSON array or NDJSON), synthetic generator. Everything parses into one `Event`; malformed records go to dead-letter with the reason. | [`source/`](src/main/java/io/conduit/source), [`EventParser`](src/main/java/io/conduit/core/EventParser.java) |
| Filtering | Keep/drop rules on severity, source, type, key or any attribute, composable with `and`, `or`, `not`. | [`Rules`](src/main/java/io/conduit/filter/Rules.java) |
| Transformation | UTC timestamp normalization with a future-skew guard; PII redaction (keyed HMAC tokens, so "same user" still joins); IP enrichment from a CIDR table that hot-reloads. | [`transform/`](src/main/java/io/conduit/transform) |
| Routing | Rules send each event to one or more sinks (fan-out); unmatched events take the default route. | [`Router`](src/main/java/io/conduit/route/Router.java) |
| Publishing | PostgreSQL (batched, idempotent upserts), Neo4j (user → IP → host graph via `UNWIND … MERGE`), rolling JSON-lines files. Retry with exponential backoff, then dead-letter. | [`sink/`](src/main/java/io/conduit/sink) |

The whole pipeline is one YAML file. Startup validates it and refuses to run on unknown types,
unknown or missing fields, broken route references, or an unreachable sink, reporting every
problem with its path (for example `unknown field(s) [colour] at config.sources[0] (synth)`).

## Concurrency model

Every hand-off is a bounded `ArrayBlockingQueue`. There is no unbounded queue anywhere, so a slow
stage slows the stage in front of it instead of growing memory.

- **Threads.** One thread per source (TCP connections get a virtual thread each). A fixed pool of N
  processing workers. Each sink has its own lanes, and each lane has its own worker thread and its
  own sink instance (bulkhead).
- **Backpressure.** When a lane is full, the source's `put` blocks. The file and TCP sources simply
  stop reading, and TCP flow control pushes back on the sender. The HTTP source waits at most
  `offerTimeout` per record, then answers `429` with `Retry-After` and a `resumeFrom` index so the
  client resends only what was not accepted.
- **Ordering.** Events are partitioned by key hash onto worker lanes, and again onto sink lanes, so
  events with the same key stay in order end to end (the stress test checks this on every run).
  Global order is not guaranteed.
- **No producer contention at sinks.** Each sink lane has one input queue per processing worker,
  so every hand-off into a sink has exactly one producer. See [Finding 2](#finding-2-a-lock-convoy-on-the-sink-queues).
- **Shared state.** The IP table is an immutable snapshot behind an `AtomicReference`, replaced
  whole on reload. Counters are `LongAdder`s. No `synchronized` on the hot path.
- **Batching.** A sink lane drains up to `batchSize` events, or whatever arrives within `linger`
  of the first, and writes them as one batch.
- **Failure isolation.** When a sink's queue stays full past `offerTimeout`, that lane is marked as
  shedding: further events for it go straight to dead-letter instead of each waiting, so a dead sink
  can stall routing for at most one timeout. Each write attempt has a time limit, so a hanging sink
  shows up as a failure. A circuit breaker stops retrying a sink that keeps failing, so its lane
  drains to dead-letter at full speed. Set `overflow: block` on a sink to choose backpressure over
  shedding.
- **Shutdown.** On SIGTERM: stop the sources, send a poison pill down each lane, let the workers
  drain, poison and flush every sink lane, then the dead-letter writer, all against one deadline
  with `awaitTermination`. A stage that misses the deadline is interrupted and reported.
- **Accounting invariant.** At shutdown the books must balance, or the run fails (exit code 1):

  ```
  ingested   = filtered + routed + dead-lettered before routing (parse, transform)
  deliveries = published + dead-lettered at a sink (overflow, write failure)
  dead-letter file records = all dead-lettered
  ```

  With one target per event this is the plain `in = published + filtered + dead-lettered`. Fan-out
  turns one event into several deliveries, so the ledger keeps the two levels apart.

## Design patterns

| Pattern | Where | Why it is there |
|---|---|---|
| Producer–consumer | Every stage boundary ([`Pipeline`](src/main/java/io/conduit/pipeline/Pipeline.java)) | Decouples stage speeds and gives backpressure through bounded queues. |
| Strategy | Filter rules ([`Rule`](src/main/java/io/conduit/filter/Rule.java)), routing policy ([`Router.Policy`](src/main/java/io/conduit/route/Router.java)), [`IdleStrategy`](src/main/java/io/conduit/pipeline/IdleStrategy.java) | Behaviour chosen from config with no branching in the core. |
| Composite | `And` / `Or` / `Not` rules | Rules nest to any depth and are evaluated through one interface. |
| Chain of responsibility | [`TransformChain`](src/main/java/io/conduit/transform/TransformChain.java) | Each step changes the event and passes it on, or stops the chain to drop it. |
| Factory with registry | [`Registries`](src/main/java/io/conduit/config/Registries.java) over `ServiceLoader` | YAML type names map to providers. Onboarding needs no core change (below). |
| Builder | [`ConduitConfig.Builder`](src/main/java/io/conduit/config/ConduitConfig.java) | Validates everything once, then hands workers an immutable config. |
| Decorator | [`CircuitBreakerSink`](src/main/java/io/conduit/sink/CircuitBreakerSink.java) → [`RetryingSink`](src/main/java/io/conduit/sink/RetryingSink.java) → [`MeteredSink`](src/main/java/io/conduit/sink/MeteredSink.java) → [`TimeLimitedSink`](src/main/java/io/conduit/sink/TimeLimitedSink.java) → sink | Resilience and metrics wrap any sink without touching its code. |
| Observer | [`PipelineListener`](src/main/java/io/conduit/observe/PipelineListener.java) | Stages report facts; the accounting ledger, metrics and logging decide what to record. |
| Bulkhead | [`SinkChannel`](src/main/java/io/conduit/pipeline/SinkChannel.java) | Each sink has its own queues and threads, so a failing sink cannot starve the others. |
| Poison pill | [`Lanes.POISON`](src/main/java/io/conduit/pipeline/Lanes.java) | Tells a consumer its producers are done, so shutdown drains rather than drops. |

### Onboarding a new source or sink

One class and one registration line; the core does not change.

```java
public final class KafkaSink implements Sink {
    public static final class Provider implements SinkProvider {
        public String type() { return "kafka"; }
        public Sink create(Props p, ComponentContext ctx) {
            return new KafkaSink(p.string("brokers"), p.string("topic"));
        }
    }
    public void write(List<Event> batch) throws Exception { /* send, throw on failure */ }
}
```

Add `io.conduit.sink.KafkaSink$Provider` to
`META-INF/services/io.conduit.sink.SinkProvider`, then use `type: kafka` in YAML. It gets lanes,
batching, retries, the circuit breaker, timeouts, metrics and dead-lettering for free, and any field
it does not read is rejected at startup as unknown.

## Operations

- **`GET /metrics`**: Prometheus text. Records per source, events filtered, routed and published
  per sink, dead-lettered by stage and origin, queue depth per lane, sink write durations and
  failures, circuit state, end-to-end latency histograms, JVM metrics.
- **`GET /health`**: `UP`, `DEGRADED` (a source failed, a circuit is open, or a lane is shedding)
  or `DOWN` (503), with per-source state, per-lane queue depth, circuit state, last sink error and
  the live accounting counters.
- **Dead-letter files** (`out/dead-letter/*.jsonl`): one JSON object per record, with stage
  (`parse`, `transform`, `sink_overflow`, `sink_write`), origin, reason, attempts, and either the raw
  input or the full event, ready to replay.
- **Exit codes**: 0 clean, 1 accounting check failed or shutdown timed out, 78 bad configuration or
  unreachable sink. On SIGTERM the JVM exits with 143 after a clean drain.

Delivery is at-least-once within a run. The PostgreSQL sink inserts with `ON CONFLICT (id) DO
NOTHING`, so a retried batch never duplicates rows; Neo4j `MERGE`s never duplicate nodes or edges,
though a retried batch can over-count the `count` property on an edge. Exactly-once across process
crashes is a non-goal.

## Testing

```bash
./gradlew test
./gradlew test -PwithStress
```

The second command adds a 200-repetition stress run.

| Kind | What it covers |
|---|---|
| Unit | Parser and malformed input, every rule operator and composition, routing policies, all transforms (including hot reload keeping the old table on a bad file), config validation errors, syslog parsing, each decorator (backoff bounds, breaker state machine on a fake clock, hang timeouts). |
| Concurrency stress | 8 producer threads released together by a `CountDownLatch` against small queues, 6 workers and multi-lane sinks; checks exact counts, no duplicates and per-key order. Repeated 15 times per run (200 with `-PwithStress`). Also stop-under-full-load, repeated. |
| Fault injection | A sink that always fails, one that fails 30% of writes, one that hangs, an outage mid-run, a slow sink with blocking overflow, malformed records, a transform that throws, a stuck sink past the shutdown deadline. |
| Backpressure | A frozen sink stops the source at exactly the sum of the queue capacities; HTTP answers 429 with a correct `resumeFrom`, then accepts the rest. |
| End-to-end | Testcontainers runs PostgreSQL 16 and Neo4j 5, sends a fixed event set through, and asserts exactly what landed in each store, that replays do not duplicate, and that pausing Neo4j mid-run does not stop PostgreSQL. |

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs everything on each push, plus a
benchmark smoke run that fails if throughput drops below half of the saved baseline.

## Benchmarks

```bash
./gradlew bench
```

That runs everything, writes `bench/results/*.json` and `summary.md`, and redraws the figures. One
scenario at a time:

```bash
./gradlew bench -PbenchArgs="scaling --runs 3 --events 3000000 --workers 1,2,4,8,16"
```

Other scenarios: `latency --rates 100000,250000`, `loss --events 10000000`, `overload --seconds 600`,
`isolation`, `report` (redraw from saved JSON). Every run ends with the accounting check and fails
loudly if it does not balance.

The benchmark pipeline is: 2 synthetic source threads → heartbeat filter → UTC normalization →
HMAC-SHA256 email redaction → CIDR enrichment of two IP fields → sink. Machine: Apple M1 MacBook
Air, 8 cores (4P + 4E), 8 GB, macOS 26.1, OpenJDK 21.0.13, G1 with a 1 GB heap and a 20 ms pause
goal. It is a fanless laptop with other apps running (about 1.3 cores of background load), so runs
vary; every scaling point is the median of three, and the whiskers show min and max.

### Why scaling stops

| Workers | Median events/s | Speedup | Cores busy | CPU µs per event |
|---:|---:|---:|---:|---:|
| 1 | 639,095 | 1.00× | 3.5 | 5.45 |
| 2 | 871,662 | 1.36× | 5.5 | 6.53 |
| 4 | 828,734 | 1.30× | 5.6 | 7.21 |
| 8 | 917,032 | 1.43× | 6.4 | 6.99 |
| 16 | 442,872 | 0.69× | 5.9 | 14.53 |

Three things explain the curve:

1. **The workers are a minority of the per-event work.** With one worker the process already keeps
   3.5 cores busy: the source threads build events, four sink lanes serialize JSON, and the GC runs.
   Adding workers only speeds up the filter-and-transform share (Amdahl's law). The same pipeline
   into a discarding sink ran at about 1.8M events/s at 4 workers in exploratory runs, which shows
   how much of the file-sink cost is JSON serialization in the sink lanes.
2. **The machine runs out of fast cores.** By 8 workers, 6.4 of 8 cores are busy, and half of those
   are efficiency cores at roughly a third of the speed. CPU per event rises as work spills onto them.
3. **16 workers oversubscribe.** 16 workers plus 2 sources and 4 sink lanes is 22 busy threads on
   8 cores. Idle workers spinning in `IdleStrategy` take time from the sink lanes, and CPU per event
   doubles. Size `workers` to roughly cores − sources − sink lanes. (Removing the spin phase was
   tried and made 8 workers twice as slow, so the strategy stays.)

### What profiling found

Early runs did not plateau; they **collapsed**: 8 workers ran at 311k events/s, against 1.0M at 4.
Profiling (thread-dump sampling and JFR) found three causes, each fixed in the code:

| Finding | Evidence | Fix |
|---|---|---|
| <a id="finding-1-a-wake-up-storm"></a>**1. Wake-up storm.** Each worker drained its lane faster than the source filled it, so it blocked after almost every event, and every `put` paid for a kernel wake-up. | Workers `WAITING` in `take()` while the single source thread was always `RUNNABLE`. | Workers drain in chunks of 256 and back off with spin → yield → `parkNanos` ([`IdleStrategy`](src/main/java/io/conduit/pipeline/IdleStrategy.java)), so producers rarely find a blocked consumer. |
| **2. Per-event latency histogram.** Sink threads recorded every event into Micrometer's client-side percentile histogram. | 250 of 338 JFR samples on sink threads in `HdrHistogram` / `WriterReaderPhaser` CAS loops; workers blocked on full sink queues. | Record a uniform sample of up to 64 events per batch ([`MetricsListener`](src/main/java/io/conduit/observe/MetricsListener.java)). Counts still come from the published counter. |
| <a id="finding-2-a-lock-convoy-on-the-sink-queues"></a>**3. Lock convoy.** All workers offered into the same sink-lane `ArrayBlockingQueue`, which has one lock. | 99 of 216 worker samples `WAITING` (untimed, so on the lock, not a full queue) in `SinkChannel.offer`, while the sink thread idled. | One input queue per (worker, sink lane), drained round-robin ([`SinkChannel`](src/main/java/io/conduit/pipeline/SinkChannel.java)). Single producer per queue, and per-key order is unchanged. |

The same benchmark command, before and after (1 source thread, discarding sink with 2 lanes,
2M events; the "before" figures are single runs, the "after" figures medians of three):

| Workers | Before | After | |
|---:|---:|---:|---|
| 1 | 178,586 | 939,793 | 5.3× |
| 4 | 1,001,383 | 1,023,463 | capped by the single source thread |
| 8 | 310,995 | 857,469 | **2.8×**, and the collapse is gone |

The runs are too noisy to say how much each fix contributed on its own, so no per-fix numbers are
claimed.

### Latency

Ingest to sink acknowledgement, 20 s per rate after a JIT warmup, 8 workers, 5 ms linger:

| Rate | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|
| 100,000/s | 3.7 ms | 6.8 ms | **18.9 ms** | 619 ms |
| 250,000/s | 4.7 ms | 31.4 ms | 192.9 ms | 379 ms |
| 458,516/s (half of peak) | 7.8 ms | 52.4 ms | 130.0 ms | 308 ms |

The PRD target (p99 < 50 ms at half of peak) is **not met** on this laptop. The peak comes from 3 s
bursts, but a fanless machine does not sustain that rate for 20 s, so half of peak is already near
sustained capacity and p99 becomes queueing delay. Two things moved the tail: a JIT warmup before
measuring (cold runs showed p99 over 300 ms even at 100k/s), and `-XX:MaxGCPauseMillis=20`, which cut
the worst young-GC pause from 123 ms to 27 ms. Run-to-run variance is large: a standalone repeat at
250k/s measured p99 30 ms. The max column shows rare 300–600 ms stalls that GC logs do not account
for; they are not investigated yet.

### Failure isolation

![Healthy sink throughput while another sink hangs](docs/isolation.svg)

A file sink and a fault sink both receive every event at 200k/s; the fault sink hangs from 5 s to
11 s. Writes time out after 1 s, the breaker opens, and the hung lane sheds to dead-letter. The
healthy sink averaged **95%** of normal during the outage and caught up straight after.

The 1.5 s dip at the start is the one coupling left between sinks: **the dead-letter writer is
shared**. Once the hung lane sheds, all 200k events/s go to a single writer on a code path the JIT
has not compiled yet. Its queue fills, and since `submit` blocks rather than drops (that is the
zero-loss rule), workers stall until it catches up. Per-sink dead-letter writers, or a larger
dead-letter queue, would remove it.

The Testcontainers suite runs the same scenario against real databases: it pauses the Neo4j
container mid-run and asserts PostgreSQL still receives all 24,000 events.

### Zero loss and bounded memory

- **10,000,000 events** into a healthy sink and a fault sink that fails 5% of writes and goes down for
  2 s: 19,990,012 deliveries = 19,631,755 published + 358,257 dead-lettered; 4,994 malformed records
  dead-lettered at parse; dead-letter file lines = 363,251 = counters. 14.2 s.
- **Overload**: an unthrottled source for 60 s (79.6M events). Heap averaged 278 MB in the first
  quarter and 326 MB in the last, peaking at 644 MB of 1 GB, and the run shut down clean.
  The PRD asks for 10 minutes; run `overload --seconds 600` for that.

## Resume bullet (measured)

> Built a multithreaded Java 21 telemetry pipeline that ingests from 4 concurrent source types and
> filters, enriches, routes and publishes to PostgreSQL, Neo4j and file sinks; sustained 1.33M
> events/s, verified zero event loss across 10M events under injected sink failures, and used
> profiling to find and remove a lock convoy and a thread wake-up storm that had collapsed 8-thread
> throughput, raising it 2.8× (311k → 857k events/s on the same benchmark)

End-to-end worker scaling was 1.43× from 1 to 8 threads, for the reasons above. Quote it only with
that explanation.

## Status against the PRD

| Success criterion | Status |
|---|---|
| All 12 functional requirements from one YAML file | Done ([`config/conduit.yaml`](config/conduit.yaml), running under Docker Compose) |
| Accounting invariant holds on every benchmark and fault-injection run | Done |
| Throughput-vs-threads benchmark recorded and charted | Done |
| Healthy sink keeps its throughput while another is down | 95% on average, with the dead-letter dip above |
| Tests and benchmark smoke run pass in CI | Workflow written; not yet run on GitHub. The CI baseline in `bench/baseline-ci.json` is a conservative placeholder: record a real one from the first CI run |
| README explains the concurrency model and each pattern | Done |
| p99 < 50 ms at half of peak | Not met (see Latency) |
| Heap flat for 10 minutes at twice peak | Measured for 60 s |

