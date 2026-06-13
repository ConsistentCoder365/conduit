# Benchmark results

Machine: Apple M1, 8 cores, 8 GB RAM, Mac OS X 26.1 aarch64, OpenJDK 64-Bit Server VM 21.0.13, heap 1024 MB.

## Throughput scaling

3,000,000 events per run, 3 runs per point, 2 synthetic source thread(s), file sink with 4 lanes.

| Workers | Median events/s | Min | Max | Speedup | Cores busy | CPU µs/event |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 639,095 | 617,119 | 652,951 | 1.00× | 3.5 | 5.45 |
| 2 | 871,662 | 788,168 | 888,266 | 1.36× | 5.5 | 6.53 |
| 4 | 828,734 | 617,527 | 891,543 | 1.30× | 5.6 | 7.21 |
| 8 | 917,032 | 885,585 | 949,864 | 1.43× | 6.4 | 6.99 |
| 16 | 442,872 | 330,106 | 470,728 | 0.69× | 5.9 | 14.53 |

## End-to-end latency

Ingest to sink acknowledgement, 20 s per rate after a JIT warmup, 8 workers, 5 ms sink linger, G1, -Xmx1g, -XX:MaxGCPauseMillis=20. Percentiles over a uniform sample of up to 64 events per batch.

| Target events/s | Achieved | p50 | p95 | p99 | max |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 99,948 | 3.7 ms | 6.8 ms | 18.9 ms | 619 ms |
| 250,000 | 249,804 | 4.7 ms | 31.4 ms | 192.9 ms | 379 ms |
| 458,516 | 456,729 | 7.8 ms | 52.4 ms | 130.0 ms | 308 ms |

## Zero loss under injected failures

10,000,000 events in 14.2 s; every event to a healthy sink and to a fault sink failing 5% of writes plus a 2 s outage.

| Counter | Value |
|---|---:|
| ingested | 10,000,000 |
| rejected_backpressure | 0 |
| filtered | 0 |
| routed | 9,995,006 |
| deliveries | 19,990,012 |
| published | 19,631,755 |
| dead_lettered_parse | 4,994 |
| dead_lettered_transform | 0 |
| dead_lettered_sink_overflow | 0 |
| dead_lettered_sink_write | 358,257 |
| dead_letter_records_written | 363,251 |

Invariant holds: **true**. Files on disk match the counters (dead-letter file lines = dead-letter records counted): **true**.

## Bounded memory under overload

60 s with an unthrottled source (1,327,445 events/s average). Heap used: 278 MB average in the first quarter, 326 MB in the last quarter, 644 MB peak, of 1024 MB max. Clean shutdown: **true**.

## Failure isolation

Healthy sink throughput: 201,666/s before the outage, 191,909/s during it (95.2%), 218,679/s after. Clean shutdown: **true**.

