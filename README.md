# EventingC3 — Tracking-event ETL (1B records/day)

A .NET 10 ETL worker that streams RAM consignment tracking events from **Kafka / Azure Event Hubs** (or
NDJSON bulk files from SFTP drops) into **ClickHouse**. It validates, normalises and enriches each record and
loads them idempotently. The design target is 1 billion records/day under worst-case conditions: bursts,
malformed input, duplicates, warehouse outages, crashes and rebalances.

```
Kafka / Event Hubs ─┐                                     ┌─▶ ClickHouse tracking_events (+ latest-status & hourly MVs)
                    ├─▶ batch ─▶ parse/validate/enrich ─▶ ┤
NDJSON(.gz) files ──┘   (back-pressured, bounded memory)  └─▶ dead-letter topic / files (original bytes + reason)
```

Architecture, failure modes and sizing are in **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**.

## Why this stack

* **.NET 10 worker**: RAM's backend standard. One process sustains several times the 1B/day average rate,
  so a Spark/Flink cluster would only add operational cost.
* **Kafka protocol**: works unchanged against Azure Event Hubs (Kafka endpoint) and plain Kafka/Redpanda.
  The partition is the unit of scale-out and ordering (keyed by waybill).
* **ClickHouse** is RAM's analytical store. The worker sends `RowBinary` over HTTP (cheapest server-side
  ingest), as one large atomic insert per batch, using `ReplacingMergeTree` with monthly partitions.

## Repository layout

| Path | What |
|---|---|
| `src/EventingC3.Etl.Core` | Domain, parser/validator, RowBinary encoder, batching, offset tracking, pipeline, metrics |
| `src/EventingC3.Etl.Infrastructure` | Kafka source + DLQ, ClickHouse HTTP sink, file source + DLQ |
| `src/EventingC3.Etl.Worker` | Host: configuration, `/healthz`, `/readyz`, `/metrics`, Dockerfile |
| `tools/EventingC3.LoadGen` | Synthetic, deliberately messy load (invalid, duplicate and id-less events) for Kafka or files |
| `bench/EventingC3.Etl.Benchmarks` | CPU micro-benchmark of the transform stage |
| `tests/EventingC3.Etl.Tests` | Unit tests (parser fuzzing, encoding, batching, retries, bisection, offset safety) |
| `tests/EventingC3.Etl.IntegrationTests` | Against real ClickHouse + Kafka: round-trip, idempotency, crash, outage, rebalance, backfill |
| `deploy/clickhouse/001_schema.sql` | Warehouse DDL (embedded into the worker for dev bootstrap) |
| `deploy/k8s/etl-worker.yaml` | AKS Deployment, PDB, KEDA lag-based autoscaling |
| `scripts/benchmark.sh` | End-to-end throughput benchmark with the real worker binary |
| `scripts/chaos-soak.sh` | Peak-rate soak with injected ClickHouse, Kafka and worker failures, plus exactness checks |
| `azure-pipelines.yml` | CI: build, unit + integration tests, image push |

## Run it locally

```bash
docker compose up -d --build                      # Redpanda, ClickHouse, 2 ETL workers
docker compose --profile load run --rm loadgen    # 1M synthetic events
curl -s localhost:8081/metrics | grep etl_records
docker compose exec clickhouse clickhouse-client -q \
  "SELECT event_type, count() FROM eventing.tracking_events FINAL GROUP BY event_type"
```

Without Docker for the worker:

```bash
dotnet run --project src/EventingC3.Etl.Worker    # uses appsettings.json (localhost:9092 / localhost:8123)
```

Bulk backfill from files:

```bash
dotnet run -c Release --project tools/EventingC3.LoadGen -- --out ./data/inbox --files 4 --count 1000000
Etl__Source=File Etl__DeadLetter=File Etl__Files__StopWhenIdle=true dotnet run --project src/EventingC3.Etl.Worker
```

## Configuration

All settings live under `Etl` in `appsettings.json` and can be overridden with environment variables
(`Etl__Pipeline__MaxBatchRecords=200000`). The main ones:

| Setting | Default | Notes |
|---|---|---|
| `Source` / `DeadLetter` | `Kafka` | `Kafka` or `File` |
| `Pipeline:MaxBatchRecords` | 100,000 | ≤ 1,000,000 so each insert stays one atomic ClickHouse block |
| `Pipeline:MaxBatchLatency` | 2 s | Bounds latency when traffic is low |
| `Pipeline:MaxConcurrentBatches` | 4 | Parallel transform/load; memory ≈ this × MaxBatchBytes × 3 |
| `Pipeline:RetryMaxDelay` | 30 s | Retries never give up. The pipeline stalls instead of dropping data |
| `Transform:MaxEventAge` | 30 days | Older events go to the DLQ; raise it for backfills |
| `Transform:DefaultUtcOffset` | +02:00 | Applied to timestamps without a zone (SAST) |
| `Kafka:ConsumerConfig` | — | Raw librdkafka overrides (SASL for Event Hubs, etc.) |
| `ClickHouse:Compression` | `none` | `gzip`/`br` for cross-zone links |
| `ClickHouse:Settings` | — | Extra insert settings, e.g. `insert_quorum` |

Event Hubs example:

```bash
Etl__Kafka__BootstrapServers=<namespace>.servicebus.windows.net:9093
Etl__Kafka__ConsumerConfig__security.protocol=SASL_SSL
Etl__Kafka__ConsumerConfig__sasl.mechanism=PLAIN
Etl__Kafka__ConsumerConfig__sasl.username='$ConnectionString'
Etl__Kafka__ConsumerConfig__sasl.password='<connection string>'
```

## Tests

```bash
dotnet test tests/EventingC3.Etl.Tests                          # unit tests, no dependencies

docker compose up -d redpanda clickhouse                        # integration tests need the real services
ETL_IT=1 ETL_IT_KAFKA=localhost:19092 dotnet test tests/EventingC3.Etl.IntegrationTests
```

## Benchmark results

Measured on a **single 4-vCPU / 15 GB sandbox VM running everything at once**: Redpanda (1 core), ClickHouse
25.8, the load generator and **one** worker process. A production deployment gives each its own hardware, so
treat these numbers as a floor.

| Test | Result |
|---|---|
| Transform CPU cost (`bench/`) | 3.4 µs/record/core for parse + validate + enrich + encode → **291k rec/s on one core** (25× the 1B/day average) |
| Backlog drain, 10M records, 12 partitions, 1 worker (`scripts/benchmark.sh`) | **130k rec/s average = 11.2 B records/day** per worker (11× target); exact result (9,850,635 unique = expected), no retries |
| Sustained live traffic at 58k rec/s (5× the 1B/day average) for 2 min | Kept up; max lag ~4.7 s (batching latency), done 1.8 s after the last record was produced; exact result |
| Container smoke test (published image) | 200k records: 196,034 loaded + 2,019 dead-lettered + 1,947 duplicates = 200,000; exact warehouse count; clean SIGTERM exit |

Reliability tests run against the real services (`tests/EventingC3.Etl.IntegrationTests`), each ending with
an exact warehouse count, all invalid records in the DLQ, and every offset committed:

| Scenario | Runs |
|---|---|
| Steady stream (300k records with 2% invalid, 3% duplicates, 2% missing ids) | 4 / 4 |
| Worker killed mid-stream, replacement resumes | 15 / 15 |
| ClickHouse unplugged for 8 s mid-insert (TCP proxy cuts connections) | 4 / 4 |
| Scale-out then scale-in rebalance under back-pressure | 23 / 23 after the fix (failed ~1 in 7 before; see ARCHITECTURE.md) |
| Gzip NDJSON file backfill | 4 / 4 |

### Chaos soak (`scripts/chaos-soak.sh`)

8 minutes of live traffic at 58k rec/s (5× the 1B/day average) through two workers, with five failures
injected on a schedule. **Result: PASS. Every record is accounted for, with no loss and no duplicates.**

| Injected at | Failure | Observed |
|---|---|---|
| 60 s | Second worker joins (rebalance under load) | No stall |
| 120 s | ClickHouse restarted | A few retried inserts, no stall |
| 200 s | Worker killed with SIGKILL, restarted 15 s later | ~40 s stall until Kafka's `session.timeout.ms` (45 s) expired the dead member, then caught up at ~100k rec/s |
| 290 s | ClickHouse frozen for 45 s (inserts hang and time out) | Back-pressure; backlog peaked at 3.6M records, cleared ~30 s after the thaw |
| 380 s | Kafka broker restarted (source, DLQ and producer all disconnected) | Reconnected automatically, no stall |

| Check | Result |
|---|---|
| Events (27.84M produced) | expected 27,422,974 = unique 27,422,974 = `FINAL` 27,422,974 |
| Dead letters | 138,633 invalid generated = 138,633 distinct in DLQ (no replays needed) |
| Latest-status view | 1,999,999 rows = 1,999,999 distinct waybills |
| Caught up after traffic stopped | 10 s; consumer lag 0, all offsets committed |
| Workers | max RSS 2.1 GB during catch-up, both exited 0 on SIGTERM, no crashes |
| ClickHouse active parts | max 23, settled to 8 (merges keep up) |

The hourly rollup came out 0.5% high (27.56M) because of rows replayed after the SIGKILL. That's expected and
documented: use `FINAL` for exact counts. To fail over faster after a hard crash, lower
`Etl__Kafka__ConsumerConfig__session.timeout.ms`; the trade-off is more false evictions during long GC or
network pauses.

Reproduce:

```bash
dotnet run -c Release --project bench/EventingC3.Etl.Benchmarks -- 2000000
RECORDS=10000000 PARTITIONS=12 ./scripts/benchmark.sh
RATE=58000 RECORDS=7000000 PARTITIONS=12 ./scripts/benchmark.sh
DURATION=480 RATE=58000 ./scripts/chaos-soak.sh     # needs containers "ch" and "rp" on the host network
```

### What these numbers do and don't show

* 1B/day is 11.6k rec/s on average. One worker on shared hardware sustains ~130k rec/s, so the cluster's
  replicas are for availability and catch-up rather than raw throughput. A 6-hour backlog (~250M records at
  peak mix) drains in ~30 min with one worker, or minutes with KEDA scaling to the partition count.
* Not tested here: multi-day runs (ClickHouse merge behaviour at 30B rows/month), a replicated or sharded
  ClickHouse cluster, and Azure Event Hubs itself (the Kafka protocol was tested against Redpanda). Do a
  staging soak on the real Event Hubs + ClickHouse cluster before production cut-over.
