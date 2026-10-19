-- EventingC3 tracking-event warehouse schema.
--
-- Table names are unqualified on purpose: apply with the target database selected, e.g.
--   clickhouse-client --database eventing --multiquery < 001_schema.sql
-- The ETL worker applies this same file (embedded) when Etl:ClickHouse:ApplySchemaOnStartup=true.
--
-- Production (replicated cluster): add ON CLUSTER '{cluster}' to each statement and use
--   ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/tracking_events', '{replica}', ingested_at)
-- with a Distributed table in front, sharded by cityHash64(waybill).

-- Fact table: one row per tracking event.
--  * ReplacingMergeTree collapses duplicate deliveries (at-least-once sources) during background merges;
--    query with FINAL (or argMax) when exact de-duplication matters.
--  * insert_deduplication_token (sent by the worker) makes retried inserts idempotent immediately; the
--    window below enables that for non-replicated tables (replicated tables use replicated_deduplication_window).
--  * Monthly partitions keep part counts low at ~30B rows/month; the worker rejects events older than
--    Transform:MaxEventAge, so one insert touches at most a couple of partitions.
CREATE TABLE IF NOT EXISTS tracking_events
(
    event_id         UUID,
    waybill          String                              CODEC(ZSTD(1)),
    event_type       LowCardinality(String),
    occurred_at      DateTime64(3, 'UTC')                CODEC(ZSTD(1)),
    hub_code         LowCardinality(String),
    region           LowCardinality(String),
    courier_id       String                              CODEC(ZSTD(1)),
    latitude         Nullable(Float64),
    longitude        Nullable(Float64),
    source_system    LowCardinality(String),
    schema_version   UInt16,
    attributes       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ingested_at      DateTime64(3, 'UTC')                CODEC(ZSTD(1)),
    source_partition Int32,
    source_offset    Int64,
    INDEX idx_event_id   event_id   TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_courier_id courier_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (waybill, occurred_at, event_id)
SETTINGS non_replicated_deduplication_window = 10000;

-- MV target tables carry their own deduplication window: without it a retried insert that the source table
-- deduplicates is still applied to the views (verified against ClickHouse 25.8 — counts doubled).

-- Latest known status per consignment (the "where is my parcel" query), maintained on insert.
CREATE TABLE IF NOT EXISTS consignment_latest_status
(
    waybill     String,
    event_type  LowCardinality(String),
    occurred_at DateTime64(3, 'UTC'),
    hub_code    LowCardinality(String),
    region      LowCardinality(String),
    event_id    UUID
)
ENGINE = ReplacingMergeTree(occurred_at)
ORDER BY waybill
SETTINGS non_replicated_deduplication_window = 10000;

CREATE MATERIALIZED VIEW IF NOT EXISTS consignment_latest_status_mv TO consignment_latest_status AS
SELECT waybill, event_type, occurred_at, hub_code, region, event_id
FROM tracking_events;

-- Hourly volume per event type and region for dashboards. Counts can exceed the exact figure only when a
-- replayed duplicate slipped past insert de-duplication (rare); use tracking_events FINAL for exact numbers.
CREATE TABLE IF NOT EXISTS tracking_events_hourly
(
    hour       DateTime('UTC'),
    event_type LowCardinality(String),
    region     LowCardinality(String),
    events     UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, event_type, region)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE MATERIALIZED VIEW IF NOT EXISTS tracking_events_hourly_mv TO tracking_events_hourly AS
SELECT toStartOfHour(occurred_at) AS hour, event_type, region, count() AS events
FROM tracking_events
GROUP BY hour, event_type, region;
