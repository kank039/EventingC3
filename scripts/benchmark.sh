#!/usr/bin/env bash
# End-to-end throughput benchmark: pre-loads a topic (the "backlog after an outage" worst case), starts the real
# worker binary, and measures how fast it drains into ClickHouse. Verifies the result is exact.
#
#   RECORDS=10000000 PARTITIONS=12 ./scripts/benchmark.sh
#
# Needs a Kafka-compatible broker and ClickHouse (docker compose up -d redpanda clickhouse).
set -euo pipefail

RECORDS=${RECORDS:-5000000}
PARTITIONS=${PARTITIONS:-12}
KAFKA=${KAFKA:-localhost:9092}
CLICKHOUSE=${CLICKHOUSE:-http://localhost:8123}
BATCH=${BATCH:-100000}
CONCURRENCY=${CONCURRENCY:-4}
RUN_ID=bench_$(date +%s)
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT="$ROOT/artifacts/benchmark"
METRICS_URL=http://127.0.0.1:8089/metrics

mkdir -p "$OUT"
ch() { curl -sS --fail "$CLICKHOUSE/" --data-binary "$1"; }
metric() { curl -s "$METRICS_URL" | awk -v m="$1" '$1==m {print $2}'; }

echo "== Building (Release)"
dotnet build -c Release "$ROOT/src/EventingC3.Etl.Worker" -o "$OUT/worker" -v q -nologo >/dev/null
dotnet build -c Release "$ROOT/tools/EventingC3.LoadGen" -o "$OUT/loadgen" -v q -nologo >/dev/null

echo "== Producing $RECORDS records to $RUN_ID ($PARTITIONS partitions)"
dotnet "$OUT/loadgen/EventingC3.LoadGen.dll" --bootstrap "$KAFKA" --topic "$RUN_ID" --partitions "$PARTITIONS" --create-topic \
  --count "$RECORDS" --invalid 0.005 --duplicates 0.01 --missing-id 0.01 --summary "$OUT/$RUN_ID.summary.json" >/dev/null
EXPECTED=$(python3 -c "import json;print(json.load(open('$OUT/$RUN_ID.summary.json'))['expectedUniqueEvents'])")
INVALID=$(python3 -c "import json;print(json.load(open('$OUT/$RUN_ID.summary.json'))['invalid'])")

echo "== Draining with one worker (batch=$BATCH, concurrency=$CONCURRENCY)"
START=$(date +%s.%N)
ASPNETCORE_URLS=http://127.0.0.1:8089 \
Etl__Kafka__BootstrapServers=$KAFKA Etl__Kafka__Topic=$RUN_ID Etl__Kafka__GroupId=$RUN_ID \
Etl__DeadLetterKafka__Topic=$RUN_ID.dlq \
Etl__ClickHouse__Endpoint=$CLICKHOUSE Etl__ClickHouse__Database=$RUN_ID Etl__ClickHouse__ApplySchemaOnStartup=true \
Etl__Pipeline__MaxBatchRecords=$BATCH Etl__Pipeline__MaxConcurrentBatches=$CONCURRENCY \
Logging__LogLevel__Default=Warning \
  dotnet "$OUT/worker/EventingC3.Etl.Worker.dll" > "$OUT/$RUN_ID.worker.log" 2>&1 &
WORKER=$!
trap 'kill $WORKER 2>/dev/null || true' EXIT

PEAK=0; LAST=0; LAST_T=$START
while true; do
  sleep 1
  LOADED=$(metric etl_records_loaded_total); LOADED=${LOADED:-0}
  REJECTED=$(curl -s "$METRICS_URL" | awk '/^etl_records_rejected_total\{/ {s+=$2} END {print s+0}')
  DUPES=$(metric etl_records_duplicates_total); DUPES=${DUPES:-0}
  DONE=$((LOADED + REJECTED + DUPES))
  NOW=$(date +%s.%N)
  RATE=$(python3 -c "print(int(($DONE-$LAST)/($NOW-$LAST_T)))")
  [ "$RATE" -gt "$PEAK" ] && PEAK=$RATE
  LAST=$DONE; LAST_T=$NOW
  printf "\r  processed %'d / %'d  (%'d rec/s)   " "$DONE" "$RECORDS" "$RATE"
  [ "$DONE" -ge "$RECORDS" ] && break
  kill -0 $WORKER 2>/dev/null || { echo; echo "worker exited"; tail -20 "$OUT/$RUN_ID.worker.log"; exit 1; }
done
END=$(date +%s.%N)
echo

FINAL=$(ch "SELECT count() FROM \`$RUN_ID\`.tracking_events FINAL")
UNIQUE=$(ch "SELECT uniqExact(event_id) FROM \`$RUN_ID\`.tracking_events")
python3 - "$START" "$END" "$RECORDS" "$PEAK" "$EXPECTED" "$FINAL" "$UNIQUE" "$INVALID" <<'PY' | tee "$OUT/$RUN_ID.result.txt"
import sys
start, end, records, peak, expected, final, unique, invalid = map(float, sys.argv[1:])
elapsed = end - start
rate = records / elapsed
print(f"records            {int(records):,}")
print(f"elapsed            {elapsed:,.1f} s (includes worker start-up and schema creation)")
print(f"average throughput {rate:,.0f} rec/s   peak {int(peak):,} rec/s")
print(f"daily capacity     {rate*86400/1e9:,.2f} B records/day per worker  (target 1B/day = 11,574 rec/s avg)")
print(f"warehouse          FINAL={int(final):,} uniq={int(unique):,} expected={int(expected):,} dead-lettered={int(invalid):,}")
print("RESULT             " + ("EXACT" if final == expected == unique else "MISMATCH"))
PY

if [ "${KEEP:-0}" != "1" ]; then ch "DROP DATABASE IF EXISTS \`$RUN_ID\`" >/dev/null; fi
