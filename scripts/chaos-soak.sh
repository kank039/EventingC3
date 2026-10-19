#!/usr/bin/env bash
# Chaos soak: live traffic at peak rate for DURATION seconds through two workers while the infrastructure is
# broken on a schedule, then verifies the warehouse holds exactly the expected events.
#
#   DURATION=480 RATE=58000 ./scripts/chaos-soak.sh
#
# Timeline (seconds after traffic starts, scaled to DURATION=480):
#   0    worker A starts alone
#   60   worker B joins                       -> cooperative rebalance under load
#   120  ClickHouse restarted                 -> in-flight inserts fail, retried
#   200  worker A killed with SIGKILL          -> no drain, no commit; restarted 15 s later
#   290  ClickHouse frozen (docker pause) 45 s -> inserts hang, time out, retried with the same token
#   380  Kafka broker restarted               -> source, DLQ and producer all lose the broker
#   end  traffic stops; wait for catch-up; workers stopped with SIGTERM; exactness verified
#
# Expects the docker containers "ch" (ClickHouse, host network :8123) and "rp" (Redpanda, host network :9092).
set -uo pipefail

DURATION=${DURATION:-480}
RATE=${RATE:-58000}
PARTITIONS=${PARTITIONS:-12}
RECORDS=$((DURATION * RATE))
RUN_ID=soak_$(date +%s)
ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT="$ROOT/artifacts/soak/$RUN_ID"
CH=http://localhost:8123
mkdir -p "$OUT"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

log() { printf '%s  t=%4ss  %s\n' "$(date +%H:%M:%S)" "$(( $(date +%s) - ${T0:-$(date +%s)} ))" "$*" | tee -a "$OUT/timeline.log"; }
ch() { curl -sS -m 10 "$CH/" --data-binary "$1" 2>/dev/null; }
at() { while [ $(( $(date +%s) - T0 )) -lt "$1" ]; do sleep 1; done; }
scaled() { echo $(( $1 * DURATION / 480 )); }

dotnet build -c Release "$ROOT/src/EventingC3.Etl.Worker" -o "$OUT/bin/worker" -v q -nologo >/dev/null || exit 1
dotnet build -c Release "$ROOT/tools/EventingC3.LoadGen" -o "$OUT/bin/loadgen" -v q -nologo >/dev/null || exit 1
dotnet "$OUT/bin/loadgen/EventingC3.LoadGen.dll" --bootstrap localhost:9092 --topic "$RUN_ID" --partitions "$PARTITIONS" \
  --create-topic --count 0 >/dev/null 2>&1

declare -A PIDS
start_worker() { # name port
  ASPNETCORE_URLS=http://127.0.0.1:$2 \
  Etl__Kafka__Topic=$RUN_ID Etl__Kafka__GroupId=$RUN_ID Etl__DeadLetterKafka__Topic=$RUN_ID.dlq \
  Etl__ClickHouse__Database=$RUN_ID Etl__ClickHouse__ApplySchemaOnStartup=true \
  Etl__ClickHouse__RequestTimeout=00:00:20 \
  Logging__LogLevel__Default=Warning \
    env 'Logging__LogLevel__EventingC3.Etl.Infrastructure.Kafka=Information' \
    dotnet "$OUT/bin/worker/EventingC3.Etl.Worker.dll" >> "$OUT/worker-$1.log" 2>&1 &
  PIDS[$1]=$!
  echo "${PIDS[$1]}" > "$OUT/pid-$1"
  log "worker $1 started (pid ${PIDS[$1]}, :$2)"
}

sampler() {
  echo "t,group_lag,rows_raw,active_parts,rss_mb_A,rss_mb_B,cpu_A,cpu_B,retries_A,retries_B" > "$OUT/samples.csv"
  while true; do
    local t=$(( $(date +%s) - T0 ))
    local lag=$(timeout 5 docker exec rp rpk group describe "$RUN_ID" 2>/dev/null | awk '/^TOTAL-LAG/ {print $2}')
    local rows=$(ch "SELECT count() FROM \`$RUN_ID\`.tracking_events")
    local parts=$(ch "SELECT count() FROM system.parts WHERE active AND database='$RUN_ID' AND table='tracking_events'")
    local line="$t,${lag:-},${rows:-},${parts:-}"
    for w in A B; do
      local pid=$(cat "$OUT/pid-$w" 2>/dev/null || echo 0)
      line="$line,$(ps -o rss= -p "$pid" 2>/dev/null | awk '{printf "%d", $1/1024}')"
    done
    for w in A B; do line="$line,$(ps -o %cpu= -p "$(cat "$OUT/pid-$w" 2>/dev/null || echo 0)" 2>/dev/null | tr -d ' ')"; done
    for port in 8091 8092; do line="$line,$(curl -s -m 2 http://127.0.0.1:$port/metrics | awk '$1=="etl_sink_retries_total" {print $2}')"; done
    echo "$line" >> "$OUT/samples.csv"
    sleep 5
  done
}

T0=$(date +%s)
log "run $RUN_ID: $RECORDS records at $RATE rec/s over ${DURATION}s, $PARTITIONS partitions"
start_worker A 8091
for _ in $(seq 1 30); do curl -sf -m 1 http://127.0.0.1:8091/healthz >/dev/null && break; sleep 1; done
curl -sf -m 1 http://127.0.0.1:8091/healthz >/dev/null || { log "worker A failed to start"; tail -20 "$OUT/worker-A.log"; exit 1; }
sleep 2
T0=$(date +%s)
dotnet "$OUT/bin/loadgen/EventingC3.LoadGen.dll" --bootstrap localhost:9092 --topic "$RUN_ID" --count "$RECORDS" --rate "$RATE" \
  --acks all --invalid 0.005 --duplicates 0.01 --missing-id 0.01 --summary "$OUT/producer-summary.json" > "$OUT/producer.log" 2>&1 &
PRODUCER=$!
log "producer started (pid $PRODUCER)"
sampler & SAMPLER=$!
trap 'kill $SAMPLER $PRODUCER ${PIDS[A]:-} ${PIDS[B]:-} 2>/dev/null' EXIT

at "$(scaled 60)";  start_worker B 8092
at "$(scaled 120)"; log "CHAOS: restarting ClickHouse"; docker restart ch >/dev/null; log "ClickHouse back"
at "$(scaled 200)"; log "CHAOS: SIGKILL worker A"; kill -9 "${PIDS[A]}"
at "$(scaled 215)"; start_worker A 8091
at "$(scaled 290)"; log "CHAOS: freezing ClickHouse for 45s"; docker pause ch >/dev/null
at "$(( $(scaled 290) + 45 ))"; docker unpause ch >/dev/null; log "ClickHouse unfrozen"
at "$(scaled 380)"; log "CHAOS: restarting Kafka broker"; docker restart rp >/dev/null; log "Kafka broker back"

wait "$PRODUCER"; PRODUCER_EXIT=$?
log "producer finished (exit $PRODUCER_EXIT)"
EXPECTED=$(python3 -c "import json;print(json.load(open('$OUT/producer-summary.json'))['expectedUniqueEvents'])")
INVALID=$(python3 -c "import json;print(json.load(open('$OUT/producer-summary.json'))['invalid'])")

log "waiting for catch-up (expected $EXPECTED unique events)"
CAUGHT_UP=0
for _ in $(seq 1 240); do
  U=$(ch "SELECT uniqExact(event_id) FROM \`$RUN_ID\`.tracking_events")
  LAG=$(timeout 5 docker exec rp rpk group describe "$RUN_ID" 2>/dev/null | awk '/^TOTAL-LAG/ {print $2}')
  if [ "${U:-0}" -ge "$EXPECTED" ] && [ "${LAG:-1}" = "0" ]; then CAUGHT_UP=1; break; fi
  sleep 5
done
log "caught up: $CAUGHT_UP (unique=$U lag=$LAG)"

for w in A B; do kill -TERM "${PIDS[$w]}" 2>/dev/null; done
for w in A B; do wait "${PIDS[$w]}"; echo "$?" > "$OUT/exit-$w"; done
log "workers stopped (exit A=$(cat "$OUT/exit-A") B=$(cat "$OUT/exit-B"))"
kill $SAMPLER 2>/dev/null

# ---- Verification -------------------------------------------------------------------------------------------
FINAL=$(ch "SELECT count() FROM \`$RUN_ID\`.tracking_events FINAL")
UNIQUE=$(ch "SELECT uniqExact(event_id) FROM \`$RUN_ID\`.tracking_events")
RAW=$(ch "SELECT count() FROM \`$RUN_ID\`.tracking_events")
HOURLY=$(ch "SELECT sum(events) FROM \`$RUN_ID\`.tracking_events_hourly")
LATEST=$(ch "SELECT count() FROM \`$RUN_ID\`.consignment_latest_status FINAL")
WAYBILLS=$(ch "SELECT uniqExact(waybill) FROM \`$RUN_ID\`.tracking_events")
DLQ_TOTAL=$(docker exec rp rpk topic describe "$RUN_ID.dlq" -p 2>/dev/null | awk 'NR>1 {s+=$NF} END {print s+0}')
DLQ_DISTINCT=$(docker exec rp rpk topic consume "$RUN_ID.dlq" -o start -n "$DLQ_TOTAL" -f json 2>/dev/null | python3 -c "
import sys, json
text, pos, seen = sys.stdin.read(), 0, set()
decoder = json.JSONDecoder()
while True:
    while pos < len(text) and text[pos].isspace(): pos += 1
    if pos >= len(text): break
    record, pos = decoder.raw_decode(text, pos)   # rpk prints pretty, multi-line JSON objects back to back
    h = {x['key']: x['value'] for x in record.get('headers', [])}
    seen.add((h.get('dlq.partition'), h.get('dlq.offset')))
print(len(seen))")
FAILED_DELIVERIES=$(grep -c "failed delivery" "$OUT/producer.log")
CRASH=$(grep -lE "Unhandled exception|crit:" "$OUT"/worker-*.log 2>/dev/null | wc -l)

python3 - "$OUT" <<PY | tee "$OUT/report.txt"
import csv
rows = list(csv.DictReader(open("$OUT/samples.csv")))
def num(v):
    try: return float(v)
    except: return None
lags = [num(r['group_lag']) for r in rows if num(r['group_lag']) is not None]
parts = [num(r['active_parts']) for r in rows if num(r['active_parts']) is not None]
rss = [num(r[k]) for r in rows for k in ('rss_mb_A','rss_mb_B') if num(r[k])]
cpu = [num(r[k]) for r in rows for k in ('cpu_A','cpu_B') if num(r[k])]
expected, final, unique = $EXPECTED, $FINAL, $UNIQUE
print("=== Chaos soak report: $RUN_ID ===")
print(f"traffic            $RECORDS records at $RATE rec/s for ${DURATION}s ({$RATE*86400/1e9:.1f}B/day rate), producer exit $PRODUCER_EXIT, failed deliveries $FAILED_DELIVERIES")
print(f"caught up          {'yes' if $CAUGHT_UP else 'NO'}")
print(f"events             expected {expected:,}  unique {unique:,}  FINAL {final:,}  raw rows {$RAW:,} (replayed duplicates pending merge: {$RAW-final:,})")
print(f"dead letters       generated invalid {$INVALID:,}  distinct in DLQ {$DLQ_DISTINCT:,}  total DLQ messages {$DLQ_TOTAL:,}")
print(f"views              hourly sum {$HOURLY:,}  latest-status rows {$LATEST:,} (distinct waybills {$WAYBILLS:,})")
print(f"consumer lag       max {max(lags):,.0f} records ({max(lags)/$RATE:.1f}s of traffic), final {lags[-1]:,.0f}" if lags else "consumer lag n/a")
print(f"active parts       max {max(parts):.0f}, final {parts[-1]:.0f}" if parts else "parts n/a")
print(f"worker memory      max RSS {max(rss):,.0f} MB" if rss else "rss n/a")
print(f"worker cpu         max {max(cpu):.0f}% (ps average since start)" if cpu else "cpu n/a")
print(f"worker exits       A=$(cat "$OUT/exit-A") B=$(cat "$OUT/exit-B"), logs with crashes: $CRASH")
ok = unique == expected == final and $DLQ_DISTINCT == $INVALID and $CAUGHT_UP == 1 and $LATEST == $WAYBILLS
print("RESULT             " + ("PASS - exact, no loss, no duplicates" if ok else "FAIL"))
PY
