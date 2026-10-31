#!/usr/bin/env bash
# Stage 3 버그 재현: KAFKA-18376 (작은 max.poll.records에서 consumer 프로토콜 컨슈머의 CPU 과다)
#   repro-18376.sh prepare            bench 토픽 생성, 레코드 적재, 클라이언트 4.1.2/4.2.0 이미지 준비
#   repro-18376.sh run [reps]         클라이언트 버전 × 프로토콜 × max.poll.records 행렬 측정
#   repro-18376.sh profile <variant>  JFR로 애플리케이션 스레드 CPU 샘플을 모아 상위 프레임을 집계
#                                     variant: classic-4.1.2 | consumer-4.1.2 | consumer-4.2.0 | classic-4.2.0
#   repro-18376.sh report             결과 표 출력
#   repro-18376.sh clean              bench 토픽 삭제
set -euo pipefail
. "$(dirname "$0")/lib.sh"

BENCH_TOPIC="${BENCH_TOPIC:-bench-18376}"
BENCH_RECORDS="${BENCH_RECORDS:-2000000}"
BENCH_VALUE_BYTES="${BENCH_VALUE_BYTES:-20}"
BENCH_CPUS="${BENCH_CPUS:-2}"
BENCH_MPR="${BENCH_MPR:-1 5 500}"
BENCH_VARIANTS="${BENCH_VARIANTS:-classic-4.1.2 consumer-4.1.2 consumer-4.2.0 classic-4.2.0}"
OUT_DIR="$RUNS_DIR/repro-18376"
NETWORK="rebalance-lab_default"

app_image() {
  case "$1" in
    4.1.2) echo rebalance-lab-app:latest ;;
    *) echo "rebalance-lab-app:clients-$1" ;;
  esac
}

build_clients() {
  local v
  for v in 4.1.2 4.2.0; do
    if ! docker image inspect "$(app_image "$v")" >/dev/null 2>&1; then
      log "build $(app_image "$v")"
      docker build -q --build-arg KAFKA_CLIENTS_VERSION="$v" -t "$(app_image "$v")" "$LAB_ROOT/lab-app" >/dev/null
    fi
  done
}

cmd_prepare() {
  require_cluster
  build_clients
  mkdir -p "$OUT_DIR"
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$BENCH_TOPIC" >/dev/null 2>&1 || true
  sleep 3
  # 소비 비용만 재므로 복제는 필요 없다. 디스크(브로커당 256MB)를 아끼려고 RF 1로 둔다.
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --create --topic "$BENCH_TOPIC" \
    --partitions 3 --replication-factor 1 --config min.insync.replicas=1 --config retention.bytes=-1 >/dev/null
  log "preload $BENCH_RECORDS records x ${BENCH_VALUE_BYTES}B into $BENCH_TOPIC"
  docker run --rm --network "$NETWORK" -e KAFKA_HEAP_OPTS=-Xmx256m --entrypoint /opt/kafka/bin/kafka-producer-perf-test.sh \
    "rebalance-lab-kafka:${KAFKA_VERSION:-4.1.2}" --topic "$BENCH_TOPIC" --num-records "$BENCH_RECORDS" \
    --record-size "$BENCH_VALUE_BYTES" --throughput -1 \
    --producer-props bootstrap.servers="$BOOTSTRAP_INTERNAL" acks=1 linger.ms=20 batch.size=262144 2>&1 | tail -1
}

# variant 하나를 한 번 실행하고 BENCH 결과 JSON 한 줄을 출력한다.
bench_once() {
  local variant="$1" mpr="$2" extra_opts="${3:-}" protocol version max
  protocol="${variant%-*}"
  version="${variant#*-}"
  max="$BENCH_RECORDS"
  # 작은 max.poll.records는 느리므로 레코드 수를 줄여 같은 시간 안에 끝낸다. 비교는 1만 건당 CPU로 한다.
  [ "$mpr" -le 5 ] && max=$(( BENCH_RECORDS / 4 ))
  docker run --rm --network "$NETWORK" --cpus "$BENCH_CPUS" -m 768m -v "$OUT_DIR:/out" \
    -e BOOTSTRAP_SERVERS="$BOOTSTRAP_INTERNAL" -e TOPIC="$BENCH_TOPIC" -e GROUP_PROTOCOL="$protocol" \
    -e MAX_POLL_RECORDS="$mpr" -e MAX_RECORDS="$max" \
    -e JAVA_TOOL_OPTIONS="-Xmx384m $extra_opts" \
    "$(app_image "$version")" bench-consumer 2>/dev/null | sed -n 's/^BENCH //p'
}

cmd_run() {
  local reps="${1:-3}" variant mpr i line
  require_cluster
  mkdir -p "$OUT_DIR"
  local out="$OUT_DIR/results.jsonl"
  for mpr in $BENCH_MPR; do
    for i in $(seq 1 "$reps"); do
      for variant in $BENCH_VARIANTS; do
        line="$(bench_once "$variant" "$mpr")"
        [ -n "$line" ] || { log "WARN: $variant mpr=$mpr produced no result"; continue; }
        echo "$line" >> "$out"
        log "$variant mpr=$mpr rep=$i $(echo "$line" | python3 -c 'import json,sys; d=json.loads(sys.stdin.read()); print("%.0f rec/s, %.1f cpu-ms/10k, %.2f cores" % (d["recordsPerSec"], d["cpuMsPer10kRecords"], d["cpuCores"]))')"
      done
    done
  done
  cmd_report
}

cmd_profile() {
  local variant="${1:?variant}" mpr="${2:-1}" jfr name
  require_cluster
  mkdir -p "$OUT_DIR"
  name="profile-$variant-mpr$mpr"
  jfr="/out/$name.jfr"
  log "JFR $variant mpr=$mpr"
  bench_once "$variant" "$mpr" "-XX:StartFlightRecording=filename=$jfr,settings=profile,dumponexit=true" \
    > "$OUT_DIR/$name.json"
  docker run --rm -v "$OUT_DIR:/out" --entrypoint jfr rebalance-lab-app:latest \
    print --json --events jdk.ExecutionSample,jdk.NativeMethodSample --stack-depth 64 "$jfr" > "$OUT_DIR/$name.samples.json"
  {
    echo "$(cat "$OUT_DIR/$name.json")"
    echo
    python3 "$LAB_ROOT/scripts/jfr_top.py" "$OUT_DIR/$name.samples.json" --filter kafka
  } | tee "$OUT_DIR/$name.top.txt"
}

cmd_report() {
  python3 - "$OUT_DIR/results.jsonl" <<'PY'
import collections, json, statistics, sys
rows = collections.defaultdict(list)
for line in open(sys.argv[1]):
    d = json.loads(line)
    rows[(d["maxPollRecords"], d["protocol"], d["client"])].append(d)
print("| max.poll.records | protocol | client | runs | records/s | CPU ms / 1만 건 | 사용 코어 | app 스레드 CPU 비중 | poll당 컨텍스트 스위치(애플리케이션 / background) |")
print("|---:|---|---|---:|---:|---:|---:|---:|---|")
for (mpr, proto, client), ds in sorted(rows.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])):
    med = lambda k: statistics.median(x[k] for x in ds)
    main = statistics.median(x["threadCpuMs"].get("main", 0) / max(sum(x["threadCpuMs"].values()), 1e-9) for x in ds)
    sw = [x.get("switchesPerPoll") for x in ds if x.get("switchesPerPoll") is not None]
    cs = "-"
    if sw:
        m = statistics.median(x.get("java", 0) for x in sw)
        b = statistics.median(sum(v for k, v in x.items() if k.startswith("consumer_backgr")) for x in sw)
        cs = "%.2f / %.2f" % (m, b)
    print("| %d | %s | %s | %d | %.0f | %.1f | %.2f | %.0f%% | %s |" % (mpr, proto, client, len(ds), med("recordsPerSec"),
          med("cpuMsPer10kRecords"), med("cpuCores"), main * 100, cs))
PY
}

case "${1:-}" in
  prepare) cmd_prepare ;;
  run) cmd_run "${2:-3}" ;;
  profile) cmd_profile "${2:?variant}" "${3:-1}" ;;
  report) cmd_report ;;
  clean) kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$BENCH_TOPIC" ;;
  *) sed -n '2,9p' "$0"; exit 2 ;;
esac
