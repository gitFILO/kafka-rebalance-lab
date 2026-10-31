#!/usr/bin/env bash
# Stage 1 처리량·지연 측정. kafka-producer-perf-test를 별도 컨테이너로 띄워 설정을 바꿔 가며 잰다.
#   perf.sh sweep <묶음>        한 가지 변수만 바꾸는 측정 묶음. 묶음 이름은 docs/stage1-throughput/README.md의 표
#   perf.sh all                 payload를 뺀 모든 묶음을 PERF_REPEAT회(기본 3회) 반복
#   perf.sh point key=value ... 한 점만 측정(예: acks=1 linger=20 batch=65536 brokercpus=1)
#   perf.sh report              results/runs/perf/report.md와 그래프 생성. 같은 점의 반복은 중앙값으로 묶는다
set -euo pipefail
. "$(dirname "$0")/lib.sh"

PERF_DIR="$RUNS_DIR/perf"
PERF_TOPIC="${PERF_TOPIC:-perf}"
PERF_PARTITIONS="${PERF_PARTITIONS:-6}"
PERF_CPUS="${PERF_CPUS:-4}"
RECORD_BYTES="${RECORD_BYTES:-512}"
POINT_SEC="${POINT_SEC:-15}"
PERF_REPEAT="${PERF_REPEAT:-3}"
# --warmup-records는 4.2부터 있다. 지연을 정상 상태만으로 재는 load-steady 묶음에만 쓴다.
PERF_STEADY_IMAGE="${PERF_STEADY_IMAGE:-apache/kafka:4.2.1}"
NETWORK="rebalance-lab_default"
SNAPSHOT_CONTAINER=prometheus
CSV="$PERF_DIR/results.csv"

payload_file() {
  local f="$PERF_DIR/payload.txt"
  [ -f "$f" ] && { echo "$f"; return; }
  # 실제 이벤트와 비슷하게 키 이름은 반복되고 값은 다양한 JSON 레코드를 만든다.
  # 줄 수가 적으면 한 배치에 같은 줄이 겹쳐 압축률이 부풀어서 배치 크기보다 훨씬 많이 둔다.
  python3 - "$f" "$RECORD_BYTES" <<'PY'
import json, random, sys
random.seed(7)
path, size = sys.argv[1], int(sys.argv[2])
events = ["page_view", "add_to_cart", "checkout", "login", "search", "refund"]
with open(path, "w") as out:
    for i in range(20000):
        rec = {
            "eventId": "%032x" % random.getrandbits(128),
            "type": random.choice(events),
            "userId": "user-%06d" % random.randint(0, 200000),
            "sessionId": "%016x" % random.getrandbits(64),
            "ts": 1767225600000 + i * 37,
            "device": random.choice(["ios", "android", "web"]),
            "country": random.choice(["KR", "JP", "US", "DE", "VN"]),
            "amount": round(random.random() * 100000, 2),
            "items": [{"sku": "SKU-%05d" % random.randint(0, 50000), "qty": random.randint(1, 5)}
                      for _ in range(random.randint(1, 4))],
        }
        line = json.dumps(rec, separators=(",", ":"))
        pad = size - len(line) - 12
        if pad > 0:
            rec["note"] = "".join(random.choice("abcdefghijklmnopqrstuvwxyz ") for _ in range(pad))
            line = json.dumps(rec, separators=(",", ":"))
        out.write(line[:size] if len(line) > size else line)
        out.write("\n")
PY
  echo "$f"
}

volume_mb() {
  docker exec broker-1 df -m /var/lib/kafka-data | awk 'END {print $2}'
}

# 측정점 하나가 RF 3이라 브로커마다 이만큼 쌓인다. BrokerDiskSpaceLow(여유 25%)에 닿지 않게 볼륨의 70%까지만 쓴다.
point_max_mb() {
  if [ -n "${POINT_MAX_MB:-}" ]; then
    echo "$POINT_MAX_MB"
  else
    echo $(( $(volume_mb) * 70 / 100 ))
  fi
}

disk_free_mb() {
  local b free min=""
  for b in broker-1 broker-2 broker-3; do
    free="$(docker exec "$b" df -m /var/lib/kafka-data | awk 'END {print $4}')"
    if [ -z "$min" ] || [ "$free" -lt "$min" ]; then min="$free"; fi
  done
  echo "$min"
}

wait_disk() {
  local need="$1" deadline=$(( $(date +%s) + 120 )) free
  while :; do
    free="$(disk_free_mb)"
    [ "$free" -ge "$need" ] && return 0
    [ "$(date +%s)" -ge "$deadline" ] && die "broker disk free ${free}MB < ${need}MB"
    sleep 1
  done
}

recreate_topic() {
  local need="$1"
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$PERF_TOPIC" >/dev/null 2>&1 || true
  local deadline=$(( $(date +%s) + 60 ))
  # 삭제 직후 같은 이름으로 만들면 TopicExistsException이 난다. 사라질 때까지 기다린다.
  while kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --list 2>/dev/null | grep -qx "$PERF_TOPIC"; do
    [ "$(date +%s)" -ge "$deadline" ] && die "topic $PERF_TOPIC not deleted"
    sleep 1
  done
  wait_disk "$need"
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --create --topic "$PERF_TOPIC" \
    --partitions "$PERF_PARTITIONS" --replication-factor 3 --config min.insync.replicas=2 >/dev/null
}

# 짧은 측정점은 수집 주기(5초)로 구간을 자를 수 없으므로 측정 전후에 exporter를 직접 읽는다.
broker_snapshot() {
  local b
  for b in broker-1 broker-2 broker-3; do
    echo "broker $b"
    docker exec "$SNAPSHOT_CONTAINER" wget -qO- "http://$b:9404/metrics" \
      | grep -E '^process_cpu_seconds_total |^kafka_network_requestmetrics_[a-z]+timems_(mean|count_total)\{request="Produce"'
  done
}

broker_config() {
  kafka_bin kafka-configs.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --alter --entity-type brokers --entity-default "$@" >/dev/null
}

begin_measure() {
  require_cluster
  # 토픽을 지워도 파일은 브로커 기본값(60초)만큼 남는다. 이 값은 토픽 설정으로 줄지 않아 측정 동안만 낮춘다.
  broker_config --add-config log.segment.delete.delay.ms=1000
  trap end_measure EXIT
  [ "$(volume_mb)" -ge 512 ] || log "WARN: 브로커 볼륨이 $(volume_mb)MB다. 최대 부하 점이 1초대로 끝나 JVM 워밍업만 잰다. BROKER_DISK_MB=1024 ./scripts/cluster.sh reset 후 측정한다."
  if command -v pmset >/dev/null && pmset -g batt | grep -q "Battery Power"; then
    log "WARN: 배터리 전원이다. 같은 설정이 AC보다 처리량이 낮고 p99가 튀었다. 전원을 연결하고 측정한다."
  fi
}

# 전원과 덮개 상태가 바뀌면 같은 설정도 결과가 달라진다. 점마다 기록하고, PERF_REQUIRE_HOST를 주면 다를 때 멈춘다.
host_state() {
  local power=unknown lid=unknown
  if command -v pmset >/dev/null; then
    if pmset -g batt | grep -q "AC Power"; then power=ac; else power=battery; fi
    if ioreg -r -k AppleClamshellState -d 1 | grep -q 'AppleClamshellState" = Yes'; then lid=closed; else lid=open; fi
  fi
  echo "$power-$lid"
}

# 수집이 지연에 주는 영향을 재려고 멈춘다. 멈춘 동안 스냅샷은 Prometheus 대신 Alertmanager 컨테이너에서 읽는다.
pause_scrape() {
  docker pause prometheus >/dev/null
  SNAPSHOT_CONTAINER=alertmanager
}

resume_scrape() {
  docker unpause prometheus >/dev/null 2>&1 || true
  SNAPSHOT_CONTAINER=prometheus
}

set_broker_cpus() {
  docker update --cpus "$1" broker-1 broker-2 broker-3 >/dev/null
}

# --cpus 0은 제한을 풀지 못하고 무시된다. 호스트 코어 수로 되돌린다.
clear_broker_cpus() {
  set_broker_cpus "$(docker info --format '{{.NCPU}}')"
}

end_measure() {
  resume_scrape
  clear_broker_cpus || true
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$PERF_TOPIC" >/dev/null 2>&1 || true
  # 먼저 되돌리면 지운 파일이 60초 더 남아 다음 측정이 기다린다. 사라진 뒤에 되돌린다.
  ( wait_disk $(( $(volume_mb) * 90 / 100 )) ) || true
  broker_config --delete-config log.segment.delete.delay.ms || true
}

# 프로듀서 CPU는 컨테이너가 끝나면 읽을 수 없어 같은 셸에서 마지막에 출력한다.
run_producer() {
  local records="$1" rps="$2" acks="$3" batch="$4" linger="$5" compression="$6" inflight="$7" warmup="$8" payload="$9"
  local image="rebalance-lab-kafka:${KAFKA_VERSION:-4.1.2}" payload_args="--payload-file /perf/$(basename "$(payload_file)")"
  [ "$payload" = random ] && payload_args="--record-size $RECORD_BYTES"
  if [ "$warmup" = half ]; then
    image="$PERF_STEADY_IMAGE"
    payload_args="$payload_args --warmup-records $(( records / 2 ))"
  fi
  # shellcheck disable=SC2016,SC2086
  docker run --rm --network "$NETWORK" --cpus "$PERF_CPUS" -m "${PERF_MEM:-768m}" \
    -e KAFKA_HEAP_OPTS="${PERF_HEAP_OPTS:--Xmx512m}" -e KAFKA_OPTS="${PERF_JVM_OPTS:-}" \
    -v "$PERF_DIR:/perf" --entrypoint sh "$image" \
    -c '/opt/kafka/bin/kafka-producer-perf-test.sh "$@"; echo "cgroup_usage_usec $(sed -n "s/^usage_usec //p" /sys/fs/cgroup/cpu.stat)"' sh \
    --topic "$PERF_TOPIC" --num-records "$records" $payload_args --throughput "$rps" --print-metrics \
    --producer-props bootstrap.servers="$BOOTSTRAP_INTERNAL" acks="$acks" batch.size="$batch" linger.ms="$linger" \
    compression.type="$compression" buffer.memory=67108864 max.in.flight.requests.per.connection="$inflight"
}

# point acks=all batch=65536 linger=5 compression=none rps=-1 inflight=5 brokercpus=0 warmup=none payload=file label=...
cmd_point() {
  local sweep="adhoc" label="" acks=all batch=65536 linger=5 compression=none rps=-1 inflight=5 brokercpus=0 warmup=none payload=file kv
  for kv in "$@"; do
    case "$kv" in
      sweep=*) sweep="${kv#*=}" ;; label=*) label="${kv#*=}" ;; acks=*) acks="${kv#*=}" ;;
      batch=*) batch="${kv#*=}" ;; linger=*) linger="${kv#*=}" ;; compression=*) compression="${kv#*=}" ;;
      rps=*) rps="${kv#*=}" ;; inflight=*) inflight="${kv#*=}" ;; brokercpus=*) brokercpus="${kv#*=}" ;;
      warmup=*) warmup="${kv#*=}" ;; payload=*) payload="${kv#*=}" ;; *) die "unknown option $kv" ;;
    esac
  done
  [ -n "$label" ] || label="acks=$acks batch=$batch linger=$linger comp=$compression rps=$rps inflight=$inflight brokercpus=$brokercpus"
  mkdir -p "$PERF_DIR"

  local point_mb max_records records
  point_mb="$(point_max_mb)"
  max_records=$(( point_mb * 1024 * 1024 / RECORD_BYTES ))
  if [ "$rps" = -1 ]; then
    records="$max_records"
  else
    records=$(( rps * POINT_SEC ))
    [ "$records" -gt "$max_records" ] && records="$max_records"
  fi

  local state
  state="$(host_state)"
  if [ -n "${PERF_REQUIRE_HOST:-}" ] && [ "$state" != "$PERF_REQUIRE_HOST" ]; then
    die "측정 조건이 $state 로 바뀌었다(요구: $PERF_REQUIRE_HOST)"
  fi
  recreate_topic $(( point_mb + 40 ))
  log "point [$sweep] $label records=$records"
  broker_snapshot > "$PERF_DIR/snapshot-before.txt"
  payload_file >/dev/null
  [ "$brokercpus" = 0 ] || set_broker_cpus "$brokercpus"
  local start end
  start="$(python3 -c 'import time; print(time.time())')"
  run_producer "$records" "$rps" "$acks" "$batch" "$linger" "$compression" "$inflight" "$warmup" "$payload" > "$PERF_DIR/last-point.log" 2>&1
  end="$(python3 -c 'import time; print(time.time())')"
  [ "$brokercpus" = 0 ] || clear_broker_cpus
  [ "$(host_state)" = "$state" ] || state=changed
  broker_snapshot > "$PERF_DIR/snapshot-after.txt"
  python3 "$LAB_ROOT/scripts/perf_report.py" parse "$CSV" "$sweep" "$label" "$acks" "$batch" "$linger" \
    "$compression" "$rps" "$records" "$RECORD_BYTES" "$start" "$end" \
    "$PERF_DIR/snapshot-before.txt" "$PERF_DIR/snapshot-after.txt" "$state" < "$PERF_DIR/last-point.log"
}

# 재시작 직후의 브로커 JVM은 코드가 덜 컴파일돼 있어 처음 몇 점이 불리하다(첫 점 124MB/s, 넷째 점 352MB/s). 버리고 시작한다.
sweep_warmup() {
  local i
  for i in 1 2 3 4; do cmd_point sweep=warmup label=warmup; done
}

sweep_acks() {
  local a
  for a in 0 1 all; do cmd_point sweep=acks label="acks=$a" acks="$a" batch=65536 linger=5; done
}

sweep_batch() {
  local b
  for b in 4096 16384 65536 262144 1048576; do cmd_point sweep=batch label="batch.size=$b" batch="$b" linger=5; done
}

# 최대 부하의 지연은 버퍼 대기가 대부분이라 acks의 지연 비용은 고정 부하에서 따로 잰다.
sweep_acks_fixed() {
  local a
  for a in all 1 0; do cmd_point sweep=acks-fixed label="acks=$a" acks="$a" batch=65536 linger=5 rps=10000; done
}

# 고정 부하에서 linger만 바꾼다. 처리량은 같고 요청 수와 지연이 바뀌는 모습을 본다.
sweep_linger() {
  local l
  for l in 0 2 5 20 50 100; do cmd_point sweep=linger label="linger.ms=$l" batch=65536 linger="$l" rps=10000; done
}

sweep_compression() {
  local c
  for c in none gzip snappy lz4 zstd; do
    cmd_point sweep=compression label="$c" batch=131072 linger=20 compression="$c"
  done
}

# 압축 묶음은 코덱마다 처리량이 달라 CPU와 지연을 나란히 비교할 수 없다. 같은 부하에서 다시 잰다.
sweep_compression_fixed() {
  local c
  for c in none gzip snappy lz4 zstd; do
    cmd_point sweep=compression-fixed label="$c" batch=131072 linger=20 compression="$c" rps=40000
  done
}

sweep_load() {
  local r
  for r in 2000 5000 10000 20000 40000 80000 160000 320000 480000 560000 -1; do
    cmd_point sweep=load label="target=$r" batch=65536 linger=5 rps="$r"
  done
}

steady_point() {
  cmd_point sweep="$1" label="target=$2" batch=65536 linger=5 rps="$2" warmup=half
}

# 앞 절반을 뺀 정상 상태 곡선을 수집을 켠 채와 멈춘 채로 번갈아 잰다. 따로 재면 그 사이의 전원 차이가 수집의 영향으로 보인다.
sweep_load_steady() {
  local r
  for r in 160000 320000 480000 560000 640000; do
    steady_point load-steady-scrape "$r"
    pause_scrape
    steady_point load-steady "$r"
    steady_point load-steady "$r"
    resume_scrape
    steady_point load-steady-scrape "$r"
  done
}

# --record-size는 레코드마다 난수를 채운다. 그 비용이 프로듀서의 한계를 낮추는지 미리 만든 페이로드와 비교한다.
sweep_payload() {
  local p
  for p in random file; do cmd_point sweep=payload label="$p" batch=65536 linger=5 payload="$p"; done
}

# 브로커는 연결마다 요청을 하나씩 처리하므로 in-flight를 늘려도 처리량이 아니라 대기만 는다. 그 모습을 본다.
sweep_inflight() {
  local n
  for n in 1 2 5; do cmd_point sweep=inflight label="in-flight=$n" batch=65536 linger=5 inflight="$n"; done
}

# 제한 없는 브로커는 프로듀서 하나로 포화시킬 수 없다. CPU를 제한해 브로커가 먼저 차는 모습과 코어당 처리량을 본다.
sweep_broker_cpu() {
  local c
  for c in 0.5 1 2; do cmd_point sweep=broker-cpu label="brokers ${c} core" batch=65536 linger=5 brokercpus="$c"; done
}

cmd_sweep() {
  case "$1" in
    acks) sweep_acks ;; acks-fixed) sweep_acks_fixed ;; batch) sweep_batch ;; linger) sweep_linger ;;
    compression) sweep_compression ;; compression-fixed) sweep_compression_fixed ;;
    load) sweep_load ;; load-steady) sweep_load_steady ;; inflight) sweep_inflight ;;
    broker-cpu) sweep_broker_cpu ;; payload) sweep_payload ;; warmup) sweep_warmup ;;
    *) die "unknown sweep $1" ;;
  esac
}

cmd_all() {
  local i s
  sweep_warmup
  for i in $(seq 1 "$PERF_REPEAT"); do
    log "== pass $i/$PERF_REPEAT"
    for s in acks acks-fixed batch linger compression compression-fixed load load-steady inflight broker-cpu; do cmd_sweep "$s"; done
  done
  python3 "$LAB_ROOT/scripts/perf_report.py" report "$CSV"
}

case "${1:-}" in
  sweep) begin_measure; cmd_sweep "${2:?sweep name}" ;;
  all) begin_measure; cmd_all ;;
  point) shift; begin_measure; cmd_point "$@" ;;
  report) python3 "$LAB_ROOT/scripts/perf_report.py" report "$CSV" ;;
  *) sed -n '2,6p' "$0"; exit 2 ;;
esac
