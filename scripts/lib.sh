#!/usr/bin/env bash
# 공통 함수. 다른 스크립트에서 source 한다. macOS 기본 bash 3.2와 호환되게 작성한다.

LAB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNS_DIR="$LAB_ROOT/results/runs"
TOPIC="${TOPIC:-lab-events}"
PARTITIONS="${PARTITIONS:-12}"
CONSUMERS="consumer-1 consumer-2 consumer-3 consumer-4 consumer-5 consumer-6"
BOOTSTRAP_INTERNAL="broker-1:9092,broker-2:9092,broker-3:9092"

# 실험 공통 파라미터. 환경 변수로 덮어쓸 수 있다.
export TOPIC
export RATE="${RATE:-600}"
export PROCESS_MS="${PROCESS_MS:-2}"
export MAX_POLL_INTERVAL_MS="${MAX_POLL_INTERVAL_MS:-30000}"
export MAX_POLL_RECORDS="${MAX_POLL_RECORDS:-100}"
WARMUP_SEC="${WARMUP_SEC:-20}"
OBSERVE_SEC="${OBSERVE_SEC:-90}"
ROLL_INTERVAL_SEC="${ROLL_INTERVAL_SEC:-10}"
TARGET_MEMBER="${TARGET_MEMBER:-consumer-3}"

compose() {
  docker compose --progress quiet --profile lab -f "$LAB_ROOT/docker-compose.yml" "$@"
}

log() {
  printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

die() {
  log "ERROR: $*" >&2
  exit 1
}

now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

kafka_bin() {
  local tool="$1"
  shift
  docker exec broker-1 "/opt/kafka/bin/$tool" "$@"
}

load_setting() {
  local file="$LAB_ROOT/settings/$1.env"
  [ -f "$file" ] || die "unknown setting '$1' (settings/*.env 참고)"
  unset GROUP_PROTOCOL ASSIGNOR REMOTE_ASSIGNOR STATIC_MEMBERSHIP
  # shellcheck disable=SC1090
  . "$file"
  export GROUP_PROTOCOL ASSIGNOR REMOTE_ASSIGNOR STATIC_MEMBERSHIP
}

# RUN_ID, RUN_DIR_HOST, GROUP_ID를 만든다.
new_run() {
  local label="$1"
  RUN_ID="$(date +%Y%m%d-%H%M%S)-$label"
  RUN_DIR_HOST="$RUNS_DIR/$RUN_ID"
  GROUP_ID="lab-$RUN_ID"
  export RUN_ID GROUP_ID
  mkdir -p "$RUN_DIR_HOST/control"
  RUN_STARTED_EPOCH="$(date +%s)"
  {
    echo "RUN_ID=$RUN_ID"
    echo "GROUP_ID=$GROUP_ID"
    echo "STARTED_EPOCH=$RUN_STARTED_EPOCH"
    echo "KAFKA_VERSION=${KAFKA_VERSION:-4.1.2}"
    echo "TOPIC=$TOPIC"
    echo "PARTITIONS=$PARTITIONS"
    echo "RATE=$RATE"
    echo "PROCESS_MS=$PROCESS_MS"
    echo "MAX_POLL_INTERVAL_MS=$MAX_POLL_INTERVAL_MS"
    echo "MAX_POLL_RECORDS=$MAX_POLL_RECORDS"
    echo "GROUP_PROTOCOL=${GROUP_PROTOCOL:-}"
    echo "ASSIGNOR=${ASSIGNOR:-}"
    echo "REMOTE_ASSIGNOR=${REMOTE_ASSIGNOR:-}"
    echo "STATIC_MEMBERSHIP=${STATIC_MEMBERSHIP:-}"
    echo "WARMUP_SEC=$WARMUP_SEC"
    echo "OBSERVE_SEC=$OBSERVE_SEC"
  } > "$RUN_DIR_HOST/run.env"
  log "run=$RUN_ID group=$GROUP_ID"
}

# 기존 run 디렉터리를 이어서 쓴다(migration.sh 용).
resume_run() {
  RUN_DIR_HOST="$1"
  [ -f "$RUN_DIR_HOST/run.env" ] || die "run.env not found in $RUN_DIR_HOST"
  # shellcheck disable=SC1091
  . "$RUN_DIR_HOST/run.env"
  RUN_STARTED_EPOCH="$STARTED_EPOCH"
  export RUN_ID GROUP_ID GROUP_PROTOCOL ASSIGNOR REMOTE_ASSIGNOR STATIC_MEMBERSHIP RATE PROCESS_MS MAX_POLL_INTERVAL_MS MAX_POLL_RECORDS
}

mark() {
  echo "$(now_ms),$1" >> "$RUN_DIR_HOST/markers.csv"
  log "MARK $1"
}

require_cluster() {
  local b
  for b in broker-1 broker-2 broker-3; do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$b" 2>/dev/null)" = "healthy" ] \
      || die "$b is not healthy. 먼저 ./scripts/cluster.sh up 을 실행한다."
  done
}

stop_apps() {
  compose rm -sf producer lag-monitor $CONSUMERS >/dev/null 2>&1 || true
}

wait_group() {
  python3 "$LAB_ROOT/scripts/wait.py" group "$RUN_DIR_HOST" --members "$1" --partitions "$PARTITIONS" --timeout "${2:-180}"
}

wait_assigned() {
  python3 "$LAB_ROOT/scripts/wait.py" assigned "$RUN_DIR_HOST" --member "$1" --since "$2" --timeout "${3:-180}"
}

snapshot_group() {
  local name="$1"
  {
    echo "# $(date '+%F %T') describe --state"
    kafka_bin kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --group "$GROUP_ID" --state 2>&1
    echo
    echo "# describe --members --verbose"
    kafka_bin kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --group "$GROUP_ID" --members --verbose 2>&1
    echo
    echo "# describe (offsets)"
    kafka_bin kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --group "$GROUP_ID" 2>&1
  } > "$RUN_DIR_HOST/describe-$name.txt"
  log "snapshot describe-$name.txt"
}

collect_logs() {
  local c
  for c in broker-1 broker-2 broker-3; do
    docker logs --since "$RUN_STARTED_EPOCH" "$c" 2>&1 | grep -F "$GROUP_ID" > "$RUN_DIR_HOST/broker-group-$c.log" || true
  done
  for c in $CONSUMERS producer lag-monitor; do
    if docker inspect "$c" >/dev/null 2>&1; then
      docker logs "$c" > "$RUN_DIR_HOST/container-$c.log" 2>&1 || true
    fi
  done
}

start_apps() {
  log "start lag-monitor, producer"
  compose up -d --no-deps lag-monitor producer >/dev/null
  log "start $*"
  compose up -d --no-deps "$@" >/dev/null
}

# ---- 시나리오 동작 ----

action_restart() {
  local member="$1" t
  t="$(now_ms)"
  mark "RESTART $member"
  compose restart -t 20 "$member" >/dev/null
  wait_assigned "$member" "$t" 180 || log "WARN: $member did not get partitions within timeout"
}

action_kill9() {
  mark "KILL9 $1"
  docker kill --signal KILL "$1" >/dev/null
}

action_stall() {
  local member="$1" ms="${2:-$((MAX_POLL_INTERVAL_MS + 15000))}"
  mark "STALL $member ${ms}ms"
  echo "$ms" > "$RUN_DIR_HOST/control/$member.stall"
}

action_start() {
  mark "START $*"
  compose up -d --no-deps "$@" >/dev/null
}
