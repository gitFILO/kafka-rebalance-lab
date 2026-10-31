#!/usr/bin/env bash
# Stage 4 단일 장애 훈련.
#   drill.sh list
#   drill.sh start <Dxx>     워크로드 기동 → 워밍업 → 장애 주입. 이후 대시보드로 진단하고 직접 조치한다.
#   drill.sh verify          현재 훈련의 복구 기준을 판정한다.
#   drill.sh fix             모범 조치를 적용한다(직접 조치한 뒤 비교용).
#   drill.sh stop            장애 흔적 정리, 워크로드 종료.
#   drill.sh selftest <Dxx>  주입 → 신호 판정 → 모범 조치 → 복구 판정을 자동으로 수행한다(환경 검증용).
set -euo pipefail
. "$(dirname "$0")/lib.sh"
. "$LAB_ROOT/scripts/lib-fault.sh"

DRILL_STATE="$RUNS_DIR/.current-drill"
WORKLOAD_STATE="$RUNS_DIR/.current-workload"
DRILL_WARMUP_SEC="${DRILL_WARMUP_SEC:-30}"

# ---- 드릴 정의가 쓰는 공통 함수 ----

expect() {
  local desc="$1"
  shift
  local out
  if out="$("$@" 2>&1)"; then
    log "PASS $desc ($out)"
    echo "PASS $desc ($out)" >> "$RUN_DIR_HOST/selftest.txt"
    return 0
  fi
  log "FAIL $desc ($out)"
  echo "FAIL $desc ($out)" >> "$RUN_DIR_HOST/selftest.txt"
  return 1
}

consumers_running() {
  local m out=""
  for m in $CONSUMERS; do
    [ "$(docker inspect -f '{{.State.Running}}' "$m" 2>/dev/null)" = "true" ] && out="$out $m"
  done
  echo "$out"
}

# 컨슈머를 모두 내리고 그룹이 Empty가 될 때까지 기다린다. 오프셋 리셋 전제 조건이다.
stop_consumers() {
  local running
  running="$(consumers_running)"
  echo "$running" > "$RUN_DIR_HOST/control/.stopped-consumers"
  mark "STOP_CONSUMERS$running"
  # shellcheck disable=SC2086
  [ -n "$running" ] && compose stop -t 20 $running >/dev/null 2>&1
  local deadline=$(( $(date +%s) + 90 ))
  until kafka_bin kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --group "$GROUP_ID" --state 2>/dev/null \
      | grep -qE '\s(Empty|Dead)\s'; do
    [ "$(date +%s)" -ge "$deadline" ] && { log "WARN: group not empty"; return 1; }
    sleep 3
  done
}

start_consumers() {
  local list
  list="$(cat "$RUN_DIR_HOST/control/.stopped-consumers" 2>/dev/null || echo "consumer-1 consumer-2 consumer-3")"
  mark "START_CONSUMERS $list"
  # shellcheck disable=SC2086
  compose up -d --no-deps $list >/dev/null
}

reset_offsets() {
  kafka_bin kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --group "$GROUP_ID" --reset-offsets "$@" --execute
}

alert_firing() {
  $PROMQ wait "count(ALERTS{alertname=\"$1\",alertstate=\"firing\"})" '>=' 1 --timeout "${2:-180}"
}

preferred_election() {
  kafka_bin kafka-leader-election.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --election-type PREFERRED \
    --all-topic-partitions >/dev/null 2>&1 || true
}

wait_brokers_healthy() {
  local b
  for b in broker-1 broker-2 broker-3; do
    local deadline=$(( $(date +%s) + 180 ))
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "$b" 2>/dev/null)" = "healthy" ]; do
      [ "$(date +%s)" -ge "$deadline" ] && die "$b not healthy"
      sleep 3
    done
  done
}

# ---- 정의 로드 ----

load_drill() {
  local id="$1" file
  file="$(ls "$LAB_ROOT/scripts/drills/$id"-*.sh 2>/dev/null | head -1)"
  [ -n "$file" ] || die "unknown drill '$id' (drill.sh list)"
  DRILL_ID="$id"
  DRILL_FILE="$file"
  drill_env() { :; }
  drill_teardown() { :; }
  # shellcheck disable=SC1090
  . "$file"
}

resume_drill() {
  [ -f "$DRILL_STATE" ] || die "진행 중인 훈련이 없다. drill.sh start <Dxx>"
  local id dir
  read -r id dir < "$DRILL_STATE"
  load_drill "$id"
  resume_run "$dir"
}

cmd_list() {
  local f
  for f in "$LAB_ROOT"/scripts/drills/D*.sh; do
    (
      DRILL_TITLE=""
      # shellcheck disable=SC1090
      . "$f"
      printf '%-4s %s\n' "$(basename "$f" | cut -d- -f1)" "$DRILL_TITLE"
    )
  done
}

cmd_start() {
  load_drill "$1"
  [ -f "$DRILL_STATE" ] && die "이미 진행 중인 훈련이 있다. drill.sh stop 을 먼저 실행한다."
  require_cluster
  log "== $DRILL_ID $DRILL_TITLE"
  fault_clear_all
  drill_env
  "$LAB_ROOT/scripts/workload.sh" start "drill-$DRILL_ID"
  resume_run "$(cat "$WORKLOAD_STATE")"
  echo "$DRILL_ID $RUN_DIR_HOST" > "$DRILL_STATE"
  log "warmup ${DRILL_WARMUP_SEC}s"
  sleep "$DRILL_WARMUP_SEC"
  mark "INJECT $DRILL_ID"
  drill_inject
  log "장애를 주입했다. Grafana(http://localhost:3000)에서 증상을 확인하고 docs/stage4-drills/$(basename "$DRILL_FILE" .sh).md 의 진행 방식을 따른다."
}

cmd_stop() {
  if [ -f "$DRILL_STATE" ]; then
    resume_drill
    mark "DRILL_STOP $DRILL_ID"
    drill_teardown
    rm -f "$DRILL_STATE"
  fi
  fault_clear_all
  "$LAB_ROOT/scripts/workload.sh" stop
  log "drill stopped"
}

cmd_selftest() {
  local result=0
  cmd_start "$1"
  : > "$RUN_DIR_HOST/selftest.txt"
  drill_signal || result=1
  mark "FIX $DRILL_ID"
  drill_fix
  drill_recovered || result=1
  if [ "$result" = 0 ]; then
    echo "RESULT PASS" >> "$RUN_DIR_HOST/selftest.txt"
  else
    echo "RESULT FAIL" >> "$RUN_DIR_HOST/selftest.txt"
  fi
  local dir="$RUN_DIR_HOST"
  cmd_stop
  log "selftest $1: $(tail -1 "$dir/selftest.txt") ($dir/selftest.txt)"
  return "$result"
}

case "${1:-}" in
  list) cmd_list ;;
  start) cmd_start "${2:?usage: drill.sh start <Dxx>}" ;;
  verify) resume_drill; : > "$RUN_DIR_HOST/selftest.txt"; drill_recovered && log "복구 기준 충족" || { log "복구 기준 미충족"; exit 1; } ;;
  fix) resume_drill; mark "FIX $DRILL_ID"; drill_fix ;;
  stop) cmd_stop ;;
  selftest) cmd_selftest "${2:?usage: drill.sh selftest <Dxx>}" ;;
  *) sed -n '2,9p' "$0"; exit 2 ;;
esac
