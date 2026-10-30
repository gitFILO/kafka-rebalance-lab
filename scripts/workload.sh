#!/usr/bin/env bash
# 장애 훈련·카오스용 정상 상태 워크로드. 프로듀서 1, 컨슈머 N, lag-monitor를 띄워 두고 유지한다.
#   workload.sh start [label]   기본: consumer 프로토콜, 컨슈머 3대, 600 msg/s
#   workload.sh scale <n>       컨슈머 수 조정(1~6)
#   workload.sh status | stop
set -euo pipefail
. "$(dirname "$0")/lib.sh"

STATE="$RUNS_DIR/.current-workload"
WORKLOAD_SETTING="${WORKLOAD_SETTING:-consumer}"
WORKLOAD_CONSUMERS="${WORKLOAD_CONSUMERS:-3}"

members_upto() {
  local n="$1" i=1 out=""
  while [ "$i" -le "$n" ]; do out="$out consumer-$i"; i=$((i + 1)); done
  echo "$out"
}

workload_start() {
  local label="${1:-workload}"
  load_setting "$WORKLOAD_SETTING"
  require_cluster
  stop_apps
  new_run "$label"
  {
    echo "WORKLOAD_SETTING=$WORKLOAD_SETTING"
    echo "WORKLOAD_CONSUMERS=$WORKLOAD_CONSUMERS"
    for v in VALUE_BYTES BUFFER_MEMORY MAX_REQUEST_SIZE TRANSACTIONAL TRANSACTION_TIMEOUT_MS ISOLATION_LEVEL \
             AUTO_OFFSET_RESET POISON_POLICY POISON_RETRIES POISON_BACKOFF_MS; do
      eval "[ -n \"\${$v:-}\" ] && echo \"$v=\${$v}\"" || true
    done
  } >> "$RUN_DIR_HOST/run.env"
  echo "$RUN_DIR_HOST" > "$STATE"
  # shellcheck disable=SC2046
  start_apps $(members_upto "$WORKLOAD_CONSUMERS")
  wait_group "$WORKLOAD_CONSUMERS" 180 || log "WARN: group not stable yet"
  mark "WORKLOAD_READY consumers=$WORKLOAD_CONSUMERS"
}

workload_resume() {
  [ -f "$STATE" ] || die "실행 중인 워크로드가 없다. workload.sh start 를 먼저 실행한다."
  resume_run "$(cat "$STATE")"
}

case "${1:-}" in
  start)
    workload_start "${2:-workload}"
    ;;
  scale)
    workload_resume
    n="${2:?usage: workload.sh scale <n>}"
    for m in $CONSUMERS; do
      i="${m#consumer-}"
      if [ "$i" -le "$n" ]; then
        docker inspect -f '{{.State.Running}}' "$m" 2>/dev/null | grep -q true || action_start "$m"
      else
        if docker inspect "$m" >/dev/null 2>&1; then
          mark "STOP $m"
          compose rm -sf "$m" >/dev/null 2>&1 || true
        fi
      fi
    done
    ;;
  status)
    workload_resume
    echo "run=$RUN_ID group=$GROUP_ID"
    tail -1 "$RUN_DIR_HOST/group.csv" 2>/dev/null || true
    compose ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}' producer lag-monitor $CONSUMERS 2>/dev/null || true
    ;;
  stop)
    if [ -f "$STATE" ]; then
      resume_run "$(cat "$STATE")"
      mark "WORKLOAD_STOP"
      collect_logs
      rm -f "$STATE"
    fi
    stop_apps
    ;;
  *)
    echo "usage: $0 <start [label]|scale <n>|status|stop>"
    exit 2
    ;;
esac
