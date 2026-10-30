#!/usr/bin/env bash
# classic ↔ consumer 온라인 마이그레이션 실습. 하나의 run 디렉터리를 이어 쓰며 단계별로 실행한다.
set -euo pipefail
. "$(dirname "$0")/lib.sh"

CURRENT="$RUNS_DIR/.current-migration"

usage() {
  cat <<EOF
usage: $0 <command> [args]
  start                      classic(cooperative-sticky) 그룹 6멤버로 시작한다. ASSIGNOR=range 로 바꿀 수 있다.
  status                     그룹 type/state, 멤버별 프로토콜, 브로커 전환 로그를 출력한다.
  upgrade <consumer-N>       해당 멤버를 group.protocol=consumer 로 재시작한다.
  upgrade-all                consumer-1..6 을 순서대로 upgrade 한다.
  downgrade <consumer-N>     해당 멤버를 group.protocol=classic 으로 재시작한다.
  downgrade-all              consumer-1..6 을 순서대로 downgrade 한다.
  trigger <kill9|stall|restart> <consumer-N>
                             혼합 상태에서 장애를 발생시킨다. 이후 마커 기준으로 analyze 한다.
  finish                     로그를 수집하고 애플리케이션을 정리한 뒤 분석한다.
EOF
  exit 2
}

current_run() {
  [ -f "$CURRENT" ] || die "진행 중인 마이그레이션 run이 없다. 먼저 '$0 start' 를 실행한다."
  resume_run "$(cat "$CURRENT")"
}

cmd_start() {
  export GROUP_PROTOCOL=classic
  export ASSIGNOR="${ASSIGNOR:-cooperative-sticky}"
  export STATIC_MEMBERSHIP="${STATIC_MEMBERSHIP:-false}"
  export REMOTE_ASSIGNOR="${REMOTE_ASSIGNOR:-}"
  require_cluster
  stop_apps
  new_run "migration"
  echo "SETTING=migration" >> "$RUN_DIR_HOST/run.env"
  echo "SCENARIO=migration" >> "$RUN_DIR_HOST/run.env"
  echo "$RUN_DIR_HOST" > "$CURRENT"
  start_apps $CONSUMERS
  wait_group 6 180 || die "group did not stabilize"
  sleep "$WARMUP_SEC"
  mark TRIGGER
  snapshot_group start
  cmd_status
}

cmd_status() {
  current_run
  echo "group=$GROUP_ID run=$RUN_DIR_HOST"
  echo
  echo "== group (lag-monitor 최근 샘플)"
  tail -n 1 "$RUN_DIR_HOST/group.csv" | awk -F, '{printf "type=%s state=%s members=%s group_epoch=%s consumer_protocol_members=%s classic_protocol_members=%s\n", $2, $3, $4, $5, $8, $9}'
  echo
  echo "== members (client_id / member_epoch / upgraded / partitions)"
  local last_ts
  last_ts="$(tail -n 1 "$RUN_DIR_HOST/members.csv" | cut -d, -f1)"
  grep "^$last_ts," "$RUN_DIR_HOST/members.csv" | sort -t, -k2 | awk -F, '{printf "%-12s epoch=%-4s upgraded=%-5s [%s]\n", $2, $5, $6, $7}'
  echo
  echo "== protocol overrides (control/*.protocol)"
  local f
  for f in "$RUN_DIR_HOST"/control/*.protocol; do
    [ -e "$f" ] || continue
    echo "$(basename "$f" .protocol)=$(cat "$f")"
  done
  echo
  echo "== broker log: group conversion"
  local b
  for b in broker-1 broker-2 broker-3; do
    docker logs --since "$RUN_STARTED_EPOCH" "$b" 2>&1 | grep -F "$GROUP_ID" | grep -E 'Converted|Cannot upgrade|Cannot downgrade' | sed "s/^/$b: /" || true
  done
}

switch_member() {
  local member="$1" protocol="$2"
  echo "$protocol" > "$RUN_DIR_HOST/control/$member.protocol"
  mark "SWITCH $member -> $protocol"
  action_restart "$member"
  wait_group 6 180 || log "WARN: group not stable after $member switch"
}

cmd_switch_all() {
  local protocol="$1" c
  for c in $CONSUMERS; do
    switch_member "$c" "$protocol"
    sleep "$ROLL_INTERVAL_SEC"
  done
  cmd_status
}

cmd_trigger() {
  local kind="$1" member="$2"
  case "$kind" in
    kill9) action_kill9 "$member" ;;
    stall) action_stall "$member" ;;
    restart) action_restart "$member" ;;
    *) usage ;;
  esac
}

cmd_finish() {
  current_run
  mark OBSERVE_END
  snapshot_group end
  collect_logs
  stop_apps
  rm -f "$CURRENT"
  python3 "$LAB_ROOT/scripts/analyze.py" run "$RUN_DIR_HOST"
}

case "${1:-}" in
  start) cmd_start ;;
  status) cmd_status ;;
  upgrade) [ $# -eq 2 ] || usage; current_run; switch_member "$2" consumer; cmd_status ;;
  downgrade) [ $# -eq 2 ] || usage; current_run; switch_member "$2" classic; cmd_status ;;
  upgrade-all) current_run; cmd_switch_all consumer ;;
  downgrade-all) current_run; cmd_switch_all classic ;;
  trigger) [ $# -eq 3 ] || usage; current_run; cmd_trigger "$2" "$3" ;;
  finish) cmd_finish ;;
  *) usage ;;
esac
