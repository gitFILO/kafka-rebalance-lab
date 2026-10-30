#!/usr/bin/env bash
# 실험 1회(설정 1개 × 시나리오 1개)를 실행하고 결과를 results/runs/<RUN_ID>에 남긴다.
set -euo pipefail
. "$(dirname "$0")/lib.sh"

usage() {
  cat <<EOF
usage: $0 <setting> <scenario>
  setting : eager | cooperative | cooperative-static | consumer | consumer-static
  scenario: rolling-restart | kill9 | stall | scale-out
env     : RATE=$RATE PROCESS_MS=$PROCESS_MS MAX_POLL_INTERVAL_MS=$MAX_POLL_INTERVAL_MS
          WARMUP_SEC=$WARMUP_SEC OBSERVE_SEC=$OBSERVE_SEC TARGET_MEMBER=$TARGET_MEMBER
EOF
  exit 2
}

[ $# -eq 2 ] || usage
SETTING="$1"
SCENARIO="$2"
case "$SCENARIO" in rolling-restart|kill9|stall|scale-out) ;; *) usage ;; esac

load_setting "$SETTING"
require_cluster
stop_apps
new_run "$SETTING-$SCENARIO"
echo "SETTING=$SETTING" >> "$RUN_DIR_HOST/run.env"
echo "SCENARIO=$SCENARIO" >> "$RUN_DIR_HOST/run.env"

if [ "$SCENARIO" = "scale-out" ]; then
  INITIAL="consumer-1 consumer-2 consumer-3"
  INITIAL_COUNT=3
else
  INITIAL="$CONSUMERS"
  INITIAL_COUNT=6
fi

cleanup() {
  collect_logs
  stop_apps
}
trap cleanup EXIT

start_apps $INITIAL
log "wait for group: members=$INITIAL_COUNT, partitions=$PARTITIONS"
wait_group "$INITIAL_COUNT" 180 || die "group did not stabilize"
log "warmup ${WARMUP_SEC}s"
sleep "$WARMUP_SEC"
mark WARMUP_DONE
snapshot_group before

mark TRIGGER
case "$SCENARIO" in
  rolling-restart)
    for c in $CONSUMERS; do
      action_restart "$c"
      wait_group 6 180 || log "WARN: group not stable after $c restart"
      sleep "$ROLL_INTERVAL_SEC"
    done
    mark ROLL_DONE
    sleep 30
    ;;
  kill9)
    action_kill9 "$TARGET_MEMBER"
    sleep 15
    snapshot_group during
    sleep $((OBSERVE_SEC - 15))
    ;;
  stall)
    action_stall "$TARGET_MEMBER"
    sleep 15
    snapshot_group during
    sleep $((OBSERVE_SEC - 15))
    ;;
  scale-out)
    action_start consumer-4 consumer-5 consumer-6
    wait_group 6 180 || log "WARN: group not stable after scale-out"
    snapshot_group during
    sleep "$OBSERVE_SEC"
    ;;
esac
mark OBSERVE_END
snapshot_group after

trap - EXIT
cleanup
log "analyze"
python3 "$LAB_ROOT/scripts/analyze.py" run "$RUN_DIR_HOST"
