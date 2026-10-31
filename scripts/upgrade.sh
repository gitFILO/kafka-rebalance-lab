#!/usr/bin/env bash
# 노드 단위 롤링 업그레이드·롤백 도구
#   upgrade.sh status                    노드별 이미지 버전과 finalized feature
#   upgrade.sh roll <node> <version>     노드 하나를 해당 버전 이미지로 재생성하고 정상화까지 기다린다
#   upgrade.sh roll-all <version>        컨트롤러(리더는 마지막) → 브로커 순서로 한 대씩 교체
#   upgrade.sh rollback-all <version>    브로커(역순) → 컨트롤러(리더는 마지막) 순서로 한 대씩 교체
#   upgrade.sh finalize <release>        kafka-features upgrade --release-version <release>
set -euo pipefail
. "$(dirname "$0")/lib.sh"

NODES="controller-1 controller-2 controller-3 broker-1 broker-2 broker-3"

node_var() {
  echo "$1" | tr 'a-z-' 'A-Z_' | sed 's/$/_VERSION/'
}

node_version() {
  docker inspect -f '{{.Config.Image}}' "$1" 2>/dev/null | sed 's/.*://'
}

ensure_image() {
  local v="$1"
  docker image inspect "rebalance-lab-kafka:$v" >/dev/null 2>&1 && return 0
  log "build rebalance-lab-kafka:$v"
  docker build -q --build-arg KAFKA_VERSION="$v" -t "rebalance-lab-kafka:$v" "$LAB_ROOT/docker/kafka" >/dev/null
}

set_node_version() {
  local node="$1" v="$2" var tmp
  var="$(node_var "$node")"
  tmp="$(mktemp)"
  [ -f "$NODE_VERSIONS_FILE" ] && grep -v "^$var=" "$NODE_VERSIONS_FILE" > "$tmp" || true
  echo "$var=$v" >> "$tmp"
  mv "$tmp" "$NODE_VERSIONS_FILE"
}

wait_healthy_node() {
  local node="$1" timeout="${2:-150}" deadline
  deadline=$(( $(date +%s) + timeout ))
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$node" 2>/dev/null)" = "healthy" ]; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$node" 2>/dev/null)" != "true" ]; then
      log "$node exited. 마지막 로그:"
      docker logs --tail 15 "$node" 2>&1 | sed 's/^/    /'
      return 1
    fi
    [ "$(date +%s)" -ge "$deadline" ] && { log "$node not healthy in ${timeout}s"; return 1; }
    sleep 3
  done
}

urp_count() {
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --under-replicated-partitions 2>/dev/null \
    | grep -c 'Partition:' || true
}

wait_urp_zero() {
  local deadline=$(( $(date +%s) + ${1:-180} )) n
  while :; do
    n="$(urp_count)"
    [ "$n" = 0 ] && { log "under-replicated partitions: 0"; return 0; }
    [ "$(date +%s)" -ge "$deadline" ] && { log "WARN: URP=$n after timeout"; return 1; }
    sleep 3
  done
}

active_controller() {
  kafka_bin kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" describe --status 2>/dev/null \
    | awk -F': *' '/LeaderId/ {print $2}'
}

cmd_roll() {
  local node="$1" v="$2" from
  case " $NODES " in *" $node "*) ;; *) die "unknown node $node" ;; esac
  ensure_image "$v"
  from="$(node_version "$node")"
  if [ "${RUN_DIR_HOST:-}" ] && [ -d "${RUN_DIR_HOST:-}" ]; then mark "ROLL $node $from->$v"; else log "ROLL $node $from->$v"; fi
  set_node_version "$node" "$v"
  compose up -d --no-deps "$node" >/dev/null
  wait_healthy_node "$node" || return 1
  case "$node" in broker-*) wait_urp_zero 240 || return 1 ;; esac
  log "$node now $(node_version "$node")"
}

cmd_roll_all() {
  local v="$1" leader c b
  leader="controller-$(active_controller)"
  for c in controller-1 controller-2 controller-3; do
    [ "$c" = "$leader" ] && continue
    cmd_roll "$c" "$v"
  done
  cmd_roll "$leader" "$v"
  for b in broker-1 broker-2 broker-3; do
    cmd_roll "$b" "$v"
  done
}

cmd_rollback_all() {
  local v="$1" leader c b
  for b in broker-3 broker-2 broker-1; do
    cmd_roll "$b" "$v"
  done
  # 브로커를 교체하는 동안 리더가 바뀔 수 있으므로 컨트롤러 차례에 조회한다.
  leader="controller-$(active_controller)"
  for c in controller-3 controller-2 controller-1; do
    [ "$c" = "$leader" ] && continue
    cmd_roll "$c" "$v"
  done
  cmd_roll "$leader" "$v"
}

cmd_status() {
  local n
  for n in $NODES; do
    printf '%-13s %s\n' "$n" "$(node_version "$n" || echo '-')"
  done
  echo
  kafka_bin kafka-features.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" describe 2>/dev/null \
    | awk '{printf "%-34s finalized=%-8s supported-max=%s\n", $2, $8, $6}' || true
}

case "${1:-}" in
  status) cmd_status ;;
  roll) cmd_roll "${2:?node}" "${3:?version}" ;;
  roll-all) cmd_roll_all "${2:?version}" ;;
  rollback-all) cmd_rollback_all "${2:?version}" ;;
  finalize)
    kafka_bin kafka-features.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" upgrade --release-version "${2:?release, e.g. 4.1}"
    ;;
  *) echo "usage: $0 <status|roll <node> <version>|roll-all <version>|rollback-all <version>|finalize <release>>"; exit 2 ;;
esac
