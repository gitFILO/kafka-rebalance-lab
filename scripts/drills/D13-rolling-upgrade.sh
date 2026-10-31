# shellcheck shell=bash
DRILL_TITLE="롤링 업그레이드 finalize 후 롤백 실패"
FROM_VERSION="${FROM_VERSION:-4.0.1}"
TO_VERSION="${TO_VERSION:-4.1.2}"
FINALIZE_RELEASE="${FINALIZE_RELEASE:-4.1}"
ROLLBACK_BROKER="${ROLLBACK_BROKER:-broker-3}"
UPGRADE="$LAB_ROOT/scripts/upgrade.sh"

# 업그레이드 전 버전으로 클러스터를 새로 만든다. 데이터와 그룹은 모두 지워진다.
drill_env() {
  log "D13: $FROM_VERSION 클러스터로 재구성한다(약 1분)"
  KAFKA_VERSION="$FROM_VERSION" "$LAB_ROOT/scripts/cluster.sh" reset >/dev/null
}

drill_inject() {
  export RUN_DIR_HOST RUN_ID
  # 동료가 절차대로 롤링 업그레이드를 마치고 바로 finalize까지 했다.
  "$UPGRADE" roll-all "$TO_VERSION"
  mark "FINALIZE $FINALIZE_RELEASE"
  "$UPGRADE" finalize "$FINALIZE_RELEASE"
  # 다음 날 broker-3에서 성능 저하가 의심돼 그 브로커만 이전 버전으로 되돌렸다.
  "$UPGRADE" roll "$ROLLBACK_BROKER" "$FROM_VERSION" || true
}

drill_signal() {
  local r=0
  expect "$ROLLBACK_BROKER not running" bash -c "[ \"\$(docker inspect -f '{{.State.Running}}' $ROLLBACK_BROKER)\" != true ] && echo stopped" || r=1
  expect "brokers up == 2" $PROMQ wait 'count(up{job="broker"} == 1)' '==' 2 --timeout 60 || r=1
  expect "BrokerDown alert firing" alert_firing BrokerDown 120 || r=1
  expect "broker log says unsupported metadata.version" bash -c \
    "docker logs $ROLLBACK_BROKER 2>&1 | grep -m1 -oE 'No MetadataVersion with feature level [0-9]+|does not support finalized version [0-9]+ of metadata.version'" || r=1
  return $r
}

drill_fix() {
  "$UPGRADE" roll "$ROLLBACK_BROKER" "$TO_VERSION"
  preferred_election
}

drill_recovered() {
  local r=0
  expect "brokers up == 3" $PROMQ wait 'count(up{job="broker"} == 1)' '==' 3 --timeout 120 || r=1
  expect "under-replicated == 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 || r=1
  expect "all nodes on $TO_VERSION" all_nodes_on "$TO_VERSION" || r=1
  return $r
}

all_nodes_on() {
  local n v bad=""
  for n in controller-1 controller-2 controller-3 broker-1 broker-2 broker-3; do
    v="$(docker inspect -f '{{.Config.Image}}' "$n" 2>/dev/null | sed 's/.*://')"
    [ "$v" = "$1" ] || bad="$bad $n=$v"
  done
  [ -z "$bad" ] && echo "ok" || { echo "mismatch:$bad"; return 1; }
}

drill_teardown() {
  local n v
  for n in controller-1 controller-2 controller-3 broker-1 broker-2 broker-3; do
    v="$(docker inspect -f '{{.Config.Image}}' "$n" 2>/dev/null | sed 's/.*://')"
    [ "$v" = "${KAFKA_VERSION:-4.1.2}" ] || "$UPGRADE" roll "$n" "${KAFKA_VERSION:-4.1.2}" || true
  done
  rm -f "$NODE_VERSIONS_FILE"
}
