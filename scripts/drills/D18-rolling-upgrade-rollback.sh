# shellcheck shell=bash
DRILL_TITLE="롤링 업그레이드 무중단 판정과 finalize 전 롤백"
FROM_VERSION="${FROM_VERSION:-4.0.1}"
TO_VERSION="${TO_VERSION:-4.1.2}"
# broker-1은 kafka_bin이 붙는 컨테이너라 표적으로 쓰지 않는다. 정지 중 exec가 실패하면 upgrade.sh의 URP 확인이 0으로 읽힌다.
TARGET_BROKER="${TARGET_BROKER:-broker-2}"
FREEZE_MS="${FREEZE_MS:-1200}"
RUN_MS="${RUN_MS:-400}"
UPGRADE="$LAB_ROOT/scripts/upgrade.sh"
CONTINUITY="python3 $LAB_ROOT/scripts/continuity.py"
CLUSTER_NODES="controller-1 controller-2 controller-3 broker-1 broker-2 broker-3"
FAILED_TOTAL='sum(lab_producer_failedtotal)'
PRODUCE_LATENCY='max(kafka_producer_producer_metrics_request_latency_avg)'

# 업그레이드 전 버전으로 클러스터를 새로 만든다. 데이터와 그룹은 모두 지워진다.
drill_env() {
  log "D18: $FROM_VERSION 클러스터로 재구성한다"
  KAFKA_VERSION="$FROM_VERSION" "$LAB_ROOT/scripts/cluster.sh" reset >/dev/null
}

drill_inject() {
  export RUN_DIR_HOST RUN_ID
  metadata_version > "$RUN_DIR_HOST/control/.mv-before"
  $PROMQ value "$FAILED_TOTAL" > "$RUN_DIR_HOST/control/.failed-before"
  mark "UPGRADE_START"
  "$UPGRADE" roll-all "$TO_VERSION"
  mark "UPGRADE_DONE"
  # scrape 주기(5초)가 지나야 구간 끝까지의 실패가 값에 반영된다.
  sleep 6
  $PROMQ value "$FAILED_TOTAL" > "$RUN_DIR_HOST/control/.failed-after"
  # 업그레이드 여진이 남은 채 켜면 회귀가 없어도 drill_signal의 지연 판정이 통과한다.
  $PROMQ wait "$PRODUCE_LATENCY" '<' 35 --timeout 120 > "$RUN_DIR_HOST/control/.latency-settled" || true
  mark "OBSERVE_START"
  regression_on
}

# 회귀는 TO_VERSION 바이너리의 결함이다. 재생성 중인 옛 컨테이너도 태그가 그대로라 upgrade.sh가 compose보다 먼저 쓰는 목표 버전까지 본다.
regression_on() {
  local pin
  pin="$(echo "$TARGET_BROKER" | tr 'a-z-' 'A-Z_')_VERSION=$TO_VERSION"
  nohup bash -c '
    f="$(awk -v v="$3" "BEGIN {print v / 1000}")"; r="$(awk -v v="$4" "BEGIN {print v / 1000}")"
    while [ "$(docker inspect -f "{{.Config.Image}}" "$0" 2>/dev/null | sed "s/.*://")" = "$1" ] \
      && grep -qxF "$2" "$5" 2>/dev/null; do
      docker pause "$0" >/dev/null 2>&1; sleep "$f"
      docker unpause "$0" >/dev/null 2>&1; sleep "$r"
    done' "$TARGET_BROKER" "$TO_VERSION" "$pin" "$FREEZE_MS" "$RUN_MS" "$NODE_VERSIONS_FILE" >/dev/null 2>&1 &
  echo $! > "$FREEZE_DIR/.freeze-$TARGET_BROKER.pid"
}

drill_signal() {
  local r=0
  expect "no record lost during upgrade" \
    $CONTINUITY "$RUN_DIR_HOST" --from-marker UPGRADE_START --to-marker UPGRADE_DONE || r=1
  expect "no producer send failed during upgrade" no_failed_sends_during_upgrade || r=1
  expect "metadata.version not finalized" metadata_version_unchanged || r=1
  expect "producer latency settled before observation" latency_settled || r=1
  expect "producer request latency avg > 40ms" $PROMQ wait "$PRODUCE_LATENCY" '>' 40 --timeout 120 || r=1
  expect "ProduceLatencyHigh alert firing" alert_firing ProduceLatencyHigh 300 || r=1
  return $r
}

drill_fix() {
  export RUN_DIR_HOST RUN_ID
  mark "ROLLBACK_START"
  # 실패해도 drill_recovered가 판정하도록 selftest를 끊지 않는다.
  "$UPGRADE" rollback-all "$FROM_VERSION" || log "WARN: rollback-all failed"
  mark "ROLLBACK_DONE"
  preferred_election
}

drill_recovered() {
  local r=0
  expect "brokers up == 3" $PROMQ wait 'count(up{job="broker"} == 1)' '==' 3 --timeout 120 || r=1
  expect "all nodes on $FROM_VERSION" all_nodes_on "$FROM_VERSION" || r=1
  expect "under-replicated == 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 || r=1
  expect "metadata.version unchanged" metadata_version_unchanged || r=1
  expect "producer request latency avg < 35ms" $PROMQ wait "$PRODUCE_LATENCY" '<' 35 --timeout 180 || r=1
  # 직접 되돌리면 ROLLBACK_START 마커가 없으므로 롤백을 포함하는 UPGRADE_DONE 이후 전체를 본다.
  expect "no record lost since upgrade finished" $CONTINUITY "$RUN_DIR_HOST" --from-marker UPGRADE_DONE || r=1
  expect "no producer send failed through rollback" no_failed_sends_through_rollback || r=1
  return $r
}

drill_teardown() {
  local n v
  fault_unfreeze "$TARGET_BROKER"
  for n in $CLUSTER_NODES; do
    v="$(node_version "$n")"
    [ "$v" = "${KAFKA_VERSION:-4.1.2}" ] || "$UPGRADE" roll "$n" "${KAFKA_VERSION:-4.1.2}" || true
  done
  rm -f "$NODE_VERSIONS_FILE"
}

node_version() {
  docker inspect -f '{{.Config.Image}}' "$1" 2>/dev/null | sed 's/.*://' || true
}

all_nodes_on() {
  local n v bad=""
  for n in $CLUSTER_NODES; do
    v="$(node_version "$n")"
    [ "$v" = "$1" ] || bad="$bad $n=$v"
  done
  [ -z "$bad" ] && echo "ok" || { echo "mismatch:$bad"; return 1; }
}

metadata_version() {
  kafka_bin kafka-features.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" describe 2>/dev/null \
    | awk '$2 == "metadata.version" {print $8}'
}

metadata_version_unchanged() {
  local before now
  before="$(cat "$RUN_DIR_HOST/control/.mv-before")"
  now="$(metadata_version)"
  echo "$before -> $now"
  [ "$now" = "$before" ] && case "$now" in "${FROM_VERSION%.*}"-*) ;; *) false ;; esac
}

no_failed_sends_through_rollback() {
  local before now
  before="$(cat "$RUN_DIR_HOST/control/.failed-before")"
  now="$($PROMQ value "$FAILED_TOTAL")"
  echo "failedtotal $before -> $now"
  [ "$before" != nan ] && [ "$now" != nan ] && [ "$before" = "$now" ]
}

no_failed_sends_during_upgrade() {
  local before after
  before="$(cat "$RUN_DIR_HOST/control/.failed-before")"
  after="$(cat "$RUN_DIR_HOST/control/.failed-after")"
  echo "failedtotal $before -> $after"
  [ "$before" != nan ] && [ "$after" = "$before" ]
}

latency_settled() {
  cat "$RUN_DIR_HOST/control/.latency-settled"
  grep -q '^ok' "$RUN_DIR_HOST/control/.latency-settled"
}
