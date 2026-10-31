# shellcheck shell=bash
DRILL_TITLE="재배치 후 남은 복제 throttle"
TARGET_BROKER="${TARGET_BROKER:-broker-3}"
THROTTLE_RATE="${THROTTLE_RATE:-10240}"

drill_inject() {
  # 어제 broker-3으로 복제본을 옮기며 --throttle 10KB/s를 걸었고, 끝난 뒤 --verify를 하지 않아 설정이 남았다.
  fault_throttle_set "$THROTTLE_RATE" 6
  mark "THROTTLE_LEFTOVER rate=$THROTTLE_RATE"
  # 오늘 정기 패치로 broker-3을 재시작했다. 내려가 있는 동안 쌓인 분량을 throttle 속도로만 따라잡는다.
  fault_broker_stop "$TARGET_BROKER"
  sleep 45
  fault_broker_start "$TARGET_BROKER"
}

drill_signal() {
  local r=0
  expect "brokers up == 3" $PROMQ wait 'count(up{job="broker"} == 1)' '==' 3 --timeout 120 || r=1
  expect "under-replicated stays > 0 for 90s" \
    $PROMQ hold 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '>' 0 --for 90 || r=1
  expect "UnderReplicatedPartitions alert firing" alert_firing UnderReplicatedPartitions 180 || r=1
  return $r
}

drill_fix() {
  fault_throttle_clear
}

drill_recovered() {
  local r=0
  expect "under-replicated == 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 240 || r=1
  expect "no throttle config left" no_throttle_left || r=1
  return $r
}

no_throttle_left() {
  local n
  n="$( { kafka_bin kafka-configs.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --entity-type brokers --describe; topic_config --describe; } \
    2>/dev/null | grep -c 'throttled' || true)"
  echo "throttle keys=$n"
  [ "$n" = 0 ]
}

drill_teardown() {
  fault_throttle_clear
}
