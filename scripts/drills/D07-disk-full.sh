# shellcheck shell=bash
DRILL_TITLE="브로커 디스크 풀"
TARGET_BROKER="${TARGET_BROKER:-broker-3}"

drill_inject() {
  fault_disk_fill "$TARGET_BROKER" 0
}

drill_signal() {
  local r=0
  expect "disk free < 5MB on $TARGET_BROKER" \
    $PROMQ wait "lab_disk_usablebytes{broker=\"$TARGET_BROKER\"}" '<' 5000000 --timeout 60 || r=1
  expect "$TARGET_BROKER down" $PROMQ wait 'sum(up{job="broker"})' '==' 2 --timeout 180 || r=1
  expect "under-replicated > 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '>' 0 --timeout 60 || r=1
  return $r
}

drill_fix() {
  fault_disk_clear "$TARGET_BROKER"
  fault_broker_start "$TARGET_BROKER"
  $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 >/dev/null || true
  preferred_election
}

drill_recovered() {
  local r=0
  expect "brokers up == 3" $PROMQ wait 'sum(up{job="broker"})' '==' 3 --timeout 120 || r=1
  expect "under-replicated == 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 || r=1
  expect "disk free > 50MB" \
    $PROMQ wait "lab_disk_usablebytes{broker=\"$TARGET_BROKER\"}" '>' 50000000 --timeout 30 || r=1
  return $r
}
