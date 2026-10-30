# shellcheck shell=bash
DRILL_TITLE="브로커 1대 비정상 종료"
TARGET_BROKER="${TARGET_BROKER:-broker-2}"

drill_inject() {
  fault_broker_kill "$TARGET_BROKER"
}

drill_signal() {
  local r=0
  expect "brokers up == 2" $PROMQ wait 'sum(up{job="broker"})' '==' 2 --timeout 60 || r=1
  expect "under-replicated > 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '>' 0 --timeout 90 || r=1
  expect "producer keeps acking" $PROMQ wait 'rate(lab_producer_ackedtotal[30s])' '>' 400 --timeout 90 || r=1
  return $r
}

drill_fix() {
  fault_broker_start "$TARGET_BROKER"
  $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 >/dev/null || true
  preferred_election
}

drill_recovered() {
  local r=0
  expect "under-replicated == 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 || r=1
  expect "leader count balanced (max-min <= 2)" \
    $PROMQ wait 'max(kafka_server_replicamanager_leadercount{job="broker"}) - min(kafka_server_replicamanager_leadercount{job="broker"})' '<=' 2 --timeout 120 || r=1
  return $r
}
