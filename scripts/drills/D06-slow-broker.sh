# shellcheck shell=bash
DRILL_TITLE="느린 브로커(주기적 정지)"
TARGET_BROKER="${TARGET_BROKER:-broker-1}"
FREEZE_MS="${FREEZE_MS:-1200}"
RUN_MS="${RUN_MS:-400}"

drill_inject() {
  fault_freeze "$TARGET_BROKER" "$FREEZE_MS" "$RUN_MS"
}

drill_signal() {
  local r=0
  expect "producer request latency avg > 40ms" \
    $PROMQ wait 'max(kafka_producer_producer_metrics_request_latency_avg)' '>' 40 --timeout 120 || r=1
  expect "produce remote time p99 rises on healthy brokers" \
    $PROMQ wait 'max(kafka_network_requestmetrics_remotetimems{request="Produce",quantile="0.99",instance!="'"$TARGET_BROKER"'"})' '>' 300 --timeout 120 || r=1
  expect "all brokers still up, no URP" \
    $PROMQ hold 'sum(up{job="broker"}) == 3 and sum(kafka_server_replicamanager_underreplicatedpartitions) == 0' '==' 3 --for 20 || r=1
  return $r
}

drill_fix() {
  # 느린 브로커를 격리한다. controlled shutdown으로 리더가 옮겨 가고 ISR에서 빠진다.
  fault_broker_stop "$TARGET_BROKER"
}

drill_recovered() {
  expect "producer request latency avg < 35ms" \
    $PROMQ wait 'max(kafka_producer_producer_metrics_request_latency_avg)' '<' 35 --timeout 180
}

drill_teardown() {
  fault_unfreeze "$TARGET_BROKER"
  fault_broker_start "$TARGET_BROKER" || true
  preferred_election
}
