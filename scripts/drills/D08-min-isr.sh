# shellcheck shell=bash
DRILL_TITLE="min.insync.replicas 미달로 쓰기 거부"

drill_inject() {
  fault_broker_stop broker-1
  fault_broker_stop broker-2
}

drill_signal() {
  local r=0
  expect "under min ISR > 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underminisrpartitioncount)' '>' 0 --timeout 90 || r=1
  expect "acked rate collapses" $PROMQ wait 'rate(lab_producer_ackedtotal[30s])' '<' 50 --timeout 90 || r=1
  expect "producer retries or broker rejects" \
    $PROMQ wait 'sum(kafka_producer_producer_metrics_record_retry_rate) + sum(rate(kafka_server_brokertopicmetrics_failedproducerequests_total{topic="lab-events"}[1m]))' '>' 0 --timeout 90 || r=1
  return $r
}

drill_fix() {
  fault_broker_start broker-1
  fault_broker_start broker-2
  $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 >/dev/null || true
  preferred_election
}

drill_recovered() {
  local r=0
  expect "under min ISR == 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underminisrpartitioncount)' '==' 0 --timeout 120 || r=1
  expect "acked rate > 500" $PROMQ wait 'rate(lab_producer_ackedtotal[30s])' '>' 500 --timeout 180 || r=1
  return $r
}
