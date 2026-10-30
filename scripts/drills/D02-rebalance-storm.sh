# shellcheck shell=bash
DRILL_TITLE="poll 간격 초과로 인한 리밸런스 폭풍"

drill_inject() {
  ctl_set process_ms 350
}

drill_signal() {
  local r=0
  expect "time-between-poll max > max.poll.interval.ms" \
    $PROMQ wait 'max(kafka_consumer_consumer_metrics_time_between_poll_max)' '>' 30000 --timeout 180 || r=1
  expect "members lose partitions repeatedly" \
    $PROMQ wait 'sum(increase(lab_consumer_losttotal[3m])) + sum(increase(lab_consumer_revokedtotal[3m]))' '>' 1 --timeout 120 || r=1
  return $r
}

drill_fix() {
  # 배치 하나의 처리 시간을 max.poll.interval.ms 아래로 줄인다(350ms × 10 = 3.5s).
  export MAX_POLL_RECORDS=10
  mark "REDEPLOY consumers MAX_POLL_RECORDS=10"
  # shellcheck disable=SC2046
  compose up -d --no-deps $(consumers_running) >/dev/null
}

drill_recovered() {
  local r=0
  sleep 60
  expect "time-between-poll max < 30s for 60s" \
    $PROMQ hold 'max(kafka_consumer_consumer_metrics_time_between_poll_max)' '<' 30000 --for 60 || r=1
  expect "no lost partitions in last 1m" \
    $PROMQ wait 'sum(increase(lab_consumer_losttotal[1m]))' '==' 0 --timeout 60 || r=1
  return $r
}

drill_teardown() {
  ctl_clear process_ms
}
