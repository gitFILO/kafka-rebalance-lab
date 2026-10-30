# shellcheck shell=bash
DRILL_TITLE="큰 메시지로 인한 RecordTooLargeException"

drill_inject() {
  ctl_set producer.large "1200000 10000"
}

drill_signal() {
  expect "RecordTooLargeException on producer" \
    $PROMQ wait 'sum(increase(lab_producer_error_count{error="RecordTooLargeException"}[2m]))' '>' 0 --timeout 60
}

drill_fix() {
  # 토픽과 프로듀서 한도를 함께 올린다. 한쪽만 올리면 다른 쪽에서 거부된다.
  topic_config --alter --add-config max.message.bytes=4194304
  export MAX_REQUEST_SIZE=4194304
  mark "REDEPLOY producer MAX_REQUEST_SIZE=4194304"
  compose up -d --no-deps producer >/dev/null
}

drill_recovered() {
  local r=0
  sleep 15
  expect "large records acked" $PROMQ wait 'increase(lab_producer_largesenttotal[1m])' '>' 0 --timeout 60 || r=1
  expect "no RecordTooLargeException in last 1m" \
    $PROMQ wait 'sum(increase(lab_producer_error_count{error="RecordTooLargeException",instance="producer"}[1m])) or vector(0)' '==' 0 --timeout 90 || r=1
  expect "producer failed rate == 0" $PROMQ wait 'rate(lab_producer_failedtotal[1m])' '==' 0 --timeout 60 || r=1
  return $r
}

drill_teardown() {
  ctl_clear producer.large
  unset MAX_REQUEST_SIZE
}
