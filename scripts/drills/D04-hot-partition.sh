# shellcheck shell=bash
DRILL_TITLE="키 쏠림으로 인한 hot partition"
HOT_PARTITION="${HOT_PARTITION:-3}"

drill_inject() {
  ctl_set producer.rate 900
  ctl_set producer.skew "$HOT_PARTITION:70"
}

drill_signal() {
  local r=0
  expect "p$HOT_PARTITION lag > 3000" \
    $PROMQ wait "lab_partition_lag{partition=\"$HOT_PARTITION\"}" '>' 3000 --timeout 180 || r=1
  expect "p$HOT_PARTITION holds > 60% of lag" \
    $PROMQ wait "sum(lab_partition_lag{partition=\"$HOT_PARTITION\"}) / sum(lab_partition_lag)" '>' 0.6 --timeout 60 || r=1
  return $r
}

drill_fix() {
  # 상류의 키 설계를 고친 상황이다. 컨슈머 증설로는 파티션 하나의 처리량 한계를 넘지 못한다.
  ctl_clear producer.skew
  ctl_clear producer.rate
}

drill_recovered() {
  expect "p$HOT_PARTITION lag < 500" \
    $PROMQ wait "lab_partition_lag{partition=\"$HOT_PARTITION\"}" '<' 500 --timeout 420
}

drill_teardown() {
  ctl_clear producer.skew
  ctl_clear producer.rate
}
