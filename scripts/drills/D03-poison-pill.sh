# shellcheck shell=bash
DRILL_TITLE="poison pill: 같은 오프셋에서 무한 재시도"
POISON_PARTITION="${POISON_PARTITION:-5}"

drill_inject() {
  ctl_set producer.poison "$POISON_PARTITION"
}

drill_signal() {
  local r=0
  expect "poison failures" $PROMQ wait 'sum(increase(lab_consumer_poisonfailtotal[1m]))' '>' 3 --timeout 90 || r=1
  expect "lag concentrated on p$POISON_PARTITION" \
    $PROMQ wait "lab_partition_lag{partition=\"$POISON_PARTITION\"}" '>' 1000 --timeout 120 || r=1
  return $r
}

drill_fix() {
  stop_consumers
  reset_offsets --topic "$TOPIC:$POISON_PARTITION" --shift-by 1
  start_consumers
}

drill_recovered() {
  local r=0
  expect "p$POISON_PARTITION lag < 500" \
    $PROMQ wait "lab_partition_lag{partition=\"$POISON_PARTITION\"}" '<' 500 --timeout 240 || r=1
  expect "no poison failures in last 1m" \
    $PROMQ wait 'sum(increase(lab_consumer_poisonfailtotal[1m]))' '==' 0 --timeout 90 || r=1
  return $r
}
