# shellcheck shell=bash
DRILL_TITLE="열린 채 남은 트랜잭션이 LSO를 막음"

drill_env() {
  export TRANSACTIONAL=true TRANSACTION_TIMEOUT_MS=900000 ISOLATION_LEVEL=read_committed
}

drill_inject() {
  mark "KILL9 producer"
  docker kill --signal KILL producer >/dev/null
  sleep 3
  compose up -d --no-deps producer >/dev/null
}

drill_signal() {
  local r=0
  expect "HW - LSO gap grows" \
    $PROMQ wait 'sum(lab_partition_logendoffset - lab_partition_laststableoffset)' '>' 5000 --timeout 120 || r=1
  expect "committed lag grows" $PROMQ wait 'lab_group_totallag' '>' 5000 --timeout 60 || r=1
  return $r
}

# 파티션마다 가장 오래된 열린 트랜잭션(= LSO를 붙잡은 것)을 abort 한다.
abort_stuck_transactions() {
  local p start
  for p in $(seq 0 $((PARTITIONS - 1))); do
    start="$(kafka_bin kafka-transactions.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" describe-producers --topic "$TOPIC" --partition "$p" 2>/dev/null \
      | awk 'NR > 1 && $NF != "-1" && $NF != "None" {print $NF}' | sort -n | head -1)"
    if [ -n "$start" ]; then
      log "abort p$p start-offset=$start"
      kafka_bin kafka-transactions.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" abort --topic "$TOPIC" --partition "$p" --start-offset "$start" >/dev/null 2>&1 \
        || log "WARN: abort p$p failed"
    fi
  done
}

drill_fix() {
  abort_stuck_transactions
}

drill_recovered() {
  local r=0
  expect "HW - LSO gap < 2000" \
    $PROMQ wait 'sum(lab_partition_logendoffset - lab_partition_laststableoffset)' '<' 2000 --timeout 120 || r=1
  expect "committed lag < 1000" $PROMQ wait 'lab_group_totallag' '<' 1000 --timeout 240 || r=1
  return $r
}

drill_teardown() {
  unset TRANSACTIONAL TRANSACTION_TIMEOUT_MS ISOLATION_LEVEL
}
