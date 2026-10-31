# shellcheck shell=bash
DRILL_TITLE="랙 장애 후 unclean leader election 판단"
STALE_BROKER="${STALE_BROKER:-broker-3}"
LOST_BROKERS="${LOST_BROKERS:-broker-1 broker-2}"
STALE_SEC="${STALE_SEC:-60}"
# clean: 브로커 복구를 기다린다. unclean: 뒤처진 브로커를 리더로 강제 선출한다(유실 측정용).
D17_FIX="${D17_FIX:-clean}"

end_offsets() {
  kafka_bin kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --topic "$TOPIC" --time latest 2>/dev/null \
    | sort -t: -k2 -n
}

drill_inject() {
  # broker-3은 정기 패치로 내려가 있었다. 그동안 들어온 쓰기는 broker-1, broker-2에만 있다.
  fault_broker_stop "$STALE_BROKER"
  end_offsets > "$RUN_DIR_HOST/d17-offsets-stale.txt"
  sleep "$STALE_SEC"
  end_offsets > "$RUN_DIR_HOST/d17-offsets-before.txt"
  # broker-1, broker-2가 있는 랙의 전원이 나갔다.
  mark "RACK_POWER_LOSS $LOST_BROKERS"
  # shellcheck disable=SC2086
  docker kill --signal KILL $LOST_BROKERS >/dev/null
  # 패치를 마친 broker-3이 돌아왔다. broker-1, broker-2의 복구 시점은 알 수 없다.
  fault_broker_start "$STALE_BROKER"
}

drill_signal() {
  local r=0
  expect "OfflinePartitions alert firing" alert_firing OfflinePartitions 120 || r=1
  expect "offline partitions > 0 while $STALE_BROKER is up" \
    $PROMQ hold 'max(kafka_controller_kafkacontroller_offlinepartitionscount{job="controller"})' '>' 0 --for 30 || r=1
  KAFKA_CLI_CONTAINER="$STALE_BROKER" kafka_bin kafka-topics.sh --bootstrap-server "$STALE_BROKER:9092" --describe \
    --topic "$TOPIC" > "$RUN_DIR_HOST/d17-describe-offline.txt" 2>&1 || true
  expect "no unclean election before the fix" \
    $PROMQ wait 'max(increase(kafka_controller_controllerstats_uncleanleaderelections_total[2m]))' '==' 0 --timeout 5 || r=1
  return $r
}

drill_fix() {
  local b
  if [ "$D17_FIX" = unclean ]; then
    mark "UNCLEAN_ELECTION"
    kafka_bin kafka-leader-election.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --election-type UNCLEAN \
      --all-topic-partitions 2>&1 | tail -3
    report_loss
  fi
  for b in $LOST_BROKERS; do
    fault_broker_start "$b"
  done
  $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 240 >/dev/null || true
  preferred_election
  # 리더 복구 뒤에도 프로듀서가 옛 리더 정보를 붙잡고 있는 경우가 있었다(2회 중 1회). 클라이언트 재시작이 마지막 조치다.
  if ! $PROMQ wait 'sum(rate(lab_producer_ackedtotal[30s]))' '>' 500 --timeout 90 >/dev/null; then
    mark "PRODUCER_RESTART"
    compose restart producer >/dev/null
  fi
}

# broker-3이 내려간 시점과 rack 장애 직전의 log end offset 차이가 ack를 받고도 사라지는 레코드의 하한이다.
report_loss() {
  python3 - "$RUN_DIR_HOST/d17-offsets-before.txt" "$RUN_DIR_HOST/d17-offsets-stale.txt" <<'PY' | tee "$RUN_DIR_HOST/d17-loss.txt"
import sys
def load(p):
    out = {}
    for line in open(p):
        parts = line.strip().split(":")
        if len(parts) == 3:
            out[int(parts[1])] = int(parts[2])
    return out
before, after = load(sys.argv[1]), load(sys.argv[2])
lost = {p: before[p] - after.get(p, 0) for p in before}
print("partition lost_records")
for p in sorted(lost):
    print("%9d %d" % (p, lost[p]))
print("total lost acked records: %d" % sum(v for v in lost.values() if v > 0))
PY
}

drill_recovered() {
  local r=0
  expect "offline partitions == 0" \
    $PROMQ wait 'max(kafka_controller_kafkacontroller_offlinepartitionscount{job="controller"})' '==' 0 --timeout 180 || r=1
  expect "under-replicated == 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 240 || r=1
  expect "producer acks back to target" $PROMQ wait 'sum(rate(lab_producer_ackedtotal[30s]))' '>' 500 --timeout 180 || r=1
  return $r
}
