# shellcheck shell=bash
DRILL_TITLE="컨트롤러 쿼럼 상실"
SECOND_FAULT_BROKER="${SECOND_FAULT_BROKER:-broker-2}"
QUIET_SEC="${QUIET_SEC:-60}"

active_controller_container() {
  local id
  id="$(kafka_bin kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" describe --status 2>/dev/null \
    | awk -F': *' '/LeaderId/ {print $2}')"
  echo "controller-${id:-1}"
}

drill_inject() {
  local leader other c
  leader="$(active_controller_container)"
  for c in controller-1 controller-2 controller-3; do
    [ "$c" != "$leader" ] && { other="$c"; break; }
  done
  # 같은 호스트에 몰려 있던 컨트롤러 2대가 하이퍼바이저 장애로 함께 내려갔다.
  mark "CONTROLLERS_DOWN $leader $other"
  docker kill --signal KILL "$leader" "$other" >/dev/null
  log "컨트롤러 2대 중단. ${QUIET_SEC}초 뒤 브로커 하나가 추가로 죽는다."
  sleep "$QUIET_SEC"
  # 쿼럼이 없는 동안 브로커가 하나 더 죽으면 펜싱도 ISR 축소도 일어나지 않는다.
  mark "SECOND_FAULT $SECOND_FAULT_BROKER"
  fault_broker_kill "$SECOND_FAULT_BROKER"
}

drill_signal() {
  local r=0
  expect "NoActiveController alert firing" alert_firing NoActiveController 60 || r=1
  expect "ProducerThroughputDrop alert firing" alert_firing ProducerThroughputDrop 240 || r=1
  expect "topic creation fails without quorum" topic_create_fails || r=1
  return $r
}

topic_create_fails() {
  local c
  c="$(cli_container)"
  docker exec "$c" sh -c 'printf "default.api.timeout.ms=20000\nrequest.timeout.ms=10000\n" > /tmp/d14.properties'
  if KAFKA_CLI_CONTAINER="$c" kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --create --topic d14-probe \
      --partitions 1 --replication-factor 1 --command-config /tmp/d14.properties >/dev/null 2>&1; then
    echo "created (quorum alive?)"
    return 1
  fi
  echo "create failed"
}

drill_fix() {
  local c
  for c in controller-1 controller-2 controller-3; do
    [ "$(docker inspect -f '{{.State.Running}}' "$c")" = true ] || docker start "$c" >/dev/null
  done
  $PROMQ wait 'sum(kafka_controller_kafkacontroller_activecontrollercount{job="controller"})' '==' 1 --timeout 120 >/dev/null || true
  fault_broker_start "$SECOND_FAULT_BROKER"
  $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 >/dev/null || true
  preferred_election
}

drill_recovered() {
  local r=0
  expect "active controller == 1" \
    $PROMQ wait 'sum(kafka_controller_kafkacontroller_activecontrollercount{job="controller"})' '==' 1 --timeout 120 || r=1
  expect "under-replicated == 0" \
    $PROMQ wait 'sum(kafka_server_replicamanager_underreplicatedpartitions)' '==' 0 --timeout 180 || r=1
  expect "producer acks back to target" $PROMQ wait 'sum(rate(lab_producer_ackedtotal[30s]))' '>' 500 --timeout 180 || r=1
  return $r
}

drill_teardown() {
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic d14-probe >/dev/null 2>&1 || true
}
