# shellcheck shell=bash
DRILL_TITLE="compact 토픽이 정리되지 않고 계속 자람"
STATE_TOPIC="${STATE_TOPIC:-lab-state}"
STATE_SIZE_EXPR="max(sum by (instance) (kafka_log_log_size{topic=\"$STATE_TOPIC\"}))"

state_topic_config() {
  kafka_bin kafka-configs.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --entity-type topics --entity-name "$STATE_TOPIC" "$@" 2>&1 \
    | grep -v '^$' || true
}

# 상태 토픽과 상태 프로듀서는 이 훈련에서만 쓴다. 정상 상태(정리가 도는 상태)를 먼저 만든다.
drill_env() {
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$STATE_TOPIC" >/dev/null 2>&1 || true
  sleep 3
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --create --if-not-exists --topic "$STATE_TOPIC" \
    --partitions 3 --replication-factor 3 --config cleanup.policy=compact --config min.insync.replicas=2 \
    --config min.cleanable.dirty.ratio=0.3 >/dev/null
  compose up -d --no-deps state-producer >/dev/null
}

drill_inject() {
  # 다른 팀의 토픽 생성 템플릿(이벤트 로그용)을 그대로 복사해 적용했다.
  state_topic_config --alter --add-config segment.ms=604800000,segment.bytes=1073741824 >/dev/null
}

drill_signal() {
  local r=0
  expect "CompactedTopicGrowing alert firing" alert_firing CompactedTopicGrowing 600 || r=1
  expect "one segment per partition (active only)" \
    $PROMQ wait "max(kafka_log_log_numlogsegments{topic=\"$STATE_TOPIC\"})" '==' 1 --timeout 30 || r=1
  return $r
}

drill_fix() {
  state_topic_config --alter --delete-config segment.ms,segment.bytes >/dev/null
}

drill_recovered() {
  local r=0
  expect "state topic shrinks below 15MB per broker" $PROMQ wait "$STATE_SIZE_EXPR" '<' 15e6 --timeout 300 || r=1
  expect "segments roll again" $PROMQ wait "max(kafka_log_log_numlogsegments{topic=\"$STATE_TOPIC\"})" '>' 1 --timeout 120 || r=1
  return $r
}

drill_teardown() {
  compose rm -sf state-producer >/dev/null 2>&1 || true
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --delete --topic "$STATE_TOPIC" >/dev/null 2>&1 || true
}
