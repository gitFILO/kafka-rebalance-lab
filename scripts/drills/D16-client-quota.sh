# shellcheck shell=bash
DRILL_TITLE="기본 클라이언트 쿼터로 인한 소비 지연"
QUOTA_BYTES="${QUOTA_BYTES:-2048}"
SERVICE_QUOTA_BYTES="${SERVICE_QUOTA_BYTES:-10485760}"

drill_inject() {
  # 플랫폼 팀이 시끄러운 테넌트를 막으려고 모든 클라이언트에 기본 쿼터를 걸었다. 이 서비스는 예외 등록을 하지 않았다.
  client_quota_config --entity-default --alter --add-config "consumer_byte_rate=$QUOTA_BYTES" >/dev/null
}

drill_signal() {
  local r=0
  expect "ClientThrottled alert firing" alert_firing ClientThrottled 180 || r=1
  expect "ConsumerLagGrowing alert firing" alert_firing ConsumerLagGrowing 300 || r=1
  expect "consumers wait on fetches (poll idle ratio > 0.35, baseline about 0.2)" \
    $PROMQ wait 'avg(kafka_consumer_consumer_metrics_poll_idle_ratio_avg)' '>' 0.35 --timeout 60 || r=1
  return $r
}

# 기본 쿼터는 그대로 두고 이 서비스의 client-id만 예외로 올린다. 쿼터는 브로커마다 따로 적용된다.
drill_fix() {
  local c
  for c in $CONSUMERS; do
    client_quota_config --entity-name "$c" --alter --add-config "consumer_byte_rate=$SERVICE_QUOTA_BYTES" >/dev/null
  done
}

drill_recovered() {
  local r=0
  expect "fetch throttle time == 0" \
    $PROMQ wait 'max(kafka_consumer_consumer_fetch_manager_metrics_fetch_throttle_time_avg)' '==' 0 --timeout 120 || r=1
  expect "lag back under 5000" $PROMQ wait 'lab_group_totallag' '<' 5000 --timeout 300 || r=1
  return $r
}

drill_teardown() {
  fault_quota_clear
}
