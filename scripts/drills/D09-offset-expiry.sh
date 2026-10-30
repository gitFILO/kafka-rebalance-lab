# shellcheck shell=bash
DRILL_TITLE="오프셋 만료 후 latest 리셋으로 인한 조용한 유실"
PAUSE_SEC="${PAUSE_SEC:-150}"

recreate_brokers() {
  log "recreate brokers (offsets.retention.minutes=${OFFSETS_RETENTION_MINUTES:-default})"
  compose up -d broker-1 broker-2 broker-3 >/dev/null
  wait_brokers_healthy
}

drill_env() {
  # 축척: 운영의 7일 보존을 1분으로 줄인다.
  export OFFSETS_RETENTION_MINUTES=1 OFFSETS_RETENTION_CHECK_INTERVAL_MS=15000
  recreate_brokers
}

drill_inject() {
  # 정지 완료가 아니라 정지 시작 시각을 기준으로 삼아야 리셋이 마지막 커밋 이전으로 돌아간다.
  mark "PAUSE_BEGIN"
  stop_consumers
  log "consumers paused for ${PAUSE_SEC}s (배포 중단을 가정)"
  sleep "$PAUSE_SEC"
  mark "PAUSE_END"
  start_consumers
}

drill_signal() {
  local r=0
  expect "offsets expired on coordinator" \
    $PROMQ wait 'sum(increase(kafka_server_group_coordinator_metrics_offset_expiration_count[10m]))' '>' 0 --timeout 60 || r=1
  sleep 20
  expect "processed offsets have holes" sh -c "python3 '$LAB_ROOT/scripts/analyze.py' gaps '$RUN_DIR_HOST' | tail -1 | grep -v 'total_missing=0 '" || r=1
  return $r
}

drill_fix() {
  local since
  since="$(awk -F, '$2 == "PAUSE_BEGIN" {print $1}' "$RUN_DIR_HOST/markers.csv" | tail -1)"
  stop_consumers
  reset_offsets --all-topics --to-datetime "$(python3 -c "import datetime,sys; print(datetime.datetime.fromtimestamp(int(sys.argv[1])/1000, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000'))" "$since")"
  start_consumers
}

drill_recovered() {
  expect "backlog reprocessed (lag < 1000)" $PROMQ wait 'lab_group_totallag' '<' 1000 --timeout 300 || return 1
  sleep 10
  expect "no holes after reprocessing" sh -c "python3 '$LAB_ROOT/scripts/analyze.py' gaps '$RUN_DIR_HOST' | tail -1 | grep 'total_missing=0 '"
}

drill_teardown() {
  unset OFFSETS_RETENTION_MINUTES OFFSETS_RETENTION_CHECK_INTERVAL_MS
  recreate_brokers
}
