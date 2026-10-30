# shellcheck shell=bash
CARD_ID=C1
CARD_TITLE="다운스트림 지연 → 리밸런스 폭풍 → 보존 기간 초과 삭제 → 유실"
CARD_TYPE="인과 연쇄형"

card_params() {
  echo "C1_SLOW_MS=$(rand 25 50)"
  echo "C1_STORM_MS=$(rand 320 450)"
  echo "C1_STEP_SEC=$(rand 40 90)"
}

# 전제: 로그 수집 토픽은 4분만 보존한다. 토픽 설정에서 확인할 수 있는 운영 조건이다.
card_env() {
  topic_config --alter --add-config retention.ms=240000,segment.ms=60000 >/dev/null
}

card_inject() {
  chaos_event "downstream slow: process_ms=$C1_SLOW_MS"
  ctl_set process_ms "$C1_SLOW_MS"
  sleep "$C1_STEP_SEC"
  chaos_event "downstream worse: process_ms=$C1_STORM_MS (batch $((C1_STORM_MS * 100))ms > max.poll.interval.ms 30000)"
  ctl_set process_ms "$C1_STORM_MS"
}

card_validate() {
  local r=0
  expect "lag grows" $PROMQ wait 'lab_group_totallag' '>' 5000 --timeout 240 || r=1
  expect "poll interval exceeded (storm)" \
    $PROMQ wait 'max(kafka_consumer_consumer_metrics_time_between_poll_max)' '>' 30000 --timeout 300 || r=1
  expect "retention deletes past committed offset" \
    $PROMQ wait 'count(max by (partition) (kafka_log_log_logstartoffset{topic="lab-events"}) > on (partition) max by (partition) (lab_partition_committedoffset))' '>' 0 --timeout 600 || r=1
  chaos_event "validate: downstream recovered (process_ms=2)"
  ctl_clear process_ms
  sleep 90
  expect "processed offsets have holes" \
    sh -c "python3 '$LAB_ROOT/scripts/analyze.py' gaps '$RUN_DIR_HOST' | tail -1 | grep -v 'total_missing=0 '" || r=1
  return $r
}

card_teardown() {
  ctl_clear process_ms
}

card_answer() {
  cat <<MD
# $CARD_ID $CARD_TITLE ($CARD_TYPE)

## 근본 원인

다운스트림(처리 대상 저장소) 지연. 레코드당 처리 시간이 ${C1_SLOW_MS}ms, 이후 ${C1_STORM_MS}ms로 늘었다.

## 인과 사슬

1. 처리 시간 증가로 처리량이 유입량(600 msg/s)보다 작아져 lag이 쌓인다.
2. 배치 하나(max.poll.records=100)의 처리 시간이 $((C1_STORM_MS * 100))ms로 max.poll.interval.ms(30000)를 넘는다. 멤버가 poll timeout으로 그룹을 떠나고 다시 합류하기를 반복한다(리밸런스 폭풍). 처리량이 0에 가까워진다.
3. 토픽 보존 기간이 4분(retention.ms=240000)이라 커밋 오프셋보다 앞선 세그먼트가 삭제된다. log start offset이 committed offset을 추월한다.
4. 컨슈머가 다시 fetch하면 OffsetOutOfRange가 나고 auto.offset.reset=latest로 위치가 끝으로 이동한다. 삭제 구간과 건너뛴 구간이 유실된다.

## 핵심 신호

- Overview: committed lag 증가, Members/Assigned가 흔들림, Rebalance callbacks 증가
- Clients: Time between poll max가 30s를 넘음, Process delay는 앱 내부 지표라 실제 장애에서는 다운스트림 응답 시간으로 본다
- Cluster: Log start offset 상승(retention 삭제)
- 처리 기록: \`python3 scripts/analyze.py gaps <run 디렉터리>\`의 missing(유실 확정). 파티션이 다른 멤버로 옮겨 간 뒤 점프하면 Skipped offsets 패널에는 잡히지 않는다

## 올바른 조치(우선순위)

1. 보존 기간을 즉시 늘린다(kafka-configs.sh --alter --add-config retention.ms=...). 유실을 멈추는 유일한 조치이며 가장 먼저 해야 한다.
2. 폭풍을 멈춘다: max.poll.records를 줄여 배치 처리 시간을 poll 간격 아래로 낮춘다.
3. 다운스트림 지연 원인을 해결하거나 처리량을 확보한다.
4. 유실 범위를 확정한다: 파티션별 log start offset과 committed offset 차이, 처리 기록의 구멍(\`analyze.py gaps\`).

## 해로운 조치

- 컨슈머를 반복 재시작하거나 증설한다: 리밸런싱만 늘고 처리량은 늘지 않는다.
- 오프셋을 latest로 리셋한다: 유실을 스스로 확정한다.
- 보존 기간 연장 없이 원인 분석부터 한다: 분석하는 동안 삭제가 진행된다.
MD
}
