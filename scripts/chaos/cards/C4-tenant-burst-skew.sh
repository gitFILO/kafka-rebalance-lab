# shellcheck shell=bash
CARD_ID=C4
CARD_TITLE="대형 고객 이벤트: 트래픽 버스트와 키 쏠림"
CARD_TYPE="인과 연쇄형"

card_params() {
  echo "C4_PARTITION=$(rand 0 11)"
  echo "C4_PERCENT=$(rand 55 70)"
  echo "C4_RATE=$(rand 1000 1200)"
}

card_noise_ok() { [ "$1" != 3 ]; }

card_inject() {
  chaos_event "tenant event: rate=$C4_RATE, ${C4_PERCENT}% keyed to partition $C4_PARTITION"
  ctl_set producer.rate "$C4_RATE"
  ctl_set producer.skew "$C4_PARTITION:$C4_PERCENT"
}

card_validate() {
  local r=0
  expect "hot partition lag > 3000" \
    $PROMQ wait "lab_partition_lag{partition=\"$C4_PARTITION\"}" '>' 3000 --timeout 240 || r=1
  expect "hot partition holds > 50% of lag" \
    $PROMQ wait "sum(lab_partition_lag{partition=\"$C4_PARTITION\"}) / sum(lab_partition_lag)" '>' 0.5 --timeout 120 || r=1
  "$LAB_ROOT/scripts/workload.sh" scale 6 >/dev/null
  chaos_event "validate: scaled to 6 consumers"
  sleep 60
  expect "scale-out does not drain hot partition" \
    $PROMQ wait "deriv(lab_partition_lag{partition=\"$C4_PARTITION\"}[1m])" '>' 0 --timeout 60 || r=1
  return $r
}

card_teardown() {
  ctl_clear producer.rate
  ctl_clear producer.skew
}

card_answer() {
  cat <<MD
# $CARD_ID $CARD_TITLE ($CARD_TYPE)

## 근본 원인

특정 고객(키)의 이벤트로 전체 유입이 ${C4_RATE} msg/s로 늘었고, 그중 ${C4_PERCENT}%가 같은 키라 파티션 $C4_PARTITION 하나에 몰렸다.

## 인과 사슬

1. 전체 유입 증가로 모든 지표가 같이 오르므로 처음에는 용량 부족으로 보인다.
2. 파티션 $C4_PARTITION 의 유입이 컨슈머 1대의 처리 한계(레코드당 2ms, 약 500 msg/s 미만)를 넘는다.
3. 파티션은 한 그룹 안에서 한 멤버만 처리하므로 증설해도 그 파티션의 처리량은 늘지 않는다.

## 핵심 신호

- Overview: Committed lag by partition에서 한 파티션만 증가
- Clients: Processed per partition에서 해당 파티션이 상한에 붙음
- Overview: 해당 파티션 소유 멤버만 처리량이 높고 나머지는 여유

## 올바른 조치

1. 파티션별 lag 분포로 쏠림을 확인한다(전체 lag만 보면 오진한다).
2. 증설이 효과 없다는 것을 근거로 설명한다.
3. 대응: 상류 키 설계 변경(키 salting, 고객 단위 분리 토픽), 해당 파티션 처리 비용 축소. 이벤트가 끝나면 자연 회복되는지 판단한다.

## 해로운 조치

- 파티션 수를 늘린다: 같은 키는 여전히 한 파티션으로 가고, 다른 키의 파티션 매핑이 바뀌어 키 순서 보장이 깨진다.
- 컨슈머를 계속 증설한다: 리밸런싱만 늘어난다.
MD
}
