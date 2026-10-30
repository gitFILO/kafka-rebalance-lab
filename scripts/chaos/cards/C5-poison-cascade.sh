# shellcheck shell=bash
CARD_ID=C5
CARD_TITLE="poison pill + 블로킹 재시도 → poll timeout → 연쇄 크래시"
CARD_TYPE="인과 연쇄형"

card_params() {
  echo "C5_PARTITION=$(rand 0 11)"
}

# 전제: 앱은 처리 실패 시 같은 레코드를 5초 간격으로 10회 재시도하고, 그래도 실패하면 예외로 종료한다.
card_env() {
  export POISON_POLICY=inline POISON_RETRIES=10 POISON_BACKOFF_MS=5000
}

card_noise_ok() { [ "$1" != 1 ]; }

card_inject() {
  chaos_event "poison record to partition $C5_PARTITION"
  ctl_set producer.poison "$C5_PARTITION"
}

card_validate() {
  local r=0
  expect "poison failures" $PROMQ wait 'sum(increase(lab_consumer_poisonfailtotal[2m]))' '>' 0 --timeout 90 || r=1
  expect "group shrinks" $PROMQ wait 'lab_group_members' '<' 3 --timeout 180 || r=1
  expect "cascade reaches all members" $PROMQ wait 'lab_group_members' '==' 0 --timeout 420 || r=1
  return $r
}

card_teardown() {
  unset POISON_POLICY POISON_RETRIES POISON_BACKOFF_MS
}

card_answer() {
  cat <<MD
# $CARD_ID $CARD_TITLE ($CARD_TYPE)

## 근본 원인

파티션 $C5_PARTITION 에 처리할 수 없는 레코드(poison pill) 1건. 앱의 재시도 정책(5초 × 10회, poll 루프 안에서 블로킹)이 이를 장애로 키웠다.

## 인과 사슬

1. 파티션 $C5_PARTITION 소유 멤버가 poison 레코드에서 재시도를 시작한다. poll 루프가 멈춘다.
2. 30초(max.poll.interval.ms) 뒤 poll timeout으로 그룹에서 빠지고 파티션이 다른 멤버에게 넘어간다.
3. 커밋이 진행되지 않았으므로 다음 멤버도 같은 레코드에서 멈춘다. 첫 멤버는 50초 뒤 재시도 소진으로 프로세스가 종료된다.
4. 같은 과정이 반복되어 모든 멤버가 차례로 종료된다. 전 파티션 처리 중단.

## 핵심 신호

- Overview: Members 감소, Commit failures/errors/poison 증가, 전 파티션 lag 증가
- Clients: Poison fail, Stalled/errors, Last poll seconds ago 증가
- 컨테이너 로그: POISON_FAIL partitions=[$C5_PARTITION] offset=..., 이어서 ERROR와 종료

## 올바른 조치

1. 종료 로그와 이벤트에서 파티션 $C5_PARTITION 의 같은 오프셋이 반복되는 것을 확인한다.
2. 그룹이 비어 있는 상태에서 해당 파티션만 오프셋을 1 이동한다(kafka-consumer-groups.sh --reset-offsets --topic lab-events:$C5_PARTITION --shift-by 1 --execute). 건너뛴 레코드는 별도로 보관·재처리 계획을 세운다.
3. 컨슈머를 재기동하고 lag 회복을 확인한다.
4. 재발 방지: 재시도 총 시간을 max.poll.interval.ms보다 짧게, 소진 시 DLQ로 보내고 진행(skip).

## 해로운 조치

- 오프셋 이동 없이 재기동한다: 같은 레코드에서 같은 연쇄가 반복된다.
- 그룹 전체를 latest로 리셋한다: 정상 레코드까지 유실한다.
- max.poll.interval.ms만 늘린다: 전파 속도만 늦춘다.
MD
}
