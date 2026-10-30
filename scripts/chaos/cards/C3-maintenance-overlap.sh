# shellcheck shell=bash
CARD_ID=C3
CARD_TITLE="브로커 롤링 점검과 컨슈머 배포가 겹침"
CARD_TYPE="운영 동시성형"

card_params() {
  echo "C3_DEPLOY_OFFSET_SEC=$(rand 5 40)"
  echo "C3_BROKER_ORDER=\"$(printf 'broker-1\nbroker-2\nbroker-3\n' | awk -v r="$RANDOM" 'BEGIN{srand(r)} {print rand() "\t" $0}' | sort | cut -f2 | tr '\n' ' ')\""
}

card_noise_ok() { [ "$1" != 1 ]; }

roll_brokers() {
  local b
  for b in $C3_BROKER_ORDER; do
    chaos_event "maintenance: restart $b"
    docker restart -t 30 "$b" >/dev/null
    local deadline=$(( $(date +%s) + 120 ))
    until [ "$(docker inspect -f '{{.State.Health.Status}}' "$b" 2>/dev/null)" = "healthy" ]; do
      [ "$(date +%s)" -ge "$deadline" ] && break
      sleep 3
    done
    sleep 15
  done
  chaos_event "maintenance: done"
}

roll_consumers() {
  local m
  sleep "$C3_DEPLOY_OFFSET_SEC"
  for m in consumer-1 consumer-2 consumer-3; do
    chaos_event "deploy: restart $m"
    compose restart -t 20 "$m" >/dev/null 2>&1 || true
    sleep 8
  done
  chaos_event "deploy: done"
}

card_inject() {
  roll_brokers &
  roll_consumers &
  wait
}

card_validate() {
  local r=0
  expect "commit failures" $PROMQ wait 'sum(increase(lab_consumer_commitfailedtotal[5m]))' '>' 0 --timeout 240 || r=1
  expect "maintenance finished" sh -c "for i in \$(seq 1 100); do grep -q 'maintenance: done' '$SEALED/timeline.log' && exit 0; sleep 3; done; exit 1" || r=1
  sleep 20
  expect "no loss" sh -c "python3 '$LAB_ROOT/scripts/analyze.py' gaps '$RUN_DIR_HOST' | tail -1 | grep 'total_missing=0 '" || r=1
  return $r
}

card_answer() {
  cat <<MD
# $CARD_ID $CARD_TITLE ($CARD_TYPE)

## 근본 원인

독립된 두 변경이 같은 시간대에 실행됐다. 브로커 롤링 재시작(순서: $C3_BROKER_ORDER)과, ${C3_DEPLOY_OFFSET_SEC}초 뒤 시작된 컨슈머 롤링 배포다. 각각은 단독으로 안전한 작업이다.

## 인과 사슬

1. 브로커 재시작으로 그룹 코디네이터(__consumer_offsets 파티션 리더)와 데이터 파티션 리더가 이동한다. 이동 중 커밋은 NOT_COORDINATOR, COORDINATOR_LOAD_IN_PROGRESS 등으로 실패한다.
2. 같은 시점에 컨슈머 재시작으로 리밸런싱이 일어나 파티션이 다른 멤버로 옮겨 간다.
3. 커밋에 실패한 파티션이 그 직후 다른 멤버로 옮겨 가면, 새 소유자는 마지막으로 성공한 커밋부터 읽어 그 구간을 다시 처리한다(중복). 두 사건의 시점이 맞물려야 하므로 회차마다 중복 규모가 다르고 0일 수도 있다.
4. 오프셋 구멍은 생기지 않는다. 유실은 없다.

## 핵심 신호

- Cluster: Brokers up 일시 감소, leader count 변동, URP 일시 증가
- Overview: Commit failures, Rebalance callbacks 증가, 그룹 상태 전이
- 처리 기록: analyze.py gaps → missing=0, duplicates ≥ 0

## 올바른 조치

1. 두 사건의 시간대를 분리해 식별한다(브로커 재시작 시각, 컨슈머 재시작 시각).
2. 유실 여부를 먼저 판정한다(gaps의 missing). 유실이 없으면 데이터 조치는 필요 없다.
3. 중복이 있으면 규모를 산정하고 다운스트림 멱등성 여부를 확인한다.
4. 재발 방지: 변경 작업 직렬화(change freeze), 배포 전 클러스터 상태 확인 게이트.

## 해로운 조치

- 중복을 없애겠다고 오프셋을 앞으로 리셋한다: 실제 유실을 만든다.
- 원인을 브로커나 컨슈머 한쪽으로만 결론 낸다: 재발 방지책이 틀린다.
MD
}
