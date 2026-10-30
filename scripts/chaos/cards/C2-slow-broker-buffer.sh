# shellcheck shell=bash
CARD_ID=C2
CARD_TITLE="느린 브로커 → acks=all 지연 → 프로듀서 버퍼 고갈 → send() 블로킹"
CARD_TYPE="인과 연쇄형"

card_params() {
  local b
  b="$(rand 1 3)"
  echo "C2_BROKER=broker-$b"
  echo "C2_FREEZE_MS=$(rand 2200 3000)"
}

card_inject() {
  chaos_event "periodic freeze on $C2_BROKER: ${C2_FREEZE_MS}ms stop / 300ms run"
  fault_freeze "$C2_BROKER" "$C2_FREEZE_MS" 300
}

card_validate() {
  local r=0
  expect "producer request latency rises" \
    $PROMQ wait 'max(kafka_producer_producer_metrics_request_latency_avg)' '>' 150 --timeout 120 || r=1
  expect "buffer exhausted (bufferpool wait or send blocked)" \
    $PROMQ wait 'max(kafka_producer_producer_metrics_bufferpool_wait_ratio) + sum(rate(lab_producer_sendblockedmstotal[30s])) / 1000' '>' 0.05 --timeout 180 || r=1
  expect "upstream requests skipped" $PROMQ wait 'increase(lab_producer_skippedtotal[2m])' '>' 0 --timeout 180 || r=1
  return $r
}

card_teardown() {
  fault_unfreeze "$C2_BROKER"
}

card_answer() {
  cat <<MD
# $CARD_ID $CARD_TITLE ($CARD_TYPE)

## 근본 원인

$C2_BROKER 프로세스가 주기적으로 멈춘다(${C2_FREEZE_MS}ms 정지, 300ms 실행 반복). 긴 GC 정지나 호스트 정체(CPU steal, 스왑)에 해당한다. 브로커는 살아 있고 세션 타임아웃(9초)과 replica.lag.time.max.ms(30초) 안에서 응답하므로 fencing도 ISR 축소도 일어나지 않는다.

## 인과 사슬

1. $C2_BROKER 가 멈춘 동안 그 브로커가 리더인 파티션의 Produce 응답이 지연된다.
2. 모든 파티션의 복제본이 $C2_BROKER 에도 있으므로(RF=3), acks=all은 $C2_BROKER 의 follower fetch를 기다린다. 다른 브로커가 리더인 파티션도 Produce remote time이 늘어난다.
3. 요청 지연이 늘면 미전송 배치가 프로듀서 버퍼에 쌓인다. buffer.memory=512KB가 고갈되면 send()가 max.block.ms까지 블로킹된다.
4. 송신 스레드가 막혀 상류 요청을 받지 못한다(skipped). 컨슈머 쪽은 정상이라 lag 대시보드만 보면 놓친다.

## 핵심 신호

- Clients 프로듀서: request latency 상승, buffer available 감소, bufferpool wait ratio, send() blocked, skipped
- Cluster: Produce p99 breakdown에서 $C2_BROKER 를 제외한 브로커의 remote time만 높다(복제 대기) → $C2_BROKER 가 느린 팔로워
- Overview: Brokers up이 2와 3 사이를 오가지만 URP 0, Fenced brokers 0. 죽은 브로커가 아니라 $C2_BROKER 의 메트릭 수집이 멈춘 순간에 시간 초과로 실패하는 것이다
- Prometheus: $C2_BROKER 의 scrape_duration_seconds 증가, 간헐적 수집 실패

## 올바른 조치

1. 느린 브로커를 특정한다(노드별 지연, remote time).
2. 해당 브로커를 격리한다(controlled shutdown). 리더가 이동하고 ISR에서 빠져 acks=all이 정상 브로커만 기다린다. min.insync.replicas=2는 유지된다.
3. 원인(GC 설정, 호스트 자원)을 조치한 뒤 재투입하고 preferred leader election을 실행한다.

## 해로운 조치

- buffer.memory만 늘린다: 증상을 늦출 뿐이고 지연이 계속되면 다시 고갈된다.
- acks를 1로 낮춘다: 내구성을 포기한다. 일시 조치로 쓰려면 위험을 명시해야 한다.
- 컨슈머를 건드린다: 문제는 생산 경로에 있다.
MD
}
