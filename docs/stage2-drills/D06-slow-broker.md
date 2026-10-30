# D06. 느린 브로커: 죽지 않은 브로커가 전체를 늦춘다

## 목적

죽은 브로커보다 진단이 어려운 "살아 있지만 느린 브로커"를 찾는다. `acks=all`에서 복제본 하나의 지연이 다른 브로커의 produce 지연으로 전파되는 경로를 요청 시간 분해(queue, local, remote)로 확인한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | broker-1이 긴 GC 정지나 호스트 정체(CPU steal, 스왑)로 주기적으로 멈춘다. 프로세스는 살아 있다. |
| 주입 | broker-1 컨테이너를 1.2초 정지, 0.4초 실행으로 반복(`docker pause`/`unpause`) |
| 예상 증상 | 프로듀서 요청 지연이 평소의 2~3배로 오르고, 정상 브로커의 Produce remote time p99가 수백 ms로 오른다. 모든 브로커가 up이고 URP는 0이다. |
| 모범 조치 | 느린 브로커를 격리(정지)한다. |
| 소요 | 20분 |

## 진행 방식

```bash
./scripts/drill.sh start D06
```

1. Overview에서 Producer request latency와 Producer send / ack / fail / skip을 본다. Brokers up은 3인지 확인한다.
2. Overview의 Produce p99 by broker와 Cluster의 Produce p99 breakdown(remote / local / queue)을 브로커별로 비교한다.
3. Clients의 Producer node latency와 Cluster의 scrape 지표(브로커별 `scrape_duration_seconds`)를 본다.
4. 느린 브로커가 어느 쪽인지 판정한다. remote time이 오른 브로커와 문제 브로커가 같은지 생각한다.
5. 조치한다: `docker stop broker-1`(controlled shutdown).
6. `drill.sh verify`, `drill.sh stop`(정리 단계에서 broker-1 재기동과 preferred election을 한다).

확인할 것:

- remote time이 broker-1이 아닌 브로커에서 오르는 이유
- 정지가 1.2초씩 반복되는데도 broker-1이 fence되지도 ISR에서 빠지지도 않는 이유. 정지가 얼마나 길어야 각각 일어나는가
- 정지 대신 리더만 옮기는 방법과 그 한계

---

## 해설

### 메커니즘

`acks=all` produce 요청은 리더가 로컬에 쓴 뒤, ISR의 모든 복제본이 그 오프셋까지 가져갈 때까지 purgatory에서 기다린다. 이 대기가 요청 시간의 remote time이다. broker-1이 팔로워인 파티션에서는 broker-1의 fetch가 늦으므로, 리더인 broker-2와 broker-3의 remote time이 오른다. broker-1이 리더인 파티션에서는 클라이언트와 broker-1 사이 왕복 자체가 늦다.

broker-1은 멈췄다 깨어나면 바로 fetch해 따라잡는다. 정지 시간(1.2초)이 `replica.lag.time.max.ms`(30초)보다 훨씬 짧으므로 ISR에서 빠지지 않고, `broker.session.timeout.ms`(9초)보다 짧으므로 컨트롤러도 fence하지 않는다. 지연만 계속 전파한다. 장애가 자동으로 격리되지 않는 이유다.

broker-1 자신의 remote time은 낮다. broker-1이 리더인 파티션의 팔로워(broker-2, broker-3)는 정상이기 때문이다. "remote time이 높은 브로커"가 아니라 "그 브로커들이 공통으로 기다리는 복제본"이 범인이다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | 프로듀서 request latency 상승, 브로커 3대 up | 성능 장애 |
| 범위 | Produce p99 breakdown의 remote가 정상 브로커에서 상승 | 복제 대기가 원인 |
| 원인 | remote time이 broker-1을 뺀 두 브로커에서만 높음, broker-1의 scrape duration이 불규칙 | broker-1이 느린 팔로워 |
| 확인 | broker-1의 GC 로그, 호스트 CPU steal·스왑 지표. 이 랩에서는 주입 기록이 정답이다 | 원인 확정 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 느린 브로커 정지 | ISR에서 빠져 지연이 사라진다 | 내구성 여유가 1 줄어든다(D05 상태) |
| 리더만 다른 브로커로 이동 | broker-1 리더 파티션의 지연 해소 | 팔로워 복제 대기는 남는다 |
| `acks=1`로 변경 | 지연 해소 | 유실 위험. 장애 중 설정 변경은 하지 않는다 |
| `replica.lag.time.max.ms` 축소 | 느린 복제본이 빨리 빠진다 | 일시 지연에도 ISR이 흔들린다 |

### 코드 근거 (4.1.2)

- `Partition.checkEnoughReplicasReachOffset()`(`core/.../cluster/Partition.scala:1089`): `acks=all` 완료 조건. 모든 ISR이 오프셋에 도달해야 한다.
- `Partition.getOutOfSyncReplicas()`(1297): `replica.lag.time.max.ms` 동안 따라오지 못한 복제본만 ISR에서 제거한다.
- `RequestChannel.scala:217`: `apiRemoteTimeMs` 계산. 요청 시간 분해 메트릭의 근거다.

### 재발 방지

- 알람: 브로커별 `kafka_network_requestmetrics_remotetimems{request="Produce",quantile="0.99"}`가 평소의 5배 이상 5분 지속
- 브로커 JVM GC 정지 시간과 호스트 CPU steal, 스왑 지표를 같은 대시보드에 둔다.

### 재현 충실도

`docker pause`는 cgroup freezer로 프로세스 전체를 멈춘다. JVM GC 메트릭에는 정지가 기록되지 않으므로 GC 패널로 원인을 확정할 수는 없다. 실무에서는 GC 정지, 디스크 지연, 네트워크 지연이 같은 증상을 만들며, 진단 경로(remote time 분해로 느린 복제본 특정)는 같다. Docker Desktop 커널은 tc qdisc를 지원하지 않아 네트워크 지연(netem)은 쓰지 않는다.
