# D08. min.insync.replicas 미달로 쓰기 거부

## 목적

ISR이 `min.insync.replicas`보다 작아지면 Kafka가 가용성보다 내구성을 택해 쓰기를 거부하는 동작을 확인한다. "브로커 1대는 살아 있는데 왜 쓰기가 안 되는가"에 답하고, 이 상태에서 해서는 안 되는 조치(min ISR 낮추기, unclean election)를 구분한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 랙 하나가 정전돼 broker-1, broker-2가 같이 내려갔다. |
| 주입 | broker-1, broker-2 정지 |
| 예상 증상 | 모든 파티션의 ISR이 1이 된다. 프로듀서 ack가 멈추고 재시도가 늘어난다. 컨슈머는 남은 데이터를 읽는다. |
| 모범 조치 | 브로커를 복구한다. |
| 소요 | 15분 |

## 진행 방식

```bash
./scripts/drill.sh start D08
```

1. Overview의 Brokers up, Under min ISR, Offline partitions를 본다.
2. Overview의 Producer send / ack / fail / skip, Clients의 Record send / error / retry rate와 Buffer available / total을 본다.
3. 브로커 쪽 거부를 확인한다: Cluster의 Failed produce / fetch requests, 브로커 로그의 `NotEnoughReplicas`.
4. 컨슈머 처리량을 본다. 읽기는 왜 계속되는가.
5. 조치한다: `docker start broker-1 broker-2`, URP 0 확인 후 preferred election.
6. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- Offline partitions가 0인데 쓰기가 안 되는 이유
- 프로듀서는 오류를 바로 앱에 돌려주는가, 재시도하는가. `delivery.timeout.ms`가 지나면 어떻게 되는가
- `min.insync.replicas`를 1로 낮추면 쓰기가 재개된다. 그 대가는 무엇인가

---

## 해설

### 메커니즘

리더는 `acks=all` 요청을 받으면 쓰기 전에 현재 ISR 크기를 `min.insync.replicas`와 비교한다. ISR이 1이고 min ISR이 2이면 `NotEnoughReplicasException`으로 거부한다. 이 오류는 재시도 가능 오류라 프로듀서는 `delivery.timeout.ms`(기본 120초)까지 재시도하고, 그동안 레코드는 accumulator에 쌓인다. 시간이 지나면 앱 콜백에 실패가 전달된다.

리더는 살아 있으므로 파티션은 offline이 아니고, fetch(읽기)는 HW까지 정상 동작한다. 쓰기만 막힌다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | ack 처리량 급감, 재시도 증가 | 쓰기 장애 |
| 범위 | Brokers up 1, Under min ISR = 전 파티션, Offline 0 | 내구성 기준 미달 |
| 원인 | 브로커 로그와 Failed produce / fetch requests의 `NotEnoughReplicas` | min ISR 정책에 의한 거부 |
| 확인 | 컨슈머 읽기는 정상 | 리더 장애가 아니다 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 브로커 복구 | 정상화 | 복구 시간 동안 쓰기 중단 |
| 토픽 `min.insync.replicas=1` | 쓰기 재개 | 남은 브로커 1대가 죽으면 ack한 데이터가 유실된다. 복구 후 반드시 되돌려야 한다 |
| 프로듀서 `acks=1` | 쓰기 재개 | 같은 위험. 배포가 필요하다 |
| unclean leader election | 해당 없음(리더 존재) | 리더가 없을 때도 유실을 감수하는 조치다 |

어떤 조치를 택할지는 서비스 결정이다. 쓰기 중단과 유실 위험 중 무엇이 더 비싼지 미리 정해 둬야 장애 중에 판단할 수 있다.

### 코드 근거 (4.1.2)

- `Partition.appendRecordsToLeader()`(`core/.../cluster/Partition.scala:1361`): ISR 크기가 min ISR보다 작고 `requiredAcks == -1`이면 1371행에서 `NotEnoughReplicasException`을 던진다.

### 재발 방지

- 알람: `sum(kafka_server_replicamanager_underminisrpartitioncount) > 0`(즉시)
- 랙 인식 배치(`broker.rack`)로 복제본을 서로 다른 장애 도메인에 둔다. 이 랩은 랙을 나누지 않았다.

### 재현 충실도

실제 랙 장애에서는 컨트롤러 일부도 함께 내려갈 수 있다. 이 훈련은 컨트롤러 쿼럼을 유지한다.
