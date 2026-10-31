# D14. 컨트롤러 쿼럼 상실

## 목적

KRaft 컨트롤러 쿼럼이 사라졌을 때 무엇이 계속 동작하고 무엇이 멈추는지 확인한다. 쿼럼 상실 자체는 바로 장애로 보이지 않다가 다른 장애가 겹치는 순간 클러스터 전체를 멈춘다. 이 지연된 영향을 이해하고, 복구 순서(쿼럼 먼저)를 판단한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 같은 하이퍼바이저에 몰려 있던 컨트롤러 2대가 함께 내려갔다. 1분 뒤 broker-2가 비정상 종료됐다. |
| 주입 | 활성 컨트롤러와 다른 컨트롤러 1대 SIGKILL, 60초 뒤 broker-2 SIGKILL |
| 예상 증상 | 처음 60초는 생산·소비가 정상이다. broker-2가 죽은 뒤 생산이 0으로 떨어지고 복구되지 않는다. 그룹이 불안정해진다. 토픽 생성이 시간 초과로 실패한다. |
| 모범 조치 | 컨트롤러를 먼저 살려 쿼럼을 복구하고, 그다음 broker-2를 복구한다. |
| 소요 | 20분 |

## 진행 방식

```bash
./scripts/drill.sh start D14
```

1. 알람 순서를 기록한다(`./scripts/alerts.sh history 10m`). 처음 울린 알람과 사용자 영향이 시작된 시각이 다르다.
2. Overview의 Active controller, Brokers up, Producer send / ack / fail / skip을 본다.
3. Cluster의 "KRaft 컨트롤러" 행을 본다. Raft current leader, Raft epoch가 어떻게 움직이는가.
4. 브로커 쪽에서 쿼럼 상태를 확인한다:
   ```bash
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server broker-1:9092 describe --status
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-1:9092 --describe --topic lab-events
   ```
5. broker-2가 죽은 뒤에도 파티션 리더와 ISR이 바뀌지 않는 것을 확인한다. 왜 그런가.
6. 조치 순서를 정하고 실행한다.
7. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- 쿼럼이 없는 60초 동안 생산이 정상이었던 이유
- broker-2가 리더가 아닌 파티션까지 쓰기가 멈춘 이유
- broker-2를 먼저 살리면 어떻게 되는가. 컨트롤러를 먼저 살리면 어떻게 되는가

---

## 해설

### 메커니즘

KRaft에서 메타데이터 변경은 모두 컨트롤러 쿼럼의 로그에 기록돼야 효력이 있다. 리더 선출, ISR 축소·확장, 브로커 펜싱, 토픽 생성, 설정 변경이 여기에 속한다. 쿼럼(3대 중 2대)이 없으면 이 변경이 하나도 일어나지 않는다.

이미 리더가 정해진 파티션의 읽기·쓰기는 메타데이터 변경이 필요 없다. 그래서 컨트롤러만 죽은 동안은 아무 일도 없어 보인다. 실측에서 이 60초 동안 생산은 600 msg/s 그대로였다.

broker-2가 죽으면 두 가지가 동시에 막힌다.

1. broker-2가 리더였던 파티션: 새 리더를 뽑을 컨트롤러가 없다. 리더 없는 상태로 남는다. 메타데이터에는 여전히 broker-2가 리더로 보인다.
2. broker-2가 팔로워인 파티션: 리더는 broker-2가 따라오지 않는 것을 감지하고 ISR에서 빼려 한다(`replica.lag.time.max.ms`). 하지만 ISR 축소는 컨트롤러에 `AlterPartition`을 보내 승인받아야 확정된다. 승인 전까지 리더는 "최대 ISR"을 기존 ISR(broker-2 포함)로 유지하고, `acks=all` 쓰기는 broker-2의 확인을 기다린다. 오지 않으므로 요청이 시간 초과된다.

RF 3, 브로커 3대라 모든 파티션이 둘 중 하나에 해당한다. 그래서 생산 전체가 멈춘다. 그룹 코디네이터가 broker-2에 있던 그룹은 코디네이터 이동(`__consumer_offsets` 리더 선출)도 못 해 불안정해진다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | `NoActiveController`(page), `ControllerQuorumDegraded` | 쿼럼 상실. 이 시점에는 사용자 영향이 없다 |
| 영향 시작 | `BrokerDown`, 이어서 acked 0, `ConsumerGroupUnstable`, `ProducerThroughputDrop` | 두 번째 장애가 겹쳤다 |
| 범위 | 리더·ISR이 broker-2가 죽은 뒤에도 바뀌지 않음. Offline partitions 지표도 갱신되지 않는다(계산할 컨트롤러가 없다) | 메타데이터가 얼어 있다 |
| 확인 | `describe --status` 실패 또는 LeaderId 없음, 토픽 생성 시간 초과 | 쿼럼이 원인이다 |

실측 타임라인(주입 기준): 컨트롤러 중단 0초 → `NoActiveController` 30초 → broker-2 중단 61초 → `BrokerDown` 100초 → 생산 0 약 105초 → `ConsumerGroupUnstable` 135초 → `ProducerThroughputDrop` 140초. 조치 후 15초 안에 생산이 돌아왔다.

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 컨트롤러 복구 → broker-2 복구 | 쿼럼이 돌아오는 즉시 컨트롤러가 broker-2를 펜싱하고 리더를 다시 뽑고 ISR을 줄인다. broker-2 없이도 생산이 재개된다 | 컨트롤러 데이터 디렉터리가 살아 있어야 한다 |
| broker-2만 먼저 복구 | 효과 없다 | 브로커는 시작할 때 컨트롤러가 "따라잡았다"고 확인해 줄 때까지 서비스를 시작하지 않는다. 쿼럼이 없으면 계속 기다린다 |
| 살아 있는 브로커 재시작 | 상황이 나빠진다 | 재시작한 브로커도 같은 이유로 돌아오지 못한다. 남아 있던 리더까지 잃는다 |
| 컨트롤러 1대로 쿼럼 재구성 | 최후 수단 | 이 레포는 `controller.quorum.voters`로 voter를 고정했다(정적 쿼럼). 동적 쿼럼(`kraft.version=1`)이어도 voter 제거·추가는 쿼럼 리더를 거쳐야 하므로 쿼럼이 없으면 할 수 없다. 남는 길은 메타데이터 유실을 감수하는 수동 복구뿐이다 |

### 코드 근거 (4.1.2)

- ISR 축소 대기: `Partition.maybeShrinkIsr()`(`core/src/main/scala/kafka/cluster/Partition.scala:1231`)는 `prepareIsrShrink()`(1774행)로 `PendingShrinkIsr` 상태를 만들고 컨트롤러에 제출한다. 이 상태의 최대 ISR은 축소 전 ISR이다(1780행 주석).
- `acks=all` 판정: `Partition.checkEnoughReplicasReachOffset()`(1089행)은 `partitionState.maximalIsr`(1092행)로 확인할 복제본을 고른다. 축소가 확정되기 전에는 죽은 복제본도 기다린다.
- 브로커 시작: `BrokerServer.startup()`(`core/src/main/scala/kafka/server/BrokerServer.scala:549`)은 활성 컨트롤러가 하트비트 응답으로 "따라잡음"을 확인해 줄 때까지 기다린다(`initialCatchUpFuture`). 기한은 `server.max.startup.time.ms`(기본 무제한)다.

### 재발 방지

- 알람: `NoActiveController`를 page로 둔다. 사용자 영향이 없어도 다음 장애 하나로 전체가 멈추는 상태라 즉시 대응 대상이다.
- 컨트롤러는 서로 다른 장애 도메인(호스트, 랙, 가용 영역)에 둔다. 3대보다 5대가 2대 동시 장애를 견딘다.
- 운영 작업(브로커 롤링 재시작, 재배치)을 시작하기 전에 쿼럼 상태를 확인하는 단계를 둔다.

### 재현 충실도

컨트롤러 2대를 동시에 SIGKILL했다. 실제 하이퍼바이저 장애와 같지만 네트워크 분할(컨트롤러는 살아 있지만 서로 못 봄)은 다루지 않는다. 분할에서는 소수 쪽 컨트롤러가 계속 선거를 시도하며 Raft epoch가 올라가는 모습이 보인다.
