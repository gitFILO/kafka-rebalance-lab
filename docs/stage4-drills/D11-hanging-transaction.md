# D11. 열린 채 남은 트랜잭션이 LSO를 막음

## 목적

트랜잭션 프로듀서가 비정상 종료되면 열린 트랜잭션이 남고, `read_committed` 컨슈머는 그 지점(LSO) 너머를 읽지 못한다. 프로듀서는 정상이고 컨슈머도 오류가 없는데 lag만 늘어나는 상황을 HW와 LSO의 차이로 진단하고, `kafka-transactions.sh`로 해당 트랜잭션을 중단(abort)한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 트랜잭션 프로듀서가 OOM으로 강제 종료됐다. 재기동한 인스턴스는 새 `transactional.id`를 쓴다. |
| 주입 | 트랜잭션 모드 프로듀서(`transaction.timeout.ms`=15분)를 `kill -9` 후 재기동. 컨슈머는 `read_committed` |
| 예상 증상 | 새 프로듀서는 정상 전송한다. 컨슈머 lag이 계속 늘어난다. |
| 모범 조치 | 파티션마다 LSO를 붙잡은 트랜잭션을 찾아 abort한다. |
| 소요 | 25분 |

## 진행 방식

```bash
./scripts/drill.sh start D11
```

1. Overview의 Committed lag과 Consumer processed를 본다. 컨슈머가 멈췄는가, 읽을 것이 없는가.
2. Clients의 HW - LSO by partition을 본다.
3. 열린 트랜잭션을 찾는다.

```bash
docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-transactions.sh --bootstrap-server broker-1:9092 \
  find-hanging --broker-id 4
docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-transactions.sh --bootstrap-server broker-1:9092 \
  describe-producers --topic lab-events --partition 0
```

4. `describe-producers` 출력에서 CurrentTransactionStartOffset이 있는 producer를 찾는다. 같은 파티션에 여럿이면 가장 작은 값이 LSO를 붙잡은 트랜잭션이다(새 프로듀서의 진행 중 트랜잭션도 보인다).
5. 파티션별로 abort한다.

```bash
docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-transactions.sh --bootstrap-server broker-1:9092 \
  abort --topic lab-events --partition 0 --start-offset <START_OFFSET>
```

6. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- 아무 조치도 하지 않으면 언제 스스로 풀리는가
- 같은 `transactional.id`로 재기동했다면 무엇이 달라지는가
- `find-hanging`이 이 트랜잭션을 보여 주지 않는 경우가 있다면 이유는 무엇인가

---

## 해설

### 메커니즘

트랜잭션 레코드는 커밋이나 중단 마커가 쓰이기 전까지 미완료 상태다. 파티션의 LSO(last stable offset)는 열린 트랜잭션 중 가장 이른 시작 오프셋이다. `read_committed` 컨슈머는 LSO까지만 받으므로, LSO 뒤에 쌓인 커밋된 레코드도 읽지 못한다.

프로듀서가 죽으면 트랜잭션 코디네이터는 `transaction.timeout.ms`가 지난 뒤에 트랜잭션을 중단한다. 이 훈련은 15분이라 그동안 LSO가 고정된다. 같은 `transactional.id`로 재기동했다면 `initTransactions()`가 epoch를 올리며 이전 트랜잭션을 즉시 중단했겠지만, 재기동한 인스턴스가 새 id를 쓰므로 그런 정리가 없다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | lag 증가, 컨슈머 오류 없음, 처리량 감소 | 컨슈머가 읽을 데이터를 받지 못한다 |
| 범위 | HW - LSO가 모든 파티션에서 증가 | LSO 고정 |
| 원인 | `describe-producers`의 오래된 CurrentTransactionStartOffset, 그 producer id가 현재 프로듀서가 아님 | 고아 트랜잭션 |
| 확인 | 해당 트랜잭션 abort 후 LSO가 HW를 따라감 | 원인 확정 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| `kafka-transactions.sh abort` | LSO 즉시 진행 | 그 트랜잭션의 레코드는 버려진다(이미 실패한 트랜잭션이므로 정상) |
| `transaction.timeout.ms`까지 대기 | 자동 해소 | 그동안 지연. 기본 60초, 최대 15분 |
| 같은 `transactional.id`로 프로듀서 재기동 | 이전 트랜잭션 fence | 인스턴스별 고정 id 설계가 전제다 |
| 컨슈머를 `read_uncommitted`로 변경 | lag 해소 | 중단될 레코드까지 읽는다. 하지 않는다 |

### 코드 근거 (4.1.2)

- `UnifiedLog.firstUnstableOffset()`(`storage/.../log/UnifiedLog.java:642`): 열린 트랜잭션 중 가장 이른 시작 오프셋. LSO의 근거다.
- `TransactionsCommand`(`tools/.../TransactionsCommand.java`): `AbortTransactionCommand`(97), `DescribeProducersCommand`(293), `FindHangingTransactionsCommand`(529)

### 재발 방지

- 알람: 파티션별 `lab_partition_logendoffset - lab_partition_laststableoffset`이 평소 트랜잭션 크기의 10배 이상 5분 지속
- `transactional.id`를 인스턴스 정체성(예: StatefulSet 순번)에 묶어 재기동 시 같은 id를 쓰게 한다.
- `transaction.timeout.ms`는 실제 트랜잭션 최대 길이에 맞춰 짧게 둔다.

### 재현 충실도

`find-hanging`은 브로커가 트랜잭션 코디네이터 상태와 대조해 판단하므로, 타임아웃이 남은 트랜잭션은 "hanging"이 아니라 정상 진행 중으로 볼 수 있다. 그래서 이 훈련의 모범 경로는 `describe-producers`다.
