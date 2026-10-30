# D03. poison pill: 같은 오프셋에서 무한 재시도

## 목적

처리할 수 없는 레코드 1건이 파티션 하나를 무기한 멈추는 상황을 진단하고, 해당 오프셋만 정확히 건너뛰는 운영 조치(오프셋 리셋)를 익힌다. 오프셋 리셋의 전제 조건과 위험을 이해한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 상류에서 스키마가 깨진 레코드 1건이 들어왔다. 앱은 실패한 레코드를 다시 시도하는 정책(seek 후 1초 대기)을 쓴다. |
| 주입 | 파티션 5에 poison 레코드 1건(`control/producer.poison`) |
| 예상 증상 | 파티션 5의 lag만 계속 증가한다. 해당 멤버의 다른 파티션도 느려진다. |
| 모범 조치 | 컨슈머를 내리고 파티션 5의 오프셋을 1 이동한 뒤 재기동한다. |
| 소요 | 20분 |

## 진행 방식

```bash
./scripts/drill.sh start D03
```

1. Overview의 Committed lag by partition에서 어느 파티션이 오르는지 본다.
2. Clients의 Processed per partition에서 그 파티션의 처리량을 본다. 0인가, 느린가.
3. 그 파티션을 누가 소유하는지 찾는다: `docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group <GROUP_ID> --members --verbose` (`GROUP_ID`는 `run.env`).
4. 소유 멤버의 로그와 이벤트에서 반복되는 오프셋을 찾는다: `grep POISON results/runs/<RUN_ID>/events-consumer-*.csv | tail`.
5. 조치한다. 오프셋 리셋은 그룹에 활성 멤버가 없어야 한다.

```bash
docker compose --profile lab stop consumer-1 consumer-2 consumer-3
docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 \
  --group <GROUP_ID> --reset-offsets --topic lab-events:5 --shift-by 1 --dry-run   # 확인 후 --execute
docker compose --profile lab start consumer-1 consumer-2 consumer-3
```

6. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- `--dry-run` 출력의 NEW-OFFSET이 poison 레코드의 다음 오프셋인지 확인했는가.
- 컨슈머를 내리지 않고 리셋하면 어떤 오류가 나는가.
- 건너뛴 레코드는 어떻게 보존·재처리해야 하는가.

---

## 해설

### 메커니즘

앱이 실패한 레코드의 오프셋으로 `seek()`하므로 다음 `poll()`은 같은 레코드를 다시 가져온다. 커밋은 처리에 성공한 레코드까지만 진행되므로 파티션 5의 커밋 오프셋이 고정되고, 유입은 계속되므로 lag이 선형으로 증가한다. 재시도마다 1초를 대기하므로 같은 멤버의 다른 파티션도 처리 속도가 떨어진다. poll 루프 자체는 돌기 때문에 그룹은 정상이다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | 파티션 1개 lag만 선형 증가 | 파티션 고유의 문제(데이터 또는 쏠림) |
| 범위 | Processed per partition에서 그 파티션만 0 | 쏠림(D04)이면 처리량은 상한에 붙고 0이 아니다 |
| 원인 | 이벤트·로그에 같은 오프셋의 처리 실패가 반복 | poison pill |
| 확인 | Poison fail / skipped 패널, `POISON_FAIL ... offset=N` | 오프셋 N 확정 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 해당 파티션만 `--shift-by 1` | 그 레코드만 건너뛴다 | 그룹 전체 중지가 필요하다. 레코드 1건 유실(별도 보관 필요) |
| `--to-offset N+1` | 같은 효과, 명시적 | 오프셋을 잘못 적으면 유실 또는 중복 |
| 앱 정책을 skip/DLQ로 바꿔 재배포 | 재발 방지까지 해결 | 코드·설정 변경이 필요하다 |
| 그룹 전체 `--to-latest` | 빠르다 | 정상 레코드까지 대량 유실. 하지 않는다 |

### 코드 근거 (4.1.2)

- `ConsumerGroupCommand.java:667`: 그룹이 비활성(Empty, Dead)이 아니면 "Assignments can only be reset if the group ... is inactive" 오류로 거부한다.
- 앱 쪽 처리: `lab-app/.../LabConsumer.handlePoison()`. 재시도 정책이 장애 범위를 결정한다.

### 재발 방지

- 알람: 특정 파티션의 커밋 오프셋이 N분간 변하지 않는데 log end offset은 증가(`delta(lab_partition_committedoffset[5m]) == 0 and delta(lab_partition_logendoffset[5m]) > 0`).
- 앱 설계: 재시도 횟수 상한, 소진 시 DLQ 토픽으로 보내고 진행. 역직렬화 실패는 재시도해도 성공하지 않으므로 즉시 DLQ.

### 재현 충실도

실무의 poison pill은 역직렬화 예외로 `poll()` 자체가 실패하는 경우가 많다(`RecordDeserializationException`). 이 훈련은 처리 단계 실패로 단순화했다. 오프셋 리셋 절차는 같다.
