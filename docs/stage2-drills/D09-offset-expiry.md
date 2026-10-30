# D09. 오프셋 만료 후 latest 리셋으로 인한 조용한 유실

## 목적

오류도 lag도 남기지 않는 유실을 찾는다. 컨슈머 그룹이 오래 비어 있으면 커밋 오프셋이 만료되고, 다시 시작한 컨슈머가 `auto.offset.reset=latest`로 중간 데이터를 건너뛴다. 대시보드가 정상으로 보이는 장애를 처리 기록과 시간으로 추적하고, 시간 기준 오프셋 리셋으로 재처리한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 배포 문제로 컨슈머가 한동안 모두 내려가 있다가 다시 올라왔다. |
| 주입 | 컨슈머 전체를 150초 정지 후 재기동. 축척을 위해 `offsets.retention.minutes=1`, 만료 검사 주기 15초로 브로커를 다시 만든다. |
| 예상 증상 | 재기동 후 lag은 0에 가깝다. 정지 기간에 유입된 레코드는 처리되지 않았다. |
| 모범 조치 | 컨슈머를 내리고 정지 시작 시각으로 오프셋을 리셋해 재처리한다. |
| 소요 | 25분 |

## 진행 방식

```bash
./scripts/drill.sh start D09     # 브로커 재생성과 150초 정지가 포함돼 약 5분 걸린다
```

1. 재기동 후 Overview의 Committed lag을 본다. 정지 기간에 쌓였어야 할 lag이 어디로 갔는지 생각한다.
2. Cluster의 그룹 코디네이터 행에서 Offset commit / expiration rate를 본다.
3. 컨슈머 이벤트에서 OFFSET_JUMP를 찾는다: `grep OFFSET_JUMP results/runs/<RUN_ID>/events-consumer-*.csv`.
4. 유실 규모를 계산한다: `python3 scripts/analyze.py gaps results/runs/<RUN_ID>`.
5. 재처리한다. 리셋 기준 시각은 컨슈머가 내려가기 시작한 시각(`markers.csv`의 PAUSE_BEGIN, epoch ms를 UTC로 변환)이다. 정지가 끝난 시각을 쓰면 마지막 커밋과 정지 사이의 레코드가 빠진다.

```bash
docker compose --profile lab stop consumer-1 consumer-2 consumer-3
docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 \
  --group <GROUP_ID> --reset-offsets --all-topics --to-datetime 2026-10-30T03:00:00.000 --dry-run   # 확인 후 --execute
docker compose --profile lab start consumer-1 consumer-2 consumer-3
```

6. lag이 다 빠질 때까지 기다린 뒤 `drill.sh verify`로 구멍이 사라졌는지 확인하고 `drill.sh stop`(브로커를 기본 설정으로 다시 만든다).

확인할 것:

- 이 장애를 Committed lag 대시보드만으로 감지할 수 있는가
- `--to-datetime` 리셋이 만드는 중복의 범위
- 운영 기본값(7일)에서 같은 일이 일어나는 조건

---

## 해설

### 메커니즘

그룹 코디네이터는 `offsets.retention.check.interval.ms` 주기로 만료 대상 오프셋을 지운다. 4.1.2 기준 만료는 그룹이 구독하지 않는 토픽의 오프셋에만 적용되며, 그룹이 비어 있으면 구독 토픽이 없으므로 모든 오프셋이 대상이 된다. 기준 시각은 프로토콜마다 다르다.

| 그룹 유형 | 만료 기준 |
|---|---|
| consumer 프로토콜(ConsumerGroup) | 마지막 커밋 시각 + `offsets.retention.minutes` |
| classic(ClassicGroup), Empty 상태 | 그룹이 Empty가 된 시각 + `offsets.retention.minutes` |

오프셋이 지워진 뒤 다시 합류한 컨슈머는 커밋 오프셋이 없으므로 `auto.offset.reset`을 따른다. `latest`이면 현재 LEO부터 읽는다. 정지 기간의 레코드는 브로커에 그대로 있지만 아무도 읽지 않는다. lag은 0부터 다시 계산되므로 대시보드에는 이상이 없다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | 컨슈머 재기동 직후 lag이 예상보다 작음, OFFSET_JUMP 이벤트 | 오프셋 위치 이상 |
| 범위 | `analyze.py gaps`의 total_missing | 유실 규모 확정 |
| 원인 | Offset commit / expiration rate의 expiration 증가, 정지 시간 > retention | 오프셋 만료 |
| 확인 | 재기동 직후 `--describe`의 CURRENT-OFFSET이 정지 직전 값보다 훨씬 큼 | latest 리셋 확정 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| `--to-datetime <정지 시작>` | 유실 구간 재처리 | 정지 직전 커밋 이후~정지 시작 사이 레코드가 중복 처리된다. 처리가 멱등이어야 한다 |
| `--to-offset`(파티션별 정지 직전 커밋값) | 중복 최소 | 정지 직전 값을 따로 기록해 뒀어야 한다 |
| 아무것도 하지 않음 | 없음 | 유실 확정. retention이 지나면 복구 불가 |

### 코드 근거 (4.1.2)

- `OffsetMetadataManager.cleanupExpiredOffsets()`(`group-coordinator/.../OffsetMetadataManager.java:1022`): 구독하지 않는 토픽의 오프셋만 만료 대상으로 본다.
- `ConsumerGroup.offsetExpirationCondition()`(`ConsumerGroup.java:812`): 커밋 시각 기준
- `ClassicGroup.offsetExpirationCondition()`(`ClassicGroup.java:949`): Empty 전이 시각 기준
- `GroupCoordinatorShard.java:1005`: 주기적 만료 작업 등록
- `SubscriptionState.resetInitializingPositions()`(`clients/.../consumer/internals/SubscriptionState.java:869`): 커밋 오프셋이 없는 파티션의 위치를 리셋 정책으로 초기화한다.

### 재발 방지

- 알람: 그룹 멤버 수 0이 `offsets.retention.minutes`의 절반 이상 지속
- `auto.offset.reset=earliest`로 바꾸면 유실 대신 중복이 된다. 처리 멱등성과 함께 정한다.
- 장기 중단이 예정된 경우 중단 직전 오프셋을 저장해 둔다(`--describe` 출력 보관).

### 재현 충실도

보존 시간을 1분으로 줄였다. 운영 기본값 7일에서는 장기 중단된 배치 컨슈머, 폐기 후 재사용한 그룹 ID에서 같은 일이 생긴다.
