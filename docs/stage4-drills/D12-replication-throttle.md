# D12. 재배치 후 남은 복제 throttle

## 목적

파티션 재배치에 쓰는 복제 throttle이 정리되지 않고 남았을 때 일어나는 일을 확인한다. 브로커가 모두 살아 있는데 URP가 끝나지 않는 상황에서 "따라잡는 중"과 "따라잡을 수 없음"을 구분하고, 재배치 절차에서 `--verify`가 왜 필수인지 이해한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 어제 broker-3 쪽으로 복제본을 옮기며 `--throttle`을 걸었고, 끝난 뒤 `--verify`를 하지 않았다. 오늘 정기 패치로 broker-3을 재시작했다. |
| 주입 | 모든 브로커에 복제 속도 10KB/s, `lab-events`의 throttled replicas에 broker-3 지정. broker-3 45초 정지 후 시작 |
| 예상 증상 | 브로커 3대가 모두 올라와도 URP가 줄지 않는다. broker-3의 복제 유입이 10KB/s에 붙어 있다. |
| 모범 조치 | 브로커와 토픽의 throttle 설정을 제거한다. |
| 소요 | 20분 |

## 진행 방식

```bash
./scripts/drill.sh start D12
```

1. 알람을 본다(`./scripts/alerts.sh`). broker-3이 내려간 동안 `BrokerDown`이 울리고, 올라온 뒤에도 `UnderReplicatedPartitions`가 남는다.
2. Overview의 Brokers up과 Under-replicated를 본다. 브로커는 3대인데 URP가 남아 있다.
3. Cluster의 "복제와 리더" 행에서 Under-replicated by broker와 Replication bytes in by broker를 본다. broker-3의 복제 유입만 낮고 평평하다.
4. 브로커 로그에서 오류를 찾는다. 오류가 없다는 것도 근거다.
5. 브로커와 토픽의 동적 설정을 확인한다. `--describe`는 동적으로 바꾼 값만 보여 준다.
   ```bash
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker-1:9092 --entity-type brokers --describe
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker-1:9092 --entity-type topics --entity-name lab-events --describe
   ```
6. 조치하고, URP가 줄어드는 속도를 본다.
7. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- URP가 남아 있는데 프로듀서 지연과 처리량은 정상인 이유
- throttle을 지운 뒤 URP가 0이 되기까지 걸린 시간과 그때 broker-3의 복제 유입 속도
- 이 상태에서 broker-1이나 broker-2가 하나 더 죽으면 어떻게 되는가

---

## 해설

### 메커니즘

복제 throttle은 두 설정이 함께 있어야 동작한다. 브로커의 `leader.replication.throttled.rate`, `follower.replication.throttled.rate`가 속도를 정하고, 토픽의 `leader.replication.throttled.replicas`, `follower.replication.throttled.replicas`가 어느 파티션의 어느 복제본에 적용할지 정한다. `kafka-reassign-partitions.sh --throttle`은 이 네 가지를 한꺼번에 설정하고, `--verify`가 재배치 완료를 확인하면서 지운다. `--verify`를 하지 않으면 재배치가 끝나도 그대로 남는다.

throttle은 ISR 밖의 복제본에만 걸린다. 평소에는 broker-3이 ISR 안에 있어서 남은 설정이 아무 영향도 주지 않는다. 이 훈련의 함정은 여기 있다. 설정이 남은 채 몇 주가 지나도 아무 문제가 없다가, broker-3이 한 번 재시작해 ISR에서 빠지는 순간 따라잡기가 10KB/s로 묶인다. 유입이 그보다 빠르면 broker-3은 영원히 ISR로 돌아오지 못한다.

그동안 리더와 남은 팔로워 하나가 ISR을 이루므로 `min.insync.replicas=2`는 만족한다. 쓰기는 정상이다. 다만 복제본 하나가 사실상 없는 상태라 브로커 하나만 더 잃으면 D08(min ISR 미달)이 된다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | `UnderReplicatedPartitions`가 브로커 복구 뒤에도 남는다 | 복제가 따라잡지 못한다 |
| 범위 | broker-3이 팔로워인 파티션만 URP. broker-3의 ReplicationBytesIn이 일정한 낮은 값 | 한 브로커의 복제 경로 |
| 원인 후보 | 디스크·네트워크 병목이면 지연 지표와 로그가 함께 나빠진다. 지표가 깨끗하고 속도가 정확히 일정하면 인위적 제한이다 | 설정 의심 |
| 확인 | 브로커 동적 설정의 `*.replication.throttled.rate`, 토픽의 `*.replication.throttled.replicas` | throttle 잔존 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 브로커·토픽의 throttle 설정 삭제 | 즉시 최대 속도로 따라잡는다 | 따라잡는 동안 리더의 네트워크와 디스크 읽기가 늘어 다른 요청 지연이 오를 수 있다 |
| 속도만 올림(예: 50MB/s) | 따라잡기 속도와 서비스 영향 사이를 조절한다 | 끝난 뒤 다시 지워야 한다 |
| `kafka-reassign-partitions.sh --verify` 재실행 | 재배치 JSON이 남아 있으면 정석 경로로 정리된다 | 재배치 계획 파일이 없으면 쓸 수 없다 |
| broker-3 재시작 | 효과 없다 | 다시 ISR에서 빠질 뿐이다 |

### 재배치 정석 절차

```bash
# 1. 계획 생성 (또는 직접 작성)
kafka-reassign-partitions.sh --bootstrap-server broker-1:9092 --generate \
  --topics-to-move-json-file topics.json --broker-list 4,5,6 > plan.txt
# 2. 실행. 브로커당 복제 속도 상한을 함께 건다
kafka-reassign-partitions.sh --bootstrap-server broker-1:9092 --execute \
  --reassignment-json-file reassign.json --throttle 10485760
# 3. 진행 중 속도 조정(같은 명령에 --additional --throttle)
kafka-reassign-partitions.sh --bootstrap-server broker-1:9092 --execute --additional \
  --reassignment-json-file reassign.json --throttle 52428800
# 4. 완료 확인과 throttle 제거. 완료될 때까지 반복한다
kafka-reassign-partitions.sh --bootstrap-server broker-1:9092 --verify \
  --reassignment-json-file reassign.json
```

`--verify` 출력에 "Clearing broker-level throttles"와 "Clearing topic-level throttles"가 나와야 정리된 것이다. throttle 값은 "평소 유입량 + 여유"보다 커야 재배치가 끝난다. 유입보다 작으면 새 복제본이 영원히 따라잡지 못한다.

### 코드 근거 (4.1.2)

- 리더 쪽: `ReplicaManager.shouldLeaderThrottle()`(`core/src/main/scala/kafka/server/ReplicaManager.scala:1985`)는 팔로워가 ISR 밖이고, 파티션이 throttled 목록에 있고, 쿼터를 넘었을 때만 fetch 응답에서 그 파티션을 뺀다. 1734행에서 팔로워 fetch에 적용한다.
- 팔로워 쪽: `RemoteLeaderEndPoint.shouldFollowerThrottle()`(`core/src/main/scala/kafka/server/RemoteLeaderEndPoint.scala:228`)이 같은 조건으로 fetch 요청에서 파티션을 뺀다(179행).
- 사용량 기록: `ReplicaFetcherThread.processPartitionData()`(`ReplicaFetcherThread.scala:143`)는 ISR 안팎과 무관하게 throttled 파티션의 바이트를 쿼터에 기록한다. 그래서 ISR 안 복제본의 트래픽이 많으면 ISR 밖 복제본에 남는 몫이 더 작아진다.
- 쿼터 판정: `ReplicationQuotaManager.isQuotaExceeded()`, `isThrottled()`(`core/src/main/java/kafka/server/ReplicationQuotaManager.java:83`, `:98`).

### 재발 방지

- 알람: `UnderReplicatedPartitions`(2분). 브로커가 모두 살아 있는데 이 알람이 울리면 throttle을 먼저 의심하는 항목을 런북에 둔다.
- 재배치 작업 체크리스트에 `--verify` 완료와 설정 잔존 확인(`kafka-configs.sh --describe`에 `throttled`가 없음)을 넣는다.
- 주기 점검으로 브로커 동적 설정 중 `*.throttled.*`가 있는지 확인한다.

### 재현 충실도

실제 재배치로 throttle을 만들지 않고 설정을 직접 넣었다. 남는 설정의 모양은 `--throttle`이 만든 것과 같다. 실측 기준으로 throttle 제거 후 URP는 15초 안에 0이 됐다. 브로커가 45초만 내려가 있었기 때문이다. 오래 내려가 있던 브로커라면 따라잡기 트래픽이 리더에 부담이 되므로 속도를 단계적으로 올리는 편이 낫다.
