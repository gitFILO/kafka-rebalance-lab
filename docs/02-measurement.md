# 02. 측정 방법

결과표의 네 가지 측정 항목(리밸런싱 시간, 정지된 파티션 수, lag 피크, 중복 건수)을 어떻게 정의하고 계산하는지 기술한다. 숫자를 해석하기 전에 정의와 한계를 먼저 확인한다.

## 1. 관찰 구간

모든 지표는 `markers.csv`의 `TRIGGER`부터 `OBSERVE_END`까지를 관찰 구간으로 계산한다. 중복 건수만 run 전체 값과 구간 값을 함께 낸다.

```
그룹 시작 → 안정화 대기 → WARMUP_SEC → WARMUP_DONE → TRIGGER(시나리오 동작) → OBSERVE_SEC → OBSERVE_END
```

## 2. 측정 항목

### 리밸런싱 시간

한 값으로는 설명이 안 되므로 관점별로 세 값을 낸다.

| 지표 | 출처 | 정의 |
|---|---|---|
| 그룹 비안정 누적 | `group.csv` (브로커 관측) | 구간 안에서 그룹 state가 `Stable`이 아닌 샘플 간격의 합. classic은 `PreparingRebalance`/`CompletingRebalance`, consumer는 `Assigning`/`Reconciling`이 해당한다. |
| 멤버별 latency-total 증가분 최대 | `metrics-*.csv` (클라이언트 관측) | `rebalance-latency-total`의 구간 내 증가분 중 최댓값. 한 멤버가 리밸런싱에 쓴 누적 시간이다. |
| 1회 평균 latency | `metrics-*.csv` | 모든 멤버의 latency-total 증가분 합 / `rebalance-total` 증가분 합 |

그룹 비안정 누적과 클라이언트 latency는 크게 다를 수 있다. 검증 run(cooperative × kill9)에서 그룹은 약 2초 동안 `PreparingRebalance`였지만 멤버의 latency-total 증가분은 25ms였다. classic 클라이언트는 JoinGroup을 보낸 시점부터 재기 때문에, 하트비트 응답(`REBALANCE_IN_PROGRESS`)으로 리밸런싱을 알아차리기까지의 대기(최대 `heartbeat.interval.ms`)는 클라이언트 값에 들어가지 않는다.

보조 지표로 트리거부터 첫 콜백·마지막 콜백까지의 시간, 멤버당 리밸런싱 횟수, revoke/assign/lost된 파티션 수를 함께 낸다. kill -9에서는 "트리거→첫 콜백"이 곧 장애 감지 시간(세션 타임아웃)이다.

`rebalance-latency-*`는 두 프로토콜에서 이름이 같지만 측정 구간이 다르다. 표의 숫자를 직접 비교하기 전에 아래 코드를 확인한다.

- classic: `AbstractCoordinator`가 JoinGroup을 보내기 시작한 시각부터 SyncGroup 응답을 받은 시각까지 기록한다(`lastRebalanceStartMs`, `successfulRebalanceSensor.record`).
- consumer: `AbstractMembershipManager.transitionTo`에서 멤버 상태가 리밸런싱 시작 상태로 바뀐 시각부터 완료 상태로 바뀐 시각까지 기록한다(`isStartingRebalance`, `isCompletingRebalance`).

### 정지된 파티션 수

파티션마다 처리 기록(`processed-*.csv`)의 시각을 정렬하고, 연속된 두 처리 사이 공백이 1000ms를 넘으면 그 파티션을 "정지"로 센다. 구간 끝까지 처리가 없으면 끝까지를 공백으로 본다.

- 파티션당 유입이 초당 50건이므로 정상 상태의 공백은 수십 ms다. 1초를 넘는 공백은 소유권 공백, 처리 중단, backlog 밀림 중 하나다.
- 정지 파티션을 **재할당 / 비재할당**으로 나눈다. 재할당은 구간 안에서 `ASSIGNED` 콜백에 등장한 파티션이다.
  - eager는 리밸런싱마다 모든 파티션이 revoke 후 다시 assign되므로 모두 재할당으로 잡힌다.
  - cooperative, consumer에서 비재할당 정지가 나오면 리밸런싱이 원인이 아니다. 대개 같은 컨슈머가 새로 받은 파티션의 backlog를 처리하느라 기존 파티션 처리가 밀린 것이다. `pause_by_partition_ms`와 이벤트 로그로 확인한다.
- 최대 정지 시간과 정지 파티션·초 합(공백의 합)도 함께 낸다.

### lag 피크

`lag.csv`의 합계 lag(로그 끝 오프셋 − 커밋 오프셋, 파티션 12개 합)을 쓴다.

| 지표 | 정의 |
|---|---|
| 기준값 | 트리거 직전 10초 합계 lag의 중앙값 |
| 피크 | 구간 내 최댓값 |
| 회복 시점 | 피크 이후 합계 lag이 `max(2 × 기준값, 100)` 이하로 처음 내려온 시각(트리거 기준) |

커밋 오프셋 기준이므로 커밋이 실패하거나 지연되는 동안에는 실제 처리 위치보다 lag이 크게 보인다. 커밋 실패 이벤트 수와 함께 읽는다.

### 중복 건수

같은 (파티션, 오프셋)이 처리 기록에 두 번 이상 나타난 횟수의 합이다. 처리 기록은 커밋 전에 flush하므로 "처리했지만 커밋하지 못한 레코드"를 다른 멤버가 다시 처리하면 중복으로 잡힌다.

- kill -9: 마지막 flush 이후 기록되지 않은 처리분은 "처리하지 않은 것"으로 본다. 컨테이너가 죽기 직전 버퍼는 사라지므로 이 정의가 외부 부수 효과(DB 쓰기 등)가 없는 처리와 일치한다.
- 누락 오프셋은 파티션별 처리된 최소~최대 오프셋 사이에 빠진 개수다. 이 실험 설계에서는 0이어야 한다. 0이 아니면 커밋 로직을 의심한다.

## 3. 측정의 한계

| 한계 | 영향 | 대응 |
|---|---|---|
| lag-monitor 샘플링 1초 | 1초보다 짧은 비안정 상태를 놓친다. cooperative의 짧은 리밸런싱은 0으로 나올 수 있다. | 클라이언트 지표(latency-total)와 함께 본다. `SAMPLE_MS=200`으로 줄일 수 있다. |
| 메트릭 덤프 5초 | latency-total 증가분이 구간 경계에서 최대 5초 밀린다. | 구간 앞뒤 5초를 포함해 계산한다. |
| 마커는 호스트 시계, 이벤트는 컨테이너 시계 | Docker Desktop VM 시계와 호스트 시계 차이(보통 수 ms)가 섞인다. | 초 단위 지표만 해석한다. |
| 재시작된 멤버의 카운터 초기화 | 새 프로세스의 메트릭은 0부터 시작한다. | 감소가 보이면 새 값 전체를 증가분으로 더한다. |
| 1회 측정 | 하트비트 주기, 코디네이터 위치에 따라 편차가 있다. | 셀마다 2~3회 반복하고 중앙값을 쓴다. |

## 4. 수동 관측 도구

자동 기록 외에 실험 중 직접 확인할 때 쓰는 명령이다.

### kafka-consumer-groups.sh

```bash
G=lab-<RUN_ID>   # run 시작 시 출력되는 group 값
docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group $G --state
docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group $G --members --verbose
docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --list --type
```

- `--state`: classic은 `ASSIGNMENT-STRATEGY`에 클라이언트 할당자 이름, consumer는 서버 할당자 이름과 `GROUP-EPOCH`, `TARGET-ASSIGNMENT-EPOCH`가 나온다.
- `--members --verbose`: `CURRENT-EPOCH`, `CURRENT-ASSIGNMENT`, `TARGET-EPOCH`, `TARGET-ASSIGNMENT` 열이 있고 classic 그룹에서는 `-`로 나온다. consumer 그룹에서 classic 멤버가 섞이면 `UPGRADED` 열이 추가된다. 현재 할당과 목표 할당이 다르면 조정(reconciliation) 중이다.
- `--list --type`: 그룹마다 Classic/Consumer 타입을 함께 출력한다.

### JMX

```bash
# 호스트에서 GUI로 접속
jconsole localhost:9103

# CLI로 조회. 컨슈머 컨테이너의 네트워크 네임스페이스를 공유해 localhost로 접속한다.
docker run --rm --network container:consumer-3 apache/kafka:4.1.2 \
  /opt/kafka/bin/kafka-jmx.sh --one-time \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9103/jmxrmi \
  --object-name 'kafka.consumer:type=consumer-coordinator-metrics,client-id=consumer-3'
```

주로 볼 속성은 `rebalance-latency-avg/max/total`, `rebalance-total`, `failed-rebalance-total`, `last-rebalance-seconds-ago`, `assigned-partitions`, `partition-revoked-latency-avg`, `partition-assigned-latency-avg`, `partition-lost-latency-avg`다. consumer 프로토콜에서는 `kafka.consumer:type=consumer-metrics`의 `time-between-network-thread-poll-avg`, `application-event-queue-size` 같은 스레드 모델 관련 메트릭이 추가된다.

### 브로커 로그

```bash
docker logs broker-2 2>&1 | grep -F "$G"
```

그룹 코디네이터는 `__consumer_offsets`의 해당 파티션 리더 브로커에 있다. 로그 접두어 `[GroupCoordinator id=N topic=__consumer_offsets partition=P]`로 어느 브로커인지 확인한다. classic은 `Preparing to rebalance group`, `Stabilized group`, consumer는 `[GroupId ...]` 접두어 메시지를 본다. run 종료 시 `broker-group-broker-N.log`로 자동 수집된다.
