# 알람(Alertmanager)

대시보드는 사람이 보고 있을 때만 장애를 알려 준다. 실무에서 장애 대응은 대부분 알람이 울리면서 시작한다. 이 레포는 모니터링과 함께 알람을 처음부터 켜 두고, Stage 5 블라인드 훈련에서는 알람을 감지 시점의 기준으로 쓴다.

## 목적

- 증상 기준 알람을 직접 설계하고, 임계값과 지속 시간(`for`)이 탐지 속도와 오탐 사이에서 어떤 거래를 하는지 이해한다.
- 알람 하나를 받았을 때 "무엇을 먼저 보는가"를 런북으로 고정한다. 아래 알람별 절이 그 런북이다.
- 같은 장애에서 알람 여러 개가 한꺼번에 울릴 때 원인 쪽과 결과 쪽을 구분한다. Alertmanager 억제(inhibit) 규칙으로 이를 표현한다.
- 훈련(Stage 4)마다 어떤 알람이 울리고 어떤 알람이 침묵하는지 기록해, 알람이 놓치는 장애가 무엇인지 안다.

## 개요

| 구성 요소 | 위치 | 역할 |
|---|---|---|
| 알람 규칙 | [monitoring/prometheus/rules/lab-alerts.yml](../monitoring/prometheus/rules/lab-alerts.yml) | Prometheus가 5초마다 평가한다. 조건이 참이 되면 pending, `for`를 채우면 firing |
| Alertmanager | [monitoring/alertmanager/alertmanager.yml](../monitoring/alertmanager/alertmanager.yml), http://localhost:9093 | 알람을 묶고(group), 억제하고(inhibit), 수신자에게 보낸다. 이 레포는 외부로 보내지 않는다 |
| Grafana | Overview 맨 위 "알람" 행, 모든 대시보드의 빨간 어노테이션 | 지금 울리는 알람 표와 발화 구간 표시 |
| CLI | `scripts/alerts.sh` | 현재 알람, pending 알람, 구간별 발화 이력과 최초 발화 시각 |

```bash
./scripts/alerts.sh              # Alertmanager가 받은 알람(억제 여부 포함)
./scripts/alerts.sh pending      # 조건은 참이지만 for를 채우지 못한 알람
./scripts/alerts.sh history 30m  # 최근 30분 동안 발화한 알람과 최초 발화 시각
./scripts/alerts.sh watch        # 5초마다 갱신
```

### 설계 원칙

| 원칙 | 이 레포의 적용 |
|---|---|
| 원인이 아니라 증상에 건다 | "브로커 프로세스 죽음"보다 "응답하는 브로커 수", "확인 처리량 하락", "lag 증가"에 건다. 원인은 사람이 진단한다 |
| 심각도는 대응 시점으로 나눈다 | `page`: 지금 깨워야 한다(쓰기·읽기 불가, 유실 위험). `ticket`: 업무 시간에 본다(성능 저하, 여유 감소) |
| 결과 알람은 억제한다 | `BrokerDown`이 울리면 `UnderReplicatedPartitions`, `ProduceLatencyHigh`를 억제한다 |
| 모든 알람에 런북이 있다 | 규칙의 `runbook` 주석이 이 문서의 절을 가리킨다 |
| 침묵도 결과다 | 알람이 하나도 울리지 않는 장애(D09 오프셋 만료가 대표적)를 기록하고 앱 수준 지표를 추가한다 |

### 축척

실습에서 수 분 안에 울리도록 지속 시간을 줄였다. 실무 값은 대략 아래처럼 환산한다.

| 항목 | 이 레포 | 실무 출발점 |
|---|---|---|
| 평가 주기 | 5초 | 15~60초 |
| 가용성 알람 `for` | 10~30초 | 1~5분 |
| 복제·성능 알람 `for` | 1~2분 | 5~15분 |
| lag 증가 판정 | `deriv(...[2m]) > 0`이 1분 | 10~30분 추세, 또는 lag을 시간으로 환산한 SLO |
| 디스크 | 25%·10% | 여유 일수(증가 속도 기반 예측, `predict_linear`) |

## 진행 방식

1. `./scripts/cluster.sh up` 후 http://localhost:9093 이 열리는지, `./scripts/alerts.sh`가 "firing 알람 없음"을 출력하는지 확인한다. 클러스터만 떠 있고 워크로드가 없을 때도 알람이 없어야 한다.
2. `./scripts/workload.sh start`로 정상 부하를 걸고 5분 동안 `alerts.sh pending`을 본다. 정상 상태에서 pending이 보이면 임계값이 너무 낮다.
3. 알람 하나를 일부러 울려 본다: `docker stop broker-2` → 30초 뒤 `BrokerDown`(page), 2분 뒤 `UnderReplicatedPartitions`가 firing이지만 Alertmanager에서는 inhibited로 보인다. `docker start broker-2`로 되돌리고 알람이 resolved 되는지 본다.
4. Stage 4 훈련마다 `alerts.sh history`로 어떤 알람이 몇 초 만에 울렸는지 런북에 적는다.
5. 규칙을 고치면 `curl -X POST localhost:9090/-/reload`로 다시 읽힌다. 문법 검사는 `docker run --rm --entrypoint promtool -v $PWD/monitoring/prometheus:/p prom/prometheus:v3.5.0 check rules /p/rules/lab-alerts.yml`.

## 알람 목록

| 알람 | 심각도 | 조건(요약) | 런북 |
|---|---|---|---|
| BrokerDown | page | 응답하는 브로커 < 3, 30초 | [바로가기](#brokerdown) |
| ControllerQuorumDegraded | ticket | 응답하는 컨트롤러 < 3, 30초 | [바로가기](#controllerquorumdegraded) |
| NoActiveController | page | 활성 컨트롤러 0, 15초 | [바로가기](#noactivecontroller) |
| OfflinePartitions | page | 리더 없는 파티션 > 0, 10초 | [바로가기](#offlinepartitions) |
| UnderMinIsrPartitions | page | ISR < min ISR 파티션 > 0, 15초 | [바로가기](#underminisrpartitions) |
| UnderReplicatedPartitions | ticket | URP > 0, 2분 | [바로가기](#underreplicatedpartitions) |
| UncleanLeaderElection | page | 최근 10분 unclean 선출 > 0 | [바로가기](#uncleanleaderelection) |
| ProduceLatencyHigh | ticket | 브로커 Produce p99 > 300ms, 1분 | [바로가기](#producelatencyhigh) |
| RequestHandlerSaturated | ticket | 요청 처리 스레드 유휴율 < 30%, 2분 | [바로가기](#requesthandlersaturated) |
| BrokerDiskSpaceLow / Critical | ticket / page | 여유 < 25% 30초 / < 10% 10초 | [바로가기](#brokerdiskspacelow) |
| OfflineLogDirectory | page | 오프라인 로그 디렉터리 > 0 | [바로가기](#offlinelogdirectory) |
| CompactedTopicGrowing, LogCleanerThreadDead | ticket, page | compact 토픽 브로커당 30MB 초과 1분 / 클리너 스레드 사망 | [바로가기](#compactedtopicgrowing) |
| ProducerThroughputDrop | page | 확인 처리량 < 목표의 80%, 1분 | [바로가기](#producerthroughputdrop) |
| ProducerSendErrors | ticket | 오류 종류별 2분 증가 > 0 | [바로가기](#producersenderrors) |
| ProducerBufferExhausted | ticket | 버퍼 대기 비율 > 5%, 30초 | [바로가기](#producerbufferexhausted) |
| ConsumerLagGrowing / Critical | ticket / page | lag > 1만이면서 증가 1분 / lag > 10만 1분 | [바로가기](#consumerlaggrowing) |
| PartitionConsumptionStuck | ticket | 파티션 커밋 오프셋 2분 정지, 새 레코드 유입, 1분 | [바로가기](#partitionconsumptionstuck) |
| PartitionLagSkew | ticket | 최대 파티션 lag > 2000이면서 평균의 5배, 2분 | [바로가기](#partitionlagskew) |
| ConsumerPollIntervalNearLimit | ticket | poll 간격 > 24초(30초의 80%) | [바로가기](#consumerpollintervalnearlimit) |
| ConsumerGroupUnstable | ticket | 그룹이 Stable 아님, 1분 | [바로가기](#consumergroupunstable) |
| ConsumerSkippedRecords | page | 처리하지 않고 건너뛴 오프셋 5분 증가 > 0 | [바로가기](#consumerskippedrecords) |
| ClientThrottled | ticket | 클라이언트 throttle 시간 > 0, 1분 | [바로가기](#clientthrottled) |
| OpenTransactionStuck | ticket | log end offset - LSO > 3000, 1분 | [바로가기](#opentransactionstuck) |

### 억제 규칙

| 원인 알람 | 억제되는 알람 | 이유 |
|---|---|---|
| BrokerDown | UnderReplicatedPartitions, ProduceLatencyHigh | 브로커가 없으면 복제 지연과 지연 증가는 당연한 결과다 |
| NoActiveController | ControllerQuorumDegraded | 더 심한 쪽만 남긴다 |
| BrokerDiskSpaceCritical | BrokerDiskSpaceLow(같은 broker) | 같은 |
| ConsumerLagCritical | ConsumerLagGrowing | 같은 |

억제는 Alertmanager의 알림에만 적용된다. Prometheus의 `ALERTS` 시계열과 Grafana 표에는 억제된 알람도 firing으로 남는다. 억제가 원인을 숨길 수도 있다는 점을 기억한다. 브로커가 죽은 동안 별개 원인의 복제 지연이 겹쳐도 알림은 하나만 온다.

---

## 알람별 런북

각 절은 "무엇을 의미하는가 → 처음 5분에 볼 것 → 흔한 원인 후보" 순서다. 원인 후보는 가능성 목록이지 정답이 아니다. 후보마다 확인 방법이 있어야 소거할 수 있다.

### BrokerDown

- 의미: Prometheus가 브로커 JMX exporter에서 지표를 못 가져온다. 프로세스 종료, 컨테이너 정지, 또는 프로세스가 살아 있지만 응답하지 못하는 상태(긴 GC, 호스트 정체)다.
- 처음 볼 것: `docker ps -a`로 컨테이너 상태와 종료 코드, `docker logs --tail 100 <broker>`의 마지막 ERROR, Overview의 Fenced brokers. 응답 없음이 간헐적이면 `up`이 0과 1을 오간다(Cluster 대시보드 Brokers up).
- 흔한 원인 후보: 비정상 종료(OOM, 디스크 풀), 시작 실패(설정 오류, 지원하지 않는 metadata.version), 프로세스 정지(GC, 호스트 자원 부족).

### ControllerQuorumDegraded

- 의미: 컨트롤러 3대 중 일부가 응답하지 않는다. 과반(2대)이 살아 있으면 서비스 영향은 없지만, 한 대 더 잃으면 쿼럼이 사라진다.
- 처음 볼 것: `docker ps -a | grep controller`, `kafka-metadata-quorum.sh --bootstrap-server broker-1:9092 describe --status`의 CurrentVoters와 LeaderId, `describe --replication`의 Lag.
- 흔한 원인 후보: 컨트롤러 프로세스 종료, 컨트롤러 간 네트워크 단절, 메타데이터 로그 디스크 문제.

### NoActiveController

- 의미: KRaft 쿼럼에 리더가 없다. 기존 리더가 있는 파티션의 읽기·쓰기는 한동안 계속되지만, 메타데이터 변경(ISR 축소·확장, 리더 선출, 브로커 펜싱, 토픽 생성·설정 변경)이 전부 멈춘다. 이 상태에서 브로커 장애가 겹치면 자동 복구가 일어나지 않는다.
- 처음 볼 것: 살아 있는 컨트롤러 수, 컨트롤러 로그의 선거(election) 반복 여부, 프로듀서 확인 처리량. `kafka-topics.sh --create`가 시간 초과로 실패하면 쿼럼 상실이 맞다.
- 흔한 원인 후보: 컨트롤러 과반 동시 장애(같은 호스트·랙 배치), 컨트롤러 간 네트워크 분할.

### OfflinePartitions

- 의미: 리더를 선출할 수 없는 파티션이 있다. ISR(그리고 ELR)에 살아 있는 복제본이 없다. 해당 파티션은 읽기도 쓰기도 안 된다.
- 처음 볼 것: `kafka-topics.sh --describe --unavailable-partitions`로 파티션별 Leader, Isr, Elr, LastKnownElr. 어떤 브로커가 살아 있고 그 브로커가 ISR·ELR에 들어 있는지 본다.
- 흔한 원인 후보: 복제본을 가진 브로커 다수 동시 장애, 뒤처진 복제본만 살아남은 경우. 판단 기준은 [UncleanLeaderElection](#uncleanleaderelection)을 함께 본다.

### UnderMinIsrPartitions

- 의미: ISR 크기가 `min.insync.replicas`보다 작다. `acks=all` 쓰기가 `NotEnoughReplicas`로 거부된다. 읽기는 된다.
- 처음 볼 것: Under min ISR 파티션 목록(`kafka-topics.sh --describe --under-min-isr-partitions`), 내려간 브로커, 프로듀서 오류 종류.
- 흔한 원인 후보: 브로커 2대 이상 장애, 팔로워 복제 지연으로 인한 ISR 축소.

### UnderReplicatedPartitions

- 의미: 복제본 일부가 ISR에서 빠진 상태가 2분 넘게 계속된다. 브로커가 모두 살아 있는데도 이 알람이 남아 있으면 "따라잡지 못하는 팔로워"를 의심한다.
- 처음 볼 것: `kafka-topics.sh --describe --under-replicated-partitions`, 브로커별 ReplicationBytesIn, 팔로워 브로커 로그. 브로커와 토픽의 동적 설정(`kafka-configs.sh --describe --entity-type brokers`, `--entity-type topics`)에 복제 관련 값이 있는지 본다.
- 흔한 원인 후보: 재시작한 브로커의 따라잡기 중(정상, 시간이 지나면 사라짐), 복제 속도 제한, 팔로워 브로커의 디스크·네트워크 병목.

### UncleanLeaderElection

- 의미: ISR 밖의 복제본이 리더가 됐다. 그 복제본에 없던 레코드는 다른 복제본에서도 잘려 나간다. 프로듀서가 ack를 받은 데이터가 사라졌을 수 있다.
- 처음 볼 것: 누가 언제 선출했는가(`kafka-leader-election.sh` 실행 기록, 토픽·브로커의 `unclean.leader.election.enable`), 선출 직전과 직후의 파티션별 log end offset, 컨슈머 쪽 중복·누락.
- 대응: 유실 범위를 확정해 다운스트림에 알리고, 설정을 되돌린다. 유실을 감수한 결정이었는지 사후에 기록한다.

### ProduceLatencyHigh

- 의미: 브로커가 Produce 요청을 처리하는 데 오래 걸린다. 99분위가 300ms를 넘는다.
- 처음 볼 것: Cluster의 "Produce p99 breakdown"에서 어느 구간이 늘었는가. remote(복제 대기)가 늘면 팔로워 쪽, local이 늘면 리더의 디스크, queue가 늘면 요청 처리 스레드 포화다. 브로커별로 나눠 한 대만 튀는지 본다.
- 흔한 원인 후보: 느린 팔로워(acks=all이 기다림), 리더 디스크 지연, 요청 폭주.

### RequestHandlerSaturated

- 의미: 브로커의 요청 처리 스레드가 쉬지 못한다. 모든 요청의 지연이 함께 늘어난다.
- 처음 볼 것: 요청 종류별 요청률(Cluster 대시보드), 클라이언트 수 변화, 작은 요청 폭증(linger 0, max.poll.records 1 같은 설정).
- 흔한 원인 후보: 클라이언트 설정 변경으로 요청 수 폭증, 처리량 증가, 디스크 지연이 스레드를 붙잡음.

### BrokerDiskSpaceLow

`BrokerDiskSpaceCritical`도 이 절을 쓴다.

- 의미: 브로커 로그 디렉터리 여유가 25%(Critical은 10%) 아래다. 다 차면 브로커는 로그 디렉터리를 오프라인으로 돌리고 종료한다.
- 처음 볼 것: Cluster의 디스크 패널에서 증가 속도, 토픽별 크기(`kafka-log-dirs.sh --describe`), 데이터 디렉터리의 로그가 아닌 파일(`du -ah`).
- 흔한 원인 후보: 보존 설정 변경(`retention.ms`, `retention.bytes`), 유입량 증가, 정리되지 않는 compact 토픽, 로그 외 파일(힙 덤프 등).

### OfflineLogDirectory

- 의미: 브로커가 I/O 오류로 로그 디렉터리를 포기했다. 그 디렉터리의 파티션은 다른 복제본으로 리더가 옮겨 간다.
- 처음 볼 것: 브로커 로그의 `KafkaStorageException`, 디스크 여유와 파일 시스템 상태.

### CompactedTopicGrowing

`LogCleanerThreadDead`도 이 절을 쓴다.

- 의미: 키 수가 일정한 compact 토픽이 계속 자란다. 정상이면 키마다 최신 값 하나와 아직 정리되지 않은 최근 구간만 남는다. 이 토픽으로 상태를 복구하는 소비자는 복구 시간이 함께 늘어난다.
- 처음 볼 것: 토픽 설정(`cleanup.policy`, `segment.ms`, `segment.bytes`, `min.cleanable.dirty.ratio`, `max.compaction.lag.ms`), 파티션별 세그먼트 수(`kafka_log_log_numlogsegments`), 클리너 지표(`kafka_log_logcleaner_deadthreadcount`, `kafka_log_logcleanermanager_max_dirty_percent`, `time_since_last_run_ms`), 브로커 로그의 `log-cleaner`.
- 흔한 원인 후보: 활성 세그먼트가 닫히지 않음, 클리너 스레드 사망, 정리 대상 비율 미달, 클리너 처리량 부족.

### ProducerThroughputDrop

- 의미: 프로듀서가 목표 처리량만큼 ack를 받지 못한다. 쓰기 경로 어딘가가 막혔다. 사용자 영향에 가장 가까운 알람이다.
- 처음 볼 것: Overview의 send / ack / fail / skip, 프로듀서 오류 종류, Brokers up, Under min ISR, Offline partitions, Active controller.
- 흔한 원인 후보: 쓰기 거부(min ISR 미달), 리더 부재, 브로커 지연, 프로듀서 버퍼 고갈, 클라이언트 쿼터.

### ProducerSendErrors

- 의미: 콜백으로 실패가 전달된 레코드가 있다. `error` 라벨이 예외 종류다.
- 처음 볼 것: 예외 종류. `RecordTooLargeException`은 재시도해도 성공하지 않는다. `TimeoutException`(delivery timeout)은 그 전에 재시도 가능 오류가 계속됐다는 뜻이다. `NotEnoughReplicasException`은 ISR 문제다.

### ProducerBufferExhausted

- 의미: `send()`가 버퍼 공간을 기다리며 호출 스레드를 막는다. 브로커가 느리거나 응답하지 않아 accumulator가 비워지지 않는다.
- 처음 볼 것: Clients의 Buffer available / total, Producer node latency(어느 브로커로 가는 요청이 느린가), 브로커 Produce 지연.

### ConsumerLagGrowing

`ConsumerLagCritical`도 이 절을 쓴다.

- 의미: 처리 속도가 유입 속도를 못 따라간다. lag 절댓값보다 증가 추세가 먼저 알려 준다.
- 처음 볼 것: 컨슈머 처리량과 레코드당 처리 시간, poll idle ratio(낮으면 컨슈머가 바쁘다, 높으면 데이터를 못 받는다), 파티션별 lag 분포(전체인가 일부인가), 그룹 상태와 멤버 수, fetch throttle 시간.
- 흔한 원인 후보: 다운스트림 지연, 리밸런스 반복, 특정 파티션 정체, 컨슈머 수 부족, 브로커 쪽 fetch 제한.

### PartitionConsumptionStuck

- 의미: 특정 파티션의 커밋 오프셋이 멈췄는데 새 레코드는 들어온다. 그 파티션을 맡은 컨슈머가 같은 레코드에 붙잡혀 있다.
- 처음 볼 것: 해당 파티션의 담당 멤버(`kafka-consumer-groups.sh --describe`), 그 멤버의 로그와 오류 카운터, 커밋 오프셋 위치의 레코드 내용.

### PartitionLagSkew

- 의미: lag이 일부 파티션에 몰린다. 컨슈머를 늘려도 그 파티션은 한 멤버만 처리한다.
- 처음 볼 것: 파티션별 유입량(`lab_partition_logendoffset`의 증가율), 키 분포, 파티션별 담당 멤버.

### ConsumerPollIntervalNearLimit

- 의미: 한 번의 poll 루프가 `max.poll.interval.ms`의 80%를 넘겼다. 더 길어지면 멤버가 그룹에서 제외되고 리밸런스가 일어난다.
- 처음 볼 것: 배치당 처리 시간(`max.poll.records` × 레코드당 처리 시간), 처리 중 외부 호출의 지연.

### ConsumerGroupUnstable

- 의미: 그룹이 1분 넘게 Stable이 아니다. 할당을 기다리는 동안 해당 파티션은 소비되지 않는다.
- 처음 볼 것: 그룹 상태 타임라인, 멤버 수 변화, group epoch 증가 속도, 컨슈머 로그의 revoke/assign 반복.

### ConsumerSkippedRecords

- 의미: 컨슈머가 처리하지 않고 건너뛴 오프셋이 있다(앱이 직전 처리 오프셋과 새 레코드 오프셋의 차이로 감지). 조용한 유실의 직접 증거다.
- 처음 볼 것: 어떤 파티션에서 얼마나 건너뛰었는가(컨슈머 로그의 `OFFSET_JUMP`), 그 시점의 오프셋 리셋 여부(`auto.offset.reset`, 커밋 오프셋 만료), 리밸런스 직후인가.

### ClientThrottled

- 의미: 브로커가 클라이언트 응답을 일부러 늦추고 있다. 쿼터(바이트율, 요청 비율)를 넘었다는 뜻이다. 오류가 아니라 지연으로 나타나므로 lag이나 처리량 하락으로 먼저 보인다.
- 처음 볼 것: 클라이언트 쿼터 설정(`kafka-configs.sh --describe --entity-type clients`, `--entity-type users`), 어떤 client-id가 throttle을 받는가, 쿼터가 브로커마다 따로 적용된다는 점.

### OpenTransactionStuck

- 의미: 파티션의 LSO(last stable offset)가 log end offset보다 크게 뒤처져 있다. 열린 트랜잭션이 끝나지 않아 `read_committed` 컨슈머가 그 뒤를 읽지 못한다.
- 처음 볼 것: `kafka-transactions.sh --bootstrap-server broker-1:9092 find-hanging --broker-id <id>`, `describe-producers`, 해당 프로듀서의 상태.

---

## 실무 연결

이 레포는 수신자(`lab-null`)가 아무 데도 보내지 않는다. 실무에서는 심각도별로 경로를 나눈다.

```yaml
route:
  receiver: team-ticket
  routes:
    - matchers: [severity="page"]
      receiver: oncall-pager
receivers:
  - name: oncall-pager
    pagerduty_configs:
      - routing_key_file: /etc/alertmanager/secrets/pagerduty-key
  - name: team-ticket
    slack_configs:
      - api_url_file: /etc/alertmanager/secrets/slack-webhook
        channel: "#kafka-alerts"
```

비밀 값은 설정 파일에 직접 쓰지 않고 `*_file` 항목으로 분리한다.

함께 두면 좋은 장치:

- Watchdog(dead man's switch): 항상 firing인 알람을 하나 두고, 외부 서비스가 그 알람이 끊기면 경고한다. Prometheus나 Alertmanager 자체가 죽은 것을 알 수 있다. 이 레포는 단일 호스트라 생략했다.
- 알람마다 대시보드 링크(`dashboard` 주석)와 런북 링크를 붙인다.
- SLO 기반 알람: "lag이 1만 건"보다 "이벤트가 소비되기까지 5분 이상 걸린 비율"처럼 사용자 기준 지표로 옮겨 간다.

## 한계

- 앱 지표(`lab_*`)는 이 레포의 실험 앱이 노출한다. 실제 서비스에서는 같은 의미의 지표를 직접 만들어야 한다(콜백 실패 수, 처리하지 않은 오프셋 감지).
- lag은 `lag-monitor`가 커밋 오프셋 기준으로 잰다. 실무에서는 Burrow, kafka-lag-exporter 같은 별도 도구나 브로커 쪽 지표를 쓴다.
- dns_sd로 수집하므로 정지된 컨테이너는 대상 목록에서 사라진다. 그래서 브로커 알람은 `up == 0`이 아니라 "응답하는 대상 수"로 판정한다.
