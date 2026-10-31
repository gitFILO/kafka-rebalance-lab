# D18. 롤링 업그레이드 무중단 판정과 finalize 전 롤백

## 목적

롤링 업그레이드가 정말 무중단이었는지를 유실, 전송 실패, 처리 공백으로 나누어 판정한다. finalize 전 관찰 기간에 새 버전의 회귀가 드러나면 전체를 이전 버전으로 되돌리고, 되돌릴 수 있는 이유를 코드로 확정한다. D13이 finalize 후 롤백의 실패를 다룬다면 이 훈련은 finalize 전 롤백의 성공을 다룬다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 4.0.1 → 4.1.2 롤링 업그레이드를 부하 중에 끝냈다. finalize는 관찰 기간 뒤로 미뤘다. 관찰을 시작하자 프로듀서 지연이 오른다. |
| 주입 | 4.0.1 클러스터로 재구성 → 부하 중 `roll-all 4.1.2` → finalize 없이 관찰 기간 시작 |
| 예상 증상 | 업그레이드는 끝까지 진행된다. 관찰 기간에 프로듀서 요청 지연이 오르지만 브로커는 모두 up이다. |
| 모범 조치 | 업그레이드 구간의 무중단 여부를 판정하고, finalize 전임을 확인한 뒤 `upgrade.sh rollback-all 4.0.1`로 전체를 되돌린다. |
| 소요 | 30분(재구성 약 1분, 업그레이드와 롤백 각각 약 80초) |

이 훈련은 시작할 때 클러스터를 4.0.1로 새로 만든다. 기존 데이터와 그룹이 모두 지워진다. `drill.sh stop` 뒤에 노드는 4.1.2 바이너리로 돌아오지만 finalize는 하지 않으므로 metadata.version은 4.0 계열로 남고 ELR은 꺼져 있다. 4.1.2로 새로 만든 클러스터를 전제로 하는 훈련(D17)을 하기 전에 `./scripts/cluster.sh reset`을 실행한다.

## 진행 방식

```bash
./scripts/drill.sh start D18         # 재구성, 워크로드, 롤링 업그레이드를 마치고 돌아온다
./scripts/upgrade.sh status          # 노드별 버전과 finalized feature
RUN=$(cat results/runs/.current-workload)
```

1. 롤링 업그레이드 중에 Overview의 Producer send / ack / fail / skip, Consumer processed, Commit failures / errors / poison, Members / assigned partitions / epoch를 본다. 노드마다 `ROLL` 어노테이션이 남는다.
2. 업그레이드 구간을 판정한다. `UPGRADE_START`와 `UPGRADE_DONE` 마커 사이가 업그레이드 구간이다. 무중단이었다고 말할 수 있는가. 말할 수 있다면 어떤 범위에서인가.
   ```bash
   python3 scripts/continuity.py "$RUN" --from-marker UPGRADE_START --to-marker UPGRADE_DONE
   python3 scripts/promq.py value 'sum(lab_producer_failedtotal)'
   grep -c COMMIT_FAILED "$RUN"/events-consumer-*.csv
   ```
3. 관찰 기간의 이상을 좁힌다. 알람(`./scripts/alerts.sh`), Overview의 Producer request latency와 Produce p99 by broker, Cluster의 Produce p99 breakdown을 본다. 업그레이드 전과 달라진 것은 무엇인가.
4. 되돌릴 수 있는 상태인지 확인한다. `upgrade.sh status`의 metadata.version이 업그레이드 전과 같은가. 되돌리기와 버티기 중 무엇을 할지 정한다.
5. 되돌리기로 했다면 먼저 한 대만 되돌린 혼합 상태를 본다. `--dry-run`이라 아무것도 바뀌지 않는다.
   ```bash
   RUN_DIR_HOST="$RUN" ./scripts/upgrade.sh roll broker-3 4.0.1
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-features.sh --bootstrap-server broker-1:9092 upgrade --release-version 4.1 --dry-run
   ```
6. 전체를 되돌리고 리더를 제자리로 옮긴다. `./scripts/drill.sh fix`가 같은 일을 한다.
   ```bash
   RUN_DIR_HOST="$RUN" ./scripts/upgrade.sh rollback-all 4.0.1
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-leader-election.sh --bootstrap-server broker-1:9092 --election-type PREFERRED --all-topic-partitions
   ```
7. 증상이 사라졌는지 보고 `drill.sh verify`, `drill.sh stop`.

확인할 것:

- `missing=0`만으로 무중단이라 부를 수 있는가. `duplicates`와 `max_partition_gap_ms`는 무엇을 더 말해 주는가
- 구간 안의 커밋 실패는 왜 몰려서 나는가. 유실이 아니라면 무엇의 위험인가
- 그룹 에포크는 언제 올랐는가. 파티션 할당도 바뀌었는가
- 증상을 보이는 노드를 재시작하면 사라지는가. 그 결과가 되돌리기 판단에 어떤 근거가 되는가
- 5번의 `--dry-run`이 거부되는 이유
- 4.0.1 바이너리는 4.1.2가 쓴 메타데이터 로그와 `__consumer_offsets`를 어떻게 읽을 수 있는가
- 되돌리는 순서가 업그레이드의 역순인 이유

---

## 해설

### 메커니즘

무중단은 세 값으로 나누어 판정한다. `continuity.py`의 `missing`은 구간 안에서 처리된 오프셋의 구멍 수다. `lab_producer_failedtotal`은 콜백까지 올라온 전송 실패 수다. 리더 이동으로 재시도된 요청은 여기에 잡히지 않고 프로듀서 로그의 `NOT_LEADER_OR_FOLLOWER` WARN으로만 남는다. `max_partition_gap_ms`와 `max_group_gap_ms`는 처리가 멈춘 가장 긴 시간이다. 앞의 두 값은 0이 기준이다. 공백은 아직 측정하지 않았으므로 기록만 하고, 측정한 뒤 `--max-gap-ms`로 상한을 건다. `drill.sh selftest D18`은 같은 판정을 `selftest.txt`에 남긴다.

| 구간 | records | missing | duplicates | max_partition_gap_ms | max_group_gap_ms | 전송 실패 |
|---|---|---|---|---|---|---|
| 업그레이드 | 46,445 | 0 | 0 | 1,221 | 567 | 0 |
| 롤백 | 45,634 | 0 | 0 | 1,336 | 1,008 | 0 |

롤백 구간의 `ROLLBACK_START`, `ROLLBACK_DONE` 마커는 `drill.sh fix`로 되돌릴 때만 남는다. 직접 되돌렸다면 `--from-marker UPGRADE_DONE`으로 관찰 기간과 롤백을 합쳐 본다. `drill.sh verify`도 이 구간으로 판정한다.

`missing=0`이 아무 일도 없었다는 뜻은 아니다. 그룹 코디네이터가 있던 브로커가 내려가면 `commitAsync`가 실패한다. 비동기 커밋은 재시도하지 않지만 다음 배치의 커밋이 더 큰 오프셋을 덮어쓰므로 처리는 이어진다. 커밋이 밀린 동안 컨슈머가 죽으면 마지막 성공 커밋부터 다시 읽는다. 커밋 실패는 유실이 아니라 중복 범위를 늘리는 위험이다.

관찰 기간의 회귀는 `TARGET_BROKER`(기본 broker-2)를 주기적으로 정지시켜 흉내 낸 것이다. `acks=all` 쓰기가 느린 복제본을 기다리는 경로는 D06과 같다. 정지 루프는 그 브로커의 이미지가 4.1.2일 때만 돌고, `upgrade.sh`가 그 브로커의 목표 버전을 바꾸면 스스로 끝난다. 드릴 스크립트가 따로 치우지 않으므로 증상이 사라졌다면 그것이 되돌린 판단의 근거다.

finalize 전에 되돌릴 수 있는 것은 4.1.2 바이너리가 남기는 형식이 4.0.1이 읽을 수 있는 범위에 머물기 때문이다. 이 범위를 지키는 장치는 둘이다.

첫째, 메타데이터와 복제의 형식은 바이너리가 아니라 finalized MV가 정한다. 컨트롤러가 쓰는 레코드 버전, 스냅샷의 레코드 버전, 리플리카 페처의 Fetch 버전이 모두 현재 MV에서 나온다. 4.1.2 바이너리도 MV가 4.0 계열이면 Fetch v17을 보낸다. v18은 MV가 4.1-IV1이 되어야 쓰는데, 4.0.1은 v17까지만 안다. finalize 뒤에는 이 값들이 함께 오르므로 바이너리만 되돌려서는 맞지 않는다.

둘째, `__consumer_offsets`는 MV로 게이팅되지 않는다. 4.1.2 그룹 코디네이터는 MV가 4.0 계열이어도 `OffsetCommitValue`를 v4로 쓰고, `ConsumerGroupMetadataValue`에 `MetadataHash`를 넣고, 구독 메타데이터 레코드를 tombstone으로 지운다. 그런데도 되돌릴 수 있는 것은 KIP-915의 규칙 덕분이다. 레코드에 새 필드를 넣을 때는 tagged field만 쓴다. 4.0.1은 `OffsetCommitValue`를 v3로 쓰지만 v4를 읽도록 미리 선언해 두었고, 모르는 tagged field는 값으로 해석하지 않고 넘긴다. 구독 메타데이터가 지워진 그룹을 읽은 4.0.1 코디네이터는 구독 메타데이터를 다시 계산해 그룹 에포크를 올린다. 4.1.2 코디네이터도 4.0.1이 쓴 그룹을 읽으면 해시가 0이라 에포크를 올린다. 코디네이터가 두 버전 사이를 옮겨 갈 때 그룹 에포크가 오르는 이유다.

혼합 버전에서 브로커 등록은 바이너리 버전 비교가 아니라 범위 검사다. 브로커는 자기 바이너리가 지원하는 feature 범위를 등록 요청에 실어 보내고, 컨트롤러는 finalized 값이 그 범위 안에 있는지만 본다. metadata.version의 상한은 4.0.1이 level 25(4.0-IV3), 4.1.2가 level 27(4.1-IV1)이다. finalized가 level 25인 동안에는 두 바이너리가 모두 통과한다. 거부되는 경우는 D13에서 본 대로다.

컨트롤러는 이 범위를 등록 레코드에 남겨 두고 finalize 요청이 올 때 등록된 모든 노드에 대해 다시 검사한다. 4.0.1 노드가 하나라도 섞여 있으면 `--release-version 4.1`은 거부된다. 혼합 상태에서는 finalize로 되돌릴 길을 막는 실수를 컨트롤러가 막아 준다. 위험한 순간은 모든 노드가 4.1.2가 되어 이 검사가 통과하는 때다.

`rollback-all`이 업그레이드의 역순인 것은 코드가 강제해서가 아니다. 등록 검사는 순서를 보지 않고 공식 업그레이드 문서도 컨트롤러와 브로커의 선후를 정하지 않는다. 역순으로 되돌리면 업그레이드 때 이미 지나온 혼합 상태만 거꾸로 다시 지나므로, 새로운 조합을 운영 중에 처음 시험하지 않아도 된다. 활성 컨트롤러를 마지막에 두는 이유는 업그레이드와 같다. 컨트롤러 리더 이동을 한 번으로 줄인다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | 관찰 기간의 프로듀서 요청 지연 상승, Brokers up 3 | 성능 회귀 |
| 범위 | Produce p99 breakdown의 remote 상승, 표적 브로커의 `scrape_duration_seconds`가 불규칙(D06과 같은 경로) | 한 브로커가 느린 복제본이다 |
| 원인 후보 | `ROLL` 마커 이전에는 없었고, 그 브로커를 재시작해도 돌아온다 | 호스트 일시 문제보다 바이너리 변경이 유력하다 |
| 되돌림 가능 | `upgrade.sh status`의 metadata.version이 업그레이드 전과 같다 | finalize 전이므로 바이너리만 되돌리면 된다 |
| 확인 | 표적 브로커를 되돌리는 시점에 지연이 내려온다. `drill.sh verify` 통과 | 회귀가 새 바이너리에 묶여 있었다 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| `rollback-all 4.0.1`로 전체 롤백 | 회귀가 사라지고 업그레이드 전 구성으로 돌아간다. metadata.version은 그대로다 | 노드 6대를 다시 재시작하므로 커밋 실패와 처리 공백을 한 번 더 겪는다. 4.1.2의 수정 사항도 함께 포기한다 |
| 표적 브로커만 4.0.1로 되돌림 | 증상이 사라진다 | 혼합 버전이 오래 남고 그동안 finalize가 막힌다. 다른 브로커에는 같은 회귀가 없다는 근거가 없다 |
| 표적 브로커 재시작 | 일시적이다 | 바이너리가 같으므로 회귀가 돌아온다. 재시작 비용만 든다 |
| 표적 브로커 정지(D06) | 지연이 사라진다 | 내구성 여유가 1 줄어들고 원인은 남는다 |
| 4.1.2를 유지하고 패치 버전을 기다림 | 추가 재시작이 없다 | 지연을 감수한다. finalize도 미룬 채로 남는다 |
| 회귀를 두고 finalize | 상황이 나빠진다 | D13 상태가 된다. 되돌리려면 MV 하향이 먼저 필요하다 |

### 코드 근거 (4.1.2)

- 등록 요청: `BrokerServer`(`core/src/main/scala/kafka/server/BrokerServer.scala:156`)가 `BrokerFeatures.createDefault()`로 지원 범위를 만들고, `BrokerLifecycleManager.sendBrokerRegistration()`(`core/src/main/scala/kafka/server/BrokerLifecycleManager.scala:360`)이 feature별 min, max를 요청에 싣는다. metadata.version의 하한은 `MINIMUM_VERSION`, 상한은 `latestProduction()`이다(`server/src/main/java/org/apache/kafka/server/BrokerFeatures.java:71`, 73행). 4.1.2의 `LATEST_PRODUCTION`은 `IBP_4_1_IV1`이다(`server-common/.../common/MetadataVersion.java:160`).
- 등록 검사: `ClusterControlManager.registerBroker()`(`metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java:410`)는 요청의 feature마다 `processRegistrationFeature()`를 호출하고, 요청에 없는 feature도 finalized 값이 0이 아니면 같은 검사를 거친다(419행). 등록 레코드의 버전은 `featureControl.metadataVersionOrThrow().registerBrokerRecordVersion()`으로 정한다(458행). 거부 지점은 D13이 인용한 511행이다.
- finalize 잠금: `FeatureControlManager.updateFeature()`(`metadata/.../controller/FeatureControlManager.java:256`)는 `reasonNotSupported()`(321행)로 등록된 브로커(334행)와 컨트롤러(353행)의 범위를 검사한다. 이 범위는 `QuorumController.java:495`, 500행을 거쳐 `ClusterControlManager`의 `brokerSupportedFeatures()`(825행)와 `controllerSupportedFeatures()`(843행)에서 온다. 범위 밖이면 `QuorumFeatures.java:51`의 "only supports versions"로 거부한다. 256행은 `validateOnly`와 무관하게 실행되므로 `--dry-run`도 같은 답을 준다.
- MV가 정하는 레코드 버전: `MetadataVersion.java`의 `registerBrokerRecordVersion()`(224행), `partitionChangeRecordVersion()`(249행), `partitionRecordVersion()`(259행). 호출부는 `PartitionChangeBuilder.java:468`, `BrokerRegistration.java:327`, `PartitionRegistration.java:410`이다. 스냅샷은 `SnapshotEmitter.java:149`가 이미지의 MV로 옵션을 만들어 쓴다.
- 메타데이터 스키마: `git diff 4.0.1 4.1.2 -- metadata/src/main/resources/common/metadata/`의 차이는 `RegisterBrokerRecord.json`과 `BrokerRegistrationChangeRecord.json`의 tag 표기(문자열 → 숫자)뿐이다.
- 복제: `RemoteLeaderEndPoint.scala:206`은 현재 MV의 `fetchRequestVersion`을 쓴다. `MetadataVersion.java:269`에서 이 값은 4.1-IV1부터 18이고, 3.9-IV0부터 그 아래까지는 17이다.
- 그룹 레코드: `OffsetCommitValue.json:21`이 KIP-915 규칙을 적어 두었고 `topicId`는 tagged field다(36행). `GroupCoordinatorRecordHelpers.java:494`는 v4를 쓴다. `ConsumerGroupMetadataValue.json:29`의 `MetadataHash`도 tagged field다. `GroupMetadataManager.java`는 해시가 다르면 그룹 에포크를 올리고(3712행) 남은 구독 메타데이터를 tombstone으로 지운다(3732행). 이 경로에 MV 분기는 없다. 모르는 tagged field의 처리는 `MessageDataGenerator.java:526`이 생성하는 `readUnknownTaggedField()`다.
- 레코드 로드: `CoordinatorRecordSerde.java:80`은 값의 버전이 지원 범위 밖이면 `UnknownRecordVersionException`을 던지고, `CoordinatorLoaderImpl.scala:186`은 모르는 레코드 타입만 경고 후 건너뛴다.
- 읽는 쪽(4.0.1 태그): 되돌린 뒤 읽는 바이너리는 4.0.1이므로 이 항목만 4.0.1의 줄 번호다. `LATEST_PRODUCTION`은 `IBP_4_0_IV3`(`MetadataVersion.java:135`), `FetchRequest.json:59`는 `4-17`, `OffsetCommitValue.json:22`는 `0-4`, `ConsumerGroupMetadataValue.json:20`은 `flexibleVersions 0+`다. `GroupMetadataManager.java`는 구독 메타데이터 tombstone을 빈 맵으로 재생하고(3820행), 다시 계산한 값이 다르면 에포크를 올린다(2814행).
- 커밋 실패: `CommitRequestManager.commitAsync()`(`clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java:392`)에는 재시도 경로가 없다. 재시도 가능한 오류는 `RetriableCommitFailedException`으로 감싸 돌려준다(489행).

### 재발 방지

- 업그레이드 런북에 finalize 전 관찰 기간과 되돌리는 조건을 미리 적는다. 어떤 지표가 얼마나 나빠지면 되돌릴지를 작업 전에 정한다.
- 업그레이드마다 `continuity.py` 결과와 전송 실패 수를 변경 기록에 남긴다. 공백 상한을 정한 뒤에는 `--max-gap-ms`로 판정한다.
- finalize 승인 전에 `rollback-all`을 같은 구성에서 리허설한다.
- 알람: 관찰 기간의 지연은 `ProduceLatencyHigh`, 전송 실패는 `ProducerSendErrors`가 맡는다. 이 훈련에서 `ProduceLatencyHigh`는 회귀가 시작되고 약 2분 40초 뒤에 울렸다.
- 노드 재시작 중 `BrokerDown` 처리는 D13의 재발 방지를 따른다.

### 재현 충실도

회귀는 주기적 정지로 흉내 낸 것이다. D06과 같은 `docker pause` 방식이고, 이미지 태그로 새 버전에만 묶었을 뿐 4.1.2 바이너리에 실제 결함이 있는 것은 아니다. 실제 회귀는 특정 요청 경로나 부하에서만 드러나고 모든 브로커에 고르게 나타날 수 있다. 여기서는 표적 브로커 한 대에만 걸리므로 범위를 좁히기가 실제보다 쉽다. 정지 루프는 한 번 끝나면 다시 돌지 않으므로, 되돌린 뒤 4.1.2로 다시 올려도 회귀는 재현되지 않는다.

브로커와 컨트롤러 데이터는 tmpfs 볼륨에 있지만 disk-monitor가 볼륨을 붙잡고 있어 컨테이너를 다시 만들어도 남는다. 4.0.1 바이너리는 4.1.2가 쓴 세그먼트, 메타데이터 로그, `__consumer_offsets`를 실제로 읽는다. 다만 데이터가 작고 관찰 기간이 수 분이다. 운영에서 수일 동안 4.1.2가 쓴 레코드를 되돌리는 경우보다 위험이 작게 보인다.

워크로드는 consumer 프로토콜 그룹과 비트랜잭션 프로듀서만 쓴다. `__transaction_state`, Streams 그룹, Share 그룹의 레코드는 이 훈련으로 검증되지 않는다. 4.1에서 새로 생긴 레코드 타입을 쓰는 기능을 켠 클러스터라면 4.0.1 로더가 그 레코드를 건너뛰므로 되돌렸을 때 그 상태를 잃는다. 클라이언트 버전은 이 훈련 동안 바뀌지 않는다.
