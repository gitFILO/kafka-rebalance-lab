# D19. 컨슈머 클라이언트 업그레이드 후 롤백

## 목적

컨슈머 애플리케이션의 kafka-clients 버전을 멤버 단위로 올리고, 문제가 드러나면 되돌리는 절차를 끝까지 수행한다. 업그레이드가 무중단이었다는 말을 대시보드 인상이 아니라 처리 기록으로 판정하고, 되돌리는 구간에도 같은 기준을 적용한다. 되돌릴지 정할 때 근거를 어떻게 모으는지도 연습한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 소비 애플리케이션의 kafka-clients를 4.1.2에서 4.2.0으로 올린다. 멤버를 한 대씩 교체했고 교체는 별일 없이 끝났다. 잠시 뒤 알람이 울리기 시작한다. |
| 주입 | 워크로드 실행 중 `client-upgrade.sh roll-all 4.2.0`으로 consumer-1~3을 한 대씩 교체한다. 교체가 끝난 뒤의 주입 내용은 해설에 있다. |
| 예상 증상 | 교체 구간에는 소비가 이어진다. 교체 뒤 lag이 늘고 그룹이 안정을 유지하지 못한다. |
| 모범 조치 | 업그레이드와의 관련을 근거로 확인하고 전 멤버를 4.1.2로 되돌린다. 되돌린 구간에 유실이 없었는지 판정한다. |
| 소요 | 25분(교체 약 30초, 되돌리기 약 2분, lag 회복까지 약 4분) |

이 훈련은 시작할 때 `rebalance-lab-app:latest`와 `rebalance-lab-app:clients-4.2.0`을 현재 소스로 빌드한다. 캐시가 없으면 Maven 의존성을 내려받느라 시간이 더 걸린다. 끝나면 `results/runs/.node-versions.env`의 멤버 태그를 지우므로 다음 워크로드는 `latest`로 시작한다.

## 진행 방식

```bash
./scripts/drill.sh start D19         # 멤버 교체가 끝나야 돌아온다. 그동안 대시보드를 본다
./scripts/client-upgrade.sh status   # 멤버별 이미지와 kafka-clients 버전
RUN=$(cat results/runs/.current-workload)
```

1. 교체 중에 Overview의 Group state, Members / assigned partitions / epoch, Assigned partitions per member, Consumer processed를 본다. 멤버마다 `CLIENT_ROLL` 어노테이션이 남는다. 한 대를 교체하는 동안 나머지 멤버는 소비를 멈추는가.
2. 교체가 끝나면 무중단이었는지 판정한다.
   ```bash
   python3 scripts/continuity.py "$RUN" --from-marker CLIENT_UPGRADE_START --to-marker CLIENT_UPGRADE_DONE
   ```
   `missing`은 유실, `duplicates`는 중복, `max_partition_gap_ms`는 파티션 하나의 최대 처리 공백, `max_group_gap_ms`는 그룹 전체의 최대 처리 공백이다. 값을 기록해 둔다.
3. 알람이 울리면 `docs/03-alerting.md`의 해당 런북이 말하는 것부터 확인한다. Clients의 Processed per member, Poll idle ratio와 Overview의 Group state, Rebalance callbacks, Commit failures를 함께 본다.
4. 범위를 좁힌다. 브로커와 프로듀서 지표가 움직였는가, 특정 파티션의 문제인가 전 멤버의 문제인가, 직전 변경은 무엇인가.
5. 멤버 로그와 이벤트를 본다. 컨테이너를 교체하면 로그가 사라지므로 되돌리기 전에 본다.
   ```bash
   docker logs consumer-1 2>&1 | grep -E 'WARN|ERROR' | tail -20
   cut -d, -f3 "$RUN/events-consumer-1.csv" | sort | uniq -c
   ```
6. 업그레이드가 원인이라는 근거를 만든다. 전체를 되돌리기 전에 한 대만 되돌려 나머지와 비교할 수 있다.
   ```bash
   ./scripts/client-upgrade.sh roll consumer-1 4.1.2
   ```
7. 조치를 정한다. 되돌리려면 다음을 실행한다.
   ```bash
   ./scripts/client-upgrade.sh roll-all 4.1.2
   ```
8. 되돌린 구간을 판정한다.
   ```bash
   python3 scripts/continuity.py "$RUN" --from-marker CLIENT_UPGRADE_DONE
   ```
9. `drill.sh verify`, `drill.sh stop`.

`drill.sh verify`는 전 멤버 4.1.2, lag 5000 미만, 그룹 Stable 30초 유지, `CLIENT_UPGRADE_DONE` 이후 유실 0을 본다. 조치를 오래 미루면 lag이 보존 한도(파티션당 8MB)를 넘어 실제 유실이 생기고 유실 판정이 실패한다. 기본 부하에서 여유는 계산상 약 11분이다(12파티션 × 8MB ÷ 브로커당 약 140KB/s).

확인할 것:

- 멤버 한 대를 교체할 때 group epoch는 몇 번 오르는가. 나가는 멤버와 들어오는 멤버가 각각 무엇을 일으키는가
- `missing=0`만으로 무중단이라고 말할 수 있는가. `max_partition_gap_ms`는 무엇으로 정해지는가
- 교체 구간과 되돌린 구간의 `duplicates` 차이는 어디서 오는가. 왜 유실이 아니라 중복인가
- `client-upgrade.sh`는 올릴 때 그룹이 안정되지 않으면 멈추고, 되돌릴 때는 경고만 남기고 다음 멤버로 넘어간다. 왜 다르게 동작해야 하는가
- 되돌리지 않고 설정 변경이나 증설로 버틸 수 있는가. 각각 무엇이 남는가

---

## 해설

### 메커니즘

consumer 프로토콜에서는 멤버 한 대를 교체해도 그룹 전체가 멈추지 않는다. `compose up`이 기존 컨테이너에 SIGTERM을 보내면 LabConsumer는 레코드 경계에서 처리를 멈추고, 처리 기록을 flush하고 오프셋을 커밋한 뒤 `close()`로 내려간다. `close()`는 `onPartitionsRevoked`를 실행하고 member epoch -1 하트비트로 그룹을 떠난다. 코디네이터는 그 멤버를 제거하고 group epoch를 올린다. 다음 하트비트가 target assignment 재계산을 일으키고, 각 멤버는 자기 하트비트에서 target을 향해 움직인다. 유지하는 파티션은 소비를 이어 간다. 새 컨테이너는 epoch 0 하트비트로 합류해 group epoch를 한 번 더 올린다. 다른 멤버가 내줄 파티션은 그 멤버의 회수 응답을 받은 뒤에야 새 멤버에게 넘어간다.

그래서 한 대를 교체하면 group epoch가 두 번 오르고, 파티션은 이탈 때와 합류 때 각각 움직일 수 있다. 멈추는 것은 옮겨 가는 파티션뿐이고 그 공백이 `max_partition_gap_ms`다. 실측값은 3회에서 5.2~8.3초였고, 레코드 유실과 중복은 모두 0이었다.

`continuity.py`는 구간 안의 `processed-*.csv`를 읽어 파티션별로 처리한 오프셋의 최솟값과 최댓값 사이에서 빠진 오프셋을 유실로 센다. 커밋이 처리보다 늦으므로 at-least-once 소비에서 중복은 정상이고 유실만 실패다. `missing=0`은 빠진 레코드가 없다는 뜻일 뿐 멈추지 않았다는 뜻은 아니다. 멈춘 시간은 처리 공백으로 따로 본다. 이 훈련은 공백을 판정에 넣지 않고 `selftest.txt`에 기록만 한다. 측정값이 쌓이면 `--max-gap-ms`로 상한을 건다.

교체가 끝나면 4.2.0 멤버의 레코드당 처리 시간이 2ms에서 402ms로 늘어난다(재현 충실도 참고). `max.poll.records` 100건 배치에 약 40초가 걸려 `max.poll.interval.ms` 30초를 넘는다. 하트비트는 백그라운드 스레드가 보내므로 세션은 끊기지 않는다. 대신 같은 스레드가 poll 타이머 만료를 확인해 "Consumer poll timeout has expired" 경고를 남기고, epoch -1 하트비트로 그룹을 떠나 STALE이 된다. 애플리케이션 스레드는 이를 모른 채 배치를 끝까지 처리한다. 배치 끝의 커밋은 이미 떠난 멤버의 것이라 코디네이터가 거절하고, 다음 `poll()`에서 `onPartitionsLost`가 돈 뒤 다시 합류한다. 넘겨받은 멤버는 마지막 커밋부터 같은 레코드를 다시 처리한다. `duplicates`는 늘고 lag은 줄지 않는다. 유실은 아니다.

증상은 D02와 같지만 근거가 다르다. D02에서는 `process_ms`가 바뀌어 Process delay setting 패널이 움직인다. 여기서는 설정값이 그대로라 패널이 2에 머문다. 설정에서 이유를 찾을 수 없으므로 업그레이드 직후라는 시간 관계와 버전별 비교가 근거가 된다.

되돌리는 구간은 올리는 구간과 조건이 다르다. 한 대를 4.1.2로 되돌려도 남은 4.2.0 멤버는 긴 배치 안에 있다. 코디네이터가 파티션 회수를 요청해도 `onPartitionsRevoked`는 애플리케이션 스레드의 다음 `poll()`에서 돈다. 그동안 그 멤버는 `UNREVOKED_PARTITIONS`, 그룹은 Reconciling이고, 새 멤버는 `UNRELEASED_PARTITIONS`로 기다린다. rebalance timeout은 `max.poll.interval.ms`와 같은 값이라 30초 안에 회수가 끝나지 않으면 코디네이터가 그 멤버를 fence한다. 문제 멤버가 남아 있는 동안 Stable을 끝까지 기다리면 그 시간만큼 lag이 쌓인다. 그래서 `client-upgrade.sh`는 4.1.2로 갈 때 대기를 `ROLLBACK_TIMEOUT_SEC`(기본 60초)로 끊고 경고만 남긴다. 올릴 때는 `ROLL_TIMEOUT_SEC`(기본 180초) 안에 안정되지 않으면 롤링을 멈춘다. 올릴 때 멈추는 것은 문제를 퍼뜨리지 않으려는 것이고, 되돌릴 때 멈추지 않는 것은 문제 멤버를 빨리 없애려는 것이다.

긴 배치 중의 종료도 롤백 속도를 좌우한다. LabConsumer의 종료 훅은 메인 스레드를 15초 기다린다. 레코드 경계에서 종료 신호를 확인하지 않으면 40초 배치를 끝내지 못한 채 JVM이 내려가 커밋도 leave 하트비트도 없이 끝나고, 그 멤버의 파티션은 session timeout(45초)까지 묶인다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | `ConsumerPollIntervalNearLimit`, `ConsumerLagGrowing`, Group state 반복 전이 | 소비가 유입을 따라가지 못한다 |
| 범위 | 전 멤버의 Processed per member가 함께 떨어짐, 파티션 전체 lag 증가, 브로커 지연과 프로듀서 정상 | 특정 파티션이나 브로커가 아니라 소비 애플리케이션 쪽 |
| 변경 | `CLIENT_ROLL` 어노테이션 직후 시작, `client-upgrade.sh status`가 4.2.0 | 직전 변경이 업그레이드다 |
| 구분 | Process delay setting 2 그대로, Poll idle ratio 하락, throttle 없음 | 설정 변경이나 쿼터(D16)가 아니다. 멤버가 처리에 시간을 쓰고 있다 |
| 원인 | Time between poll max > 30000, 로그의 poll timeout 경고, 이벤트의 LOST와 COMMIT_FAILED 반복 | 배치 시간이 한도를 넘어 멤버가 쫓겨난다 |
| 확인 | 한 대만 4.1.2로 되돌렸을 때 그 멤버의 처리량만 회복 | 증상이 버전을 따라간다 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| `roll-all 4.1.2`로 전 멤버 되돌림 | 원인이 사라지고 lag이 줄기 시작한다 | 되돌린 구간에 중복이 늘어난다. 새 버전으로 얻으려던 것을 미룬다 |
| 한 대만 되돌리고 비교 | 버전 귀속을 확인한다 | 나머지 멤버는 문제 상태로 남으므로 짧게 끝낸다 |
| 전 멤버 동시 재생성으로 되돌림 | 문제 멤버가 한 번에 사라진다 | 모든 파티션이 함께 멈춘다. 소비가 이미 거의 멈춘 상태라면 손해가 작다 |
| `max.poll.records` 축소 또는 `max.poll.interval.ms` 상향 | 쫓겨남이 멈추고 커밋이 진행된다 | 처리 시간이 그대로라 멤버당 초당 약 2.5건(1000 / 402로 계산)에 머물고 lag은 계속 는다 |
| 컨슈머 증설 | 효과가 없다 | `workload.sh scale`로 뜨는 멤버도 4.2.0이다. 파티션 12개라 12대가 상한이고, 그래도 초당 약 30건(계산값)이다 |
| 4.2.0을 유지하고 수정본 배포 | 업그레이드를 포기하지 않는다 | 원인을 알고 수정을 검증할 때까지 lag이 쌓인다 |

운영 절차의 결론은 되돌리는 기준을 미리 정해 두는 것이다. 업그레이드 직후 소비 지표가 나빠지고 버전별 비교로 귀속이 확인되면 원인 분석을 기다리지 않고 되돌린다. 이 시나리오처럼 프로토콜을 바꾸지 않는 클라이언트 교체에는 D13의 finalize 같은 되돌릴 수 없는 단계가 없으므로 되돌리는 비용이 작다. 분석은 되돌린 뒤 `events-*.csv`와 남겨 둔 로그로 한다.

### 코드 근거 (4.1.2)

- 이탈 요청: `AsyncKafkaConsumer.close()`(`clients/src/main/java/org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumer.java:1426`)는 1431행에서 wakeup을 막고, 리밸런스 콜백(1444행), 그룹 이탈(1446행), 비동기 커밋 대기(1448행) 순서로 진행한다. 이탈 epoch는 `ConsumerGroupHeartbeatRequest.java:31`의 `LEAVE_GROUP_MEMBER_EPOCH = -1`이고 합류 epoch는 37행의 `JOIN_GROUP_MEMBER_EPOCH = 0`이다.
- 이탈 처리: `GroupMetadataManager.consumerGroupHeartbeat()`(`group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java:4691`)는 4695행에서 epoch -1, -2를 `consumerGroupLeave()`(3942행)로 보낸다. 정적 멤버가 아니면 `consumerGroupFenceMembers()`(4087행)가 멤버를 제거하고 4131행에서 group epoch를 올린다. 브로커 로그에는 3955행의 "left the consumer group"이 남는다.
- 합류와 target 재계산: 내부 `consumerGroupHeartbeat()`(2186행)는 2253행에서 구독 변경을 보고 group epoch를 올린다. 2292행에서 group epoch가 target assignment epoch보다 크면 `updateTargetAssignment()`를 호출하고, 2309행에서 `maybeReconcile()`로 하트비트를 보낸 멤버를 target에 맞춘다.
- 파티션 인계: `CurrentAssignmentBuilder.computeNextAssignment()`(`group-coordinator/.../modern/consumer/CurrentAssignmentBuilder.java:226`)는 회수할 파티션이 있으면 `UNREVOKED_PARTITIONS`(282행), 받을 파티션이 아직 풀리지 않았으면 `UNRELEASED_PARTITIONS`(296행)로 둔다. 회수 완료는 153행에서 멤버가 보고한 보유 파티션으로 판단한다.
- 그룹 상태: `ConsumerGroup.maybeUpdateGroupState()`(`group-coordinator/.../modern/consumer/ConsumerGroup.java:901`)는 target이 group epoch보다 뒤처지면 Assigning(906행), 어느 멤버든 target에 맞춰지지 않았으면 Reconciling(910행)이다. 맞춰졌다는 기준은 `ModernGroupMember.isReconciledTo()`(169행)의 STABLE 상태와 epoch 일치다.
- poll 간격 초과: `AbstractHeartbeatRequestManager.poll()`(`clients/.../consumer/internals/AbstractHeartbeatRequestManager.java:169`)이 타이머 만료를 확인해 경고를 남기고(170행), `transitionToSendingLeaveGroup(true)`(176행)와 leave 하트비트(177행)로 이어진다. `AbstractMembershipManager.java`는 하트비트 생성 뒤 715행에서 `transitionToStale()`(790행)로 가고, 794행에서 `onPartitionsLost` 콜백을 요청한다.
- 재합류: `poll()`이 보낸 이벤트를 `ApplicationEventProcessor.java:233`이 받아 `resetPollTimer()`(`AbstractHeartbeatRequestManager.java:263`)를 호출한다. 타이머가 만료된 상태면 `maybeRejoinStaleMember()`(`AbstractMembershipManager.java:775`)로 다시 합류한다.
- 쫓겨난 멤버의 커밋: `ConsumerGroup.validateOffsetCommit()`(656행)는 674행에서 멤버를 찾고 없으면 `UnknownMemberIdException`, 683행에서 epoch가 다르면 `StaleMemberEpochException`을 던진다. 다만 666행에서 epoch가 음수이고 그룹이 비어 있으면 통과시킨다. 세 멤버가 같은 시각에 쫓겨나 그룹이 Empty가 되면 그 커밋은 받아들여진다.
- 회수 지연: 리밸런스 콜백은 `ConsumerMembershipManager.java:357`, `379`에서 큐에 들어가고 애플리케이션 스레드가 `AsyncKafkaConsumer.processBackgroundEvents()`(2105행)에서 실행한다. 긴 배치 중에는 돌지 않는다. rebalance timeout은 `ConsumerHeartbeatRequestManager.java:74`가 `max.poll.interval.ms`로 보낸 값이다. `GroupMetadataManager.java` 3530행이 `UNREVOKED_PARTITIONS` 멤버에게 타이머를 걸고, 만료되면 `scheduleConsumerGroupRebalanceTimeout()`(4523행)의 4540행에서 fence한다.
- 지표: `KafkaConsumerMetrics.recordPollStart()`(`clients/.../internals/metrics/KafkaConsumerMetrics.java:93`)는 poll 시작 시점에 직전 poll과의 간격을 기록한다. 긴 배치가 끝나야 Time between poll max가 올라가므로 첫 경보는 쫓겨난 시각보다 늦다.
- 보존 초과 시 유실: 커밋 오프셋이 보존 범위 밖이면 `FetchCollector.java:347~351`이 위치를 리셋한다. 이 레포는 `auto.offset.reset=latest`라 건너뛴 구간이 유실이 되고 `continuity.py`의 `missing`에 잡힌다.

### 재발 방지

- 카나리: 전 멤버를 교체하기 전에 `client-upgrade.sh roll consumer-1 4.2.0`처럼 한 대만 올려 처리량, poll 간격, 커밋 실패를 같은 부하에서 비교한다.
- 교체 게이트: `client-upgrade.sh`는 Stable, 멤버 수, 파티션 할당만 본다. 이 훈련처럼 교체가 끝난 뒤 드러나는 문제는 이 게이트를 통과한다. 운영 배포에는 처리량과 poll 간격을 일정 시간 관찰하는 단계를 넣는다.
- 배치 설계: `max.poll.records` × 레코드당 처리 시간이 `max.poll.interval.ms`에 여유 있게 들어가도록 잡는다. `ConsumerPollIntervalNearLimit`는 그 여유가 사라졌다는 신호다.
- 종료 경로: 처리 루프가 종료 신호를 레코드 경계에서 확인하게 한다. 긴 배치 중에도 커밋하고 그룹을 떠나야 롤백이 session timeout에 묶이지 않는다.
- 되돌림 준비: 이전 이미지를 지우지 않는다. 되돌리는 기준(어떤 알람이 얼마나 지속되면), 명령, 판정 항목을 런북에 미리 적는다.

### 재현 충실도

이 훈련의 회귀는 `control/regression.4.2.0` 파일로 넣은 처리 지연이고 실제 4.2.0의 결함이 아니다. 4.2.0이 이렇게 느려진다는 뜻으로 읽으면 안 된다. 버전 번호만 바꾸면 어떤 버전에도 같은 방식으로 넣을 수 있다.

LabConsumer는 `process_ms`를 읽는 주기로 `regression.<kafka-clients 버전>` 파일을 읽고 값만큼 레코드당 sleep에 더한다. 버전은 `AppInfoParser.getVersion()`에서 얻으므로 같은 버전의 멤버에게만 걸린다. 4.1.2로 되돌리면 파일이 남아 있어도 지연이 사라진다. 실제 버전 회귀처럼 이미지를 바꿔야 증상이 없어진다. Process delay setting 패널은 설정값만 보여 주므로 이 지연을 반영하지 않는다.

교체 구간을 깨끗하게 판정하려고 지연은 교체가 끝난 뒤에 넣는다. 실제 회귀는 교체 중에 드러날 수도 있다. 회귀 시작에 마커를 남기지 않는 이유는 Grafana 어노테이션에 정답이 드러나기 때문이다. 복구 판정은 `CLIENT_UPGRADE_DONE`을 회귀 시작으로 쓴다.

코드 근거는 4.1.2 기준이고 회귀가 걸리는 멤버는 4.2.0이다. 두 태그를 비교하면 poll 타이머, STALE 전이, 재합류 코드는 같고, 지표 이름과 그룹(`consumer-metrics`)도 같다. 4.2.0 `AsyncKafkaConsumer.poll()`은 `AsyncPollEvent`로 구조가 바뀌었지만 841행에서 `recordPollStart`를 호출하고, 이벤트 처리에서 `resetPollTimer`를 호출한다(`ApplicationEventProcessor.java:728`). 브로커는 4.1.2로 두므로 코디네이터 쪽 근거는 그대로 적용된다.

`max.poll.interval.ms`는 30초로 줄인 값이다(운영 기본 5분). 몇 분 안에 재현하려고 지연도 그에 맞췄다. `compose up`은 기존 컨테이너를 내린 뒤 새 컨테이너를 띄운다. 새 인스턴스를 먼저 띄우는 배포 방식에서는 멤버 수가 잠시 늘어나므로 처리 공백이 달라질 수 있다.

selftest 3회 중 1회에서 쫓겨나던 4.2.0 멤버가 `IllegalStateException: No current assignment for partition`으로 프로세스째 종료됐다. 예외는 애플리케이션 코드가 아니라 `poll()` 안의 `FetchCollector.fetchRecords()`에서 났다. 이 메서드는 `isAssigned`를 먼저 확인하고 뒤에서 `partitionLead`를 부른다(`FetchCollector.java:153`, `:194`). `AsyncKafkaConsumer`에서는 백그라운드 스레드가 그 사이에 할당을 지울 수 있다는 것이 가설이다. 같은 코드가 4.1.2에도 있으므로 4.2.0만의 문제로 보지 않는다. 재현과 원인 확정은 하지 않았다. 멤버가 죽어도 복구할 수 있도록 `client-upgrade.sh roll-all`은 멈춘 멤버까지 교체한다.
