# 03. 비교 실험: 5개 설정 × 4개 시나리오

결과표 20칸을 채우는 절차다. 각 시나리오에서 어떤 메커니즘이 동작하는지, run이 끝난 뒤 무엇을 확인할지, 근거가 되는 코드가 어디에 있는지를 함께 적는다.

## 0. 진행 순서

1. `./scripts/cluster.sh up`으로 클러스터를 띄우고 [01-environment.md](../01-environment.md)의 확인 항목을 본다.
2. 시나리오 하나를 고르고, 5개 설정을 연달아 실행한다. 같은 시나리오끼리 이어서 보면 설정 간 차이가 눈에 잘 들어온다.
3. run이 끝날 때마다 출력되는 `summary.md`를 보고, 아래 "확인할 것"에 대한 답을 메모한다. 숫자가 예상과 다르면 `events-*.csv`와 `broker-group-*.log`에서 원인을 찾는다.
4. 4개 시나리오를 모두 마치면 같은 매트릭스를 1~2회 더 돌린다. 하트비트 주기 때문에 1회 측정값은 수 초씩 흔들린다.
5. `python3 scripts/analyze.py table` 출력을 [results/RESULTS.md](../../results/RESULTS.md)에 옮기고 해석을 쓴다.

소요 시간(기본 파라미터): rolling-restart 약 5분, kill9·stall·scale-out 각 약 2분 30초. 매트릭스 1회에 약 60분 걸린다.

```bash
./scripts/run.sh <setting> <scenario>              # 1칸
SCENARIOS="kill9" ./scripts/run-matrix.sh          # 한 시나리오 × 5개 설정
SETTINGS="eager consumer" ./scripts/run-matrix.sh  # 두 설정 × 4개 시나리오
./scripts/run-matrix.sh                            # 20칸 전체
python3 scripts/analyze.py table                   # 설정별 최신 run으로 결과표 출력
python3 scripts/analyze.py run results/runs/<RUN_ID>  # 한 run 재분석
```

run마다 새 group id(`lab-<RUN_ID>`)를 쓴다. 이전 run의 커밋 오프셋이나 static member 정보가 다음 run에 섞이지 않는다.

## 1. 설정

| 설정 | `group.protocol` | 할당 계산 | 리밸런싱 방식 | `group.instance.id` |
|---|---|---|---|---|
| `eager` | classic | 클라이언트(그룹 리더), `RangeAssignor` | EAGER: JoinGroup 전에 모든 멤버가 전체 파티션을 revoke | 없음 |
| `cooperative` | classic | 클라이언트(그룹 리더), `CooperativeStickyAssignor` | COOPERATIVE: 이동할 파티션만 revoke하고 다음 라운드에서 assign (KIP-429) | 없음 |
| `cooperative-static` | classic | 위와 같음 | 위와 같음 | 컨테이너 이름 (KIP-345) |
| `consumer` | consumer | 브로커 코디네이터, `uniform` | 하트비트마다 멤버별 증분 조정, 그룹 barrier 없음 (KIP-848) | 없음 |
| `consumer-static` | consumer | 위와 같음 | 위와 같음 | 컨테이너 이름 |

`eager`는 4.1 기본값을 재현한 설정이다. 기본 `partition.assignment.strategy`는 `[RangeAssignor, CooperativeStickyAssignor]`이고, 그룹은 모든 할당자가 지원하는 프로토콜 중 하나를 고르므로 EAGER로 동작한다. 기본값으로 운영 중인 classic 그룹은 cooperative가 아니라는 점을 이 설정으로 확인한다.

## 2. 시나리오

### 2.1 rolling-restart

**동작**: consumer-1부터 consumer-6까지 `docker compose restart`(SIGTERM → `close()` → 재기동)를 순서대로 실행한다. 재시작한 멤버가 파티션을 다시 받고 그룹이 `Stable`이 된 뒤 `ROLL_INTERVAL_SEC`(10초)를 기다렸다가 다음 멤버로 넘어간다. 배포 시 롤링 재시작을 재현한다.

**동작하는 메커니즘**

- dynamic member는 `close()` 시 그룹을 떠난다. classic은 LeaveGroup 요청을 보내고, consumer는 member epoch `-1`로 하트비트를 보낸다. 떠날 때 한 번, 돌아올 때 한 번 리밸런싱이 일어난다.
- static member는 `close()` 시 그룹에 남는다. classic은 LeaveGroup을 보내지 않고, consumer는 epoch `-2`로 "일시 이탈"을 알린다. 세션 타임아웃 안에 같은 `group.instance.id`로 돌아오면 기존 할당을 그대로 받는다.

**확인할 것**

1. 멤버당 리밸런싱 횟수(`rebalances_per_member_max`)는 설정별로 몇 회인가. dynamic이라면 재시작 6번에 몇 번이 나와야 맞는가.
2. eager와 cooperative의 revoke 파티션 수 합을 비교한다. eager는 리밸런싱마다 12개 전체가 revoke된다. cooperative는 몇 개인가.
3. static 설정에서 정지 파티션이 재시작한 멤버의 파티션으로 한정되는가. 최대 정지 시간은 컨테이너 재시작에 걸린 시간과 비슷한가.
4. consumer와 cooperative의 정지 파티션 수, 그룹 비안정 누적 시간 차이는 어디서 오는가. consumer에는 JoinGroup/SyncGroup barrier가 없다.
5. 중복 건수가 0인가. 0이 아니면 revoke 콜백의 커밋이 실패한 경우다. `COMMIT_FAILED` 이벤트를 찾는다.

**코드 근거 (4.1.2)**

- `clients/.../internals/AbstractCoordinator.java`: `maybeLeaveGroup`, `shouldSendLeaveGroupRequest`. static member는 `CloseOptions.GroupMembershipOperation.DEFAULT`에서 LeaveGroup을 보내지 않는다.
- `clients/.../internals/ConsumerCoordinator.java`: `onJoinPrepare`에서 generation이 초기화된 경우는 전체를 lost로 처리하고, 그 외에는 EAGER가 전체 revoke, COOPERATIVE는 구독에서 빠진 파티션만 revoke한다. `onJoinComplete`에서 COOPERATIVE는 새 할당에서 빠진 파티션을 revoke하고 다시 합류를 요청한다.
- `clients/.../internals/ConsumerMembershipManager.java`: `leaveGroupEpoch`가 static이면 `-2`, 아니면 `-1`을 고른다.
- `group-coordinator/.../GroupMetadataManager.java`: `consumerGroupStaticMemberGroupLeave`가 epoch `-2`인 멤버의 할당을 유지한다.

### 2.2 kill9

**동작**: `docker kill --signal KILL consumer-3`. 프로세스가 정리 작업 없이 사라진다. 노드 장애, OOM kill을 재현한다. 관찰 시간 90초.

**동작하는 메커니즘**: 멤버가 떠난다는 신호가 없으므로 코디네이터가 세션 타임아웃(45초) 동안 하트비트를 받지 못해야 멤버를 제거한다. classic은 브로커가 클라이언트가 보낸 `session.timeout.ms`로 타이머를 돌리고, consumer는 브로커 설정 `group.consumer.session.timeout.ms`를 쓴다.

**확인할 것**

1. 트리거→첫 콜백 시간이 약 45초인가. 두 프로토콜의 차이는 몇 초이고, 하트비트 주기(classic 3초, consumer 5초)로 설명되는가.
2. 감지 전 45초 동안 consumer-3의 파티션 2개가 멈춘다. 정지 파티션·초 합이 2 × 45초 근처인가.
3. 감지 후 리밸런싱에서 eager는 나머지 10개 파티션도 멈추는가. cooperative, consumer는 어떤가.
4. 비재할당 정지 파티션이 생겼다면 원인은 무엇인가. 새 파티션을 받은 멤버가 45초치 backlog(약 2,250건)를 처리하는 동안 기존 파티션 처리가 밀린 것인지 `pause_by_partition_ms`로 확인한다.
5. 중복 건수는 몇 건인가. 상한은 "마지막으로 커밋에 성공한 이후 처리해 기록한 레코드 수"다. `max.poll.records`, 커밋 방식과 어떤 관계인가.
6. static 설정은 dynamic과 차이가 있는가. 세션 만료 전까지는 둘 다 멤버를 기다린다.

**응용**: kill 직후 같은 instance id로 바로 재기동하면(`docker kill -s KILL consumer-3 && docker start consumer-3`) classic static과 consumer static의 동작이 다르다. consumer 그룹에서는 기존 멤버의 epoch이 `-2`가 아니면 새 멤버가 `UNRELEASED_INSTANCE_ID`로 거절된다(`GroupMetadataManager.throwIfInstanceIdIsUnreleased`). classic static은 어떻게 되는지 `container-consumer-3.log`와 브로커 로그로 확인한다.

### 2.3 stall (처리 지연, poll interval 초과)

**동작**: `control/consumer-3.stall`에 `MAX_POLL_INTERVAL_MS + 15000`(기본 45초)을 쓴다. consumer-3은 다음 배치를 처리하기 전에 45초 동안 멈춘다. `poll()` 사이 간격이 `max.poll.interval.ms`(30초)를 넘는다. 외부 API 지연, 긴 GC, 처리 로직 병목을 재현한다.

**동작하는 메커니즘**: 프로세스는 살아 있고 하트비트를 보내는 스레드도 살아 있다. 애플리케이션 스레드만 멈춘다.

- classic: heartbeat 스레드가 poll timeout을 감지하고 `maybeLeaveGroup`을 호출한다. dynamic은 LeaveGroup을 보내고, static은 보내지 않는다. 두 경우 모두 로컬 generation은 초기화된다.
- consumer: 백그라운드 스레드가 poll timer 만료를 감지하고 leave 하트비트를 보낸 뒤 `STALE` 상태로 다음 `poll()`을 기다린다. static은 epoch `-2`로 떠난다.
- 지연이 끝나면 consumer-3은 이미 받아 둔 배치를 마저 처리한다. 그 사이 파티션이 다른 멤버로 넘어갔다면 같은 레코드를 두 멤버가 처리한다.

**확인할 것**

1. 트리거→첫 콜백 시간이 약 30초(`max.poll.interval.ms`)인가. static 설정에서는 언제인가.
2. consumer-3의 파티션이 다른 멤버에게 넘어갔는가(재할당 파티션 수). static과 dynamic의 차이를 설명한다.
3. 지연이 끝난 뒤 consumer-3에서 `LOST` 콜백이 호출되는가, `REVOKED`가 호출되는가. 커밋은 어떤 예외로 실패하는가(`COMMIT_FAILED` 이벤트의 예외 이름).
4. 중복 건수는 몇 건이고 어느 파티션에서 생겼는가. 상한은 지연 직전에 받은 배치 크기(`max.poll.records=100`)와 관련이 있는가.
5. 리밸런싱 횟수는 몇 회인가. 이탈할 때와 재합류할 때 각각 일어나는가.

**코드 근거 (4.1.2)**

- `AbstractCoordinator.HeartbeatThread.run`에서 `heartbeat.pollTimeoutExpired` → `handlePollTimeoutExpiry` → `maybeLeaveGroup`
- `clients/.../internals/AbstractHeartbeatRequestManager.java`: `pollTimer.isExpired()` → `transitionToSendingLeaveGroup(true)`
- `clients/.../internals/AbstractMembershipManager.java`: `onHeartbeatRequestGenerated`에서 poll timer 만료로 떠난 멤버는 `transitionToStale`, 다음 poll에서 `maybeRejoinStaleMember`

### 2.4 scale-out

**동작**: consumer-1~3(멤버당 파티션 4개)으로 시작해 안정화한 뒤 consumer-4~6을 동시에 띄운다. 그룹이 6멤버로 안정화되면 90초 관찰한다. 트래픽 증가에 대응한 증설을 재현한다.

**동작하는 메커니즘**: 새 멤버 합류가 리밸런싱을 일으킨다. 기존 멤버는 파티션 4개 중 2개를 내놓아야 한다.

- eager: 기존 3개 멤버가 12개 전체를 revoke하고 다시 받는다.
- cooperative: 첫 라운드에서 이동할 6개만 revoke하고, 두 번째 라운드에서 새 멤버에게 assign한다.
- consumer: 코디네이터가 목표 할당(target assignment)을 계산하고, 기존 멤버가 내놓을 파티션을 revoke해 하트비트로 알린 뒤에 새 멤버에게 그 파티션을 준다.

**확인할 것**

1. revoke 파티션 수 합: eager 12, cooperative·consumer 6이 나오는가.
2. cooperative의 멤버당 리밸런싱 횟수는 몇 회인가. 두 라운드가 모두 잡히는가.
3. consumer의 `group_epoch_delta`는 얼마인가. 멤버 3개가 거의 동시에 들어오면 epoch이 몇 번 오르는가. `members.csv`에서 기존 멤버의 member epoch이 언제 따라오는지 본다.
4. 정지 파티션은 이동한 6개로 한정되는가. 이동한 파티션의 정지 시간은 설정별로 얼마인가.

## 3. 결과 기록

```bash
python3 scripts/analyze.py table > /tmp/table.md
```

출력에는 측정 항목별로 5×4 표 네 개와 각 칸의 출처 run ID가 있다. [results/RESULTS.md](../../results/RESULTS.md)에 옮긴 뒤 표 아래 해석 항목을 채운다. 반복 측정한 경우 `table`은 칸마다 가장 최근 run만 보여 주므로, 중앙값은 `results/runs/*/summary.json`에서 직접 골라 적는다.

해석은 "숫자 → 메커니즘 → 코드" 순서로 쓴다.

> 형식 예시: "kill9에서 트리거→첫 콜백은 cooperative A초, consumer B초다(3회 중앙값). 두 값 모두 세션 타임아웃 45초 근처이고, 차이는 하트비트 주기(3초/5초) 범위 안이다. 따라서 장애 감지 시간은 프로토콜보다 세션 타임아웃 설정이 결정한다. 근거: 두 프로토콜 모두 브로커가 멤버 만료를 판정한다. classic은 `GroupMetadataManager.rescheduleClassicGroupMemberHeartbeat`, consumer는 `GroupMetadataManager.scheduleConsumerGroupSessionTimeout`."

이 해석이 `results/RESULTS.md`의 프로토콜 대비표를 채우는 근거가 된다.

## 4. 응용 실험 (선택)

| 실험 | 방법 | 확인할 것 |
|---|---|---|
| 세션 타임아웃 조정 | classic: `SESSION_TIMEOUT_MS=10000 ./scripts/run.sh cooperative kill9`. consumer: 그룹 동적 설정 `kafka-configs.sh --bootstrap-server broker-1:9092 --alter --entity-type groups --entity-name <group> --add-config consumer.session.timeout.ms=60000` | 감지 시간과 오탐(일시 지연에 의한 이탈) 사이의 trade-off. consumer 그룹 값은 브로커의 `group.consumer.min/max.session.timeout.ms`(45000/60000) 범위로 제한된다. |
| 서버 할당자 변경 | `REMOTE_ASSIGNOR=range ./scripts/run.sh consumer scale-out` | uniform과 range의 이동 파티션 수 차이 |
| 처리 부하 증가 | `PROCESS_MS=6 ./scripts/run.sh cooperative kill9` | 여유 처리량이 줄면 비재할당 정지와 lag 회복 시간이 어떻게 변하는가 |
| 배치 크기 | `MAX_POLL_RECORDS=500 ./scripts/run.sh consumer stall` | 지연 시 중복 건수 상한 |
| 긴 재시작 | `STATIC_MEMBERSHIP=true` 설정에서 `docker compose stop consumer-2`, 60초 후 `start` | 세션 타임아웃을 넘긴 static member가 돌아올 때의 동작 |
