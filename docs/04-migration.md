# 04. 마이그레이션 실습: classic ↔ consumer

운영 중인 classic 그룹을 중단 없이 consumer 프로토콜로 옮기고, 혼합 상태의 동작을 측정한 뒤 다시 classic으로 되돌린다. 산출물은 [마이그레이션 런북](templates/migration-runbook.md)이다.

## 1. 배경: 브로커가 그룹을 전환하는 조건

전환은 브로커의 `GroupMetadataManager`가 판단한다. 클라이언트 설정만 바꾸면 되고 별도 명령은 없다. 아래 조건은 4.1.2 코드에서 확인한 내용이다.

### classic → consumer (온라인 업그레이드)

비어 있지 않은 classic 그룹에 consumer 프로토콜 멤버의 첫 `ConsumerGroupHeartbeat`가 들어오면 `validateOnlineUpgrade`를 거쳐 `convertToConsumerGroup`이 실행된다. 다음 중 하나라도 해당하면 전환을 거부하고 그 멤버는 `GroupIdNotFoundException`을 받는다.

| 조건 | 브로커 로그 |
|---|---|
| `group.consumer.migration.policy`가 `upgrade` 또는 `bidirectional`이 아니다 | `Cannot upgrade classic group ... because online upgrade is disabled.` |
| 그룹이 consumer embedded protocol(`protocolType=consumer`)을 쓰지 않는다 | `... because the group does not use the consumer embedded protocol.` |
| 멤버 수가 `group.consumer.max.size`를 넘는다 | `... because the group size exceeds the consumer group maximum size.` |
| 할당에 사용자 데이터(userData)가 있다. 커스텀 할당자가 해당한다. | `... because an unsupported custom assignor is in use.` |

성공하면 `[GroupId G] Converted the classic group to a consumer group.`이 남는다. 그룹이 비어 있을 때는 `Converted the empty classic group to a consumer group.`이 남는다(오프라인 전환).

### consumer → classic (온라인 다운그레이드)

consumer 프로토콜 멤버가 떠나거나 세션 만료로 제거될 때, 남은 멤버가 모두 classic 프로토콜이면 `validateOnlineDowngradeWithFencedMembers`를 거쳐 `convertToClassicGroup`이 실행된다.

| 조건 | 브로커 로그 |
|---|---|
| 남은 멤버가 모두 classic 프로토콜이다 | (아니면 전환 시도 없음) |
| 남은 멤버가 1명 이상이다 | 비어 있으면 debug 로그만 남는다 |
| 정책이 `downgrade` 또는 `bidirectional`이다 | `Cannot downgrade consumer group ... because the online downgrade is disabled.` |
| 남은 멤버 수가 `group.max.size` 이하다 | `... because its group size is greater than classic group max size.` |

성공하면 `Preparing to rebalance group G ... (reason: Downgrade group G from consumer to classic for member leaving.)`와 `[GroupId G] Converted the consumer group to a classic group.`이 남는다.

### 혼합 상태에서 classic 멤버가 처리되는 방식

전환 후 classic 멤버는 계속 JoinGroup/SyncGroup을 보낸다. consumer 그룹은 이 요청을 받아 consumer 그룹의 멤버로 관리한다(`classicGroupJoinToConsumerGroup`). 이때 다음 두 가지가 바뀐다.

- 할당은 classic 멤버의 클라이언트 할당자(`cooperative-sticky`)가 아니라 브로커의 서버 할당자(`uniform`)가 계산한다. 로그에 `Computed a new target assignment for epoch N with 'uniform' assignor`가 남는다.
- classic 멤버의 세션 타임아웃은 그 멤버가 JoinGroup에 실어 보낸 `session.timeout.ms`를 쓴다(`classicProtocolSessionTimeout`).

## 2. 절차

`scripts/migration.sh`는 run 디렉터리 하나를 이어 쓰며, 단계마다 마커를 남긴다. 단계 사이에 `status`로 상태를 확인하며 진행한다.

### 2.1 시작: classic 그룹 운영

```bash
./scripts/migration.sh start
```

- `cooperative-sticky` classic 그룹 6멤버, 프로듀서, lag-monitor가 뜬다. `ASSIGNOR=range ./scripts/migration.sh start`로 eager 그룹에서 시작할 수도 있다.
- `status` 출력에서 `type=Classic state=Stable members=6`을 확인한다. `upgraded` 열은 classic 그룹이라 비어 있다.

### 2.2 첫 consumer 멤버 투입과 자동 전환

```bash
./scripts/migration.sh upgrade consumer-1
```

consumer-1의 `control/consumer-1.protocol`에 `consumer`를 쓰고 재시작한다. 출력에서 다음을 확인한다.

1. `== group`: `type=Consumer`, `consumer_protocol_members=1`, `classic_protocol_members=5`
2. `== members`: consumer-1만 `upgraded=true`, 나머지는 `false`. 모든 멤버에 member epoch이 생겼다.
3. `== broker log`: `Converted the classic group to a consumer group.` 이 메시지를 남긴 브로커가 그룹 코디네이터다.

직접 확인한다.

```bash
G=lab-YYYYMMDD-HHMMSS-migration   # status 첫 줄의 group 값
docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group $G --state
docker exec broker-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker-1:9092 --describe --group $G --members --verbose
docker logs broker-2 2>&1 | grep -F "$G" | tail -30    # 코디네이터 브로커로 바꾼다
```

**확인할 것**

1. 전환 순간에 classic 멤버들은 어떤 콜백을 받는가. `events-consumer-2.csv` 등에서 `REVOKED`/`ASSIGNED`가 생겼는지, 파티션이 실제로 이동했는지 본다. `convertToConsumerGroup`은 진행 중인 JoinGroup/SyncGroup을 `REBALANCE_IN_PROGRESS`로 끝낸다.
2. 전환 직후 할당이 바뀌었는가. 바뀌었다면 classic 할당자(`cooperative-sticky`)의 결과와 서버 할당자(`uniform`)의 결과가 달랐기 때문인가.
3. `--describe --state`의 `ASSIGNMENT-STRATEGY`가 전환 전(`cooperative-sticky`)과 전환 후에 어떻게 다른가. 시작 시점 값은 run 디렉터리의 `describe-start.txt`에 있다.

### 2.3 혼합 상태에서 장애 측정

멤버 일부만 consumer 프로토콜인 상태에서 시나리오를 실행한다. 예를 들어 3개를 올린 뒤 classic 멤버와 consumer 멤버에 각각 장애를 낸다.

```bash
./scripts/migration.sh upgrade consumer-2
./scripts/migration.sh upgrade consumer-3
./scripts/migration.sh trigger kill9 consumer-5     # classic 멤버 장애
sleep 60; ./scripts/migration.sh status
docker start consumer-5; sleep 20
./scripts/migration.sh trigger stall consumer-2     # consumer 멤버 지연
sleep 60; ./scripts/migration.sh status
./scripts/migration.sh trigger restart consumer-6   # classic 멤버 재시작
```

`trigger`는 마커를 남긴다. 측정값은 마지막에 `finish`가 관찰 구간 전체로 계산한다. 장애별로 따로 보려면 `markers.csv`의 시각으로 `events-*.csv`, `group.csv`, `lag.csv`를 잘라 본다.

**확인할 것**

1. classic 멤버의 장애가 consumer 멤버의 처리를 멈추는가. classic 멤버의 재합류(JoinGroup)가 그룹 전체 barrier를 만드는가, 해당 멤버만 기다리는가.
2. classic 멤버 kill 시 감지 시간은 45초(`session.timeout.ms`)인가.
3. 혼합 상태의 리밸런싱 시간과 정지 파티션 수를 03의 `cooperative`, `consumer` 결과와 비교한다. 어느 쪽에 가까운가.

### 2.4 전체 전환

```bash
./scripts/migration.sh upgrade-all
```

모든 멤버가 `upgraded=true`가 되면 마이그레이션이 끝난 것이다. 이미 consumer인 멤버도 다시 재시작되므로 이 단계 자체가 consumer 그룹의 롤링 재시작 측정이 된다.

**확인할 것**

1. 2.2만 하고 바로 이 단계로 오면, consumer 멤버가 consumer-1 하나뿐인 상태에서 consumer-1이 다시 재시작된다. 이때 브로커 로그에 `Converted the consumer group to a classic group.`과 `Converted the classic group to a consumer group.`이 연달아 남는지 본다. 유일한 consumer 멤버가 떠나면 남은 멤버가 모두 classic이므로 다운그레이드 조건이 성립한다. 실제 배포에서 첫 인스턴스만 올린 뒤 그 인스턴스를 다시 재시작하면 같은 일이 일어난다.
2. 전환·역전환이 반복될 때 정지 파티션 수와 lag이 어떻게 변하는가.

### 2.5 다운그레이드

```bash
./scripts/migration.sh downgrade-all
```

consumer-1부터 차례로 classic으로 되돌린다. 마지막 consumer 멤버(consumer-6)가 떠나는 순간 그룹이 classic으로 전환된다.

**확인할 것**

1. 몇 번째 멤버에서 `Converted the consumer group to a classic group.`이 남는가. 그 직전 상태는 `consumer_protocol_members=1`인가.
2. 다운그레이드 직후 classic 리밸런싱(`Preparing to rebalance group`, `Stabilized group`)이 일어나는가. 그때 정지 파티션 수는 몇 개인가.
3. 다운그레이드 후 할당은 다시 classic 그룹 리더가 계산하는가. 브로커 로그의 `Assignment received from leader`로 확인한다.

### 2.6 종료와 분석

```bash
./scripts/migration.sh finish
```

로그를 모으고 컨테이너를 정리한 뒤 전체 구간을 분석한다. 결과 run 디렉터리는 `results/runs/<날짜>-migration`이다.

## 3. 변형 실습 (선택)

| 실습 | 방법 | 확인할 것 |
|---|---|---|
| 업그레이드 차단 | `MIGRATION_POLICY=downgrade`로 `cluster.sh reset` 후 `start` → `upgrade consumer-1` | consumer-1이 받는 오류와 브로커 로그. 그룹은 classic으로 남는가. |
| 다운그레이드 차단 | `MIGRATION_POLICY=upgrade`로 reset 후 전체 업그레이드 → `downgrade-all` | 마지막 멤버가 떠난 뒤 그룹 type. classic 멤버만 남은 consumer 그룹은 어떻게 동작하는가. |
| 오프라인 전환 | `start` 후 consumer 6개를 모두 정지하고, protocol 파일을 모두 `consumer`로 쓴 뒤 동시에 기동 | `Converted the empty classic group to a consumer group.`, 중단 시간과 lag |
| static member 마이그레이션 | `STATIC_MEMBERSHIP=true ./scripts/migration.sh start` | 같은 instance id로 프로토콜만 바꿔 재시작할 때의 동작. `validateOnlineDowngradeWithReplacedMember` 경로 |

`MIGRATION_POLICY`는 브로커 설정이므로 `MIGRATION_POLICY=upgrade ./scripts/cluster.sh reset`처럼 클러스터를 다시 만들 때 지정한다.

## 4. 런북 작성

실습이 끝나면 [templates/migration-runbook.md](templates/migration-runbook.md)를 `results/migration-runbook.md`로 복사해 채운다. 런북에는 이 실습에서 직접 본 로그 문구, 상태 값, 측정값을 근거로 적는다. 실습에서 확인하지 않은 항목은 "미확인"으로 남긴다.
