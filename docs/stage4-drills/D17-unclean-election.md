# D17. 랙 장애 후 unclean leader election 판단

## 목적

ISR에 살아 있는 복제본이 하나도 없을 때 "기다릴 것인가, 유실을 감수하고 뒤처진 복제본을 리더로 세울 것인가"를 판단한다. 4.1에서 기본으로 켜지는 ELR(KIP-966)이 이 판단을 어떻게 바꾸는지 코드와 실측으로 확인하고, 강제 선출의 유실 크기를 직접 잰다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | broker-3은 정기 패치로 내려가 있었다. 그동안 broker-1, broker-2가 있는 랙의 전원이 나갔다. 패치를 마친 broker-3이 먼저 돌아왔다. broker-1, broker-2의 복구 시점은 알 수 없다. |
| 주입 | broker-3 정지(controlled shutdown) → 60초 → broker-1, broker-2 SIGKILL → broker-3 시작 |
| 예상 증상 | 브로커 1대가 살아 있는데 모든 파티션이 offline이다. 생산·소비가 멈춘다. |
| 모범 조치 | 기본: broker-1, broker-2를 복구한다. 복구가 불가능하다고 판단될 때만 unclean election으로 broker-3을 리더로 세우고 유실을 기록한다. |
| 소요 | 25분 |

```bash
./scripts/drill.sh start D17                    # 기다리는 쪽 모범 조치
D17_FIX=unclean ./scripts/drill.sh start D17    # drill.sh fix가 강제 선출을 하고 유실을 잰다
```

## 진행 방식

1. 알람을 본다. `BrokerDown`, `OfflinePartitions`가 page로 울린다.
2. 파티션 상태를 본다. Leader, Isr, Elr, LastKnownElr 열을 읽는다:
   ```bash
   docker exec -e KAFKA_OPTS= broker-3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker-3:9092 --describe --topic lab-events
   ```
3. broker-3이 왜 리더가 될 수 없는지 설명한다. broker-3에 없는 데이터는 무엇이고 얼마나 되는가.
4. 판단을 내린다. 기다리면 언제 돌아오는가, 강제로 세우면 무엇을 잃는가. 서비스 관점(유실과 중단 중 무엇이 비싼가)으로 결정한다.
5. 결정에 따라 조치한다.
   - 기다리는 쪽: `docker start broker-1 broker-2`. 복구된 뒤 파티션 상태와 `UncleanLeaderElection` 알람을 다시 본다.
   - 강제 선출 쪽: `kafka-leader-election.sh --bootstrap-server broker-3:9092 --election-type UNCLEAN --all-topic-partitions`. 선출 직후의 log end offset을 장애 직전과 비교한다.
6. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- 기다리는 쪽을 골랐는데도 `UncleanLeaderElection` 알람이 울린다. 무엇이 선출됐는가
- 강제 선출 쪽에서 사라진 레코드 수가 broker-3이 내려가 있던 시간 × 생산 속도와 맞는가
- 강제 선출 뒤 broker-1, broker-2가 돌아오면 그들이 가진 데이터는 어떻게 되는가

---

## 해설

### 메커니즘

broker-3이 정상 종료하면 ISR에서 빠지고 ISR은 {broker-1, broker-2}가 된다. 이 둘이 동시에 죽으면 리더 후보가 없다. broker-3은 60초 분량(약 36,000건)을 갖고 있지 않으므로 ISR 밖이고, unclean election이 꺼져 있는 한 리더가 될 수 없다.

4.1.2에서 새로 만든 클러스터는 `eligible.leader.replicas.version=1`(ELR)이 켜져 있다. 동작이 세 단계에서 달라진다.

1. 장애 시점: ISR이 `min.insync.replicas` 아래로 줄면, 펜싱돼 ISR에서 빠지는 복제본을 ELR(리더 자격 유지 목록)에 넣는다. 마지막 리더는 LastKnownElr에 기록된다. broker-1, broker-2는 ELR에 들어가고 broker-3은 들어가지 않는다.
2. 복구 시점: SIGKILL로 죽은 브로커는 다음 등록 때 "비정상 종료"로 판정된다(이전 브로커 epoch를 증명하지 못한다). 컨트롤러는 비정상 종료한 복제본을 ELR에서도 뺀다. 로그 끝부분이 디스크에 안 내려갔을 수 있어서다.
3. 그 결과 ISR과 ELR이 모두 비면, 컨트롤러는 LastKnownElr에 남은 마지막 리더 하나를 리더로 세운다("balanced recovery"). 이 선출은 unclean으로 집계되고 ISR이 그 리더 하나로 초기화된다.

그래서 "기다리는 쪽"을 고르고 broker-1, broker-2를 다시 켜기만 해도 컨트롤러가 자동으로 unclean 선출을 하고, `UncleanLeaderElection` 알람이 울린다. 실측에서 매번 24건(`lab-events` 12 + `__consumer_offsets` 12)이 집계됐다. 마지막 리더는 장애 직전까지 모든 쓰기를 받았으므로 이 레포(tmpfs, 컨테이너 SIGKILL)에서는 유실이 없다. 실제 정전에서는 페이지 캐시에만 있던 마지막 쓰기가 사라질 수 있고, 이 선출이 그 유실을 확정한다.

강제 선출 쪽은 다르다. 마지막 리더도 살아 있지 않으므로 컨트롤러는 ISR 밖의 살아 있는 복제본(broker-3)을 고른다. broker-3에 없는 구간이 그대로 사라진다. 나중에 broker-1, broker-2가 돌아오면 새 리더(broker-3)의 로그에 맞춰 자기 로그를 잘라낸다. 사라진 레코드는 어디에도 남지 않는다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | `BrokerDown`, `OfflinePartitions`(page), `ProducerThroughputDrop` | 리더 없는 파티션 |
| 범위 | 전 파티션 offline. 살아 있는 브로커는 broker-3 하나 | 복수 브로커 장애 |
| 원인 | `--describe`에서 Leader none, Isr 비어 있음, Elr에 broker-1·2, broker-3은 어느 목록에도 없음 | 살아 있는 복제본이 리더 자격이 없다 |
| 유실 추정 | broker-3이 내려간 시각과 랙 장애 시각의 차이 × 생산 속도. 파티션별 log end offset으로 확인 | 강제 선출의 비용 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| broker-1, broker-2 복구를 기다림 | 실측 기준 유실 없음. 4.1에서는 마지막 리더가 자동 선출된다 | 복구까지 전 파티션 중단. 복구 시점을 모르면 중단 시간도 모른다 |
| 토픽 단위 unclean election(`--topic`, `--partition`) | 중요도가 낮은 토픽만 먼저 살린다 | 해당 토픽은 broker-3에 없는 구간을 잃는다 |
| 전체 unclean election | 즉시 재개 | 실측 36,860건 유실(파티션당 약 3,072건 = 60초 × 600 msg/s ÷ 12). `__consumer_offsets`도 되돌아가 커밋 오프셋이 과거로 돌아간다. 컨슈머가 같은 레코드를 다시 처리한다 |
| 토픽 설정 `unclean.leader.election.enable=true` | 컨트롤러가 알아서 강제 선출한다 | 결정을 사람이 하지 않게 된다. 장애가 끝나면 반드시 되돌린다 |

어느 쪽이든 결정과 근거(예상 복구 시각, 유실 추정치)를 기록하고 다운스트림에 알린다. 강제 선출을 했다면 유실 구간(파티션별 오프셋 범위)을 남겨 재전송이 가능한지 확인한다.

### 프로듀서가 회복하지 않는 경우

기다리는 쪽 selftest 2회 중 1회, 파티션이 모두 복구된 뒤에도 랩 프로듀서가 리더가 broker-1인 파티션 4개(1, 4, 8, 11)를 broker-3으로 계속 보내 `NOT_LEADER_OR_FOLLOWER`를 받았다. 브로커 쪽 오류 지표(`kafka_network_requestmetrics_errors_total{request="Produce",error="NOT_LEADER_OR_FOLLOWER"}`)는 broker-3에서만 1,082건 늘었다. 메타데이터는 세 브로커 모두 broker-1을 리더로 보여 줬고, 새로 띄운 프로듀서는 정상으로 썼다. 원인은 확정하지 못했다. 클라이언트 `Metadata`가 이미 본 것보다 낮은 leader epoch의 메타데이터를 무시하는 규칙(`clients/.../Metadata.java:583~593`)과 관련이 있을 것으로 보지만 재현 조건을 아직 좁히지 못했다.

그래서 모범 조치는 마지막 단계로 "파티션 복구 후 90초 안에 확인 처리량이 돌아오지 않으면 프로듀서를 재시작한다"를 둔다. 실무에서도 브로커 쪽이 정상인데 특정 클라이언트만 오류를 계속 받으면 그 클라이언트의 재시작이 가장 빠른 조치다. 이 현상의 재현과 원인 확정은 Stage 3 방법을 적용할 다음 후보다.

### 코드 근거 (4.1.2)

`metadata/src/main/java/org/apache/kafka/controller/` 아래 파일이다.

- 비정상 종료 판정: `ClusterControlManager.registerBroker()`(`ClusterControlManager.java:432~436`)는 새 incarnation이 등록할 때 이전 브로커 epoch가 저장된 값과 같으면 정상 종료로 본다. SIGKILL은 이 값을 남기지 못한다.
- ELR에서 제거: `ReplicationControlManager.handleBrokerShutdown()`(`ReplicationControlManager.java:1445`)은 ELR이 켜져 있고 비정상 종료면 ISR과 ELR 양쪽에서 그 브로커를 빼는 변경을 만든다(1447~1451행). `PartitionChangeBuilder.maybePopulateTargetElr()`(`PartitionChangeBuilder.java:536`)도 557행에서 비정상 종료 복제본을 ELR 후보에서 거른다.
- 마지막 리더 기록: `PartitionChangeBuilder.maybeUpdateLastKnownLeader()`(495행)는 파티션이 처음 리더를 잃을 때 직전 리더를 LastKnownElr에 넣는다.
- 선출: `electAnyLeader()`(260행)는 ISR(또는 ISR이 비었을 때 ELR) 안에서 먼저 찾고, 없으면 `canElectLastKnownLeader()`(290행)로 마지막 리더를 unclean으로 세운다(273~275행). 강제 선출(`Election.UNCLEAN`)일 때만 그다음으로 아무 살아 있는 복제본을 고른다(277~284행).
- ISR 초기화: `tryElection()`(328행)은 unclean 선출이면 ISR을 새 리더 하나로 바꾸고(351행) LeaderRecoveryState를 RECOVERING으로 둔다.

### 재발 방지

- 알람: `OfflinePartitions`(10초, page), `UncleanLeaderElection`(최근 10분 증가, page). 후자는 사람이 결정하지 않은 자동 선출도 잡는다.
- 복제본을 랙에 나눠 배치한다(`broker.rack`). 정기 패치로 브로커를 내릴 때는 다른 장애 도메인의 상태를 먼저 확인한다.
- 정전 대비가 중요한 토픽은 `flush.messages`·`flush.ms`보다 복제본을 다른 장애 도메인에 두는 편이 낫다. 동기 flush는 처리량 비용이 크다.

### 재현 충실도

컨테이너 SIGKILL은 프로세스만 죽이고 tmpfs의 데이터는 남긴다. 실제 정전에서는 디스크에 내려가지 않은 쓰기가 사라질 수 있어, 자동 선출된 마지막 리더도 일부를 잃을 수 있다. 4.0.1 이하나 ELR이 꺼진 클러스터에서는 자동 선출 단계가 없으므로 기다리는 쪽이 진짜 "unclean 없이 기다림"이 된다.
