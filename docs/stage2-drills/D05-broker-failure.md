# D05. 브로커 1대 비정상 종료

## 목적

브로커 1대가 갑자기 죽었을 때 클러스터가 스스로 처리하는 일(fencing, 리더 재선출, ISR 축소)과 운영자가 해야 하는 일(재기동, 복제 따라잡기 확인, 리더 재분배)을 구분한다. 서비스는 계속되지만 내구성 여유가 없어진 상태를 정확히 읽는 것이 목표다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | broker-2 호스트가 커널 패닉으로 재부팅됐다. |
| 주입 | `docker kill --signal KILL broker-2` |
| 예상 증상 | under-replicated partitions가 생긴다. 프로듀서와 컨슈머는 짧은 지연 후 계속 동작한다. |
| 모범 조치 | 브로커 재기동, URP 0 확인, preferred leader election |
| 소요 | 15분 |

## 진행 방식

```bash
./scripts/drill.sh start D05
```

1. Overview의 클러스터 상태 행에서 Brokers up, Active controller, Under-replicated, Under min ISR, Offline partitions를 본다.
2. Cluster의 Leader count by broker로 리더가 어느 브로커로 옮겨 갔는지 본다.
3. Overview의 Producer send / ack / fail / skip과 Consumer processed에서 처리가 끊겼는지 본다. 끊긴 시간을 잰다.
4. 상황을 판정한다. 데이터 유실 위험은 지금 어떤 상태인가(min ISR 2, 남은 복제본 2).
5. 조치한다.

```bash
docker start broker-2
# URP가 0이 될 때까지 기다린 뒤
docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server broker-1:9092 --election-type PREFERRED --all-topic-partitions
```

6. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- 브로커가 죽은 뒤 컨트롤러가 fencing하기까지 걸린 시간과 그 시간을 정하는 설정
- 재기동 직후 리더 분포. preferred election을 하지 않으면 어떤 문제가 남는가
- 이 상태에서 브로커가 1대 더 죽으면 무엇이 일어나는가(D08)

---

## 해설

### 메커니즘

KRaft에서 브로커는 컨트롤러에 주기적으로 하트비트를 보낸다. `broker.session.timeout.ms`(기본 9초) 동안 하트비트가 없으면 컨트롤러가 브로커를 fence하고, 그 브로커가 리더였던 파티션의 리더를 ISR의 다른 복제본으로 옮긴다. 메타데이터 변경이 전파되면 클라이언트는 새 리더로 요청을 보낸다. 이 구간 동안 해당 파티션의 produce와 fetch는 재시도로 지연된다.

리더가 된 브로커는 죽은 복제본을 ISR에서 뺀다. 남은 ISR이 2이므로 min ISR 2를 만족해 쓰기가 계속된다. 대신 한 대가 더 죽으면 쓰기가 거부된다.

재기동한 broker-2는 리더에게서 빠진 로그를 가져와 따라잡은 뒤 ISR로 돌아온다. `auto.leader.rebalance.enable`이 켜져 있으면 일정 주기 후 preferred leader로 리더가 돌아오지만, 그 전까지는 리더가 두 브로커에 몰려 있다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | Brokers up 2, URP > 0 | 브로커 장애 |
| 범위 | Offline partitions 0, Under min ISR 0, 프로듀서 ack 유지 | 서비스 영향 없음, 내구성 여유 없음 |
| 원인 | 컨테이너 상태(`docker ps -a`), 브로커 로그 마지막 줄 | 비정상 종료(controlled shutdown 로그 없음) |
| 확인 | 컨트롤러 로그의 fencing 기록 | 클러스터가 정상적으로 대응했다 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 재기동 후 따라잡기 대기 | 복제본 복구 | 따라잡는 동안 리더 브로커에 복제 트래픽이 몰린다 |
| preferred leader election | 리더 분포 복구 | URP가 남은 상태에서 실행하면 일부 파티션은 옮겨 가지 못한다 |
| 다른 브로커로 복제본 재배치(reassign) | 호스트가 복구 불가할 때 | 대량 복제 트래픽. throttle 설정이 필요하다 |
| 아무것도 하지 않음 | 없음 | 다음 장애에 쓰기 중단 |

### 코드 근거 (4.1.2)

- `ReplicationControlManager.maybeFenceOneStaleBroker()`(`metadata/.../controller/ReplicationControlManager.java:1670`): 세션이 만료된 브로커를 fence하고 리더를 옮긴다.
- `BrokerHeartbeatManager`(`metadata/.../controller/BrokerHeartbeatManager.java`): 브로커별 마지막 하트비트 시각을 관리한다.
- `Partition.maybeShrinkIsr()`(`core/.../cluster/Partition.scala:1231`): 따라오지 못하는 복제본을 ISR에서 제거한다.

### 재발 방지

- 알람: `sum(up{job="broker"}) < 3`(즉시), `sum(kafka_server_replicamanager_underreplicatedpartitions) > 0`이 5분 지속
- 운영 절차: 재기동 → URP 0 확인 → preferred election 순서를 런북에 둔다.

### 재현 충실도

컨테이너 kill은 프로세스 종료이므로 페이지 캐시 유실이나 디스크 손상은 재현하지 않는다. 실제 호스트 장애에서는 `unclean.leader.election`, 로그 복구 시간이 추가로 문제가 된다.
