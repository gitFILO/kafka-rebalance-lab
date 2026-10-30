# D02. poll 간격 초과로 인한 리밸런스 폭풍

## 목적

배치 처리 시간이 `max.poll.interval.ms`를 넘으면 멤버가 스스로 그룹을 떠나고 다시 들어오기를 반복한다. 이 상태에서는 처리량이 0에 가까워지고 증설도 효과가 없다. 폭풍을 lag 증가(D01)와 구분하고, 배치 크기로 멈추는 방법을 익힌다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 다운스트림이 심하게 느려져 레코드당 350ms가 걸린다. |
| 주입 | `control/process_ms`=350. 배치 100건 처리에 35초가 걸려 `max.poll.interval.ms`(30초)를 넘는다. |
| 예상 증상 | 멤버가 30초마다 파티션을 잃고 다시 받는다. 처리량이 거의 0이다. |
| 모범 조치 | `max.poll.records`를 10으로 줄여 컨슈머를 재배포한다. |
| 소요 | 20분 |

## 진행 방식

```bash
./scripts/drill.sh start D02
```

1. Overview에서 Group state, Assigned partitions per member, Rebalance callbacks를 본다. 반복되는 모양이 있는지 확인한다.
2. Clients의 Time between poll max가 30초를 넘는지 본다.
3. 컨테이너 로그를 확인한다: `docker logs consumer-1 2>&1 | grep -i 'poll timeout'`.
4. run 디렉터리의 `events-consumer-*.csv`에서 LOST/ASSIGNED 반복 주기를 확인한다.
5. 조치를 정한다. 설정을 바꿔 재배포하려면 환경 변수를 지정해 컨테이너를 다시 만든다.

```bash
# 예: 현재 run 정보를 불러온 셸에서
. results/runs/<RUN_ID>/run.env && export RUN_ID GROUP_ID
MAX_POLL_RECORDS=10 docker compose --profile lab up -d --no-deps consumer-1 consumer-2 consumer-3
```

6. `drill.sh verify`로 폭풍이 멈췄는지 확인하고 `drill.sh stop`.

확인할 것:

- 폭풍이 멈춘 뒤에도 lag이 줄지 않는다. 이유는 무엇이고 다음 조치는 무엇인가.
- 같은 상황에서 classic 프로토콜이었다면 그룹 전체에 어떤 영향이 있었는가(Stage 1 stall 결과 참고).
- `max.poll.interval.ms`를 늘리는 조치와 `max.poll.records`를 줄이는 조치의 차이는 무엇인가.

---

## 해설

### 메커니즘

consumer 프로토콜 클라이언트는 백그라운드 스레드가 하트비트를 보내는 동안 애플리케이션 스레드의 poll 간격을 따로 잰다. 간격이 `max.poll.interval.ms`를 넘으면 "Consumer poll timeout has expired" 경고를 남기고 멤버를 STALE로 전이시킨 뒤 그룹을 떠난다(leave epoch -1). 파티션은 다른 멤버에게 넘어가고, 원래 멤버는 다음 `poll()`에서 다시 합류한다. 모든 멤버가 같은 처리 시간을 가지므로 새 소유자도 30초 뒤 같은 일을 겪는다.

떠나는 시점에 처리 중인 배치의 커밋은 실패하거나 의미가 없어지므로 같은 레코드를 여러 번 처리한다. 진행 없이 중복만 늘어나는 상태다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | 처리량이 거의 0, lag 증가 | D01보다 심각하다 |
| 범위 | Rebalance callbacks(lost) 주기적 증가, 그룹 상태 반복 전이 | 리밸런싱이 원인 또는 증상 |
| 원인 | Time between poll max > 30000ms, 로그의 poll timeout 경고 | poll 간격 초과로 인한 자발적 이탈 |
| 확인 | 배치 크기 × 레코드당 처리 시간 > `max.poll.interval.ms` | 계산으로 확정 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| `max.poll.records` 축소 | 배치 시간이 poll 간격 아래로 내려가 폭풍이 멈춘다 | poll 호출과 커밋 횟수가 늘어난다. 처리량 자체는 늘지 않는다 |
| `max.poll.interval.ms` 증가 | 폭풍이 멈춘다 | 정말로 멈춘 컨슈머를 감지하는 시간도 길어진다 |
| 처리 비동기화(pause/resume) | poll 루프와 처리 분리 | 오프셋 관리가 복잡해진다 |
| 컨슈머 증설 | 효과 없음 | 리밸런싱이 더 잦아진다 |

모범 조치 후에도 처리량은 대당 약 3 msg/s에 불과하다. 폭풍을 멈추는 것은 1차 조치이고, 다운스트림 문제는 D01의 해법(처리량 확보)으로 이어서 다뤄야 한다.

### 코드 근거 (4.1.2)

- `AbstractHeartbeatRequestManager.java:170`: poll timer 만료 시 경고 로그와 STALE 전이
- `AbstractMembershipManager.transitionToStale()`(790), `maybeRejoinStaleMember()`(775): 다음 poll에서 재합류
- classic 구현: `AbstractCoordinator.java:1152` `maybeLeaveGroup(..., "consumer poll timeout has expired.")`

### 재발 방지

- 알람: `max(kafka_consumer_consumer_metrics_time_between_poll_max) > 0.8 * max.poll.interval.ms`
- 설계 기준: `max.poll.records × p99 레코드 처리 시간 < max.poll.interval.ms / 2`. Phase 7 설계 문서에서 SLO로부터 역산한다.

### 재현 충실도

`max.poll.interval.ms`를 30초로 줄였기 때문에 레코드당 350ms에서 폭풍이 난다. 운영 기본값 300초에서는 같은 배치 크기로 레코드당 3초 이상일 때 발생한다.
