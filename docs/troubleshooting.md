# 문제 해결

## 환경과 Stage 2

| 증상 | 원인 | 조치 |
|---|---|---|
| `cluster.sh up`이 `did not become healthy`로 끝난다 | Docker 메모리 부족, 이전 컨테이너 잔존 | `docker info`로 메모리 확인, `./scripts/cluster.sh down` 후 재시도 |
| `run.sh`가 `broker-N is not healthy`로 끝난다 | 클러스터 미기동 | `./scripts/cluster.sh up` |
| `group did not stabilize` | 컨슈머 기동 실패, 설정 오류 | `docker logs consumer-1`, run 디렉터리의 `container-consumer-*.log`에서 `ConfigException` 등을 찾는다 |
| consumer 설정에서 `ConfigException` | consumer 프로토콜에 classic 전용 설정 사용 | `partition.assignment.strategy`, `session.timeout.ms`, `heartbeat.interval.ms`를 넣지 않는다 |
| 컨슈머가 `ERROR` 이벤트를 남기고 종료 | 콜백·처리 코드 예외가 `poll()` 밖으로 전파 | `events-*.csv`의 `ERROR` 행 확인. 아래 "OffsetCommitCallback 인자" 참고 |
| `wait_assigned` 타임아웃 경고 | 재시작한 멤버가 할당을 못 받음 | consumer static에서 이전 멤버가 아직 남아 있으면 `UNRELEASED_INSTANCE_ID`로 거절된다. 브로커 로그 확인 |
| 결과표에서 그룹 비안정 누적이 0 | 리밸런싱이 1초 샘플 사이에 끝남 | 클라이언트 지표(latency-total)를 본다. `SAMPLE_MS=200`으로 재실행 |
| 정지 파티션에 비재할당 파티션이 섞임 | 새로 받은 파티션의 backlog 처리로 기존 파티션이 밀림 | 정상 현상이다. [stage2-rebalance/01-measurement.md](stage2-rebalance/01-measurement.md)의 정의 참고. `PROCESS_MS`를 낮추면 줄어든다 |
| 실험 중 Docker Desktop이 느려짐 | JVM 최대 15개와 Prometheus, Grafana 동시 실행 | 다른 컨테이너 정리, 메모리 할당 증가 |
| `migration.sh`가 `진행 중인 마이그레이션 run이 없다` | `start` 전 실행, 또는 `finish` 후 실행 | `./scripts/migration.sh start` |
| 이전 실험의 컨테이너가 남아 있음 | 스크립트 강제 종료 | `docker compose --profile lab rm -sf` 또는 `./scripts/cluster.sh down` |

## 모니터링

| 증상 | 원인 | 조치 |
|---|---|---|
| 브로커 컨테이너에서 실행한 Kafka CLI가 `Port already in use: 9404`로 종료 | `docker exec`가 브로커의 `KAFKA_OPTS`(JMX exporter javaagent)를 상속 | `docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/...`. 스크립트의 `kafka_bin`은 이미 이렇게 실행한다 |
| Grafana 패널이 비어 있음(No data) | 해당 run이 아직 시작되지 않았거나, 대상 컨테이너가 없음 | <http://localhost:9090/targets>에서 대상 상태 확인. 컨슈머 4~6은 증설 전까지 대상에 없다 |
| 대상은 up인데 특정 메트릭만 없음 | 메트릭 이름 규칙 차이(`_total` 접미사 제거 등) | [02-monitoring.md](02-monitoring.md)의 메트릭 이름 규칙을 확인하고 `curl -s localhost:9090/api/v1/label/__name__/values`로 실제 이름을 찾는다 |
| 어노테이션이 표시되지 않음 | Grafana 미기동, Stage 5 회차 중(의도적으로 끔) | `curl localhost:3000/api/health`. Stage 5에서는 정상이다 |
| 대시보드를 고쳤는데 반영되지 않음 | JSON을 직접 수정했거나 생성기를 다시 실행하지 않음 | `python3 monitoring/grafana/gen_dashboards.py` 후 Grafana가 30초 안에 다시 읽는다 |

## 알람

| 증상 | 원인 | 조치 |
|---|---|---|
| 조건이 참인데 알람이 오지 않음 | `for` 시간을 아직 채우지 못함, 또는 억제 규칙에 걸림 | `./scripts/alerts.sh pending`으로 대기 중인지, `./scripts/alerts.sh`의 억제 표시로 억제됐는지 본다. 억제 규칙은 [03-alerting.md](03-alerting.md#억제-규칙) |
| 알람 규칙을 고쳤는데 반영되지 않음 | Prometheus가 규칙 파일을 다시 읽지 않음 | `curl -X POST localhost:9090/-/reload` 후 <http://localhost:9090/rules>에서 확인 |
| `alerts.sh history`가 "발화한 알람 없음"인데 대시보드에는 빨간 구간이 있음 | 조회 구간이 너무 짧음, 또는 `cluster.sh reset`으로 Prometheus 데이터가 사라짐 | `./scripts/alerts.sh history 2h`처럼 구간을 늘린다 |
| 기동 직후 알람이 잠깐 울렸다 꺼짐 | 브로커가 리더를 나눠 갖기 전의 일시적인 URP·리더 불균형 | 5분 안에 해소되면 정상이다. 계속되면 `cluster.sh status`로 ISR을 본다 |

## Stage 1·3

| 증상 | 원인 | 조치 |
|---|---|---|
| `perf.sh`가 `broker disk free ...MB < ...MB`로 끝남 | 이전 측정 토픽의 파일 삭제가 아직 끝나지 않음, 워크로드가 떠 있음 | `./scripts/workload.sh stop` 후 1분 기다려 다시 실행한다. `POINT_MAX_MB`를 줄여도 된다 |
| `perf.sh`가 `WARN: 브로커 볼륨이 256MB다`를 남김 | 기본 볼륨으로 측정해 최대 부하 점이 1초 안에 끝남 | `BROKER_DISK_MB=1024 ./scripts/cluster.sh reset` 후 측정한다 |
| `perf.sh`가 `WARN: 배터리 전원이다`를 남김 | 노트북이 배터리로 돌고 있다. 같은 설정이 AC 세션보다 처리량이 약 13% 낮았고 p99가 튀었다 | 전원을 연결하고 처음부터 다시 잰다. 전원이 다른 측정끼리 비교하지 않는다 |
| `report.md` 맨 위에 `경고: 측정 조건이 섞였다`가 나옴 | 측정 중에 전원이나 덮개 상태가 바뀌었다 | `results.csv`를 지우고 같은 조건에서 다시 잰다. `PERF_REQUIRE_HOST=ac-open`을 주면 상태가 바뀌는 순간 멈춘다 |
| `perf.sh`가 `WARN: 측정이 ...초 걸려 버린다`를 남김 | 노트북이 잠들어 측정이 멈췄다 | `caffeinate -ims`로 실행하고 덮개를 닫지 않는다. 빠진 점은 `perf.sh sweep`으로 다시 잰다 |
| 첫 몇 점의 처리량이 낮음(같은 설정이 124MB/s에서 352MB/s까지 오름) | 재시작 직후 브로커 JVM이 데워지지 않았다 | `perf.sh sweep warmup`을 먼저 돌린다. `perf.sh all`은 자동으로 한다 |
| 측정이 끝났는데 대시보드가 멈춰 있음 | `load-steady` 중간에 중단되어 Prometheus가 일시 정지 상태로 남았다 | `docker unpause prometheus` |
| 같은 점을 다시 쟀더니 처리량이 20% 넘게 다름 | 호스트의 다른 부하(브라우저, IDE 빌드), Docker Desktop CPU 경합 | 측정 중 다른 작업을 멈추고 다시 잰다. 리포트는 같은 점의 마지막 값만 쓴다 |
| `repro-18376.sh run`이 `WARN: ... produced no result`를 반복 | `prepare`를 하지 않았거나 `cluster.sh reset`으로 bench 토픽이 사라짐 | `./scripts/repro-18376.sh prepare` |
| bench 결과의 분산이 큼(특히 classic) | classic은 처리량이 높아 측정 구간이 짧고 호스트 잡음 비중이 큼 | 반복 횟수를 늘리고 중앙값을 쓴다(`run 5`) |

## Stage 4·5

| 증상 | 원인 | 조치 |
|---|---|---|
| `이미 진행 중인 훈련이 있다` | 이전 훈련을 `stop`하지 않음 | `./scripts/drill.sh stop` |
| `진행 중인 회차가 있다` | 이전 카오스 회차를 `stop`하지 않음 | `./scripts/chaos.sh stop` |
| 훈련을 중단했더니 브로커가 멈춘 채(Paused)로 남음 | 정지 루프가 강제 종료됨 | `./scripts/drill.sh stop` 또는 `docker unpause broker-N` |
| 브로커 디스크가 가득 참(D07이 아닌데) | 이전 D07의 채움 파일 잔존 | `drill.sh stop`이 지운다. 수동으로는 `docker run --rm -v rebalance-lab_broker-N-data:/d alpine:3.20 rm -f /d/java_pid1.hprof` |
| D09 이후 오프셋이 1분 만에 사라짐 | D09 설정(`offsets.retention.minutes=1`)으로 만든 브로커가 남음 | `drill.sh stop`이 기본값으로 브로커를 다시 만든다. 수동으로는 `docker compose up -d broker-1 broker-2 broker-3` |
| `drill.sh selftest`가 FAIL | 호스트 부하로 임계값 미달 | `results/runs/<RUN_ID>/selftest.txt`의 `last=` 값과 임계값을 비교한다. 다른 컨테이너를 정리하고 재실행한다 |
| D13 이후 노드 버전이 섞여 있음 | 훈련 중단으로 롤백한 노드가 남음 | `./scripts/upgrade.sh status`로 확인하고 `./scripts/upgrade.sh roll broker-3 4.1.2` |
| D14 이후 브로커가 기동 직후 종료 | 컨트롤러 쿼럼이 아직 리더를 뽑지 못함 | 컨트롤러 3대를 먼저 올리고 `NoActiveController` 알람이 해소된 뒤 브로커를 올린다 |
| D17 이후 프로듀서가 일부 파티션에 계속 `NOT_LEADER_OR_FOLLOWER` | 리더가 바뀐 뒤 프로듀서 메타데이터가 갱신되지 않음(미해결, [D17 문서](stage4-drills/D17-unclean-election.md) 참고) | `docker compose restart producer` |
| `drill.sh selftest`를 백그라운드로 돌렸더니 중간에 멈추고 다음 실행이 `이미 진행 중인 훈련이 있다` | 실행한 셸이 종료되며 훈련 프로세스도 종료됨 | `./scripts/drill.sh stop` 후 `nohup`이 아니라 별도 터미널에서 실행한다 |
| `tc`, netem 관련 오류 | Docker Desktop 커널에 tc qdisc가 없음 | 이 레포는 netem을 쓰지 않는다. 느린 브로커는 `fault_freeze`로 만든다 |

## OffsetCommitCallback 인자

`commitAsync(offsets, callback)`의 콜백이 받는 `offsets` 인자는 프로토콜에 따라 다르다. consumer 프로토콜(`AsyncKafkaConsumer`)은 커밋이 실패하면 `null`을 넘긴다(`AsyncKafkaConsumer.commitAsync`의 `whenComplete`에서 `committedOffsets`가 null). classic 구현을 기준으로 콜백에서 인자를 바로 역참조하면 consumer 프로토콜로 바꾸는 순간 NPE가 `poll()` 밖으로 전파되어 컨슈머가 종료된다. 이 레포의 컨슈머는 호출 시점의 오프셋 맵을 캡처해 쓴다.

마이그레이션 전 애플리케이션 코드 점검 항목으로 런북에 넣을 만한 사례다.

## 초기화

```bash
./scripts/cluster.sh down        # 컨테이너와 데이터 삭제
rm -rf results/runs/* results/chaos/*   # 실험 기록 삭제
./scripts/cluster.sh up
```
