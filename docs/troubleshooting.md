# 문제 해결

## 환경과 Stage 1

| 증상 | 원인 | 조치 |
|---|---|---|
| `cluster.sh up`이 `did not become healthy`로 끝난다 | Docker 메모리 부족, 이전 컨테이너 잔존 | `docker info`로 메모리 확인, `./scripts/cluster.sh down` 후 재시도 |
| `run.sh`가 `broker-N is not healthy`로 끝난다 | 클러스터 미기동 | `./scripts/cluster.sh up` |
| `group did not stabilize` | 컨슈머 기동 실패, 설정 오류 | `docker logs consumer-1`, run 디렉터리의 `container-consumer-*.log`에서 `ConfigException` 등을 찾는다 |
| consumer 설정에서 `ConfigException` | consumer 프로토콜에 classic 전용 설정 사용 | `partition.assignment.strategy`, `session.timeout.ms`, `heartbeat.interval.ms`를 넣지 않는다 |
| 컨슈머가 `ERROR` 이벤트를 남기고 종료 | 콜백·처리 코드 예외가 `poll()` 밖으로 전파 | `events-*.csv`의 `ERROR` 행 확인. 아래 "OffsetCommitCallback 인자" 참고 |
| `wait_assigned` 타임아웃 경고 | 재시작한 멤버가 할당을 못 받음 | consumer static에서 이전 멤버가 아직 남아 있으면 `UNRELEASED_INSTANCE_ID`로 거절된다. 브로커 로그 확인 |
| 결과표에서 그룹 비안정 누적이 0 | 리밸런싱이 1초 샘플 사이에 끝남 | 클라이언트 지표(latency-total)를 본다. `SAMPLE_MS=200`으로 재실행 |
| 정지 파티션에 비재할당 파티션이 섞임 | 새로 받은 파티션의 backlog 처리로 기존 파티션이 밀림 | 정상 현상이다. [stage1-rebalance/01-measurement.md](stage1-rebalance/01-measurement.md)의 정의 참고. `PROCESS_MS`를 낮추면 줄어든다 |
| 실험 중 Docker Desktop이 느려짐 | JVM 최대 15개와 Prometheus, Grafana 동시 실행 | 다른 컨테이너 정리, 메모리 할당 증가 |
| `migration.sh`가 `진행 중인 마이그레이션 run이 없다` | `start` 전 실행, 또는 `finish` 후 실행 | `./scripts/migration.sh start` |
| 이전 실험의 컨테이너가 남아 있음 | 스크립트 강제 종료 | `docker compose --profile lab rm -sf` 또는 `./scripts/cluster.sh down` |

## 모니터링

| 증상 | 원인 | 조치 |
|---|---|---|
| 브로커 컨테이너에서 실행한 Kafka CLI가 `Port already in use: 9404`로 종료 | `docker exec`가 브로커의 `KAFKA_OPTS`(JMX exporter javaagent)를 상속 | `docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/...`. 스크립트의 `kafka_bin`은 이미 이렇게 실행한다 |
| Grafana 패널이 비어 있음(No data) | 해당 run이 아직 시작되지 않았거나, 대상 컨테이너가 없음 | <http://localhost:9090/targets>에서 대상 상태 확인. 컨슈머 4~6은 증설 전까지 대상에 없다 |
| 대상은 up인데 특정 메트릭만 없음 | 메트릭 이름 규칙 차이(`_total` 접미사 제거 등) | [02-monitoring.md](02-monitoring.md)의 메트릭 이름 규칙을 확인하고 `curl -s localhost:9090/api/v1/label/__name__/values`로 실제 이름을 찾는다 |
| 어노테이션이 표시되지 않음 | Grafana 미기동, Stage 3 회차 중(의도적으로 끔) | `curl localhost:3000/api/health`. Stage 3에서는 정상이다 |
| 대시보드를 고쳤는데 반영되지 않음 | JSON을 직접 수정했거나 생성기를 다시 실행하지 않음 | `python3 monitoring/grafana/gen_dashboards.py` 후 Grafana가 30초 안에 다시 읽는다 |

## Stage 2·3

| 증상 | 원인 | 조치 |
|---|---|---|
| `이미 진행 중인 훈련이 있다` | 이전 훈련을 `stop`하지 않음 | `./scripts/drill.sh stop` |
| `진행 중인 회차가 있다` | 이전 카오스 회차를 `stop`하지 않음 | `./scripts/chaos.sh stop` |
| 훈련을 중단했더니 브로커가 멈춘 채(Paused)로 남음 | 정지 루프가 강제 종료됨 | `./scripts/drill.sh stop` 또는 `docker unpause broker-N` |
| 브로커 디스크가 가득 참(D07이 아닌데) | 이전 D07의 채움 파일 잔존 | `drill.sh stop`이 지운다. 수동으로는 `docker run --rm -v rebalance-lab_broker-N-data:/d alpine:3.20 rm -f /d/java_pid1.hprof` |
| D09 이후 오프셋이 1분 만에 사라짐 | D09 설정(`offsets.retention.minutes=1`)으로 만든 브로커가 남음 | `drill.sh stop`이 기본값으로 브로커를 다시 만든다. 수동으로는 `docker compose up -d broker-1 broker-2 broker-3` |
| `drill.sh selftest`가 FAIL | 호스트 부하로 임계값 미달 | `results/runs/<RUN_ID>/selftest.txt`의 `last=` 값과 임계값을 비교한다. 다른 컨테이너를 정리하고 재실행한다 |
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
