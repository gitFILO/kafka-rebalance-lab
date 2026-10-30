# 05. 문제 해결

| 증상 | 원인 | 조치 |
|---|---|---|
| `cluster.sh up`이 `did not become healthy`로 끝난다 | Docker 메모리 부족, 이전 컨테이너 잔존 | `docker info`로 메모리 확인, `./scripts/cluster.sh down` 후 재시도 |
| `run.sh`가 `broker-N is not healthy`로 끝난다 | 클러스터 미기동 | `./scripts/cluster.sh up` |
| `group did not stabilize` | 컨슈머 기동 실패, 설정 오류 | `docker logs consumer-1`, run 디렉터리의 `container-consumer-*.log`에서 `ConfigException` 등을 찾는다 |
| consumer 설정에서 `ConfigException` | consumer 프로토콜에 classic 전용 설정 사용 | `partition.assignment.strategy`, `session.timeout.ms`, `heartbeat.interval.ms`를 넣지 않는다 |
| 컨슈머가 `ERROR` 이벤트를 남기고 종료 | 콜백·처리 코드 예외가 `poll()` 밖으로 전파 | `events-*.csv`의 `ERROR` 행 확인. 아래 "OffsetCommitCallback 인자" 참고 |
| `wait_assigned` 타임아웃 경고 | 재시작한 멤버가 할당을 못 받음 | consumer static에서 이전 멤버가 아직 남아 있으면 `UNRELEASED_INSTANCE_ID`로 거절된다. 브로커 로그 확인 |
| 결과표에서 그룹 비안정 누적이 0 | 리밸런싱이 1초 샘플 사이에 끝남 | 클라이언트 지표(latency-total)를 본다. `SAMPLE_MS=200`으로 재실행 |
| 정지 파티션에 비재할당 파티션이 섞임 | 새로 받은 파티션의 backlog 처리로 기존 파티션이 밀림 | 정상 현상이다. [02-measurement.md](02-measurement.md)의 정의 참고. `PROCESS_MS`를 낮추면 줄어든다 |
| 실험 중 Docker Desktop이 느려짐 | JVM 14개 동시 실행 | 다른 컨테이너 정리, 메모리 할당 증가 |
| `migration.sh`가 `진행 중인 마이그레이션 run이 없다` | `start` 전 실행, 또는 `finish` 후 실행 | `./scripts/migration.sh start` |
| 이전 실험의 컨테이너가 남아 있음 | 스크립트 강제 종료 | `docker compose --profile lab rm -sf` 또는 `./scripts/cluster.sh down` |

## OffsetCommitCallback 인자

`commitAsync(offsets, callback)`의 콜백이 받는 `offsets` 인자는 프로토콜에 따라 다르다. consumer 프로토콜(`AsyncKafkaConsumer`)은 커밋이 실패하면 `null`을 넘긴다(`AsyncKafkaConsumer.commitAsync`의 `whenComplete`에서 `committedOffsets`가 null). classic 구현을 기준으로 콜백에서 인자를 바로 역참조하면 consumer 프로토콜로 바꾸는 순간 NPE가 `poll()` 밖으로 전파되어 컨슈머가 종료된다. 이 레포의 컨슈머는 호출 시점의 오프셋 맵을 캡처해 쓴다.

마이그레이션 전 애플리케이션 코드 점검 항목으로 런북에 넣을 만한 사례다.

## 초기화

```bash
./scripts/cluster.sh down        # 컨테이너와 데이터 삭제
rm -rf results/runs/*            # 실험 기록 삭제
./scripts/cluster.sh up
```
