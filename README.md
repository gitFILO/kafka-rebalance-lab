# kafka-rebalance-lab

Kafka 4.1 컨슈머 그룹의 classic 프로토콜과 consumer 프로토콜(KIP-848)을 같은 조건에서 비교 측정하는 실험 환경이다. 상급 카프카 엔지니어 커리큘럼 Phase 4(비교 실험과 마이그레이션)의 실습을 모두 이 레포에서 진행한다.

## 실험 내용

### 1. 5개 설정 × 4개 시나리오 비교

| 설정 | 프로토콜 | 할당 | 리밸런싱 | static membership |
|---|---|---|---|---|
| `eager` | classic | 클라이언트 리더, `RangeAssignor` | 전체 revoke 후 재할당 | - |
| `cooperative` | classic | 클라이언트 리더, `CooperativeStickyAssignor` | 이동 파티션만 revoke (KIP-429) | - |
| `cooperative-static` | classic | 위와 같음 | 위와 같음 | O (KIP-345) |
| `consumer` | consumer | 브로커, `uniform` | 멤버별 증분 조정, barrier 없음 | - |
| `consumer-static` | consumer | 위와 같음 | 위와 같음 | O |

| 시나리오 | 동작 | 재현하는 상황 |
|---|---|---|
| `rolling-restart` | consumer-1~6을 차례로 SIGTERM 재시작 | 배포 |
| `kill9` | consumer-3에 SIGKILL | 노드 장애, OOM kill |
| `stall` | consumer-3이 45초 동안 처리를 멈춤(`max.poll.interval.ms`=30초 초과) | 외부 호출 지연, 처리 병목 |
| `scale-out` | 3대 운영 중 3대 추가 | 증설 |

측정 항목은 리밸런싱 시간, 정지된 파티션 수, lag 피크, 중복 건수다. run마다 자동으로 수집·계산하고 `analyze.py table`이 5×4 결과표를 만든다.

### 2. classic → consumer 온라인 마이그레이션

classic 그룹을 운영하는 상태에서 consumer 멤버를 롤링으로 투입하고, 첫 consumer 멤버가 들어올 때 브로커가 그룹을 전환하는 과정을 로그로 확인한다. 혼합 상태에서 장애를 내서 동작을 측정하고, 마지막으로 classic으로 다운그레이드한다.

## 이 실습으로 확인하는 것

- eager, cooperative, consumer 프로토콜이 리밸런싱 동안 멈추는 파티션의 범위와 시간
- static membership이 롤링 재시작에는 효과가 있고 kill -9에는 효과가 없는 이유
- 장애 감지 시간을 결정하는 설정(세션 타임아웃, `max.poll.interval.ms`)과 그 설정이 클라이언트에 있는지 브로커에 있는지
- 리밸런싱 중 중복이 생기는 경로와 중복 건수의 상한
- 혼합 그룹에서 할당을 누가 계산하는지, 전환과 롤백이 어떤 조건에서 일어나는지
- 각 동작을 뒷받침하는 4.1.2 소스 코드 위치

## 구성

```
controller-1..3 (KRaft quorum)   broker-1..3 (topic lab-events: 12 partitions, RF 3, min.insync.replicas 2)
producer (600 msg/s, acks=all)   consumer-1..6 (Java kafka-clients 4.1.2)   lag-monitor (Admin API, 1s)
```

컨슈머는 처리한 레코드, 리밸런싱 콜백, 코디네이터 메트릭을 CSV로 남기고, lag-monitor는 브로커가 보는 그룹 상태와 lag을 남긴다. 상세 구성은 [docs/01-environment.md](docs/01-environment.md)에 있다.

## 빠른 시작

요구 사항: Docker Desktop(메모리 8GB 이상 할당), python3. JDK는 필요 없다.

```bash
./scripts/cluster.sh up                     # 클러스터 기동, 토픽 생성
./scripts/run.sh cooperative kill9          # 1칸 실행 (약 2분 30초)
./scripts/run-matrix.sh                     # 20칸 전체 (약 60분)
python3 scripts/analyze.py table            # 결과표
./scripts/migration.sh start                # 마이그레이션 실습 시작 (docs/04 참고)
./scripts/cluster.sh down                   # 정리
```

## 문서

순서대로 읽고 진행한다.

| 문서 | 내용 |
|---|---|
| [01-environment.md](docs/01-environment.md) | 구성, 기동, 실험 애플리케이션 동작, 파라미터, 산출 파일 |
| [02-measurement.md](docs/02-measurement.md) | 측정 항목 정의, 계산 방식, 한계, 수동 관측 도구(CLI, JMX, 브로커 로그) |
| [03-experiments.md](docs/03-experiments.md) | 설정·시나리오별 메커니즘, 확인할 것, 코드 근거, 결과 기록 방법 |
| [04-migration.md](docs/04-migration.md) | 전환 조건(코드 기준), 마이그레이션 단계별 절차, 런북 작성 |
| [05-troubleshooting.md](docs/05-troubleshooting.md) | 자주 발생하는 문제와 조치 |
| [results/RESULTS.md](results/RESULTS.md) | 결과표와 해석 (직접 작성) |
| [templates/migration-runbook.md](docs/templates/migration-runbook.md) | 마이그레이션 런북 양식 |

## 레포 구조

```
docker-compose.yml        KRaft 3+3, producer, lag-monitor, consumer-1..6
settings/*.env            5개 설정 프로파일
lab-app/                  Java 실험 애플리케이션 (producer / consumer / lag-monitor)
scripts/
  cluster.sh              클러스터 up / status / down / reset
  run.sh                  설정 × 시나리오 1회 실행
  run-matrix.sh           여러 칸 연속 실행
  migration.sh            마이그레이션 단계별 실행
  analyze.py              run 분석, 결과표 생성
  wait.py, lib.sh         공통 대기 조건, 함수
docs/                     절차 문서
results/RESULTS.md        결과표와 해석
results/runs/             run별 원본 기록 (git 제외)
```

## Phase 4 완료 기준

- [ ] 5×4 결과표를 모두 채우고 설정별 차이를 해석했다 → [results/RESULTS.md](results/RESULTS.md)
- [ ] classic → consumer 마이그레이션 런북을 작성했다 → `results/migration-runbook.md`
