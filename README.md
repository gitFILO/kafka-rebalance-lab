# kafka-rebalance-lab

Kafka 4.1 클러스터에서 컨슈머 그룹 리밸런싱을 측정하고, 운영 장애를 진단·복구하는 훈련 환경이다. 상급 카프카 엔지니어 커리큘럼 Phase 4(classic과 consumer 프로토콜 비교, 마이그레이션)에서 시작해, 단일 장애 훈련과 블라인드 복합 장애 훈련까지 같은 클러스터와 같은 모니터링 위에서 진행한다.

## 목적

Kafka 동작을 문서가 아니라 측정값과 소스 코드로 설명하고, 그 지식으로 장애를 빠르게 진단하는 능력을 기른다. 모든 단계를 마치면 다음을 할 수 있어야 한다.

1. 리밸런싱 설정(eager, cooperative, static membership, consumer 프로토콜)이 장애 상황에서 만드는 차이를 수치로 제시하고, 그 차이를 4.1.2 코드 위치로 설명한다.
2. 자주 발생하는 장애 11종을 대시보드 신호로 구분하고, 부작용을 알고 조치를 고른다.
3. 원인을 모르는 상태에서 2~3개가 연쇄된 장애를 감지하고, 인과 사슬을 거꾸로 따라가 근본 원인을 확정하고, 보고서로 남긴다.

## 개요

| 단계 | 내용 | 산출물 |
|---|---|---|
| [모니터링](docs/02-monitoring.md) | 모든 JVM(컨트롤러, 브로커, 프로듀서, 컨슈머, 모니터)을 JMX exporter로 Prometheus에 수집하고 Grafana 대시보드 3개로 본다. 모든 단계가 이 화면을 기준으로 진행한다. | 대시보드 읽는 법, 주요 PromQL |
| [Stage 1. 리밸런싱 비교](docs/stage1-rebalance/README.md) | 설정 5개 × 장애 시나리오 4개 = 20칸을 측정하고, classic 그룹을 consumer 프로토콜로 온라인 전환했다가 되돌린다. | 5×4 결과표와 해석, 마이그레이션 런북 |
| [Stage 2. 단일 장애 훈련](docs/stage2-drills/README.md) | 운영에서 자주 만나는 장애 11종을 하나씩 주입하고, 진단 경로·조치·코드 근거·재발 방지를 정리한다. 장애 이름과 주입 시각을 알고 시작한다. | 훈련 기록 11건 |
| [Stage 3. 블라인드 장애 훈련](docs/stage3-chaos/README.md) | 무작위로 고른 카드가 정해지지 않은 시점에 연쇄 장애를 일으키고, 정상 운영 사건(노이즈)이 섞인다. 정답은 봉인되고 대응 후에 공개된다. | 장애 보고서와 채점 결과 |

Stage 1의 결과가 Stage 2·3에서 "리밸런싱이 원래 이 정도 걸리는가"를 판단하는 기준선이 되고, Stage 2의 진단 경로가 Stage 3에서 연쇄 장애를 분해하는 도구가 된다. 순서대로 진행한다.

공통 조건:

| 항목 | 값 |
|---|---|
| 클러스터 | Kafka 4.1.2 KRaft, controller 3대, broker 3대 |
| 토픽 | `lab-events`, 파티션 12, RF 3, `min.insync.replicas` 2 |
| 워크로드 | 프로듀서 1대(600 msg/s, `acks=all`), 컨슈머 최대 6대 |
| 모니터링 | Prometheus(5초 수집), Grafana, 실험 스크립트가 사건 시각을 어노테이션으로 남긴다 |

## 진행 방식

요구 사항: Docker Desktop(메모리 8GB 이상 할당), python3. JDK는 필요 없다.

```bash
./scripts/cluster.sh up                 # 이미지 빌드, 모니터링·클러스터 기동, 토픽 생성
open http://localhost:3000              # Grafana. Lab / Overview가 홈 화면이다
```

| 단계 | 시작 명령 | 소요 |
|---|---|---|
| 모니터링 | [docs/02-monitoring.md](docs/02-monitoring.md)의 진행 방식대로 대시보드 3개를 확인한다 | 30분 |
| Stage 1 | `./scripts/run.sh cooperative kill9`(1칸), `./scripts/run-matrix.sh`(20칸), `./scripts/migration.sh start` | 3주 |
| Stage 2 | `./scripts/drill.sh list`, `./scripts/drill.sh start D01` | 훈련당 15~25분 |
| Stage 3 | `./scripts/chaos.sh start`, `./scripts/chaos.sh report`, `./scripts/chaos.sh reveal` | 회차당 30~60분 |

정리는 `./scripts/cluster.sh down`이다(컨테이너와 데이터 볼륨을 지운다). 빈 클러스터로 다시 시작하려면 `./scripts/cluster.sh reset`.

## 문서

| 문서 | 내용 |
|---|---|
| [01-environment.md](docs/01-environment.md) | 구성, 기동, 실험 애플리케이션 동작, 런타임 제어, 파라미터, 산출 파일 |
| [02-monitoring.md](docs/02-monitoring.md) | 수집 구조, 대시보드, 주요 PromQL, 메트릭 이름 규칙과 함정 |
| [stage1-rebalance/](docs/stage1-rebalance/README.md) | 측정 정의(01), 설정·시나리오별 실험(02), 마이그레이션(03) |
| [stage2-drills/](docs/stage2-drills/README.md) | 훈련 공통 규칙, D01~D11 훈련별 문서 |
| [stage3-chaos/](docs/stage3-chaos/README.md) | 규칙, 카드 설계 기준, 비공개 카드 작성법, [채점 기준](docs/stage3-chaos/scoring.md) |
| [troubleshooting.md](docs/troubleshooting.md) | 환경·모니터링·훈련 중 자주 발생하는 문제와 조치 |
| [templates/](docs/templates/) | 마이그레이션 런북, 훈련 기록, 장애 보고서 양식 |
| [results/RESULTS.md](results/RESULTS.md) | Stage 1 결과표와 해석 (직접 작성) |

훈련 문서(Stage 2)는 위쪽에 목적·개요·진행 방식, 아래쪽에 해설(메커니즘, 진단 경로, 조치 선택지, 코드 근거, 재발 방지)을 둔다. 해설은 직접 진단한 뒤에 읽는다. Stage 3 카드 파일(`scripts/chaos/cards/`)은 정답이므로 회차를 마칠 때까지 열지 않는다.

## 레포 구조

```
docker-compose.yml        KRaft 3+3, 앱(producer, consumer-1..6, lag-monitor, disk-monitor), Prometheus, Grafana
docker/kafka/             apache/kafka + JMX exporter javaagent 이미지
monitoring/
  jmx/                    JMX exporter 규칙 (kafka.yml: 서버, client.yml: 클라이언트와 실험 앱)
  prometheus/             수집 설정
  grafana/                데이터소스·대시보드 프로비저닝, gen_dashboards.py(대시보드 생성기)
settings/*.env            Stage 1 설정 프로파일 5개
lab-app/                  Java 실험 앱 (producer / consumer / lag-monitor / disk-monitor)
scripts/
  cluster.sh              클러스터 up / status / down / reset
  run.sh, run-matrix.sh   Stage 1 설정 × 시나리오 실행
  migration.sh            Stage 1 마이그레이션 단계 실행
  workload.sh             Stage 2·3 공통 워크로드
  drill.sh, drills/       Stage 2 훈련 실행기와 훈련 정의 D01~D11
  chaos.sh, chaos/cards/  Stage 3 실행기와 카드 C1~C5 (정답 포함)
  lib.sh, lib-fault.sh    공통 함수, 장애 주입 원시 동작
  analyze.py, promq.py    run 분석·결과표, PromQL 조건 대기
docs/                     절차 문서
results/RESULTS.md        Stage 1 결과표와 해석
results/runs/             run별 원본 기록 (git 제외)
results/chaos/            Stage 3 회차 기록 (봉인 파일은 git 제외)
```

## 완료 기준

- [ ] 대시보드 3개에서 [02-monitoring.md](docs/02-monitoring.md)의 확인 항목을 모두 찾았다
- [ ] Stage 1: 5×4 결과표를 채우고 설정별 차이를 해석했다 → [results/RESULTS.md](results/RESULTS.md)
- [ ] Stage 1: classic → consumer 마이그레이션 런북을 작성했다 → `results/migration-runbook.md`
- [ ] Stage 2: D01~D11 훈련 기록을 남겼다 → `results/drills/Dxx.md`
- [ ] Stage 3: [완료 기준](docs/stage3-chaos/README.md#완료-기준)을 충족했다 → `results/chaos/<id>/report.md`
