# kafka-rebalance-lab

Kafka 4.1 클러스터에서 처리량·지연을 재고, 컨슈머 그룹 리밸런싱을 측정하고, 공개 버그를 재현하고, 운영 장애를 진단·복구하는 훈련 환경이다. 처리량 측정, classic과 consumer 프로토콜 비교와 마이그레이션, 단일 장애 훈련, 블라인드 복합 장애 훈련까지 같은 클러스터, 같은 모니터링, 같은 알람 위에서 진행한다.

## 목적

Kafka 동작을 문서가 아니라 측정값과 소스 코드로 설명하고, 그 지식으로 장애를 빠르게 진단하는 능력을 기른다. 모든 단계를 마치면 다음을 할 수 있어야 한다.

1. 프로듀서 설정(batch, linger, 압축, acks)이 처리량과 지연에 주는 영향을 곡선으로 제시하고, 그 측정값으로 클러스터 용량을 산정한다.
2. 리밸런싱 설정(eager, cooperative, static membership, consumer 프로토콜)이 장애 상황에서 만드는 차이를 수치로 제시하고, 그 차이를 4.1.2 코드 위치로 설명한다.
3. 공개된 JIRA 이슈를 재현하고, 측정과 프로파일링으로 원인 코드를 확정한다.
4. 장애 17종을 알람에서 시작해 진단하고, 부작용을 알고 조치를 고른다.
5. 원인을 모르는 상태에서 2~3개가 연쇄된 장애를 알람으로 감지하고, 인과 사슬을 거꾸로 따라가 근본 원인을 확정하고, 보고서로 남긴다.

## 개요

| 단계 | 내용 | 산출물 |
|---|---|---|
| [환경](docs/01-environment.md) · [모니터링](docs/02-monitoring.md) · [알람](docs/03-alerting.md) | 모든 JVM을 JMX exporter로 Prometheus에 수집하고 Grafana 대시보드 3개로 본다. 알람 규칙 26개가 Alertmanager로 간다. 모든 단계가 이 화면과 알람을 켠 채로 진행한다. | 대시보드 읽는 법, 알람 런북 |
| [Stage 1. 처리량과 용량](docs/stage1-throughput/README.md) | acks, batch.size, linger.ms, 압축, 부하를 하나씩 바꿔 처리량-지연 곡선을 재고, 그 값으로 용량을 산정한다. | 측정 리포트, 용량 산정서 |
| [Stage 2. 리밸런싱 비교](docs/stage2-rebalance/README.md) | 설정 5개 × 장애 시나리오 4개 = 20칸을 측정하고, classic 그룹을 consumer 프로토콜로 온라인 전환했다가 되돌린다. | 5×4 결과표와 해석, 마이그레이션 런북 |
| [Stage 3. KIP-848 버그 재현](docs/stage3-bug-repro/README.md) | KAFKA-18376(작은 `max.poll.records`에서 consumer 프로토콜 컨슈머의 CPU 과다)을 재현하고 4.1.2와 4.2.0 클라이언트를 대조해 원인 코드를 확정한다. | 재현 결과표, 원인 분석 |
| [Stage 4. 단일 장애 훈련](docs/stage4-drills/README.md) | 장애 17종(재배치 throttle, 롤링 업그레이드, 쿼럼 상실, 컴팩션, 쿼터, unclean election 포함)을 하나씩 주입하고 알람에서 시작해 진단한다. 장애 이름과 주입 시각을 알고 시작한다. | 훈련 기록 17건 |
| [Stage 5. 블라인드 장애 훈련](docs/stage5-chaos/README.md) | 무작위 카드가 정해지지 않은 시점에 연쇄 장애를 일으키고, 정상 운영 사건(노이즈)이 섞인다. 감지는 알람에서 시작한다. 정답은 봉인되고 대응 후에 공개된다. | 장애 보고서와 채점 결과 |

순서에는 이유가 있다.

- 알람과 모니터링을 먼저 켠다. 이후 모든 단계에서 "이 실험이 어떤 알람을 울리는가"를 함께 본다. Stage 2의 kill9 시나리오에서 어떤 알람이 울리고 어떤 알람이 울리지 않는지 보며 알람의 의미와 지연을 익힌다.
- Stage 1이 정상 상태의 기준선을 만든다. 브로커 한 대가 몇 MB/s를 받는지, `acks=all`이 지연을 얼마나 늘리는지 알아야 이후 단계에서 "느리다"를 판단할 수 있다. 프로듀서 내부(RecordAccumulator, Sender)를 여기서 읽어 두면 Stage 4의 min ISR, 디스크, 쿼터 훈련에서 프로듀서 증상을 바로 해석한다.
- Stage 3은 Stage 2 바로 뒤에 둔다. Stage 2에서 consumer 프로토콜을 쓰고 측정한 직후라 `AsyncKafkaConsumer`의 구조를 읽을 맥락이 살아 있고, Stage 1에서 익힌 처리량 측정과 CPU 해석을 그대로 쓴다.
- Stage 4는 진단 경로를 하나씩 익히고, Stage 5는 그것을 섞인 상황에서 쓴다. Stage 2의 결과가 "리밸런싱이 원래 이 정도 걸리는가", Stage 1의 결과가 "처리량이 원래 이 정도인가"를 판단하는 기준선이 된다.

공통 조건:

| 항목 | 값 |
|---|---|
| 클러스터 | Kafka 4.1.2 KRaft, controller 3대, broker 3대 |
| 토픽 | `lab-events`, 파티션 12, RF 3, `min.insync.replicas` 2 |
| 워크로드 | 프로듀서 1대(600 msg/s, `acks=all`), 컨슈머 최대 6대 |
| 모니터링 | Prometheus(5초 수집), Alertmanager, Grafana. 실험 스크립트가 사건 시각을, Prometheus가 알람 구간을 어노테이션으로 남긴다 |

## 진행 방식

요구 사항: Docker Desktop(메모리 8GB 이상 할당), python3. JDK는 필요 없다.

```bash
./scripts/cluster.sh up                 # 이미지 빌드, 모니터링·알람·클러스터 기동, 토픽 생성
open http://localhost:3000              # Grafana. Lab / Overview가 홈 화면이다. 맨 위 줄이 알람이다
./scripts/alerts.sh watch               # 다른 터미널에 띄워 둔다
```

| 단계 | 시작 명령 | 소요 |
|---|---|---|
| 환경·모니터링·알람 | [02-monitoring.md](docs/02-monitoring.md)와 [03-alerting.md](docs/03-alerting.md)의 진행 방식 | 1시간 |
| Stage 1 | `BROKER_DISK_MB=1024 ./scripts/cluster.sh reset`, `./scripts/perf.sh all` | 측정 50분, 문서 작성 반나절 |
| Stage 2 | `./scripts/run.sh cooperative kill9`(1칸), `./scripts/run-matrix.sh`(20칸), `./scripts/migration.sh start` | 3주 |
| Stage 3 | `./scripts/repro-18376.sh prepare`, `run 3`, `profile consumer-4.1.2` | 측정 40분, 분석 하루 |
| Stage 4 | `./scripts/drill.sh list`, `./scripts/drill.sh start D01` | 훈련당 15~25분 |
| Stage 5 | `./scripts/chaos.sh start`, `./scripts/chaos.sh report`, `./scripts/chaos.sh reveal` | 회차당 30~60분 |

정리는 `./scripts/cluster.sh down`이다(컨테이너와 데이터 볼륨을 지운다). 빈 클러스터로 다시 시작하려면 `./scripts/cluster.sh reset`.

## 문서

| 문서 | 내용 |
|---|---|
| [01-environment.md](docs/01-environment.md) | 구성, 기동, 실험 애플리케이션 동작, 런타임 제어, 파라미터, 산출 파일 |
| [02-monitoring.md](docs/02-monitoring.md) | 수집 구조, 대시보드, 주요 PromQL, 메트릭 이름 규칙과 함정 |
| [03-alerting.md](docs/03-alerting.md) | 알람 설계 원칙, 알람 목록과 억제 규칙, 알람별 런북 |
| [stage1-throughput/](docs/stage1-throughput/README.md) | 측정 방법(README), 프로듀서 내부와 측정 결과(01), 용량 산정(02) |
| [stage2-rebalance/](docs/stage2-rebalance/README.md) | 측정 정의(01), 설정·시나리오별 실험(02), 마이그레이션(03) |
| [stage3-bug-repro/](docs/stage3-bug-repro/README.md) | KAFKA-18376 재현 설계, 결과, 원인 코드, 실무 판단 |
| [stage4-drills/](docs/stage4-drills/README.md) | 훈련 공통 규칙, 권장 순서, D01~D17 훈련별 문서 |
| [stage5-chaos/](docs/stage5-chaos/README.md) | 규칙, 카드 설계 기준, 비공개 카드 작성법, [채점 기준](docs/stage5-chaos/scoring.md) |
| [troubleshooting.md](docs/troubleshooting.md) | 환경·모니터링·훈련 중 자주 발생하는 문제와 조치 |
| [templates/](docs/templates/) | 마이그레이션 런북, 훈련 기록, 장애 보고서 양식 |
| [results/capacity-plan.md](results/capacity-plan.md) | Stage 1 용량 산정서 예시 (가상 서비스, 직접 다시 작성) |
| [results/RESULTS.md](results/RESULTS.md) | Stage 2 결과표와 해석 (직접 작성) |

훈련 문서(Stage 4)는 위쪽에 목적·개요·진행 방식, 아래쪽에 해설(메커니즘, 진단 경로, 조치 선택지, 코드 근거, 재발 방지)을 둔다. 해설은 직접 진단한 뒤에 읽는다. Stage 5 카드 파일(`scripts/chaos/cards/`)은 정답이므로 회차를 마칠 때까지 열지 않는다.

## 레포 구조

```
docker-compose.yml        KRaft 3+3, 앱(producer, state-producer, consumer-1..6, lag-monitor, disk-monitor),
                          Prometheus, Alertmanager, Grafana
docker/kafka/             apache/kafka + JMX exporter javaagent 이미지(4.1.2, 업그레이드 훈련용 4.0.1)
monitoring/
  jmx/                    JMX exporter 규칙 (kafka.yml: 서버, client.yml: 클라이언트와 실험 앱)
  prometheus/             수집 설정, rules/(알람 규칙)
  alertmanager/           라우팅·억제 설정
  grafana/                데이터소스·대시보드 프로비저닝, gen_dashboards.py(대시보드 생성기)
settings/*.env            Stage 2 설정 프로파일 5개
lab-app/                  Java 실험 앱 (producer / consumer / lag-monitor / disk-monitor / bench)
scripts/
  cluster.sh              클러스터 up / status / down / reset
  alerts.sh               알람 조회(current, pending, history, first, watch)
  perf.sh, perf_report.py Stage 1 처리량 측정과 리포트
  run.sh, run-matrix.sh   Stage 2 설정 × 시나리오 실행
  migration.sh            Stage 2 마이그레이션 단계 실행
  repro-18376.sh          Stage 3 버그 재현(행렬 측정, JFR 프로파일), jfr_top.py
  workload.sh             Stage 4·5 공통 워크로드
  drill.sh, drills/       Stage 4 훈련 실행기와 훈련 정의 D01~D17
  upgrade.sh              노드 단위 롤링 업그레이드·롤백(D13)
  chaos.sh, chaos/cards/  Stage 5 실행기와 카드 C1~C5 (정답 포함)
  lib.sh, lib-fault.sh    공통 함수, 장애 주입 원시 동작
  analyze.py, promq.py    run 분석·결과표, PromQL 조건 대기
docs/                     절차 문서
results/capacity-plan.md  Stage 1 용량 산정서 예시
results/RESULTS.md        Stage 2 결과표와 해석
results/runs/             run별 원본 기록 (git 제외)
results/chaos/            Stage 5 회차 기록 (봉인 파일은 git 제외)
```

## 완료 기준

- [ ] 대시보드 3개에서 [02-monitoring.md](docs/02-monitoring.md)의 확인 항목을 모두 찾았다
- [ ] [03-alerting.md](docs/03-alerting.md)의 알람을 하나 이상 직접 울리고, Alertmanager와 Grafana에서 발화·해소를 확인했다
- [ ] Stage 1: 처리량-지연 곡선을 해석하고 용량 산정서를 작성했다 → `results/capacity-plan.md`
- [ ] Stage 2: 5×4 결과표를 채우고 설정별 차이를 해석했다 → [results/RESULTS.md](results/RESULTS.md)
- [ ] Stage 2: classic → consumer 마이그레이션 런북을 작성했다 → `results/migration-runbook.md`
- [ ] Stage 3: 재현 결과표와 원인 코드 분석을 자기 측정값으로 다시 썼다 → `results/repro-18376.md`
- [ ] Stage 4: D01~D17 훈련 기록을 남겼다 → `results/drills/Dxx.md`
- [ ] Stage 5: [완료 기준](docs/stage5-chaos/README.md#완료-기준)을 충족했다 → `results/chaos/<id>/report.md`
