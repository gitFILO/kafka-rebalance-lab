# Stage 1. 리밸런싱 비교 실험과 마이그레이션

커리큘럼 Phase 4 범위다. 같은 장애를 리밸런싱 설정만 바꿔 주입하고, 결과 차이를 수치와 4.1.2 소스 코드로 설명한다.

## 목적

리밸런싱 설정의 차이를 문서상 설명이 아니라 측정값으로 설명할 수 있게 만든다. 단계를 마치면 다음 질문에 수치와 코드 근거로 답할 수 있어야 한다.

1. 같은 장애에서 eager, cooperative, consumer 프로토콜은 파티션을 얼마나 오래, 몇 개나 멈추는가.
2. static membership은 어떤 장애에 효과가 있고 어떤 장애에는 효과가 없는가.
3. 장애 감지 시간과 중복 건수를 결정하는 설정은 무엇이고, 그 설정은 클라이언트와 브로커 중 어디에 있는가.
4. 운영 중인 classic 그룹을 중단 없이 consumer 프로토콜로 옮길 수 있는가. 옮기는 동안과 되돌릴 때 무엇이 바뀌는가.

Stage 2·3에서 장애를 진단할 때 "리밸런싱이 원래 이 정도 걸리는가, 비정상인가"를 판단하는 기준선이 이 단계의 결과다.

## 개요

| 구분 | 내용 | 산출물 |
|---|---|---|
| 비교 실험 | 설정 5개(eager, cooperative, cooperative-static, consumer, consumer-static) × 시나리오 4개(rolling-restart, kill9, stall, scale-out) = 20칸. 칸마다 리밸런싱 시간, 정지된 파티션 수, lag 피크, 중복 건수를 측정한다. | 5×4 결과표와 해석 ([results/RESULTS.md](../../results/RESULTS.md)) |
| 마이그레이션 | classic 그룹에 consumer 멤버를 롤링 투입해 자동 전환을 브로커 로그로 확인하고, 혼합 상태에서 장애를 측정한 뒤 classic으로 되돌린다. | 마이그레이션 런북 |

공통 조건은 Kafka 4.1.2 KRaft(controller 3, broker 3), 파티션 12, 컨슈머 6대, 600 msg/s, 세션 타임아웃 45초, `max.poll.interval.ms` 30초다. run마다 새 그룹을 만들고, 장애 주입 후 90초 관찰하고, CSV를 자동 분석한다. 같은 시간 동안 Prometheus가 모든 JVM을 수집하므로 대시보드로 같은 사건을 다시 볼 수 있다.

## 진행 방식

| 순서 | 문서 | 할 일 | 소요 |
|---|---|---|---|
| 1 | [01-measurement.md](01-measurement.md) | 측정 항목의 정의와 한계를 읽는다. 특히 "정지된 파티션"의 재할당/비재할당 구분 | 30분 |
| 2 | [02-experiments.md](02-experiments.md) | 시나리오별 메커니즘을 읽고 1칸을 실행한다. 대시보드와 `summary.md`를 대조한다 | 1시간 |
| 3 | 같은 문서 | `run-matrix.sh`로 20칸을 실행하고 결과표를 만든다. 필요하면 반복 측정한다 | 칸당 약 2분 30초, 1회 약 60분 |
| 4 | [results/RESULTS.md](../../results/RESULTS.md) | 결과표를 옮기고 해석을 쓴다 | |
| 5 | [03-migration.md](03-migration.md) | 마이그레이션 단계를 하나씩 실행하며 브로커 로그와 대시보드를 확인한다 | 1~2시간 |
| 6 | [templates/migration-runbook.md](../templates/migration-runbook.md) | 런북을 작성한다 | |

```bash
./scripts/cluster.sh up
./scripts/run.sh cooperative kill9      # 1칸
./scripts/run-matrix.sh                 # 20칸
python3 scripts/analyze.py table        # 결과표
./scripts/migration.sh start            # 마이그레이션
```

### 대시보드로 볼 것

실험 스크립트는 `TRIGGER`, `KILL9`, `RESTART`, `STALL`, `START`, `SWITCH` 같은 시점을 Grafana 어노테이션으로 남긴다. 칸마다 다음 패널을 본다.

| 시나리오 | Overview에서 볼 것 | Clients에서 볼 것 |
|---|---|---|
| rolling-restart | Assigned partitions per member가 계단식으로 이동하는 모양. eager는 매번 모든 멤버가 0으로 떨어진다 | Rebalance latency max, Rebalances per hour |
| kill9 | 세션 타임아웃(45초) 동안 그룹 상태는 Stable이고 lag만 오르는 구간 | Last heartbeat seconds ago는 죽은 멤버의 값이 없어진다 |
| stall | 지연 멤버의 처리량 0, 30초 후 파티션 이동. static 여부에 따라 이동 여부가 다르다 | Time between poll max가 30초를 넘는 시점 |
| scale-out | 멤버 증가, 할당이 2개씩으로 수렴하는 과정 | Rebalance callbacks |

## 완료 기준

- [ ] 5×4 결과표를 채우고 설정별 차이를 해석했다
- [ ] 대시보드에서 본 모양과 CSV 분석값이 다른 칸에 대해 이유를 적었다
- [ ] classic → consumer 마이그레이션 런북을 작성했다
