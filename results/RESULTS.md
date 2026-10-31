# Stage 2 리밸런싱 실험 결과

> 환경: Kafka 4.1.2, controller 3 / broker 3, 파티션 12, consumer 6, RATE=600, PROCESS_MS=2, MAX_POLL_INTERVAL_MS=30000
> 측정 횟수: 셀당 _회, 표의 값은 중앙값
> 지표 정의: [docs/stage2-rebalance/01-measurement.md](../docs/stage2-rebalance/01-measurement.md)

`python3 scripts/analyze.py table` 출력을 아래에 붙인다.

### 리밸런싱 시간 (그룹 비안정 누적 / 멤버별 latency-total 최대)

| 설정 | rolling-restart | kill9 | stall | scale-out |
|---|---|---|---|---|
| eager | | | | |
| cooperative | | | | |
| cooperative-static | | | | |
| consumer | | | | |
| consumer-static | | | | |

### 정지된 파티션 수: 전체(재할당/비재할당), 최대 정지 시간

| 설정 | rolling-restart | kill9 | stall | scale-out |
|---|---|---|---|---|
| eager | | | | |
| cooperative | | | | |
| cooperative-static | | | | |
| consumer | | | | |
| consumer-static | | | | |

### lag 피크 (기준값)

| 설정 | rolling-restart | kill9 | stall | scale-out |
|---|---|---|---|---|
| eager | | | | |
| cooperative | | | | |
| cooperative-static | | | | |
| consumer | | | | |
| consumer-static | | | | |

### 중복 건수

| 설정 | rolling-restart | kill9 | stall | scale-out |
|---|---|---|---|---|
| eager | | | | |
| cooperative | | | | |
| cooperative-static | | | | |
| consumer | | | | |
| consumer-static | | | | |

## 해석

각 항목을 "숫자 → 메커니즘 → 코드 근거" 순서로 쓴다.

### 1. eager vs cooperative

### 2. static membership의 효과와 한계

### 3. cooperative vs consumer

### 4. 장애 감지 시간을 결정하는 요소

### 5. 중복이 생기는 경로와 상한

### 6. 혼합 모드(마이그레이션 중) 동작

## classic과 consumer 프로토콜 대비

| 항목 | 이 실험에서의 근거 (셀, 수치) |
|---|---|
| 할당 계산 위치 | |
| 동기화(barrier 유무) | |
| 세대 관리(generation vs epoch) | |
| 장애 영향 범위 | |
| 세션 타임아웃 설정 위치 | |
| static membership | |
