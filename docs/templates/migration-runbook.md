# 런북: classic → consumer 프로토콜 마이그레이션

> 작성일: YYYY-MM-DD / 기준 버전: Kafka 4.1.x / 근거 run: `results/runs/<RUN_ID>`

## 1. 적용 범위

- 대상 그룹:
- 클라이언트 종류와 버전(Java `kafka-clients` / librdkafka 등):
- 현재 설정: `partition.assignment.strategy`, `session.timeout.ms`, `max.poll.interval.ms`, `group.instance.id` 사용 여부

## 2. 사전 점검

| 항목 | 확인 방법 | 기준 | 결과 |
|---|---|---|---|
| 브로커 `group.version` | `kafka-features.sh describe` | 1 이상 | |
| `group.consumer.migration.policy` | 브로커 설정 | `bidirectional` 또는 `upgrade` | |
| 클라이언트 할당자 | 클라이언트 설정 | userData를 쓰는 커스텀 할당자가 아님 | |
| 그룹 크기 | `--describe --state` | `group.consumer.max.size` 이하 | |
| 클라이언트 지원 여부 | 릴리스 노트 | Java 4.0+, librdkafka 2.12.0+ | |
| consumer 프로토콜 비호환 설정 제거 | 설정 diff | `partition.assignment.strategy`, `session.timeout.ms`, `heartbeat.interval.ms` 제거 | |
| 세션 타임아웃 대응 값 | 그룹 설정 `consumer.session.timeout.ms` 또는 브로커 기본값 | 기존 `session.timeout.ms`와의 차이 검토 | |

## 3. 절차

| 단계 | 작업 | 기대 상태 | 확인 명령 / 로그 | 실습 측정값 |
|---|---|---|---|---|
| 1 | 첫 인스턴스를 `group.protocol=consumer`로 재시작 | `type=Consumer`, upgraded 1 | `Converted the classic group to a consumer group.` | 정지 파티션 _, 리밸런싱 _s |
| 2 | 나머지 인스턴스 롤링 재시작 | upgraded 증가 | `--members --verbose`의 `UPGRADED` | |
| 3 | 전체 전환 완료 확인 | upgraded = 전체 | | |

## 4. 중단·롤백 기준

- 중단 조건(예: lag이 _ 이상 _분 지속, 중복 _건 이상):
- 롤백 절차: consumer 인스턴스를 classic 설정으로 롤링 재시작한다. 마지막 consumer 인스턴스가 떠날 때 `Converted the consumer group to a classic group.`
- 롤백 시 기대 영향(실습 측정값):

## 5. 혼합 상태 주의 사항

- 할당 계산 주체:
- classic 멤버 장애 시 영향(실습 측정값):
- consumer 멤버 장애 시 영향(실습 측정값):

## 6. 모니터링 항목

| 항목 | 출처 | 정상 범위 | 경보 기준 |
|---|---|---|---|
| 그룹 type/state | `kafka-consumer-groups.sh --describe --state` | | |
| lag | | | |
| `rebalance-total`, `rebalance-latency-avg` | 컨슈머 JMX | | |
| 브로커 그룹 코디네이터 로그 | `Converted`, `Cannot upgrade`, `Cannot downgrade` | | |

## 7. 실습에서 확인하지 못한 항목

-
