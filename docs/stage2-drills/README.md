# Stage 2. 단일 장애 훈련

실무에서 자주 나는 장애 11개를 하나씩 주입하고, 대시보드에서 증상을 확인한 뒤 원인을 좁히고 조치한다. 장애가 무엇인지 알고 시작하는 훈련이다.

## 목적

- 장애마다 메트릭과 로그에 남는 흔적을 직접 확인하고 목록으로 만든다. Stage 3 블라인드 훈련은 이 목록이 없으면 진단이 아니라 추측이 된다.
- 증상 → 가설 → 근거로 소거 → 조치 → 검증의 순서를 몸에 익힌다. 코드는 문서와 관찰이 어긋날 때 판정하는 용도로 쓴다.
- 조치마다 부작용을 말할 수 있게 한다. 같은 증상에 쓸 수 있는 조치가 여러 개일 때 무엇을 먼저 하고 무엇을 하지 말아야 하는지가 상급과 중급을 가른다.
- 장애마다 알람 조건과 런북 항목을 남긴다. 커리큘럼 Phase 6(관측)과 Phase 7(포스트모템)의 재료가 된다.

## 개요

| ID | 장애 | 영역 | 첫 증상 | 소요 |
|---|---|---|---|---|
| [D01](D01-downstream-latency.md) | 다운스트림 지연으로 인한 lag 누적 | 컨슈머 | 전 파티션 lag 증가 | 15분 |
| [D02](D02-rebalance-storm.md) | poll 간격 초과로 인한 리밸런스 폭풍 | 컨슈머 | 처리량 0, 그룹 상태 반복 전이 | 20분 |
| [D03](D03-poison-pill.md) | poison pill 무한 재시도 | 컨슈머 | 파티션 1개 lag만 증가 | 20분 |
| [D04](D04-hot-partition.md) | 키 쏠림으로 인한 hot partition | 컨슈머·설계 | 파티션 1개 lag 증가, 증설 무효 | 20분 |
| [D05](D05-broker-failure.md) | 브로커 1대 비정상 종료 | 브로커 | URP, 리더 이동 | 15분 |
| [D06](D06-slow-broker.md) | 느린 브로커(주기적 정지) | 브로커 | 프로듀서 지연 증가 | 20분 |
| [D07](D07-disk-full.md) | 브로커 디스크 풀 | 브로커 | 디스크 여유 0, 브로커 종료 | 20분 |
| [D08](D08-min-isr.md) | `min.insync.replicas` 미달로 쓰기 거부 | 복제 | 생산 중단, 커밋 실패 | 15분 |
| [D09](D09-offset-expiry.md) | 오프셋 만료 후 latest 리셋으로 인한 조용한 유실 | 오프셋 | 증상 없음(다운스트림 신고) | 25분 |
| [D10](D10-record-too-large.md) | 큰 메시지로 인한 `RecordTooLargeException` | 프로듀서·토픽 | 프로듀서 오류 | 15분 |
| [D11](D11-hanging-transaction.md) | 열린 트랜잭션이 LSO를 막음 | 트랜잭션 | `read_committed` lag 증가 | 25분 |

권장 순서는 표의 순서다. D01~D04는 컨슈머 쪽, D05~D08은 브로커·복제 쪽, D09~D11은 증상과 원인이 멀리 떨어진 유형이다.

### 공통 워크로드

모든 훈련은 같은 정상 상태에서 시작한다. `workload.sh`가 띄운다.

| 항목 | 값 |
|---|---|
| 그룹 프로토콜 | consumer(KIP-848), 서버 할당자 uniform (`WORKLOAD_SETTING`으로 변경) |
| 컨슈머 | 3대(consumer-1~3), 대당 파티션 4개. consumer-4~6은 증설 여유분 |
| 프로듀서 | 600 msg/s, `acks=all`, 멱등 |
| 처리 시간 | 레코드당 2ms. 컨슈머 1대 최대 처리량 약 400~500 msg/s |
| 커밋 | 배치마다 `commitAsync`, revoke 시 `commitSync` |

### 축척

로컬 환경에서 수 분 안에 재현하려고 일부 값을 줄였다. 운영 값과 비교해 해석한다.

| 항목 | 운영 기본값 | 이 레포 | 영향 받는 훈련 |
|---|---|---|---|
| `max.poll.interval.ms` | 300000 | 30000 | D02 |
| `offsets.retention.minutes` | 10080(7일) | 1 (D09에서만) | D09 |
| `log.retention.ms` | 604800000(7일) | 3600000(1시간). Stage 3은 토픽 설정으로 10분 | 전체(디스크 보호) |
| `log.roll.ms` | 604800000(7일) | 60000(1분) | 전체(디스크 보호) |
| `log.retention.check.interval.ms` | 300000 | 30000 | Stage 3 |
| 브로커 디스크 | 수 TB | 256MB tmpfs | D07 |
| `transaction.timeout.ms` | 60000 | 900000 (D11에서만) | D11 |

### 장애 주입 방식

| 장애 종류 | 방식 | 구현 |
|---|---|---|
| 느린 브로커 | 백그라운드 루프가 대상 컨테이너를 `docker pause`/`unpause`로 주기적으로 멈춘다. Docker Desktop 커널은 tc qdisc가 없어 netem 대신 쓴다 | `fault_freeze`, `fault_unfreeze` |
| 디스크 풀 | 브로커 데이터 볼륨에 파일을 채움 | `fault_disk_fill` |
| 프로세스 장애 | `docker kill -s KILL`, `docker stop`(controlled shutdown) | `fault_broker_kill`, `fault_broker_stop` |
| 앱 동작 | `control/` 파일([01-environment.md](../01-environment.md#런타임-제어)) | `ctl_set` |
| 브로커 설정 | 환경 변수를 바꿔 브로커 재생성 | D09 |

함수는 `scripts/lib-fault.sh`에 있다. 훈련 정의는 `scripts/drills/Dxx-*.sh`에 있고, 주입·신호 판정·모범 조치·복구 판정이 함수 하나씩으로 나뉘어 있다.

## 진행 방식

### 1회 진행

```bash
./scripts/drill.sh list
./scripts/drill.sh start D03     # 워크로드 기동 → 30초 워밍업 → 주입
# 대시보드로 진단하고 직접 조치한다
./scripts/drill.sh verify        # 복구 기준 판정
./scripts/drill.sh stop          # 장애 흔적 정리, 워크로드 종료
```

훈련 문서는 위쪽(목적, 개요, 진행 방식)과 아래쪽(해설)으로 나뉜다. 처음에는 위쪽만 읽고 시작한다. 직접 원인을 찾고 조치한 다음 해설을 읽고 비교한다.

1. **시작:** `drill.sh start Dxx`. 주입 시점은 Grafana에 `INJECT Dxx` 어노테이션으로 표시된다.
2. **감지:** Overview에서 이상이 보이는 패널을 찾는다. 무엇이 보였는지, 언제부터인지 적는다.
3. **범위 축소:** 영향이 전체인가 일부인가(파티션, 멤버, 브로커). 생산·저장·소비 중 어느 구간인가. Cluster, Clients 대시보드로 내려간다.
4. **근거 확보:** CLI(`kafka-consumer-groups.sh`, `kafka-topics.sh`, `kafka-transactions.sh` 등), 컨테이너 로그, run 디렉터리의 이벤트 CSV로 가설을 확인하거나 버린다.
5. **조치:** 문서의 "조치 선택지"를 보기 전에 스스로 조치를 정하고 실행한다. 막히면 `drill.sh fix`로 모범 조치를 적용하고 그 내용을 해설과 대조한다.
6. **검증:** `drill.sh verify`. 대시보드에서도 회복을 확인한다.
7. **기록:** [templates/drill-runbook.md](../templates/drill-runbook.md)를 복사해 채운다: `mkdir -p results/drills && cp docs/templates/drill-runbook.md results/drills/D01.md`.
8. **정리:** `drill.sh stop`.

### 자동 검증(selftest)

`drill.sh selftest Dxx`는 주입 → 신호 판정 → 모범 조치 → 복구 판정을 사람 개입 없이 수행하고 `results/runs/<run>/selftest.txt`에 PASS/FAIL을 남긴다. 환경을 바꾼 뒤(버전 업그레이드, 파라미터 변경) 훈련이 여전히 재현되는지 확인하는 용도다. 훈련 자체를 대신하지 않는다.

## 완료 기준

- [ ] 11개 훈련을 모두 직접 진단·조치했다(모범 조치에 의존하지 않은 횟수를 기록)
- [ ] 훈련마다 런북(증상, 진단 경로, 조치, 부작용, 알람 조건)을 작성했다 → `results/drills/`
- [ ] 장애별 흔적 목록(어느 패널, 어떤 모양)을 한 장으로 정리했다 → Stage 3 진단 기준
