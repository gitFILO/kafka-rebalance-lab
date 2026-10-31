# D10. 큰 메시지로 인한 RecordTooLargeException

## 목적

메시지 크기 한도가 클라이언트와 브로커(토픽) 양쪽에 있고, 한쪽만 올리면 다른 쪽에서 거부된다는 것을 확인한다. 오류가 어디서 나는지(클라이언트 사전 검사, 브로커 거부)를 메트릭과 예외로 구분한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 상류 서비스가 첨부 데이터를 포함한 1.2MB 메시지를 보내기 시작했다. |
| 주입 | 10초마다 1.2MB 레코드 1건(`control/producer.large` = `1200000 10000`) |
| 예상 증상 | 프로듀서 오류 메트릭에 `RecordTooLargeException`이 증가한다. 일반 레코드는 정상 전송된다. |
| 모범 조치 | 토픽 `max.message.bytes`와 프로듀서 `max.request.size`를 함께 4MB로 올린다. |
| 소요 | 15분 |

## 진행 방식

```bash
./scripts/drill.sh start D10
```

1. Clients의 Errors by type과 Overview의 Producer send / ack / fail / skip을 본다.
2. 오류가 클라이언트에서 났는지 브로커에서 났는지 가른다. Cluster의 Failed produce / fetch requests가 증가하는가.
3. 현재 한도를 확인한다.

```bash
docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker-1:9092 \
  --entity-type topics --entity-name lab-events --describe --all | grep max.message.bytes
docker exec producer env | grep -i request
```

4. 토픽 한도만 먼저 올려 본다. 오류가 사라지는가.

```bash
docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker-1:9092 \
  --entity-type topics --entity-name lab-events --alter --add-config max.message.bytes=4194304
```

5. 프로듀서 한도를 올려 재배포한다(`run.env`를 불러온 셸에서 `MAX_REQUEST_SIZE=4194304 docker compose --profile lab up -d --no-deps producer`).
6. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- 4단계 후에도 오류가 남는 이유와 그 예외가 나는 코드 위치
- 프로듀서 한도만 올렸다면 어디서 거부됐을까
- 컨슈머 쪽 한도(`max.partition.fetch.bytes`, `fetch.max.bytes`)는 왜 같이 올리지 않아도 되는가

---

## 해설

### 메커니즘

크기 검사는 두 곳에서 한다.

| 위치 | 한도 | 시점 |
|---|---|---|
| 프로듀서 `send()` | `max.request.size`(기본 1MB), `buffer.memory` | 직렬화 직후, 네트워크 전송 전 |
| 브로커 로그 append | 토픽 `max.message.bytes`(브로커 기본 `message.max.bytes`, 약 1MB) | 배치를 로그에 쓰기 전 |

1.2MB 레코드는 프로듀서 사전 검사에서 바로 실패한다. 브로커까지 가지 않으므로 Failed produce / fetch requests는 변하지 않는다. 토픽 한도만 올려도 같은 오류가 남는다. 프로듀서 한도만 올리면 브로커가 배치를 거부해 같은 예외가 응답으로 돌아온다.

컨슈머는 `fetch.max.bytes`보다 큰 배치도 첫 배치이면 받아 오므로 진행이 막히지 않는다(KIP-74).

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | Errors by type의 `RecordTooLargeException` | 크기 한도 |
| 범위 | 일반 레코드 ack 정상, 오류는 10초 주기 | 특정 레코드만 |
| 원인 | Failed produce / fetch requests 변화 없음 | 클라이언트 사전 검사에서 거부 |
| 확인 | 토픽·프로듀서 설정값과 레코드 크기 비교 | 한도 확정 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 양쪽 한도 상향 | 전송 성공 | 브로커 메모리·복제 부담, 컨슈머 배치 크기 증가 |
| 큰 데이터는 외부 저장소에 두고 참조만 전송(claim check) | 근본 해결 | 상류 변경 필요 |
| 압축 | 크기 감소 | 압축 전 크기가 프로듀서 검사 기준인 경우가 있어 효과가 제한적이다 |
| 상류에서 분할 | 한도 유지 | 재조합 로직이 필요하다 |

### 코드 근거 (4.1.2)

- `KafkaProducer.ensureValidRecordSize()`(`clients/.../producer/KafkaProducer.java:1162`): 직렬화 크기가 `max.request.size`나 `buffer.memory`보다 크면 `RecordTooLargeException`
- `UnifiedLog.java:1148`, `1509`: 배치 크기가 토픽 `max.message.bytes`보다 크면 `RecordTooLargeException`

### 재발 방지

- 알람: `sum(increase(lab_producer_error_count{error="RecordTooLargeException"}[5m])) > 0`(실무에서는 앱의 send 콜백 실패 메트릭)
- 토픽 생성 시 메시지 크기 상한을 계약으로 문서화하고 상류와 합의한다.

### 재현 충실도

실무에서는 실패한 레코드를 앱이 로그만 남기고 버리는 경우가 많아, 오류가 앱 로그에만 남는다. 이 랩은 오류를 메트릭으로 노출한다.
