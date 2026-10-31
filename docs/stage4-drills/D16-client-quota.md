# D16. 기본 클라이언트 쿼터로 인한 소비 지연

## 목적

브로커 쿼터가 클라이언트를 어떻게 늦추는지 확인한다. 쿼터는 오류를 내지 않고 응답을 지연시키기 때문에 "컨슈머가 느려졌다"로만 보인다. 컨슈머가 바쁜 것인지, 데이터를 못 받아 기다리는 것인지를 지표로 가르는 연습이다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 플랫폼 팀이 시끄러운 테넌트를 막으려고 모든 클라이언트에 기본 쿼터를 걸었다. 이 서비스는 예외 등록이 안 됐다. |
| 주입 | 클라이언트 기본값 `consumer_byte_rate=2048`(브로커당 초당 2KB) |
| 예상 증상 | 컨슈머 처리량이 떨어지고 lag이 늘어난다. 오류는 없다. 컨슈머 poll idle ratio가 오른다. |
| 모범 조치 | 기본 쿼터는 그대로 두고 이 서비스의 client-id에 충분한 쿼터를 따로 준다. |
| 소요 | 15분 |

## 진행 방식

```bash
./scripts/drill.sh start D16
```

1. 알람을 본다. `ClientThrottled`와 `ConsumerLagGrowing` 중 어느 것이 먼저 울리는가.
2. Overview의 Consumer processed, Committed lag을 본다. 전 파티션인가 일부인가.
3. Clients의 Poll idle ratio, Fetch lag max per member를 본다. 컨슈머가 처리하느라 바쁜가, 기다리는가.
4. Cluster의 Client throttle time을 본다. 어느 client-id가, 어느 브로커에서 throttle을 받는가.
5. 쿼터 설정을 확인한다:
   ```bash
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker-1:9092 --entity-type clients --describe
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker-1:9092 --entity-type users --describe
   ```
6. 조치한다. 기본 쿼터를 지우는 것과 예외를 주는 것 중 무엇이 맞는지 정하고 실행한다.
7. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- 브로커당 2KB/s인데 컨슈머 3대 전체 처리량은 얼마까지 나오는가. 쿼터 단위(client-id, 브로커)를 계산으로 맞춰 본다
- 프로듀서는 영향을 받는가
- 컨슈머 로그에 오류가 있는가

---

## 해설

### 메커니즘

브로커는 fetch 응답을 만들기 전에 응답 크기를 계산해 클라이언트의 쿼터 센서에 기록한다. 최근 창(기본 11개 × 1초)의 평균이 쿼터를 넘으면 쿼터 아래로 내려오는 데 필요한 시간을 계산한다. 그 경우 기록한 값을 되돌리고, 데이터 없는 빈 응답에 `throttle_time_ms`를 실어 즉시 보낸 뒤, 그 시간 동안 연결을 음소거(mute)해 다음 요청을 읽지 않는다(KIP-219). 클라이언트도 응답의 throttle 시간만큼 그 브로커로 요청을 보내지 않는다.

결과적으로 fetch의 상당수가 빈 응답으로 돌아오고, 데이터가 실린 응답은 드물다. 컨슈머 스레드는 `poll()` 안에서 데이터를 기다리며 시간을 보내므로 poll idle ratio가 오른다. 처리 로직이 느려서 lag이 늘 때(D01)는 반대로 idle ratio가 내려간다. 같은 lag 증가라도 이 지표 하나로 방향이 갈린다.

쿼터는 브로커마다 따로 계산된다. 브로커 3대에서 `consumer_byte_rate=2048`이면 client-id 하나가 클러스터 전체에서 최대 약 6KB/s를 받는다. 쿼터의 주체는 client-id다(사용자 인증이 없으므로). 이 레포의 컨슈머는 client-id가 `consumer-1`~`consumer-3`이라 각자 쿼터를 받는다. 같은 client-id를 여러 인스턴스가 공유하면 쿼터도 나눠 쓴다.

쿼터 적용 우선순위는 구체적인 것이 먼저다: `(user, client-id)` → `(user, 기본 client-id)` → `user` → `(기본 user, client-id)` → `(기본 user, 기본 client-id)` → `기본 user` → `client-id` → `기본 client-id`. 그래서 기본값을 두고 특정 client-id만 올리는 조치가 가능하다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | `ConsumerLagGrowing`, 컨슈머 처리량 하락 | 소비가 유입을 못 따라간다 |
| 범위 | 전 파티션, 전 멤버. 프로듀서와 브로커 지연은 정상 | 컨슈머 경로 |
| 방향 | poll idle ratio 상승(약 0.2 → 0.4대), 처리 시간 설정 변화 없음 | 처리가 느린 것이 아니라 데이터를 못 받는다 |
| 원인 | `ClientThrottled`, 브로커의 `fetch_throttle_time{client_id}`, 컨슈머의 `fetch-throttle-time-avg` | 쿼터 |
| 확인 | `kafka-configs.sh --entity-type clients --describe`의 기본 client-id 쿼터 | 기본 쿼터에 걸렸다 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 서비스 client-id에 쿼터 예외(`--entity-name consumer-1 ... consumer_byte_rate=10485760`) | 이 서비스만 회복. 기본 쿼터의 목적(시끄러운 테넌트 차단)은 유지 | client-id가 늘어날 때마다 등록해야 한다. 실무에서는 user(인증 주체) 단위로 주는 편이 관리가 쉽다 |
| 기본 쿼터 삭제 | 즉시 회복 | 플랫폼 팀이 막으려던 문제가 다시 생긴다. 내 서비스의 장애를 남의 결정을 되돌려 푸는 조치다 |
| 컨슈머 증설 | client-id가 늘어 총 쿼터가 커진다 | 증상을 쿼터 우회로 덮는다. 파티션 수 이상으로는 늘릴 수 없다 |
| `fetch.max.bytes` 조정 | 효과 없다 | 쿼터는 바이트율이라 요청 크기와 무관하다 |

### 코드 근거 (4.1.2)

- `KafkaApis.handleFetchRequest()`(`core/src/main/scala/kafka/server/KafkaApis.scala:555`): 691행에서 응답 크기를 계산하고 694행에서 `quotas.fetch.maybeRecordAndGetThrottleTimeMs()`로 기록한다. 요청 비율 쿼터(693행)와 둘 중 큰 값을 쓴다. throttle이 필요하면 701행에서 기록을 되돌리고 708행에서 빈 응답(`getThrottledResponse`)을 만든다.
- `ClientQuotaManager.recordAndGetThrottleTimeMs()`(`core/src/main/scala/kafka/server/ClientQuotaManager.scala:233`): 센서에 기록하다 `QuotaViolationException`(239행)이 나면 `throttleTime()`(339행)으로 지연 시간을 계산한다.
- `ClientQuotaManager.throttle()`(294행): 채널 음소거용 `ThrottledChannel`을 지연 큐에 넣는다. 지연이 끝나면 음소거를 푼다.
- 우선순위: 같은 파일의 기본 쿼터 콜백 `quotaLimit()`(545행 부근)이 위 순서로 쿼터를 찾는다.

### 재발 방지

- 알람: `ClientThrottled`(fetch 또는 produce throttle 평균 > 0, 1분). 실측에서 주입 72초 뒤 울렸고, `ConsumerLagGrowing`은 87초 뒤였다. 원인 쪽 알람이 먼저 온다.
- 쿼터 변경은 영향을 받는 client-id 목록과 함께 공지하는 변경 관리 대상으로 둔다.
- 서비스마다 고유한 client-id(또는 user)를 쓰게 해 쿼터 대상을 식별할 수 있게 한다.

### 재현 충실도

2KB/s는 이 레포의 처리량(약 140KB/s)에 맞춘 값이다. 실무 쿼터는 MB/s 단위이고, 피크 시간에만 넘는 경우가 많아 증상이 간헐적으로 나타난다. 이 훈련은 지속적으로 넘는 경우만 다룬다.
