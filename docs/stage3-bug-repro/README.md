# Stage 3. KIP-848 컨슈머 버그 재현: KAFKA-18376

공개된 JIRA 이슈 하나를 이 환경에서 재현하고, 원인이 되는 코드를 측정값으로 확정한다. 이슈 본문의 설명을 그대로 믿지 않고, 수정 전후 클라이언트를 같은 조건에서 비교해 "어느 줄이 비용을 만드는가"를 증명하는 것이 이 단계의 일이다.

## 목적

- 이슈 보고 → 재현 조건 설계 → 측정 → 프로파일링 → 원인 코드 확정 → 수정 버전 대조의 순서를 한 번 끝까지 해 본다.
- Stage 2에서 본 consumer 프로토콜(KIP-848)의 클라이언트 구현(`AsyncKafkaConsumer`)이 classic 구현과 어떻게 다른 스레드 구조를 갖는지 코드 수준에서 이해한다.
- "CPU가 높다"는 증상을 CPU 사용률, 레코드당 CPU, 스레드별 CPU, 컨텍스트 스위치로 나눠 보고, 어느 지표가 원인을 가리키는지 판단한다.
- 운영 판단으로 연결한다. 어떤 설정 조합에서 이 문제가 실제로 영향을 주고, 무엇으로 피하는가.

## 개요

| 항목 | 내용 |
|---|---|
| 이슈 | [KAFKA-18376](https://issues.apache.org/jira/browse/KAFKA-18376) High CPU load when AsyncKafkaConsumer uses a small max poll value |
| 증상 | `group.protocol=consumer`에서 `max.poll.records`가 작으면 classic보다 처리량이 크게 낮고 레코드당 CPU가 크게 높다 |
| 영향 | 4.0.x, 4.1.x 클라이언트(브로커 버전과 무관) |
| 수정 | 4.2.0. `poll()`이 백그라운드 스레드를 기다리지 않는 구조(`AsyncPollEvent`)로 바뀌었다. 같은 릴리스의 KAFKA-19589(위치 검증 생략), KAFKA-19820(fetch 가능 파티션 복사 제거)이 함께 기여한다 |
| 재현 환경 | 브로커 4.1.2. 컨슈머만 클라이언트 4.1.2와 4.2.0 두 이미지로 바꿔 끼운다 |
| 재현 결과 | 재현됨. `max.poll.records=1`에서 4.1.2 consumer는 classic 대비 처리량 1/29, 레코드당 CPU 11.6배. 4.2.0에서 처리량 16배 회복 |
| 확정한 원인 | poll 한 번마다 애플리케이션 스레드와 백그라운드 스레드 사이에 블로킹 왕복이 두 번 일어난다(`AsyncKafkaConsumer.java:838`, `:1835`). 실제 일보다 스레드 깨우기와 대기가 시간을 쓴다 |

이 이슈를 고른 이유는 세 가지다. 클라이언트만 바꾸면 재현되므로 이 레포의 클러스터를 그대로 쓸 수 있다. 증상이 수치로 나오므로 재현 여부를 판정할 수 있다. 수정 커밋이 있어 수정 전후를 같은 조건에서 비교할 수 있다. 정답이 있는 문제로 방법을 익힌 뒤, 아직 열려 있는 이슈에 같은 방법을 쓰는 것이 다음 단계다.

## 진행 방식

| 순서 | 할 일 | 산출물 | 소요 |
|---|---|---|---|
| 1 | JIRA 본문과 댓글을 읽고 보고자의 주장을 가설로 적는다. "무엇을 바꾸면 증상이 커지고 작아지는가" | 가설 3줄 | 20분 |
| 2 | 재현 조건을 설계한다. 아래 "재현 설계"를 읽기 전에 직접 정해 보고 비교한다 | 조건표 | 20분 |
| 3 | `prepare`, `run`으로 행렬을 측정한다 | 결과표 | 약 25분 |
| 4 | `profile`로 스레드별 샘플을 모으고, 결과표의 컨텍스트 스위치와 함께 원인 후보를 좁힌다 | 상위 프레임 요약 | 10분 |
| 5 | 4.1.2와 4.2.0의 `AsyncKafkaConsumer.poll()`을 나란히 읽고, 측정값과 일치하는 줄을 확정한다 | 원인 코드와 근거 | 1시간 |
| 6 | 운영 판단(아래 "실무 판단")을 자기 말로 다시 쓴다 | 한 쪽 보고서 | 30분 |

```bash
./scripts/cluster.sh up                       # 워크로드는 띄우지 않는다
./scripts/repro-18376.sh prepare              # bench-18376 토픽, 2M × 20B 적재, 4.1.2/4.2.0 클라이언트 이미지
./scripts/repro-18376.sh run 3                # 4개 변형 × max.poll.records 1·5·500 × 3회
./scripts/repro-18376.sh report               # 중앙값 표
./scripts/repro-18376.sh profile consumer-4.1.2 1
./scripts/repro-18376.sh profile consumer-4.2.0 1
./scripts/repro-18376.sh profile classic-4.1.2 1
./scripts/repro-18376.sh clean
```

결과는 `results/runs/repro-18376/`에 쌓인다. `results.jsonl`이 run별 원본이고, `profile-*.top.txt`가 프로파일 요약이다.

---

## 재현 설계

poll 한 번에 드는 고정 비용을 키워서 보이게 하는 것이 설계의 중심이다.

| 결정 | 이유 |
|---|---|
| 레코드 20바이트, 처리 로직 없음 | 레코드 처리 비용을 0에 가깝게 만들어 poll 경로의 비용만 남긴다 |
| 미리 적재한 2M 건을 earliest부터 읽음 | 브로커 쪽 유입 속도가 병목이 되지 않게 한다. 측정 내내 fetch 버퍼에 데이터가 있다 |
| 토픽 3파티션, RF 1 | 복제가 측정에 끼어들지 않게 한다 |
| 컨슈머 컨테이너 `--cpus 2`, 매 run 새 JVM과 새 그룹 | CPU를 고정해 run 사이를 비교 가능하게 한다. 이전 run의 커밋 오프셋이 영향을 주지 않는다 |
| 측정 구간은 첫 레코드부터 마지막 레코드까지 | 그룹 참여와 할당 시간을 뺀다 |
| 변수: 클라이언트 4.1.2/4.2.0 × `group.protocol` classic/consumer × `max.poll.records` 1·5·500 | 이슈의 주장("작은 max poll 값")과 수정 여부를 각각 분리한다 |
| 지표: records/s, 프로세스 CPU, 스레드별 CPU, 스레드별 컨텍스트 스위치 | CPU가 "일"에 쓰이는지 "대기와 전환"에 쓰이는지 가른다 |

컨텍스트 스위치는 `/proc/self/task/*/status`의 `voluntary_ctxt_switches`와 `nonvoluntary_ctxt_switches`를 측정 전후로 읽어 poll 횟수로 나눈다. 리눅스 스레드 이름이 15자로 잘리므로 애플리케이션 스레드는 `java`, 백그라운드 스레드는 `consumer_backgr`로 보인다. 프로세스 CPU는 1,024번의 poll마다 한 번만 읽는다. 매 poll마다 읽으면 그 시스템 호출 자체가 비용의 13%를 차지해 측정을 왜곡했다.

## 결과

조건별 3회의 중앙값이다. 측정 환경은 macOS Docker Desktop(리눅스 VM)에서 컨슈머 2 CPU 제한, 브로커 4.1.2다.

| max.poll.records | protocol | client | records/s | CPU ms / 1만 건 | 사용 코어 | poll당 컨텍스트 스위치(애플리케이션 / background) |
|---:|---|---|---:|---:|---:|---|
| 1 | classic | 4.1.2 | 340,533 | 53.8 | 1.83 | 0.00 / - |
| 1 | classic | 4.2.0 | 369,633 | 48.4 | 1.80 | 0.00 / - |
| 1 | consumer | 4.1.2 | **11,604** | **624.4** | **0.73** | **2.00 / 1.99** |
| 1 | consumer | 4.2.0 | 190,059 | 99.4 | 1.88 | 0.04 / 0.04 |
| 5 | classic | 4.1.2 | 763,751 | 26.2 | 2.00 | 0.00 / - |
| 5 | classic | 4.2.0 | 803,583 | 24.6 | 2.01 | 0.00 / - |
| 5 | consumer | 4.1.2 | 59,782 | 150.8 | 0.88 | 2.00 / 1.99 |
| 5 | consumer | 4.2.0 | 439,308 | 44.8 | 1.97 | 0.03 / 0.03 |
| 500 | classic | 4.1.2 | 3,784,352 | 5.0 | 1.86 | 0.04 / - |
| 500 | classic | 4.2.0 | 4,983,914 | 3.9 | 1.92 | 0.04 / - |
| 500 | consumer | 4.1.2 | 2,333,194 | 6.9 | 1.60 | 2.03 / 1.78 |
| 500 | consumer | 4.2.0 | 4,005,635 | 5.0 | 1.99 | 0.04 / 0.54 |

classic 컨슈머는 백그라운드 네트워크 스레드가 없다(하트비트 스레드만 있고 poll과 무관하다). 그래서 background 칸을 `-`로 둔다.

### 읽는 법

1. 재현 여부: `max.poll.records=1`에서 4.1.2 consumer는 classic의 1/29 처리량이고 레코드당 CPU는 11.6배다. `max.poll.records`를 500으로 올리면 격차가 1.6배로 줄어든다. 이슈의 주장과 일치한다.
2. CPU 바운드가 아니다: 4.1.2 consumer는 2코어를 받고도 0.73코어만 쓴다. 일이 많아서 느린 것이 아니라 기다리느라 느리다. poll 한 번에 약 86µs가 걸리고(classic은 약 3µs), 그 대부분이 두 스레드가 서로를 기다리는 시간이다.
3. 대기의 횟수: 두 스레드 모두 poll 한 번에 정확히 두 번씩 잠들었다 깨어난다(2.00 / 1.99). `max.poll.records`와 무관하게 2회이므로 "poll마다 고정으로 두 번의 왕복"이 있다는 뜻이다. 4.2.0에서는 0.04로 사라진다.
4. 이슈 제목이 "High CPU"인 이유: 처리량이 고정된 실제 서비스를 생각하면 된다. 초당 1만 건을 `max.poll.records=1`로 소비하면 4.1.2 consumer는 약 0.62코어, classic은 약 0.05코어를 쓴다. 벤치마크처럼 최대 속도로 읽으면 사용률은 오히려 낮아 보인다. 사용률이 아니라 레코드당 CPU를 봐야 하는 이유다.
5. 수정 후에도 남는 차이: 4.2.0 consumer는 `max.poll.records=1`에서 아직 classic의 절반이다. 두 스레드 구조의 고정 비용(이벤트 큐, 스레드 깨우기)이 남아 있다. 500에서는 classic과 20% 안쪽으로 붙는다.
6. 의미 없는 차이: classic 4.1.2와 4.2.0의 차이는 run 사이 편차(classic은 최대 38%) 안에 있다. classic 경로는 이 수정과 무관하다.

## 원인 코드 확정

경로는 `clients/src/main/java/org/apache/kafka/clients/consumer/internals/`다. 줄 번호는 4.1.2와 4.2.0 릴리스 소스 기준이다.

### 스레드 구조

classic 컨슈머(`ClassicKafkaConsumer`)는 `poll()`을 호출한 애플리케이션 스레드가 직접 네트워크 I/O를 한다. consumer 프로토콜 컨슈머(`AsyncKafkaConsumer`)는 모든 네트워크 작업과 그룹 상태 관리를 백그라운드 스레드(`ConsumerNetworkThread`)에 맡긴다. 애플리케이션 스레드는 이벤트(`ApplicationEvent`)를 큐에 넣고, 필요하면 그 이벤트의 future가 완료되기를 기다린다.

- `ApplicationEventHandler.add()`(4.1.2 `events/ApplicationEventHandler.java:95`)는 이벤트를 큐에 넣고 `networkThread.wakeup()`을 부른다. 이 호출은 `Selector.wakeup()`을 거쳐 eventfd에 쓰는 시스템 호출이다. `epoll_wait`에서 잠든 백그라운드 스레드를 깨운다.
- `addAndGet()`(같은 파일 122~131)은 `add()` 후 `ConsumerUtils.getResult(event.future())`로 완료를 기다린다. 애플리케이션 스레드가 park된다.

### 4.1.2의 poll 한 번

```java
// AsyncKafkaConsumer.java 4.1.2
public ConsumerRecords<K, V> poll(final Duration timeout) {              // 819
    do {
        PollEvent event = new PollEvent(timer.currentTimeMs());          // 831
        applicationEventHandler.add(event);                              // 835  깨우기 1
        ConsumerUtils.getResult(event.reconcileAndAutoCommit(), ...);    // 838  대기 1
        updateAssignmentMetadataIfNeeded(timer);                         // 846  → 1911 → updateFetchPositions(1830)
        final Fetch<K, V> fetch = pollForFetches(timer);                 // 1757 → collectFetch(1812)
        if (!fetch.isEmpty()) {
            sendPrefetches(timer);                                       // 1901
            return ...;
        }
    } while (timer.notExpired());
}

private boolean updateFetchPositions(final Timer timer) {               // 1830
    CheckAndUpdatePositionsEvent e = new CheckAndUpdatePositionsEvent(...);
    cachedSubscriptionHasAllFetchPositions = applicationEventHandler.addAndGet(e);  // 1835  깨우기 2, 대기 2
}

private Fetch<K, V> collectFetch() {                                     // 1812
    final Fetch<K, V> fetch = fetchCollector.collectFetch(fetchBuffer);
    applicationEventHandler.wakeupNetworkThread();                       // 1816  깨우기 3
    return fetch;
}
```

poll 한 번의 흐름은 이렇다.

1. `PollEvent`를 넣고 백그라운드 스레드를 깨운 뒤, 백그라운드가 리밸런스 조정과 자동 커밋 판단을 끝낼 때까지 기다린다(838).
2. 위치 검증 이벤트(`CheckAndUpdatePositionsEvent`)를 넣고 다시 기다린다(1835). 위치가 이미 모두 유효해도 매번 백그라운드에 물어본다.
3. fetch 버퍼에서 레코드를 꺼내고 백그라운드를 한 번 더 깨운다(1816).

1과 2가 블로킹 왕복이다. 왕복 하나는 "eventfd 쓰기 → 백그라운드가 `epoll_wait`에서 깨어나 `runOnce()`로 이벤트 처리 → future 완료 → 애플리케이션 스레드 unpark"로 이뤄진다. 결과표의 "poll당 컨텍스트 스위치 2.00 / 1.99"가 이 두 번의 왕복과 정확히 맞는다.

### 프로파일 근거

JFR을 `settings=profile`로 켜고 `max.poll.records=1`에서 50만 건을 읽었다.

| 변형 | 스레드 | 샘플 종류 | 상위 경로 |
|---|---|---|---|
| consumer 4.1.2 | 애플리케이션(main) | 네이티브 158건 | 100% `ApplicationEventHandler.wakeupNetworkThread → Selector.wakeup → EventFD.set0`. 그중 54.4%가 `collectFetch`, 45.6%가 `updateFetchPositions → addAndGet` |
| consumer 4.1.2 | 백그라운드 | 네이티브 1,284건 | 97.5% `EPoll.wait`(`ConsumerNetworkThread.runOnce → NetworkClient.poll`) |
| consumer 4.1.2 | 둘 다 | Java 실행 30건 | 39초 동안 30건. 두 스레드가 거의 내내 park 상태거나 시스템 호출 안에 있다 |
| consumer 4.2.0 | 애플리케이션 | 네이티브 11건 | `sendPrefetches → add → wakeup` 하나만 남는다 |
| classic 4.1.2 | 애플리케이션 | 네이티브 146건 | 98% `Selector.select`. 스레드 전환 없이 직접 I/O |

JFR의 Java 실행 샘플은 실행 중인 스레드만 잡는다. park된 시간은 어떤 샘플에도 잡히지 않는다. 이 프로파일만 보면 "wakeup 시스템 호출이 비싸다"로 오해하기 쉽다. 실제 비용은 시스템 호출 자체보다 그 뒤의 대기다. park 시간은 컨텍스트 스위치 횟수와 낮은 코어 사용률(0.73)이 보여 준다. 샘플링 프로파일러 하나로 결론을 내리지 않고 다른 지표로 교차 확인해야 하는 사례다.

### 4.2.0의 수정

```java
// AsyncKafkaConsumer.java 4.2.0
public ConsumerRecords<K, V> poll(final Duration timeout) {              // 836
    do {
        checkInflightPoll(timer, firstPass);                             // 858
        final Fetch<K, V> fetch = pollForFetches(timer);                 // 1872 → collectFetch(1933)
        ...
    } while (timer.notExpired());
}

private void checkInflightPoll(Timer timer, boolean firstPass) {         // 893
    if (inflightPoll == null) {
        inflightPoll = new AsyncPollEvent(calculateDeadlineMs(timer), time.milliseconds());  // 903
        applicationEventHandler.add(inflightPoll);                       // 기다리지 않는다
    }
    ...
}

private Fetch<K, V> collectFetch() {                                     // 1933
    if (positionsValidator.canSkipUpdateFetchPositions()) {              // 1940
        return fetchCollector.collectFetch(fetchBuffer);
    }
    if (inflightPoll != null && !inflightPoll.isValidatePositionsComplete()) {
        return Fetch.empty();
    }
    return fetchCollector.collectFetch(fetchBuffer);
}
```

- 대기 1 제거: 조정, 자동 커밋, 위치 검증을 하나의 `AsyncPollEvent`로 합치고, 애플리케이션 스레드는 넣기만 하고 기다리지 않는다. 이전 이벤트가 아직 처리 중이면 새로 넣지도 않는다.
- 대기 2 제거: 위치가 이미 유효하면(`canSkipUpdateFetchPositions`) 백그라운드에 묻지 않고 바로 버퍼에서 꺼낸다.
- `collectFetch`마다 하던 깨우기도 사라졌다.

poll당 컨텍스트 스위치가 2.00에서 0.04로 떨어진 것이 이 세 가지 변경의 측정상 증거다.

### 이슈 본문과 다른 점

JIRA 본문은 이벤트마다 UUID를 만드는 비용도 원인으로 든다. 4.1.2의 `ApplicationEvent`는 이미 UUID를 만들지 않는다. 이 환경에서 남은 비용은 스레드 사이의 왕복이고, UUID는 원인이 아니다. 이슈 본문은 보고 시점의 코드 기준이므로, 재현할 때는 대상 버전의 코드로 주장을 하나씩 다시 확인해야 한다.

## 실무 판단

영향을 받는 조건은 세 가지가 겹칠 때다. 클라이언트가 4.0.x나 4.1.x이고, `group.protocol=consumer`이고, poll 한 번에 받는 레코드 수가 작다. 작은 레코드 수는 `max.poll.records`를 일부러 낮춘 경우뿐 아니라 유입이 느려 poll마다 몇 건만 오는 경우도 포함한다.

| 선택지 | 효과 | 부작용 |
|---|---|---|
| 클라이언트를 4.2 이상으로 올린다 | 근본 해결. 이 측정에서 처리량 16배 | 클라이언트 업그레이드 검증 비용. 4.2의 다른 변경 사항 확인 필요 |
| `group.protocol=classic` 유지 | 이 문제를 피한다 | KIP-848의 증분 리밸런스 이점을 포기한다(Stage 2 결과 참고) |
| `max.poll.records`를 올리고 앱 안에서 나눠 처리 | 왕복 비용을 많은 레코드에 나눠 낸다 | poll 간격이 길어져 `max.poll.interval.ms` 여유가 줄어든다. 처리 도중 장애 시 재처리 범위가 커진다 |

감지 방법:

- 레코드당 CPU(컨슈머 CPU 시간 ÷ 처리 건수)가 classic 대비 크게 높다.
- 컨슈머 프로세스의 컨텍스트 스위치가 처리 건수에 비례해 많다(`pidstat -wt -p <pid> 1`, 또는 `/proc/<pid>/task/*/status`).
- `poll-idle-ratio-avg`가 높고 `time-between-poll-avg`가 짧은데 처리량이 낮다.

## 재현 충실도와 한계

| 항목 | 이 환경 | 실제와의 차이 |
|---|---|---|
| 워크로드 | 20바이트 레코드, 처리 없음 | 실제 앱은 레코드 처리 비용이 있어 poll 고정 비용의 비중이 작다. 격차는 이 표보다 작게 나온다 |
| CPU | Docker Desktop VM, 컨테이너 2 CPU 제한 | 스레드 깨우기 지연은 커널, 가상화, CPU 경합에 따라 다르다. 베어메탈에서는 왕복이 더 빠를 수 있다 |
| 브로커 | 로컬 네트워크, RF 1 | 네트워크 지연이 없어 클라이언트 쪽 비용이 그대로 드러난다 |
| 반복 | 조건별 3회 중앙값 | run 사이 편차(최대-최소 ÷ 중앙값)는 consumer 3~14%, classic 7~38%다. consumer 4.1.2와 다른 변형의 차이는 수 배 단위라 결론이 바뀌지 않는다. classic 4.1.2와 4.2.0의 차이(최대 30%)는 편차 안이라 의미를 두지 않는다 |
| 프로파일 | 측정 run과 별도 run | JFR 부하가 처리량 수치에 섞이지 않게 분리했다 |
| 4.2.0 클라이언트 + 4.1.2 브로커 | 프로토콜 호환 | 수정 효과는 클라이언트만의 변화다 |

## 다음 단계

같은 방법을 열린 이슈에 적용한다. JIRA에서 `project = KAFKA AND component = consumer AND labels = kip-848 AND resolution = Unresolved`로 찾고, 이 레포에서 재현할 수 있는 것(클라이언트나 브로커 버전만 바꾸면 되는 것)을 고른다. 재현 스크립트, 측정 표, 원인 코드 줄 번호를 이슈 댓글로 남기면 그 자체로 기여가 된다.
