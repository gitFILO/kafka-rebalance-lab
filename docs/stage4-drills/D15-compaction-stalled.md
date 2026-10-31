# D15. compact 토픽이 정리되지 않고 계속 자람

## 목적

log compaction이 "언제, 무엇을" 정리하는지 확인한다. 클리너가 살아 있고 설정도 `cleanup.policy=compact`인데 토픽이 계속 자라는 상황에서 활성 세그먼트의 의미를 이해하고, 세그먼트 설정이 compaction에 어떤 영향을 주는지 판단한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 사용자 상태를 담는 compact 토픽 `lab-state`(키 1,000개, 초당 400건 갱신)를 만들 때 다른 팀의 이벤트 로그 템플릿을 그대로 복사했다. |
| 주입 | `lab-state`에 `segment.ms=604800000`(7일), `segment.bytes=1073741824`(1GB) 적용 |
| 예상 증상 | 키 수는 일정한데 토픽 크기가 선형으로 늘어난다. 파티션마다 세그먼트가 1개뿐이다. 클리너 오류는 없다. |
| 모범 조치 | 세그먼트 설정을 토픽 기본값(브로커 설정)으로 되돌려 세그먼트가 닫히게 한다. |
| 소요 | 20분(알람까지 약 5분) |

이 훈련만 `state-producer`와 `lab-state` 토픽을 추가로 쓴다. `drill.sh start D15`가 만들고 `stop`이 지운다.

## 진행 방식

```bash
./scripts/drill.sh start D15
```

1. 알람을 기다린다. `CompactedTopicGrowing`이 울린다.
2. Cluster의 "디스크와 로그" 행에서 Log size by topic과 Segments per partition을 본다.
3. 같은 행의 Log cleaner 패널을 본다. 클리너 스레드는 살아 있는가, 마지막 실행은 언제인가, dirty 비율은 얼마인가.
4. 토픽 설정을 본다:
   ```bash
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker-1:9092 --entity-type topics --entity-name lab-state --describe
   docker exec broker-1 ls -la /var/lib/kafka-data/logs/lab-state-0/
   ```
5. 조치하고, 크기가 언제 어떻게 줄어드는지 본다.
6. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- 클리너가 정상인데 정리가 안 되는 이유
- 조치 직후 크기가 바로 줄지 않는다. 언제 줄기 시작하는가
- 이 토픽으로 상태를 복구하는 컨슈머(예: Streams의 changelog 복원)는 어떤 영향을 받는가

---

## 해설

### 메커니즘

로그는 세그먼트 파일의 연속이고, 쓰기는 항상 마지막 세그먼트(활성 세그먼트)에만 일어난다. 클리너는 활성 세그먼트를 정리 대상에서 뺀다. 정리할 수 있는 범위의 상한(first uncleanable offset)이 활성 세그먼트의 시작 오프셋보다 클 수 없다. 세그먼트가 닫히지 않으면 정리할 것이 없다.

세그먼트는 크기(`segment.bytes`)나 시간(`segment.ms`) 조건을 넘는 첫 쓰기에서 닫힌다. 이 토픽의 유입은 브로커당 약 100KB/s라 1GB를 채우려면 약 3시간이 걸리고, 7일은 말할 것도 없다. 이 레포 브로커는 `log.roll.ms=60000`이라 평소에는 1분마다 세그먼트가 닫히고 클리너가 그 뒤를 정리한다. 토픽 설정이 이를 덮어써 세그먼트가 닫히지 않게 됐다.

크기가 클리너 기준(`min.cleanable.dirty.ratio`)과 무관하게 자란다는 점이 함정이다. dirty 비율은 "닫힌 세그먼트 중 정리 안 된 비율"이라, 닫힌 세그먼트가 없으면 dirty 비율도 낮게 보인다. 클리너 지표는 모두 정상으로 보이고 크기만 늘어난다.

조치 후에도 크기는 바로 줄지 않는다. 다음 쓰기에서 세그먼트가 닫히고, 클리너가 다음 주기에 그 세그먼트를 정리하고, 정리된 세그먼트가 교체된 뒤 기존 파일이 `file.delete.delay.ms`(기본 60초) 뒤에 지워진다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | `CompactedTopicGrowing`, 디스크 여유 감소 | compact 토픽이 정리되지 않는다 |
| 범위 | `lab-state`만 자란다. `lab-events`는 정상 | 토픽 한정 |
| 원인 후보 | 클리너 사망(`deadthreadcount` > 0), 클리너 처리량 부족(dirty 비율 높음), 정리 대상 없음 | 세 가지를 지표로 가른다 |
| 확인 | 클리너 살아 있음, 세그먼트 수 1, 토픽 설정의 `segment.ms`, `segment.bytes` | 활성 세그먼트가 닫히지 않는다 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| `segment.ms`, `segment.bytes` 삭제(브로커 기본값 사용) | 다음 쓰기에서 세그먼트가 닫히고 정리가 재개된다 | 브로커 기본값이 운영 환경에 맞는지 따로 확인해야 한다 |
| `max.compaction.lag.ms` 설정 | 세그먼트가 그 시간을 넘으면 정리 대상으로 강제한다(KIP-354). 활성 세그먼트도 이 조건으로 닫힌다 | 너무 작으면 세그먼트가 잘게 쪼개지고 클리너 부하가 는다 |
| `segment.ms`만 줄임 | 시간 기준으로 닫힌다 | 유입이 몰리면 세그먼트가 커질 수 있다 |
| 브로커 재시작 | 효과 없다 | 재시작해도 활성 세그먼트는 그대로다 |

### 코드 근거 (4.1.2)

- `LogCleanerManager.cleanableOffsets()`(`storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleanerManager.java:692`): 730~735행에서 first uncleanable offset 후보에 `log.activeSegment().baseOffset()`을 넣는다. 활성 세그먼트는 정리 대상이 될 수 없다.
- `LogCleanerManager.grabFilthiestCompactedLog()`(240행): 283행에서 `cleanableRatio() > minCleanableRatio`이거나 compaction 지연 강제 조건일 때만 고른다.
- `LogSegment.shouldRoll()`(`storage/.../log/LogSegment.java:167`): 크기가 `segment.bytes`를 넘거나, 비어 있지 않고 `segment.ms`(jitter 차감)를 넘었을 때 세그먼트를 닫는다. 호출은 쓰기 경로의 `UnifiedLog.maybeRoll()`(`UnifiedLog.java:2052`)에서 일어난다.
- `LogConfig.maxSegmentMs()`(`LogConfig.java:383`): compact 토픽에 `max.compaction.lag.ms`가 있으면 `segment.ms`와 둘 중 작은 값으로 세그먼트를 닫는다. 위 조치 표의 두 번째 선택지가 활성 세그먼트에도 듣는 이유다.

### 재발 방지

- 알람: `CompactedTopicGrowing`(compact 토픽 크기가 기준을 넘음, 1분). 실측에서 주입 5분 14초 뒤 울렸다. 실무에서는 "키 수 × 평균 값 크기"의 몇 배를 기준으로 잡는다.
- 보조 알람: compact 토픽의 세그먼트 수가 오래 1이면 경고한다.
- 토픽 생성 템플릿을 용도별(이벤트 로그, 상태)로 나눠 관리한다.

### 재현 충실도

실제로는 세그먼트가 언젠가 닫히므로(1GB 또는 7일) "정리가 늦게, 크게 몰려서 일어난다"가 더 흔한 모습이다. 이 레포는 디스크가 작아 그 전에 문제가 드러난다.
