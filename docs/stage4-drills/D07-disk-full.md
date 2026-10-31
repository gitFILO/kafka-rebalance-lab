# D07. 브로커 디스크 풀

## 목적

로그 디렉터리에 쓰기가 실패하면 브로커가 스스로 종료하는 동작을 확인하고, 공간 확보 → 재기동 → 복제 복구 순서로 복구한다. 디스크 사용량 알람이 왜 다른 알람보다 먼저 있어야 하는지 이해한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | broker-3 데이터 볼륨에 누군가 큰 파일(힙 덤프)을 남겼다. |
| 주입 | broker-3 데이터 볼륨(256MB tmpfs)을 `java_pid1.hprof` 파일로 채운다. |
| 예상 증상 | 디스크 여유가 0이 되고 broker-3이 종료된다. URP가 생긴다. |
| 모범 조치 | 원인 파일 삭제, 브로커 재기동, URP 0 확인, preferred election |
| 소요 | 20분 |

## 진행 방식

```bash
./scripts/drill.sh start D07
```

1. Cluster의 Disk free by broker와 Overview의 Brokers up, Under-replicated를 본다. 순서대로 무엇이 먼저 변했는지 기록한다.
2. 브로커가 왜 내려갔는지 로그에서 확인한다: `docker logs broker-3 2>&1 | grep -iE 'log dir|No space|Shutdown' | tail`.
3. 공간을 차지한 파일을 찾는다. 브로커가 꺼져 있어도 볼륨은 남아 있다.

```bash
docker run --rm -v rebalance-lab_broker-3-data:/data alpine du -ah /data | sort -h | tail
```

4. 원인 파일을 지우고 재기동한다.

```bash
docker run --rm -v rebalance-lab_broker-3-data:/data alpine rm -f /data/java_pid1.hprof
docker start broker-3
```

5. URP 0을 확인하고 preferred election(D05 참고)을 실행한다.
6. `drill.sh verify`, `drill.sh stop`.

확인할 것:

- 브로커가 쓰기 실패 후 바로 종료한 이유(로그 디렉터리가 1개)
- 다른 브로커는 같은 데이터를 갖고 있는데 디스크가 차지 않은 이유
- 운영에서 공간을 확보할 때 로그 세그먼트를 직접 지우면 안 되는 이유

---

## 해설

### 메커니즘

로그 세그먼트 쓰기나 롤링에서 IOException이 나면 `LogDirFailureChannel`에 해당 디렉터리가 실패로 보고된다. ReplicaManager는 그 디렉터리의 파티션을 오프라인으로 만들고, 남은 정상 로그 디렉터리가 없으면 브로커를 종료한다. 이 브로커는 `log.dirs`가 하나뿐이라 바로 종료된다. 컨트롤러는 브로커를 fence하고 리더를 옮긴다(D05와 같은 경로).

원인 파일은 broker-3의 볼륨에만 있으므로 다른 브로커는 영향이 없다. 공간을 비우지 않고 재기동하면 같은 이유로 다시 종료된다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | Disk free 급감, 이어서 Brokers up 2 | 디스크 원인의 브로커 종료 |
| 범위 | 한 브로커만 디스크 감소 | 토픽 데이터 증가가 아니다(그랬다면 전 브로커가 같이 증가) |
| 원인 | 브로커 로그의 log dir 실패 기록, `du`로 찾은 비로그 파일 | 외부 파일 |
| 확인 | 파일 삭제 후 여유 공간 회복 | 원인 확정 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| 원인 파일 삭제 후 재기동 | 정상 복구 | 원인 파일이 필요한 것인지 먼저 확인해야 한다 |
| 볼륨 확장 | 근본 여유 확보 | 시간이 걸린다 |
| 토픽 retention 단축 | 로그 공간 확보 | 브로커가 꺼져 있으면 적용되지 않는다. 다른 브로커의 데이터도 줄어든다 |
| 세그먼트 파일 직접 삭제 | 공간 확보 | 인덱스와 체크포인트가 어긋나 로그 복구가 필요해진다. 하지 않는다 |

### 코드 근거 (4.1.2)

- `ReplicaManager.handleLogDirFailure()`(`core/.../server/ReplicaManager.scala:2544`): 실패한 디렉터리의 파티션을 오프라인으로 만든다.
- `LogManager.scala:223`: 남은 정상 로그 디렉터리가 없으면 "Shutdown broker because all log dirs in ... have failed"로 종료한다.

### 재발 방지

- 알람: `lab_disk_usablebytes / lab_disk_totalbytes < 0.2`(실무에서는 node exporter `node_filesystem_avail_bytes`)
- 데이터 디렉터리에 다른 프로세스의 파일(힙 덤프, 로그)이 쓰이지 않도록 경로를 분리한다. JVM `-XX:HeapDumpPath`가 데이터 볼륨을 가리키는지 확인한다.

### 재현 충실도

256MB tmpfs라 수 초 안에 찬다. 실무에서는 수 시간에 걸쳐 차므로 알람으로 막을 시간이 있다.
