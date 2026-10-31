# Stage 1. 처리량과 용량

프로듀서 설정 네 가지(acks, batch.size, linger.ms, 압축)와 부하를 하나씩 바꿔 처리량과 지연을 재고, 그 측정값으로 클러스터 용량을 산정한다. 이후 모든 단계에서 "정상 상태의 처리량과 지연이 원래 얼마인가"를 판단하는 기준선이 이 단계의 결과다.

## 목적

단계를 마치면 다음 질문에 측정값과 코드 근거로 답할 수 있어야 한다.

1. `acks=all`은 `acks=1`보다 지연을 얼마나 늘리고 처리량을 얼마나 줄이는가. 그 차이는 어느 코드 경로에서 생기는가.
2. batch.size와 linger.ms는 각각 무엇을 바꾸는가. 부하가 낮을 때와 높을 때 효과가 왜 다른가.
3. 압축 코덱마다 압축률, 처리량, 브로커 CPU가 어떻게 다르고, 어떤 상황에서 무엇을 고르는가.
4. 부하를 올리면 지연이 어디서 꺾이는가(포화점). 포화의 원인은 프로듀서, 네트워크, 브로커 중 어디인가.
5. "초당 N건, 평균 M바이트, 보존 D일, RF 3"이 주어지면 브로커 수, 디스크, 네트워크를 근거와 함께 산정한다.

## 개요

### 측정 방법

`scripts/perf.sh`가 `kafka-producer-perf-test.sh`를 별도 컨테이너(CPU 4코어, 힙 512MB)로 띄워 클러스터에 보낸다. 한 점마다 측정 토픽 `perf`(파티션 6, RF 3, `min.insync.replicas=2`)를 새로 만들고, 끝나면 perf-test의 요약(처리량, 지연 백분위), 프로듀서 메트릭(평균 배치 크기, 요청당 레코드, 큐 대기, 압축률), 같은 구간의 브로커 지표(CPU, Produce 단계별 평균 시간)를 `results/runs/perf/results.csv`에 한 줄로 남긴다.

`perf.sh all`은 브로커 JVM을 데우는 측정 4회를 버린 뒤 아래 묶음을 3회 반복하고, 리포트는 열마다 중앙값을 쓴다.

| 묶음 | 바꾸는 값 | 고정 값 | 보는 것 |
|---|---|---|---|
| acks | 0, 1, all | batch 64KB, linger 5ms, 최대 부하 | 최대 처리량 |
| acks-fixed | all, 1, 0 | batch 64KB, linger 5ms, 고정 부하 10k rec/s | 요청 지연, Produce 단계별 시간 |
| batch | 4KB ~ 1MB(5점) | acks=all, linger 5ms, 최대 부하 | 최대 처리량, 요청당 레코드, GB당 CPU |
| linger | 0 ~ 100ms(6점) | acks=all, batch 64KB, 고정 부하 10k rec/s | 요청당 레코드, p50·p99 |
| compression | none, gzip, snappy, lz4, zstd | acks=all, batch 128KB, linger 20ms, 최대 부하 | 처리량(압축 전 기준), 압축률 |
| compression-fixed | 같은 코덱 | 같은 설정, 고정 부하 40k rec/s | 브로커 CPU, 프로듀서 CPU, 지연 |
| load | 2k ~ 560k rec/s와 무제한(11점) | acks=all, batch 64KB, linger 5ms | 처리량-지연 곡선 |
| load-steady | 160k ~ 640k rec/s(5점) | 같은 설정, 앞 절반 제외. 점마다 Prometheus 수집을 켠 측정과 멈춘 측정을 번갈아 잰다 | 정상 상태의 처리량-지연 곡선, 수집의 영향 |
| inflight | 1, 2, 5 | acks=all, batch 64KB, 최대 부하 | 처리량과 요청 지연 |
| broker-cpu | 브로커당 0.5, 1, 2코어 | acks=all, batch 64KB, 최대 부하 | 브로커 포화, 코어당 처리량 |

`load-steady`만 4.2 perf-test(`apache/kafka:4.2.1`)를 쓴다. 앞부분을 통계에서 빼는 `--warmup-records`가 4.2부터 있기 때문이다. 이 묶음을 재는 동안에는 Prometheus를 번갈아 일시 정지하므로 대시보드가 끊겨 보인다. 결과는 `load-steady`(수집 멈춤)와 `load-steady-scrape`(수집 켬) 두 묶음으로 남는다. `all`에 넣지 않은 `payload` 묶음은 perf-test의 페이로드 생성 비용을 재는 수동 측정이다.

레코드는 약 510B의 JSON 한 줄이다(`results/runs/perf/payload.txt`, 2만 줄). 실제 이벤트처럼 키 이름은 반복되고 값은 다양하다. 모든 묶음이 같은 페이로드를 쓴다.

"최대 부하"는 `--throughput -1`이다. 프로듀서가 보낼 수 있는 만큼 보내므로, 이때의 지연은 버퍼에서 기다린 시간이 대부분이다. 지연을 설정 간에 공정하게 비교하려면 acks-fixed, linger, load처럼 부하를 고정해야 한다. 최대 부하의 지연은 "포화 상태에서 얼마나 쌓이는가"로만 읽는다.

브로커 지표는 Prometheus가 아니라 측정 직전과 직후에 exporter를 직접 읽어 구한다. 수집 주기가 5초라 2~3초짜리 측정을 잘라 낼 수 없고, Grafana의 백분위와 유휴율은 이전 측정이 섞인 값이기 때문이다. 이유는 [01-producer-tuning.md](01-producer-tuning.md)의 "측정 도구에서 배운 것"에 있다.

### 결과 파일

| 파일 | 내용 |
|---|---|
| `results/runs/perf/results.csv` | 측정마다 한 줄. 같은 점을 다시 재면 줄이 추가된다. 워밍업 측정은 `warmup` 묶음으로 남고 리포트에서 빠진다 |
| `results/runs/perf/report.md` | 묶음별 중앙값 표, 반복 간 처리량 편차, 그래프(`acks.svg`, `batch.svg`, `linger.svg`, `compression.svg`, `load.svg`) |
| `results/runs/perf/last-point.log` | 마지막 점의 perf-test 원본 출력 |

### 재현 충실도

| 항목 | 실제 운영 | 이 랩 |
|---|---|---|
| 디스크 | SSD·HDD, 페이지 캐시와 fsync 비용이 있다 | tmpfs(메모리). 디스크 쓰기 비용이 거의 없으므로 브로커 쪽 병목은 CPU와 네트워크 스레드에서 먼저 나타난다 |
| 네트워크 | 브로커 간 수 Gbps, 복제가 대역을 나눠 쓴다 | 한 호스트 안의 Docker 브리지. 대역은 사실상 메모리 복사 속도이고 지연은 1ms 미만이다 |
| CPU | 브로커마다 전용 코어 | 모든 컨테이너가 Docker Desktop의 코어를 나눠 쓴다. 브로커 3대와 프로듀서가 같은 CPU를 두고 경쟁한다 |
| 크기 | 브로커 수십 대, 파티션 수천 개 | 브로커 3대, 측정 토픽 파티션 6개 |
| 측정 길이 | 몇 분 이상 | 디스크가 메모리라 최대 부하 한 점이 2~3초다. 프로듀서 JVM이 데워지는 구간이 결과에 들어간다 |

절대값은 이 노트북에서만 의미가 있다. 옮겨 쓸 수 있는 것은 설정 간의 상대 차이, 곡선의 모양, 포화의 원인을 찾는 방법이다. 용량 산정 문서는 이 점을 반영해 "이 환경에서 잰 브로커당 한계"를 실제 하드웨어 값으로 바꿔 끼우는 형태로 쓴다.

## 진행 방식

| 순서 | 할 일 | 산출물 | 소요 |
|---|---|---|---|
| 1 | 묶음마다 결과를 먼저 예측해 적는다. "batch를 1MB로 올리면 처리량은 몇 배가 되는가", "linger 100ms에서 p50은 얼마인가" | 예측 5줄 | 20분 |
| 2 | [01-producer-tuning.md](01-producer-tuning.md)의 "프로듀서 내부 구조"를 읽는다 | | 40분 |
| 3 | 측정한다. 워크로드는 끄고 클러스터만 띄운 상태에서 한다 | `results.csv`, `report.md` | 약 50분 |
| 4 | 예측과 결과를 비교하고, 틀린 예측마다 이유를 코드에서 찾는다. 01 문서의 해설과 대조한다 | 묶음별 해석 | 2시간 |
| 5 | [02-capacity-planning.md](02-capacity-planning.md)의 절차로 자기 서비스의 용량 산정서를 쓴다. 가상 서비스로 쓴 [예시](../../results/capacity-plan.md)가 있다 | `results/capacity-plan.md` | 2시간 |

```bash
BROKER_DISK_MB=1024 ./scripts/cluster.sh reset   # 브로커 볼륨을 1GB로 다시 만든다
caffeinate -ims ./scripts/perf.sh all   # 약 50분. 끝나면 report.md까지 만든다
./scripts/perf.sh report                # report.md와 그래프 다시 생성
./scripts/perf.sh sweep linger          # 한 묶음만 다시
./scripts/perf.sh point acks=1 linger=20 batch=262144   # 한 점만
./scripts/cluster.sh reset              # 끝나면 기본 볼륨(256MB)으로 되돌린다
```

브로커 볼륨을 키우는 이유는 측정 길이다. 기본 256MB로는 측정점 하나가 180MB를 넘지 못해 최대 부하가 1초 안에 끝나고, JVM이 데워지기 전의 값을 재게 된다. 측정점 크기는 볼륨의 70%로 자동 계산한다. 그 덕분에 측정 내내 `BrokerDiskSpaceLow`(여유 25%)에 닿지 않는다. 1GB 볼륨 3개는 메모리를 최대 2.1GB 쓰므로 Docker에 8GB를 준 환경을 기준으로 한다.

측정 중에는 Grafana Lab / Cluster의 브로커 CPU와 Produce 요청 지연 분해(remote, local, queue)를 같이 본다. 요청 핸들러 유휴율은 1분 이동 평균이어서 2~3초짜리 측정에는 거의 움직이지 않는다.

측정 조건은 처음부터 끝까지 같아야 한다. 전원을 연결하고 덮개를 닫지 않는다. 같은 설정의 처리량이 배터리 세션에서 315MB/s, AC 세션에서 361MB/s였고 `perf.sh`는 측정마다 전원과 덮개 상태를 기록한다. 다른 무거운 작업(IDE 빌드, 다른 컨테이너)도 멈춘다. 같은 호스트의 CPU를 나눠 쓰므로 결과가 흔들린다. 의심스러운 점은 `perf.sh point`로 다시 잰다. 노트북이 잠들면 측정이 수십 분으로 늘어나는데, 이렇게 120초를 넘긴 측정은 `WARN`을 남기고 결과에서 빠진다.

## 확인할 것

- acks-fixed 묶음에서 `acks=all`과 `acks=1`의 요청 지연 차이는 브로커 Produce 시간의 어느 항목과 일치하는가.
- batch 묶음에서 평균 배치 크기는 batch.size를 따라 올라가는가. 구간마다 처리량을 막는 것은 무엇인가.
- linger 묶음에서 처리량은 같은데 요청 수와 지연이 바뀌는 모습을 설명할 수 있는가. linger를 더 늘려도 지연이 그대로인 구간은 왜 생기는가.
- compression 묶음에서 압축률과 처리량이 가장 좋은 코덱은 각각 무엇인가. 같은 부하에서 브로커 CPU는 압축하지 않을 때보다 늘었는가 줄었는가. 브로커는 압축을 다시 푸는가.
- load와 load-steady 묶음에서 지연이 꺾이는 처리량은 얼마이고, 두 묶음의 차이는 무엇을 뜻하는가.
- inflight 묶음에서 in-flight를 다섯 배로 늘리면 처리량과 요청 지연은 어떻게 변하는가. 왜 그런가.
- broker-cpu 묶음에서 코어당 처리량은 얼마인가. 프로듀서 하나로 브로커를 포화시키지 못하는 이유는 무엇인가.

## 완료 기준

- [ ] 모든 묶음을 측정하고 예측과 결과를 비교했다
- [ ] 묶음마다 결과를 설명하는 코드 위치를 하나 이상 찾았다
- [ ] 용량 산정서를 작성했다 → `results/capacity-plan.md`
