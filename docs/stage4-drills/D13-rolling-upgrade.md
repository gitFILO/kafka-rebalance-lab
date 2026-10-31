# D13. 롤링 업그레이드 finalize 후 롤백 실패

## 목적

KRaft 클러스터의 업그레이드가 "바이너리 교체"와 "feature finalize" 두 단계로 나뉘는 이유를 직접 겪는다. finalize 전에는 자유롭게 되돌릴 수 있고 finalize 후에는 되돌릴 수 없는 경우가 생기는 것을 확인하고, 되돌릴 수 있는 범위를 코드로 확정한다.

## 개요

| 항목 | 내용 |
|---|---|
| 상황 | 동료가 4.0.1 → 4.1.2 롤링 업그레이드를 절차대로 끝내고 바로 `--release-version 4.1`로 finalize했다. 다음 날 broker-3에서 성능 저하가 의심돼 그 브로커만 4.0.1로 되돌렸다. |
| 주입 | 4.0.1 클러스터로 재구성 → 부하 중 `roll-all 4.1.2` → `finalize 4.1` → broker-3을 4.0.1로 재생성 |
| 예상 증상 | broker-3이 시작 후 약 60초 만에 종료된다. 브로커 2대로 운영되고 URP가 남는다. |
| 모범 조치 | broker-3을 4.1.2로 다시 올린다. 정말 되돌려야 한다면 metadata.version을 안전한 범위에서 낮춘 뒤 되돌린다. |
| 소요 | 30분(재구성과 롤링 업그레이드에 약 3분) |

이 훈련은 시작할 때 클러스터를 4.0.1로 새로 만든다. 기존 데이터와 그룹이 모두 지워진다. 끝난 뒤 클러스터는 4.1.2(finalize된 상태)로 남는다.

## 진행 방식

```bash
./scripts/drill.sh start D13
./scripts/upgrade.sh status          # 노드별 버전과 finalized feature
```

1. 롤링 업그레이드 중에 대시보드를 본다. 노드가 하나씩 재시작되는 동안 URP와 프로듀서 지연이 어떻게 움직이는가. `ROLL` 어노테이션이 노드마다 남는다.
2. finalize 직후 `upgrade.sh status`로 metadata.version, `eligible.leader.replicas.version`, `kraft.version`의 finalized 값을 본다. 무엇이 함께 올라갔는가.
3. broker-3이 내려간 뒤 알람(`BrokerDown`)과 broker-3 로그의 마지막 ERROR를 본다:
   `docker logs broker-3 2>&1 | grep -E 'ERROR|FATAL' | tail`
4. 컨트롤러 로그에서 broker-3의 등록 거부 이유를 찾는다.
5. 되돌리기와 다시 올리기 중 무엇을 할지 정한다. 되돌리기를 택했다면 안전하게 되돌릴 방법이 있는지 확인한다:
   ```bash
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-features.sh --bootstrap-server broker-1:9092 downgrade --feature metadata.version=25 --dry-run
   docker exec -e KAFKA_OPTS= broker-1 /opt/kafka/bin/kafka-features.sh --bootstrap-server broker-1:9092 downgrade --release-version 3.9 --dry-run
   ```
6. 조치하고, `drill.sh verify`, `drill.sh stop`.

확인할 것:

- finalize 전에 broker-3을 4.0.1로 되돌렸다면 어땠을까. `upgrade.sh`로 직접 해 본다(finalize 전 상태가 필요하면 훈련을 다시 시작한다)
- `--release-version 4.1`이 metadata.version 말고 무엇을 함께 올렸는가. 그중 되돌릴 수 없는 것은 무엇인가
- 4.0-IV3로 낮추는 것은 허용되고 3.9로 낮추는 것은 거부되는 이유

---

## 해설

### 메커니즘

KRaft 클러스터는 "어떤 형식의 메타데이터 레코드를 써도 되는가"를 feature 버전으로 관리한다. 대표가 `metadata.version`(MV)이다. 새 바이너리는 옛 MV도 이해하므로, 바이너리만 바꾼 상태에서는 클러스터가 옛 MV로 계속 동작하고 옛 바이너리로 되돌려도 문제가 없다. 실측에서 4.0.1 → 4.1.2 롤링(부하 중)은 약 78초 걸렸고, 그 상태에서 브로커 하나를 4.0.1로 되돌려도 정상 합류했다.

finalize(`kafka-features.sh upgrade --release-version 4.1`)는 MV를 `4.1-IV1`(feature level 27)로 올리고, 같은 릴리스의 기본 feature인 `eligible.leader.replicas.version=1`, `kraft.version=1`도 함께 올린다. 이때부터 컨트롤러는 새 형식의 레코드를 쓸 수 있다. CLI 출력은 `group.version`(1)과 `transaction.version`(2)까지 다섯 줄 모두 "was upgraded to"라고 쓰지만, 이 둘은 4.0 릴리스 기본값과 같은 수준이다. 무엇이 실제로 바뀌었는지는 finalize 전후의 `upgrade.sh status`를 비교해야 안다.

4.0.1 바이너리의 운영용 MV 상한은 level 25(`4.0-IV3`)다. level 26(`4.1-IV0`)은 개발용으로 정의만 되어 있고, level 27은 정의 자체가 없다. 이 브로커가 등록을 요청하면 컨트롤러가 "finalized 버전을 지원하지 않는다"며 거부한다(`UNSUPPORTED_VERSION`). 브로커는 등록 거부를 치명적 오류로 처리하고 약 60초 뒤 종료한다. broker-3 로그에는 "No MetadataVersion with feature level 27"과 "Received a fatal error while waiting for the controller to acknowledge that we are caught up"이 남는다.

되돌릴 방법은 MV를 다시 낮추는 것이다. 컨트롤러는 MV 하향을 "그 사이 버전들이 메타데이터 형식을 바꾸지 않았는가"로 판정한다. `4.0-IV2`부터 `4.1-IV1`까지는 모두 형식 변경이 없는 버전이라 `4.0-IV3`(level 25)로 낮출 수 있고, 그러면 4.0.1 브로커가 다시 합류한다. `4.0-IV1` 이하나 `--release-version 3.9`로 낮추는 것은 형식 변경을 지나야 하므로 "메타데이터를 지울 수 있다"며 거부된다. `kraft.version`은 아예 낮출 수 없다.

### 진단 경로

| 단계 | 근거 | 판단 |
|---|---|---|
| 감지 | `BrokerDown`, Brokers up 2, URP | 브로커 1대 이탈 |
| 범위 | broker-3만. 직전 작업은 broker-3 버전 변경 | 변경이 원인일 가능성이 높다 |
| 원인 | broker-3 로그의 MV level 27 오류, 컨트롤러 로그의 "Unable to register because the broker does not support finalized version" | 바이너리가 finalized MV를 지원하지 않는다 |
| 확인 | `upgrade.sh status`의 metadata.version finalized=4.1-IV1, broker-3 이미지 4.0.1 | finalize 이후 롤백 |

### 조치 선택지

| 조치 | 효과 | 부작용·한계 |
|---|---|---|
| broker-3을 4.1.2로 재생성 | 즉시 정상화 | 롤백하려던 원래 이유(성능 의심)는 남는다 |
| `downgrade --feature metadata.version=25`(4.0-IV3) 후 4.0.1로 롤백 | 4.0.1 브로커가 합류한다(실측 확인) | 4.1 MV에서만 쓰는 기능이 꺼진다. 이후 다시 finalize해야 한다. `kraft.version=1`은 그대로 남는다 |
| `downgrade --release-version 3.9` | 거부된다 | 메타데이터 형식 변경을 넘는 하향은 지원하지 않는다 |
| 컨트롤러까지 4.0.1로 되돌림 | 상황이 나빠진다 | 컨트롤러도 같은 이유로 시작하지 못하고, 쿼럼을 잃을 수 있다(D14) |

운영 절차의 결론은 단순하다. 바이너리 교체 후 충분히(수일) 관찰하고, 되돌릴 일이 없다고 판단한 뒤 finalize한다. finalize는 별도 변경으로 기록하고 승인한다.

### 코드 근거 (4.1.2)

- 등록 거부: `ClusterControlManager.registerBroker()`(`metadata/src/main/java/org/apache/kafka/controller/ClusterControlManager.java:511`)는 브로커가 보낸 지원 범위에 finalized feature 버전이 없으면 `UnsupportedVersionException`("Unable to register because the broker does not support finalized version")을 던진다.
- 하향 판정: `FeatureControlManager`(`metadata/.../controller/FeatureControlManager.java:401`)는 `MetadataVersion.checkIfMetadataChanged()`(`server-common/.../common/MetadataVersion.java:379`)로 사이 버전의 형식 변경을 확인하고, 변경이 있으면 410행에서 "Refusing to perform the requested downgrade because it might delete metadata information"으로 거부한다.
- 형식 변경 표시: `MetadataVersion.java`에서 `IBP_4_0_IV2`(103행), `IBP_4_0_IV3`(106행), `IBP_4_1_IV0`(111행), `IBP_4_1_IV1`(114행)은 모두 `didMetadataChange=false`다. 그래서 4.1-IV1 → 4.0-IV3 하향이 허용된다. 4.1.2 CLI에서 `--metadata 4.0-IV3`는 deprecated이고 `--feature metadata.version=25`가 같은 동작이다.
- `kraft.version` 하향 거부: 같은 `FeatureControlManager.java`의 298행 "Can't downgrade the version of this feature."
- 브로커 종료: `BrokerServer.startup()`(`core/src/main/scala/kafka/server/BrokerServer.scala:549`)은 컨트롤러가 "따라잡음"을 확인해 줄 때까지 기다린다. 등록이 거부되면 이 대기가 오류로 끝나고 시작을 포기한다.

### 재발 방지

- 알람: `BrokerDown`. 롤링 작업 중에는 노드별 재시작마다 이 알람이 잠깐 울리므로, 작업 중 억제(silence)를 걸고 노드마다 `upgrade.sh roll`처럼 "정상화 확인 후 다음 노드" 규칙을 지킨다.
- 업그레이드 런북에 finalize 시점, 되돌릴 수 있는 MV 범위, finalize 후 롤백 방법(가능한 하향 버전)을 미리 적는다.
- `kafka-features.sh describe` 결과를 변경 전후로 기록한다.

### 재현 충실도

실측 결과와 같은 순서로 재현한다. 롤링 순서는 비활성 컨트롤러 → 활성 컨트롤러 → 브로커다. 실제 운영에서는 브로커 한 대마다 URP 0과 지연 정상화를 확인하고 다음으로 넘어가며, 이 레포의 `upgrade.sh roll`도 같은 조건을 기다린다.
