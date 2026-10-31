#!/usr/bin/env bash
# 장애 주입 원시 동작. lib.sh 다음에 source 한다.

PROMQ="python3 $LAB_ROOT/scripts/promq.py"
COMPOSE_PROJECT="rebalance-lab"

FREEZE_DIR="$LAB_ROOT/results/runs"

# 살아 있지만 주기적으로 멈추는 브로커를 만든다(긴 GC 정지, 호스트 정체). Docker Desktop 커널은 tc qdisc를 지원하지 않아 netem을 쓸 수 없다.
fault_freeze() {
  local target="$1" freeze_ms="${2:-1200}" run_ms="${3:-400}"
  fault_unfreeze "$target"
  nohup bash -c '
    f="$(awk -v v="$1" "BEGIN {print v / 1000}")"; r="$(awk -v v="$2" "BEGIN {print v / 1000}")"
    while [ "$(docker inspect -f "{{.State.Running}}" "$0" 2>/dev/null)" = "true" ]; do
      docker pause "$0" >/dev/null 2>&1; sleep "$f"
      docker unpause "$0" >/dev/null 2>&1; sleep "$r"
    done' "$target" "$freeze_ms" "$run_ms" >/dev/null 2>&1 &
  echo $! > "$FREEZE_DIR/.freeze-$target.pid"
}

fault_unfreeze() {
  local pidf="$FREEZE_DIR/.freeze-$1.pid" i
  [ -f "$pidf" ] || return 0
  kill "$(cat "$pidf")" 2>/dev/null || true
  rm -f "$pidf"
  # 루프를 죽여도 진행 중이던 docker pause는 끝까지 실행되므로 잠시 지켜본다.
  for i in 1 2 3 4 5 6; do
    [ "$(docker inspect -f '{{.State.Paused}}' "$1" 2>/dev/null)" = "true" ] && docker unpause "$1" >/dev/null 2>&1
    sleep 0.5
  done
}

fault_disk_fill() {
  local broker="$1" leave_mb="${2:-0}" free_mb
  free_mb="$(docker exec "$broker" sh -c "df -m /var/lib/kafka-data | awk 'NR==2 {print \$4}'")"
  local fill=$(( free_mb - leave_mb ))
  [ "$fill" -gt 0 ] || return 0
  docker exec "$broker" sh -c "dd if=/dev/zero of=/var/lib/kafka-data/java_pid1.hprof bs=1M count=$fill 2>/dev/null" || true
}

# 브로커가 내려가 있어도 동작해야 하므로 볼륨을 직접 마운트해 지운다.
fault_disk_clear() {
  docker run --rm -v "${COMPOSE_PROJECT}_$1-data:/d" alpine:3.20 rm -f /d/java_pid1.hprof >/dev/null 2>&1 || true
}

fault_broker_kill() { docker kill --signal KILL "$1" >/dev/null; }
fault_broker_stop() {
  fault_unfreeze "$1"
  docker stop -t 30 "$1" >/dev/null
}
fault_broker_start() {
  docker start "$1" >/dev/null
  local deadline=$(( $(date +%s) + 120 ))
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" = "healthy" ]; do
    [ "$(date +%s)" -ge "$deadline" ] && { log "WARN: $1 not healthy in 120s"; return 1; }
    sleep 3
  done
}

# 실행 중인 앱의 control 파일. 형식은 docs/01-environment.md "런타임 제어" 참고.
ctl_set() { echo "$2" > "$RUN_DIR_HOST/control/$1"; }
ctl_clear() { rm -f "$RUN_DIR_HOST/control/$1"; }

topic_config() {
  kafka_bin kafka-configs.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --entity-type topics --entity-name "$TOPIC" "$@" 2>&1 | grep -v '^$' || true
}

leader_of_partition() {
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --topic "$TOPIC" 2>/dev/null \
    | awk -v p="$1" '$3 == "Partition:" && $4 == p {for (i = 1; i <= NF; i++) if ($i == "Leader:") print $(i + 1)}'
}

broker_container() {
  case "$1" in 4) echo broker-1 ;; 5) echo broker-2 ;; 6) echo broker-3 ;; *) echo "" ;; esac
}

BROKER_IDS="4 5 6"

broker_config() {
  kafka_bin kafka-configs.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --entity-type brokers --entity-name "$1" "${@:2}" 2>&1 \
    | grep -v '^$' || true
}

# 재배치 도구(kafka-reassign-partitions --execute --throttle)가 남기는 것과 같은 설정을 쓴다.
# 리더 쪽은 기존 복제본, 팔로워 쪽은 새로 옮겨 가는 복제본(target_id) 목록이다.
fault_throttle_set() {
  local rate="$1" target_id="$2" id leaders followers
  leaders="$(kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --topic "$TOPIC" 2>/dev/null \
    | awk -v t="$target_id" '$3 == "Partition:" {
        p = $4; for (i = 1; i <= NF; i++) if ($i == "Replicas:") { n = split($(i + 1), r, ",");
        for (j = 1; j <= n; j++) if (r[j] != t) out = out (out ? "," : "") p ":" r[j] } }
      END { print out }')"
  followers="$(kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --topic "$TOPIC" 2>/dev/null \
    | awk -v t="$target_id" '$3 == "Partition:" { out = out (out ? "," : "") $4 ":" t } END { print out }')"
  for id in $BROKER_IDS; do
    broker_config "$id" --alter --add-config "leader.replication.throttled.rate=$rate,follower.replication.throttled.rate=$rate" >/dev/null
  done
  topic_config --alter --add-config "leader.replication.throttled.replicas=[$leaders],follower.replication.throttled.replicas=[$followers]" >/dev/null
}

# CLI 한 번이 JVM 기동 1~2초다. 먼저 한 번 조회하고 실제로 있는 키만 한 번에 지운다.
topic_clear_keys() {
  local present
  present="$(topic_config --describe | { grep -oE "^ *($(echo "$*" | tr ' ' '|'))=" || true; } | tr -d ' =' | sort -u | paste -sd, -)"
  [ -n "$present" ] && topic_config --alter --delete-config "$present" >/dev/null
  return 0
}

fault_throttle_clear() {
  local id
  kafka_bin kafka-configs.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --entity-type brokers --describe 2>/dev/null \
    | awk '/^Dynamic configs for broker/ {id = $5} /throttled\.rate=/ {print id}' | sort -u | while read -r id; do
      broker_config "$id" --alter --delete-config leader.replication.throttled.rate,follower.replication.throttled.rate >/dev/null
    done
  topic_clear_keys leader.replication.throttled.replicas follower.replication.throttled.replicas
}

client_quota_config() {
  kafka_bin kafka-configs.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --entity-type clients "$@" 2>&1 | grep -v '^$' || true
}

# 출력 예: Quota configs for the default client-id are consumer_byte_rate=2048.0
#          Quota configs for client-id 'consumer-1' are consumer_byte_rate=1.0E7
fault_quota_clear() {
  local line entity key
  client_quota_config --describe | { grep '^Quota configs for' || true; } | while read -r line; do
    case "$line" in
      *"default client-id"*) entity="--entity-default" ;;
      *) entity="--entity-name $(echo "$line" | sed -E "s/.*client-id '([^']+)'.*/\1/")" ;;
    esac
    for key in $(echo "$line" | sed 's/.* are //' | tr ',' '\n' | cut -d= -f1); do
      # shellcheck disable=SC2086
      client_quota_config $entity --alter --delete-config "$key" >/dev/null
    done
  done
}

controller_running_count() {
  local c n=0
  for c in controller-1 controller-2 controller-3; do
    [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" = "true" ] && n=$((n + 1))
  done
  echo "$n"
}

# 모든 장애 흔적을 지운다. 훈련 종료와 카오스 정리에 쓴다.
fault_clear_all() {
  local b c
  for c in controller-1 controller-2 controller-3; do
    [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" = "true" ] || docker start "$c" >/dev/null 2>&1 || true
  done
  for b in broker-1 broker-2 broker-3; do
    fault_unfreeze "$b"
    fault_disk_clear "$b"
    if [ "$(docker inspect -f '{{.State.Running}}' "$b" 2>/dev/null)" != "true" ]; then
      fault_broker_start "$b" || true
    fi
  done
  # 설정되지 않은 키가 하나라도 섞이면 명령 전체가 거부되므로 있는 키만 고른다.
  topic_clear_keys retention.ms segment.ms max.message.bytes unclean.leader.election.enable
  fault_throttle_clear
  fault_quota_clear
}
