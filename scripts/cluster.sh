#!/usr/bin/env bash
# KRaft 클러스터(controller 3, broker 3) 기동·상태 확인·정리
set -euo pipefail
. "$(dirname "$0")/lib.sh"

CLUSTER_SERVICES="controller-1 controller-2 controller-3 broker-1 broker-2 broker-3"

wait_healthy() {
  local deadline=$(( $(date +%s) + 180 )) s all
  while :; do
    all=1
    for s in $CLUSTER_SERVICES; do
      [ "$(docker inspect -f '{{.State.Health.Status}}' "$s" 2>/dev/null)" = "healthy" ] || all=0
    done
    [ "$all" = 1 ] && return 0
    [ "$(date +%s)" -ge "$deadline" ] && die "cluster did not become healthy in 180s"
    sleep 3
  done
}

create_topic() {
  local deadline=$(( $(date +%s) + 120 ))
  until kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --create --if-not-exists \
      --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 3 \
      --config min.insync.replicas=2 >/dev/null 2>&1; do
    [ "$(date +%s)" -ge "$deadline" ] && die "topic creation failed"
    sleep 3
  done
  log "topic $TOPIC ready (partitions=$PARTITIONS, rf=3, min.insync.replicas=2)"
}

cmd_up() {
  log "build lab app image"
  docker build -q -t rebalance-lab-app:latest "$LAB_ROOT/lab-app" >/dev/null
  log "start $CLUSTER_SERVICES"
  compose up -d $CLUSTER_SERVICES >/dev/null
  wait_healthy
  create_topic
  cmd_status
}

cmd_status() {
  echo "== containers"
  compose ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}'
  echo
  echo "== metadata quorum"
  kafka_bin kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" describe --status 2>/dev/null \
    | grep -E 'LeaderId|LeaderEpoch|HighWatermark|CurrentVoters|CurrentObservers' || true
  echo
  echo "== features"
  kafka_bin kafka-features.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" describe 2>/dev/null | grep -E 'group.version|metadata.version' || true
  echo
  echo "== topic"
  kafka_bin kafka-topics.sh --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --topic "$TOPIC" 2>/dev/null || true
}

cmd_down() {
  log "stop and remove all containers"
  compose down -v --remove-orphans
}

case "${1:-}" in
  up) cmd_up ;;
  status) cmd_status ;;
  down) cmd_down ;;
  reset) cmd_down; cmd_up ;;
  *) echo "usage: $0 <up|status|down|reset>"; exit 2 ;;
esac
