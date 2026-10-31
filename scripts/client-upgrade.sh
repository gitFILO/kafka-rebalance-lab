#!/usr/bin/env bash
# 컨슈머 애플리케이션의 kafka-clients 버전을 멤버 단위로 교체하는 도구
#   client-upgrade.sh status                       멤버별 이미지와 kafka-clients 버전
#   client-upgrade.sh build <version>...           버전별 이미지를 현재 소스로 빌드한다
#   client-upgrade.sh roll <consumer-N> <version>  멤버 하나를 재생성하고 그룹 정상화까지 기다린다
#   client-upgrade.sh roll-all <version>           실행 중인 멤버를 한 대씩 교체한다
set -euo pipefail
. "$(dirname "$0")/lib.sh"

# latest 태그의 기준 버전이다. 이 버전으로 가는 교체는 되돌리기로 본다.
BASE_VERSION=4.1.2
WORKLOAD_STATE="$RUNS_DIR/.current-workload"
ROLL_TIMEOUT_SEC="${ROLL_TIMEOUT_SEC:-180}"
ROLLBACK_TIMEOUT_SEC="${ROLLBACK_TIMEOUT_SEC:-60}"

image_tag() {
  case "$1" in
    "$BASE_VERSION") echo latest ;;
    *) echo "clients-$1" ;;
  esac
}

# 소스가 바뀌었을 수 있으므로 이미지가 있어도 매번 빌드한다. 바뀐 것이 없으면 캐시로 끝난다.
build_image() {
  local image="rebalance-lab-app:$(image_tag "$1")"
  log "build $image"
  docker build -q --build-arg KAFKA_CLIENTS_VERSION="$1" -t "$image" "$LAB_ROOT/lab-app" >/dev/null
}

tag_var() {
  echo "$1" | tr 'a-z-' 'A-Z_' | sed 's/$/_TAG/'
}

set_member_tag() {
  local member="$1" tag="$2" var tmp
  var="$(tag_var "$member")"
  tmp="$(mktemp)"
  [ -f "$NODE_VERSIONS_FILE" ] && grep -v "^$var=" "$NODE_VERSIONS_FILE" > "$tmp" || true
  echo "$var=$tag" >> "$tmp"
  mv "$tmp" "$NODE_VERSIONS_FILE"
}

is_running() {
  [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

# 회귀로 죽은 멤버일수록 되돌려야 하므로 멈춘 컨테이너도 교체 대상에 넣는다.
existing_members() {
  local m out=""
  for m in $CONSUMERS; do
    docker inspect "$m" >/dev/null 2>&1 && out="$out $m"
  done
  echo "${out# }"
}

running_members() {
  local m out=""
  for m in $CONSUMERS; do
    is_running "$m" && out="$out $m"
  done
  echo "${out# }"
}

# 이미지 태그가 아니라 컨테이너 안의 jar를 읽는다. 태그와 내용이 어긋난 이미지도 드러난다.
client_version() {
  docker exec "$1" sh -c 'ls /app/lib/kafka-clients-*.jar' 2>/dev/null \
    | sed -E 's/.*kafka-clients-(.+)\.jar/\1/' || true
}

# 셸에서 직접 실행해도 멤버가 원래 그룹과 run 디렉터리로 재생성되어야 한다.
current_run() {
  local dir="${RUN_DIR_HOST:-}"
  [ -n "$dir" ] || dir="$(cat "$WORKLOAD_STATE" 2>/dev/null || true)"
  [ -n "$dir" ] || die "실행 중인 워크로드가 없다. workload.sh start 또는 drill.sh start 를 먼저 실행한다."
  resume_run "$dir"
}

wait_settled() {
  local member="$1" since="$2" members="$3" timeout="$4"
  wait_assigned "$member" "$since" "$timeout" && wait_group "$members" "$timeout"
}

roll_member() {
  local member="$1" v="$2" members="$3" image from since
  image="rebalance-lab-app:$(image_tag "$v")"
  if [ "$(docker inspect -f '{{.Image}}' "$member")" = "$(docker image inspect -f '{{.Id}}' "$image")" ]; then
    log "$member already on $image"
    return 0
  fi
  from="$(client_version "$member")"
  since="$(now_ms)"
  mark "CLIENT_ROLL $member $from->$v"
  set_member_tag "$member" "$(image_tag "$v")"
  # compose가 stop_grace_period 안에서 SIGTERM으로 내리므로 LabConsumer의 커밋·close·flush 경로를 탄다.
  compose up -d --no-deps "$member" >/dev/null
  if [ "$v" = "$BASE_VERSION" ]; then
    # 남은 멤버가 긴 배치 안에서 회수 ack를 못 보내면 Stable이 늦게 온다. 기다리는 만큼 lag만 쌓이므로 경고만 남긴다.
    wait_settled "$member" "$since" "$members" "$ROLLBACK_TIMEOUT_SEC" \
      || log "WARN: $member not settled in ${ROLLBACK_TIMEOUT_SEC}s, 되돌리기를 계속한다"
  else
    wait_settled "$member" "$since" "$members" "$ROLL_TIMEOUT_SEC" \
      || die "$member 교체 후 그룹이 안정되지 않아 롤링을 멈춘다"
  fi
  log "$member now $(client_version "$member")"
}

cmd_roll() {
  local member="$1" v="$2"
  case " $CONSUMERS " in *" $member "*) ;; *) die "unknown member $member" ;; esac
  is_running "$member" || die "$member is not running"
  current_run
  build_image "$v"
  roll_member "$member" "$v" "$(running_members | wc -w | tr -d ' ')"
}

cmd_roll_all() {
  local v="$1" members m n
  current_run
  members="$(existing_members)"
  [ -n "$members" ] || die "컨슈머 컨테이너가 없다"
  n="$(echo "$members" | wc -w | tr -d ' ')"
  build_image "$v"
  # 증설로 새로 뜨는 멤버가 이전 버전으로 올라오면 부분 롤백이 되어 증설이 효과 있는 것처럼 보인다.
  for m in $CONSUMERS; do
    is_running "$m" || set_member_tag "$m" "$(image_tag "$v")"
  done
  for m in $members; do
    roll_member "$m" "$v" "$n"
  done
  cmd_status
}

cmd_status() {
  local m
  for m in $CONSUMERS; do
    if is_running "$m"; then
      printf '%-11s %-32s %s\n' "$m" "$(docker inspect -f '{{.Config.Image}}' "$m")" "$(client_version "$m")"
    else
      printf '%-11s %-32s %s\n' "$m" - -
    fi
  done
}

cmd_build() {
  local v
  for v in "$@"; do
    build_image "$v"
  done
}

case "${1:-}" in
  status) cmd_status ;;
  build) shift; cmd_build "$@" ;;
  roll) cmd_roll "${2:?member}" "${3:?version}" ;;
  roll-all) cmd_roll_all "${2:?version}" ;;
  *) sed -n '2,6p' "$0"; exit 2 ;;
esac
