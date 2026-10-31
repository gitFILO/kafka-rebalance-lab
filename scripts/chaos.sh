#!/usr/bin/env bash
# Stage 5 블라인드 장애 훈련.
#   chaos.sh start [Cx]     카드를 무작위(또는 지정)로 골라 봉인하고, 정해지지 않은 시점에 주입한다.
#   chaos.sh status         경과 시간만 보여 준다.
#   chaos.sh report         장애 보고서 템플릿을 만든다(results/chaos/<id>/report.md).
#   chaos.sh reveal         봉인을 열어 정답, 실제 타임라인, 채점 기준을 보여 준다.
#   chaos.sh stop           주입 중단, 장애 흔적 정리, 워크로드 종료.
#   chaos.sh validate <Cx>  카드의 인과 사슬이 실제로 재현되는지 판정한다(카드 작성·환경 검증용, 정답이 노출된다).
set -euo pipefail
. "$(dirname "$0")/lib.sh"
. "$LAB_ROOT/scripts/lib-fault.sh"

CHAOS_ROOT="$LAB_ROOT/results/chaos"
CHAOS_STATE="$CHAOS_ROOT/.current"
CARDS_DIR="${CHAOS_CARDS_DIR:-$LAB_ROOT/scripts/chaos/cards}"
WORKLOAD_STATE="$RUNS_DIR/.current-workload"

rand() { echo $(( $1 + RANDOM % ($2 - $1 + 1) )); }

chaos_event() {
  local line
  line="$(date '+%F %T') $(now_ms) $*"
  echo "$line" >> "$SEALED/timeline.log"
}

pick_card() {
  if [ -n "${1:-}" ]; then
    ls "$CARDS_DIR/$1"-*.sh 2>/dev/null | head -1
  else
    # shellcheck disable=SC2012
    ls "$CARDS_DIR"/C*.sh | awk -v r="$RANDOM" '{a[NR] = $0} END {print a[(r % NR) + 1]}'
  fi
}

load_card() {
  card_env() { :; }
  card_teardown() { :; }
  card_noise_ok() { return 0; }
  # shellcheck disable=SC1090
  . "$1"
}

resume_chaos() {
  [ -f "$CHAOS_STATE" ] || die "진행 중인 카오스 회차가 없다. chaos.sh start"
  # shellcheck disable=SC1090
  . "$CHAOS_STATE"
  SEALED="$CHAOS_DIR/.sealed"
  load_card "$(cat "$SEALED/card.path")"
  # shellcheck disable=SC1091
  . "$SEALED/params.env"
  resume_run "$RUN_DIR"
}

# ---- 노이즈: 장애가 아닌 정상 운영 사건 ----

noise_restart() {
  local m="consumer-$(rand 1 "${WORKLOAD_CONSUMERS:-3}")"
  chaos_event "NOISE restart $m (정상 배포)"
  compose restart -t 20 "$m" >/dev/null 2>&1 || true
}

noise_stall() {
  local m="consumer-$(rand 1 "${WORKLOAD_CONSUMERS:-3}")"
  chaos_event "NOISE stall $m 4000ms (짧은 GC 정지)"
  ctl_set "$m.stall" 4000
}

noise_rate() {
  local r=$(( RATE * 120 / 100 ))
  chaos_event "NOISE producer rate $r for 60s (평상시 트래픽 변동)"
  ctl_set producer.rate "$r"
  sleep 60
  ctl_clear producer.rate
  chaos_event "NOISE producer rate restored"
}

run_noise() {
  local n i delay kind
  n="$(rand 0 "$NOISE_MAX")"
  for i in $(seq 1 "$n"); do
    delay="$(rand 20 300)"
    kind="$(rand 1 3)"
    (
      sleep "$delay"
      card_noise_ok "$kind" || exit 0
      case "$kind" in 1) noise_restart ;; 2) noise_stall ;; 3) noise_rate ;; esac
    ) &
  done
}

# ---- 명령 ----

cmd_start() {
  [ -f "$CHAOS_STATE" ] && die "진행 중인 회차가 있다. chaos.sh stop 을 먼저 실행한다."
  require_cluster
  local card id
  card="$(pick_card "${1:-}")"
  [ -n "$card" ] && [ -f "$card" ] || die "card not found in $CARDS_DIR"
  id="$(date +%Y%m%d-%H%M%S)"
  CHAOS_DIR="$CHAOS_ROOT/$id"
  SEALED="$CHAOS_DIR/.sealed"
  mkdir -p "$SEALED"
  echo "$card" > "$SEALED/card.path"
  load_card "$card"
  CHAOS_DELAY_SEC="${CHAOS_DELAY_SEC:-$(rand 30 150)}"
  {
    echo "CHAOS_DELAY_SEC=$CHAOS_DELAY_SEC"
    echo "NOISE_MAX=${CHAOS_NOISE_MAX:-2}"
    card_params
  } > "$SEALED/params.env"
  # shellcheck disable=SC1091
  . "$SEALED/params.env"
  card_answer > "$SEALED/answer.md"

  log "chaos round $id: 정상 상태 워크로드를 띄운다"
  fault_clear_all
  export LAB_ANNOTATE=0
  # 모든 회차가 같은 서비스 프로파일로 시작해야 시작 설정으로 카드를 짐작할 수 없다.
  export RATE=600 BUFFER_MEMORY=524288 VALUE_BYTES=200
  # 페이로드가 커서 브로커 기본 보존(1시간)이면 256MB 디스크가 찬다.
  topic_config --alter --add-config retention.ms=600000 >/dev/null
  card_env
  "$LAB_ROOT/scripts/workload.sh" start "chaos-$id" >/dev/null
  RUN_DIR="$(cat "$WORKLOAD_STATE")"
  {
    echo "CHAOS_ID=$id"
    echo "CHAOS_DIR=$CHAOS_DIR"
    echo "RUN_DIR=$RUN_DIR"
    echo "STARTED_MS=$(now_ms)"
  } > "$CHAOS_STATE"
  chaos_event "ROUND_START run=$RUN_DIR"
  nohup "$0" _inject > "$SEALED/injector.log" 2>&1 &
  echo "$!" > "$SEALED/injector.pid"

  local commit
  commit="$(shasum -a 256 "$SEALED/answer.md" | cut -c1-16)"
  cat <<EOF

회차 $id 시작. 워크로드: consumer 프로토콜, 컨슈머 ${WORKLOAD_CONSUMERS:-3}대, ${RATE} msg/s.
지금부터 정상 상태다. 장애는 정해지지 않은 시점에 시작한다.

  대시보드     http://localhost:3000
  run 디렉터리 $RUN_DIR
  보고서       ./scripts/chaos.sh report
  봉인 확인값  $commit  (reveal 시 같은 값이 나와야 한다)

results/chaos/$id/.sealed/ 와 카드 디렉터리는 reveal 전까지 열지 않는다.
EOF
}

cmd_inject() {
  resume_chaos
  export LAB_ANNOTATE=0
  run_noise
  sleep "$CHAOS_DELAY_SEC"
  chaos_event "INJECT_BEGIN $CARD_ID"
  card_inject
  chaos_event "INJECT_DONE $CARD_ID"
  wait
}

cmd_status() {
  resume_chaos
  local now elapsed
  now="$(now_ms)"
  elapsed=$(( (now - STARTED_MS) / 1000 ))
  echo "round=$CHAOS_ID elapsed=$((elapsed / 60))m$((elapsed % 60))s run=$RUN_DIR"
}

cmd_report() {
  resume_chaos
  local f="$CHAOS_DIR/report.md"
  if [ ! -f "$f" ]; then
    sed -e "s/{{ROUND}}/$CHAOS_ID/" -e "s/{{START}}/$(date -r $((STARTED_MS / 1000)) '+%F %T')/" \
      "$LAB_ROOT/docs/templates/incident-report.md" > "$f"
  fi
  echo "$f"
}

hms_to_ms() {
  python3 - "$1" "$2" <<'PY'
import datetime, sys
base_ms, hms = int(sys.argv[1]), sys.argv[2].strip()
base = datetime.datetime.fromtimestamp(base_ms / 1000)
try:
    t = datetime.datetime.strptime(hms, "%H:%M:%S").time()
except ValueError:
    print("")
    sys.exit(0)
dt = datetime.datetime.combine(base.date(), t)
print(int(dt.timestamp() * 1000))
PY
}

cmd_reveal() {
  resume_chaos
  local commit inject_ms
  commit="$(shasum -a 256 "$SEALED/answer.md" | cut -c1-16)"
  echo "봉인 확인값: $commit"
  echo
  cat "$SEALED/answer.md"
  echo
  echo "## 실제 타임라인"
  echo
  cut -d' ' -f1,2,4- "$SEALED/timeline.log" | sed 's/^/- /'
  echo
  inject_ms="$(awk '$4 == "INJECT_BEGIN" {print $3}' "$SEALED/timeline.log" | head -1)"
  echo "## 알람 이력 (라운드 시작 이후)"
  echo
  "$LAB_ROOT/scripts/alerts.sh" history "$((STARTED_MS / 1000))" | sed 's/^/    /' || echo "    (Prometheus 조회 실패)"
  echo
  if [ -n "$inject_ms" ]; then
    local first
    first="$("$LAB_ROOT/scripts/alerts.sh" first "$((inject_ms / 1000))" 2>/dev/null || true)"
    if [ -n "$first" ]; then
      echo "- 주입 → 첫 알람(${first#* }): $(( ${first%% *} - inject_ms / 1000 ))s"
    else
      echo "- 주입 이후 새로 울린 알람이 없다. 알람이 놓친 장애다"
    fi
    echo
  fi
  if [ -f "$CHAOS_DIR/report.md" ] && [ -n "$inject_ms" ]; then
    local detect cause dms cms
    detect="$(sed -n 's/^- 감지 시각: *//p' "$CHAOS_DIR/report.md" | head -1)"
    cause="$(sed -n 's/^- 원인 확정 시각: *//p' "$CHAOS_DIR/report.md" | head -1)"
    dms="$(hms_to_ms "$inject_ms" "$detect")"
    cms="$(hms_to_ms "$inject_ms" "$cause")"
    echo "## 시간 지표 (보고서 기준)"
    echo
    [ -n "$dms" ] && echo "- 주입 → 감지: $(( (dms - inject_ms) / 1000 ))s"
    [ -n "$cms" ] && echo "- 주입 → 원인 확정: $(( (cms - inject_ms) / 1000 ))s"
    echo
  fi
  cat "$LAB_ROOT/docs/stage5-chaos/scoring.md"
}

cmd_stop() {
  if [ -f "$CHAOS_STATE" ]; then
    resume_chaos
    if [ -f "$SEALED/injector.pid" ]; then
      pkill -P "$(cat "$SEALED/injector.pid")" 2>/dev/null || true
      kill "$(cat "$SEALED/injector.pid")" 2>/dev/null || true
    fi
    chaos_event "ROUND_STOP"
    card_teardown
    rm -f "$CHAOS_STATE"
  fi
  fault_clear_all
  "$LAB_ROOT/scripts/workload.sh" stop
  log "chaos round stopped"
}

expect() {
  local desc="$1"
  shift
  local out
  if out="$("$@" 2>&1)"; then
    log "PASS $desc ($out)"
    echo "PASS $desc ($out)" >> "$CHAOS_DIR/validate.txt"
    return 0
  fi
  log "FAIL $desc ($out)"
  echo "FAIL $desc ($out)" >> "$CHAOS_DIR/validate.txt"
  return 1
}

cmd_validate() {
  local result=0
  CHAOS_DELAY_SEC=5 CHAOS_NOISE_MAX=0 cmd_start "${1:?usage: chaos.sh validate <Cx>}"
  resume_chaos
  card_validate || result=1
  if [ "$result" = 0 ]; then echo "RESULT PASS" >> "$CHAOS_DIR/validate.txt"; else echo "RESULT FAIL" >> "$CHAOS_DIR/validate.txt"; fi
  local dir="$CHAOS_DIR"
  cmd_stop
  log "validate $1: $(tail -1 "$dir/validate.txt") ($dir/validate.txt)"
  return "$result"
}

case "${1:-}" in
  start) cmd_start "${2:-}" ;;
  _inject) cmd_inject ;;
  status) cmd_status ;;
  report) cmd_report ;;
  reveal) cmd_reveal ;;
  stop) cmd_stop ;;
  validate) cmd_validate "${2:-}" ;;
  *) sed -n '2,9p' "$0"; exit 2 ;;
esac
