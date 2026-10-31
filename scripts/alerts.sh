#!/usr/bin/env bash
# 알람 조회
#   alerts.sh                 지금 Alertmanager가 받은 알람(억제 여부 포함)
#   alerts.sh pending         Prometheus에서 조건은 참이지만 for 시간을 채우지 못한 알람
#   alerts.sh history <since> 지정 시각 이후 발화한 알람과 최초 발화 시각. since는 epoch 초 또는 '30m' 형식
#   alerts.sh first <since>   지정 시각 이후 새로 발화한 첫 알람의 epoch 초와 이름
#   alerts.sh watch           5초마다 현재 알람을 다시 출력
set -euo pipefail
. "$(dirname "$0")/lib.sh"

AM_URL="${ALERTMANAGER_URL:-http://localhost:9093}"
PROM_URL="${PROMETHEUS_URL:-http://localhost:9090}"

cmd_current() {
  curl -sf "$AM_URL/api/v2/alerts" | python3 -c '
import json, sys
alerts = json.load(sys.stdin)
if not alerts:
    print("firing 알람 없음")
    sys.exit(0)
order = {"page": 0, "ticket": 1}
alerts.sort(key=lambda a: (order.get(a["labels"].get("severity"), 9), a["startsAt"]))
for a in alerts:
    l = a["labels"]
    state = a["status"]["state"]
    if a["status"].get("inhibitedBy"):
        state = "inhibited"
    extra = ",".join("%s=%s" % (k, v) for k, v in sorted(l.items()) if k not in ("alertname", "severity", "area"))
    print("%-7s %-10s %-28s %s  %s%s" % (l.get("severity", "-"), state, l["alertname"], a["startsAt"][11:19] + "Z",
          a["annotations"].get("summary", ""), ("  [" + extra + "]") if extra else ""))
' || die "Alertmanager($AM_URL)에 연결할 수 없다"
}

cmd_pending() {
  curl -sf "$PROM_URL/api/v1/alerts" | python3 -c '
import json, sys
rows = [a for a in json.load(sys.stdin)["data"]["alerts"] if a["state"] == "pending"]
if not rows:
    print("pending 알람 없음")
for a in rows:
    print("%-28s since %s  %s" % (a["labels"]["alertname"], a["activeAt"][11:19] + "Z", a["annotations"].get("summary", "")))
'
}

to_epoch() {
  local since="$1" now
  now="$(date +%s)"
  case "$since" in
    *m) echo $(( now - ${since%m} * 60 )) ;;
    *h) echo $(( now - ${since%h} * 3600 )) ;;
    *) echo "$since" ;;
  esac
}

# ALERTS 시계열을 범위 질의해 알람별 최초 발화 시각과 지속 시간을 계산한다.
alert_rows() {
  python3 - "$PROM_URL" "$1" "$(date +%s)" "$2" <<'PY'
import json, sys, time, urllib.parse, urllib.request
url, start, end, mode = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
step = 5
q = urllib.parse.urlencode({"query": 'ALERTS{alertstate="firing"}', "start": start, "end": end, "step": step})
with urllib.request.urlopen(url + "/api/v1/query_range?" + q, timeout=10) as r:
    result = json.load(r)["data"]["result"]
rows = []
for s in result:
    ts = [float(v[0]) for v in s["values"]]
    labels = {k: v for k, v in s["metric"].items() if k not in ("__name__", "alertstate")}
    rows.append((min(ts), max(ts), min(ts) < start + step, labels))
rows.sort(key=lambda r: r[0])
if mode == "first":
    fresh = [r for r in rows if not r[2]]
    if fresh:
        print("%d %s" % (fresh[0][0], fresh[0][3]["alertname"]))
    sys.exit(0)
if not rows:
    print("이 구간에 발화한 알람 없음")
for first, last, before, l in rows:
    extra = ",".join("%s=%s" % (k, v) for k, v in sorted(l.items()) if k not in ("alertname", "severity", "area"))
    print("%s  %-7s %-28s 지속 %4ds%s%s" % (time.strftime("%H:%M:%S", time.localtime(first)), l.get("severity", "-"),
          l["alertname"], int(last - first) + step, "  (구간 시작 전부터)" if before else "",
          ("  [" + extra + "]") if extra else ""))
PY
}

case "${1:-current}" in
  current) cmd_current ;;
  pending) cmd_pending ;;
  history) alert_rows "$(to_epoch "${2:-1h}")" list ;;
  first) alert_rows "$(to_epoch "${2:?since}")" first ;;
  watch)
    while :; do
      clear
      date '+%H:%M:%S'
      cmd_current
      echo
      cmd_pending
      sleep 5
    done
    ;;
  *) sed -n '2,7p' "$0"; exit 2 ;;
esac
