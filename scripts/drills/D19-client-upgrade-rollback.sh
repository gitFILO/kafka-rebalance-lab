# shellcheck shell=bash
DRILL_TITLE="컨슈머 클라이언트 업그레이드 후 롤백"
FROM_VERSION=4.1.2
TO_VERSION=4.2.0
# 레코드당 402ms(PROCESS_MS 2 + 400)로 풀 배치 100건이 약 40초 걸려 max.poll.interval.ms 30초를 넘긴다. D02는 350ms로 약 35초다.
REGRESSION_MS="${REGRESSION_MS:-400}"
CLIENT_UPGRADE="$LAB_ROOT/scripts/client-upgrade.sh"

# 되돌리는 도중 빌드를 기다리지 않게 미리 맞춘다. 워크로드도 같은 소스의 latest로 시작한다.
drill_env() {
  "$CLIENT_UPGRADE" build "$FROM_VERSION" "$TO_VERSION"
}

drill_inject() {
  export RUN_DIR_HOST RUN_ID
  mark "CLIENT_UPGRADE_START"
  "$CLIENT_UPGRADE" roll-all "$TO_VERSION"
  mark "CLIENT_UPGRADE_DONE"
  # 회귀 시작 마커를 따로 찍으면 Grafana 어노테이션에 정답이 드러난다. CLIENT_UPGRADE_DONE을 구간 시작으로 함께 쓴다.
  ctl_set "regression.$TO_VERSION" "$REGRESSION_MS"
}

# 이미지 태그가 아니라 실행 중인 컨테이너 안의 jar 버전으로 판정한다. 훈련 중 증설했을 수 있어 워크로드 대수는 하한으로 본다.
members_on() {
  "$CLIENT_UPGRADE" status | awk -v want="$1" -v n="${WORKLOAD_CONSUMERS:-3}" '
    $2 != "-" { seen++; list = list " " $1 "=" $3; if ($3 == want) ok++ }
    END { print "members:" list; exit !(seen >= n && ok == seen) }'
}

no_records_missing() {
  python3 "$LAB_ROOT/scripts/continuity.py" "$RUN_DIR_HOST" --from-marker "$1" --to-marker "$2"
}

drill_signal() {
  local r=0
  expect "no records missing during upgrade" no_records_missing CLIENT_UPGRADE_START CLIENT_UPGRADE_DONE || r=1
  expect "all members on $TO_VERSION" members_on "$TO_VERSION" || r=1
  expect "ConsumerPollIntervalNearLimit alert firing" alert_firing ConsumerPollIntervalNearLimit 180 || r=1
  expect "time-between-poll max > max.poll.interval.ms" \
    $PROMQ wait 'max(kafka_consumer_consumer_metrics_time_between_poll_max)' '>' 30000 --timeout 180 || r=1
  # 교체 자체도 revoke를 남기므로 D02와 달리 쫓겨날 때만 생기는 lost만 본다.
  expect "members evicted from the group" \
    $PROMQ wait 'sum(increase(lab_consumer_losttotal[3m]))' '>' 0 --timeout 120 || r=1
  expect "ConsumerLagGrowing alert firing" alert_firing ConsumerLagGrowing 300 || r=1
  return $r
}

drill_fix() {
  "$CLIENT_UPGRADE" roll-all "$FROM_VERSION"
}

drill_recovered() {
  local r=0 n
  # 되돌리지 않았다면 lag을 기다려도 기준을 채울 수 없다.
  expect "all members on $FROM_VERSION" members_on "$FROM_VERSION" || return 1
  expect "lag back under 5000" $PROMQ wait 'lab_group_totallag' '<' 5000 --timeout 600 || r=1
  n="$(consumers_running | wc -w | tr -d ' ')"
  expect "group Stable with $n members for 30s" \
    $PROMQ hold "lab_group_members and on() (lab_group_state_active{state=\"Stable\"} == 1)" '==' "$n" --for 30 || r=1
  mark "CLIENT_RECOVERY_CHECK"
  expect "no records missing from regression to recovery" no_records_missing CLIENT_UPGRADE_DONE CLIENT_RECOVERY_CHECK || r=1
  return $r
}

drill_teardown() {
  local tmp
  ctl_clear "regression.$TO_VERSION"
  [ -f "$NODE_VERSIONS_FILE" ] || return 0
  tmp="$(mktemp)"
  grep -v '^CONSUMER_[0-9]*_TAG=' "$NODE_VERSIONS_FILE" > "$tmp" || true
  mv "$tmp" "$NODE_VERSIONS_FILE"
  [ -s "$NODE_VERSIONS_FILE" ] || rm -f "$NODE_VERSIONS_FILE"
}
