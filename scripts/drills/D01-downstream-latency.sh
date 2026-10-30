# shellcheck shell=bash
DRILL_TITLE="다운스트림 지연으로 인한 lag 누적"

drill_inject() {
  ctl_set process_ms 8
}

drill_signal() {
  local r=0
  expect "total lag > 3000" $PROMQ wait 'lab_group_totallag' '>' 3000 --timeout 120 || r=1
  expect "members stay 3 (no storm)" $PROMQ hold 'lab_group_members' '==' 3 --for 20 || r=1
  return $r
}

drill_fix() {
  "$LAB_ROOT/scripts/workload.sh" scale 6
}

drill_recovered() {
  expect "total lag < 500" $PROMQ wait 'lab_group_totallag' '<' 500 --timeout 420
}

drill_teardown() {
  ctl_clear process_ms
}
