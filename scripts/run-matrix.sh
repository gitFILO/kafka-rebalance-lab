#!/usr/bin/env bash
# 5개 설정 × 4개 시나리오를 순서대로 실행한다. 인자로 일부만 지정할 수 있다.
#   ./scripts/run-matrix.sh                       # 20칸 전체
#   SETTINGS="eager consumer" ./scripts/run-matrix.sh
#   SCENARIOS="kill9" ./scripts/run-matrix.sh
set -euo pipefail
DIR="$(dirname "$0")"

SETTINGS="${SETTINGS:-eager cooperative cooperative-static consumer consumer-static}"
SCENARIOS="${SCENARIOS:-rolling-restart kill9 stall scale-out}"

for scenario in $SCENARIOS; do
  for setting in $SETTINGS; do
    echo "==================== $setting × $scenario ===================="
    "$DIR/run.sh" "$setting" "$scenario" || echo "!! $setting × $scenario failed, continue"
  done
done

python3 "$DIR/analyze.py" table
