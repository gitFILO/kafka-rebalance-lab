#!/usr/bin/env python3
"""실험 결과 분석.

  analyze.py run <run_dir>        한 run을 분석해 summary.json, summary.md를 쓴다
  analyze.py table [--runs DIR]   run별 summary.json을 모아 5x4 결과표(markdown)를 출력한다

지표 정의는 docs/stage1-rebalance/01-measurement.md 참고.
"""
import argparse
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

LAB_ROOT = Path(__file__).resolve().parent.parent
SETTINGS = ["eager", "cooperative", "cooperative-static", "consumer", "consumer-static"]
SCENARIOS = ["rolling-restart", "kill9", "stall", "scale-out"]
CALLBACKS = {"REVOKED", "ASSIGNED", "LOST"}

PAUSE_THRESHOLD_MS = 1000


def read_env(path):
    env = {}
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                env[k] = v
    return env


def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def markers(run_dir):
    result = []
    p = run_dir / "markers.csv"
    if p.exists():
        for line in p.read_text().splitlines():
            ts, label = line.split(",", 1)
            result.append((int(ts), label))
    return result


def marker_ts(marks, label):
    for ts, l in marks:
        if l == label:
            return ts
    return None


def reassigned_partitions(run_dir, start, end):
    result = set()
    for f in run_dir.glob("events-*.csv"):
        for e in read_csv(f):
            if e["event"] == "ASSIGNED" and e["partitions"] and start <= int(e["ts"]) <= end:
                result.update(int(p) for p in e["partitions"].split(";"))
    return result


def analyze_processing(run_dir, start, end, partitions):
    """처리 기록으로 중복, 누락, 파티션별 처리 공백을 계산한다."""
    seen = defaultdict(int)
    times = defaultdict(list)
    dup_in_window = 0
    for f in sorted(run_dir.glob("processed-*.csv")):
        for row in read_csv(f):
            ts = int(row["ts"])
            key = (int(row["partition"]), int(row["offset"]))
            seen[key] += 1
            if seen[key] > 1 and start <= ts <= end:
                dup_in_window += 1
            times[key[0]].append(ts)

    duplicates = sum(c - 1 for c in seen.values() if c > 1)
    dup_partitions = len({p for (p, _), c in seen.items() if c > 1})

    offsets = defaultdict(set)
    for (p, o) in seen:
        offsets[p].add(o)
    missing = 0
    for p, os_ in offsets.items():
        missing += (max(os_) - min(os_) + 1) - len(os_)

    pauses = {}
    for p in range(partitions):
        ts_list = sorted(times.get(p, []))
        prev = max([t for t in ts_list if t <= start], default=start)
        worst = 0
        total = 0
        for t in ts_list:
            if t <= start:
                continue
            if t > end:
                break
            gap = t - prev
            if gap > PAUSE_THRESHOLD_MS:
                worst = max(worst, gap)
                total += gap
            prev = t
        tail = end - prev
        if tail > PAUSE_THRESHOLD_MS:
            worst = max(worst, tail)
            total += tail
        pauses[p] = (worst, total)

    paused = {p: v for p, v in pauses.items() if v[0] > 0}
    moved = reassigned_partitions(run_dir, start, end)
    return {
        "reassigned_partitions": len(moved),
        "paused_reassigned": len([p for p in paused if p in moved]),
        "paused_not_reassigned": len([p for p in paused if p not in moved]),
        "records_processed": sum(seen.values()),
        "duplicates": duplicates,
        "duplicates_in_window": dup_in_window,
        "duplicate_partitions": dup_partitions,
        "missing_offsets": missing,
        "paused_partitions": len(paused),
        "max_pause_ms": max((v[0] for v in paused.values()), default=0),
        "pause_partition_sec": round(sum(v[1] for v in paused.values()) / 1000, 1),
        "pause_by_partition_ms": {str(p): v[0] for p, v in sorted(paused.items())},
    }


def analyze_events(run_dir, start, end):
    events = []
    for f in sorted(run_dir.glob("events-*.csv")):
        events.extend(read_csv(f))
    events.sort(key=lambda r: int(r["ts"]))
    window = [e for e in events if start <= int(e["ts"]) <= end]
    callbacks = [e for e in window if e["event"] in CALLBACKS and e["partitions"]]

    def count(kind):
        return sum(len(e["partitions"].split(";")) for e in window if e["event"] == kind and e["partitions"])

    return {
        "first_callback_after_trigger_ms": int(callbacks[0]["ts"]) - start if callbacks else None,
        "last_callback_after_trigger_ms": int(callbacks[-1]["ts"]) - start if callbacks else None,
        "revoked_partitions": count("REVOKED"),
        "assigned_partitions": count("ASSIGNED"),
        "lost_partitions": count("LOST"),
        "commit_failed": sum(1 for e in window if e["event"] == "COMMIT_FAILED"),
        "errors": [f"{e['member']}: {e['detail']}" for e in window if e["event"] == "ERROR"],
    }


def counter_delta(rows, name, start, end):
    """재시작으로 카운터가 0부터 다시 시작하는 경우를 감안해 구간 내 증가분을 더한다."""
    by_member = defaultdict(list)
    for r in rows:
        if r["name"] == name:
            by_member[r["member"]].append((int(r["ts"]), float(r["value"])))
    deltas = {}
    for member, points in by_member.items():
        points.sort()
        total = 0.0
        prev = None
        for ts, v in points:
            if ts > end:
                break
            if prev is not None and ts > start:
                total += v - prev if v >= prev else v
            prev = v
        deltas[member] = total
    return deltas


def analyze_metrics(run_dir, start, end):
    rows = []
    for f in sorted(run_dir.glob("metrics-*.csv")):
        rows.extend(read_csv(f))
    # 5초 간격 덤프이므로 트리거 직전 덤프부터 관찰 종료 후 첫 덤프까지 포함한다.
    lo, hi = start - 5000, end + 5000
    counts = counter_delta(rows, "rebalance-total", lo, hi)
    latency_total = counter_delta(rows, "rebalance-latency-total", lo, hi)
    failed = counter_delta(rows, "failed-rebalance-total", lo, hi)
    total_count = sum(counts.values())
    return {
        "rebalances_per_member_max": int(max(counts.values(), default=0)),
        "rebalance_latency_total_ms_per_member_max": int(max(latency_total.values(), default=0)),
        "rebalance_latency_avg_ms": int(sum(latency_total.values()) / total_count) if total_count else None,
        "failed_rebalances": int(sum(failed.values())),
    }


def analyze_group(run_dir, start, end):
    rows = [r for r in read_csv(run_dir / "group.csv") if start <= int(r["ts"]) <= end]
    unstable = 0
    prev_ts = None
    states = []
    for r in rows:
        ts = int(r["ts"])
        if prev_ts is not None and r["state"].lower() != "stable":
            unstable += ts - prev_ts
        prev_ts = ts
        if not states or states[-1] != r["state"]:
            states.append(r["state"])
    epochs = [int(r["group_epoch"]) for r in rows if r["group_epoch"]]
    return {
        "group_type_end": rows[-1]["type"] if rows else None,
        "group_unstable_ms": unstable,
        "group_state_sequence": states,
        "group_epoch_delta": (max(epochs) - min(epochs)) if epochs else None,
    }


def analyze_lag(run_dir, start, end):
    rows = read_csv(run_dir / "lag.csv")
    before = [int(r["total_lag"]) for r in rows if start - 10_000 <= int(r["ts"]) < start]
    window = [(int(r["ts"]), int(r["total_lag"])) for r in rows if start <= int(r["ts"]) <= end]
    if not window:
        return {"lag_baseline": None, "lag_peak": None, "lag_peak_after_trigger_ms": None, "lag_recovered_after_trigger_ms": None}
    baseline = int(statistics.median(before)) if before else 0
    peak_ts, peak = max(window, key=lambda x: x[1])
    threshold = max(2 * baseline, 100)
    recovered = None
    for ts, lag in window:
        if ts > peak_ts and lag <= threshold:
            recovered = ts - start
            break
    return {
        "lag_baseline": baseline,
        "lag_peak": peak,
        "lag_peak_after_trigger_ms": peak_ts - start,
        "lag_recovered_after_trigger_ms": recovered,
    }


def run_cmd(run_dir):
    run_dir = run_dir.resolve()
    env = read_env(run_dir / "run.env")
    marks = markers(run_dir)
    start = marker_ts(marks, "TRIGGER")
    end = marker_ts(marks, "OBSERVE_END") or max((ts for ts, _ in marks), default=None)
    if start is None or end is None:
        sys.exit(f"TRIGGER/OBSERVE_END marker missing in {run_dir}/markers.csv")
    partitions = int(env.get("PARTITIONS", "12"))

    summary = {
        "run_id": env.get("RUN_ID", run_dir.name),
        "setting": env.get("SETTING"),
        "scenario": env.get("SCENARIO"),
        "window_ms": end - start,
        "markers": [{"after_trigger_ms": ts - start, "label": l} for ts, l in marks],
    }
    summary.update(analyze_group(run_dir, start, end))
    summary.update(analyze_metrics(run_dir, start, end))
    summary.update(analyze_events(run_dir, start, end))
    summary.update(analyze_processing(run_dir, start, end, partitions))
    summary.update(analyze_lag(run_dir, start, end))

    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    md = render_run(summary)
    (run_dir / "summary.md").write_text(md)
    print(md)


def fmt_ms(v):
    if v is None:
        return "-"
    return f"{int(v)}ms" if abs(v) < 1000 else f"{v / 1000:.1f}s"


def render_run(s):
    lines = [
        f"# {s['run_id']}",
        "",
        f"- setting: `{s['setting']}` / scenario: `{s['scenario']}` / 관찰 구간: {fmt_ms(s['window_ms'])}",
        f"- 그룹 상태 전이: {' → '.join(s['group_state_sequence'])} (최종 type={s['group_type_end']})",
        "",
        "| 지표 | 값 |",
        "|---|---|",
        f"| 리밸런싱 시간: 그룹 비안정 누적(브로커 관측, 1초 샘플) | {fmt_ms(s['group_unstable_ms'])} |",
        f"| 리밸런싱 시간: 멤버별 rebalance-latency-total 증가분 최대 | {fmt_ms(s['rebalance_latency_total_ms_per_member_max'])} |",
        f"| 리밸런싱 1회 평균 latency(구간 내 latency-total 증가분 / 횟수) | {fmt_ms(s['rebalance_latency_avg_ms'])} |",
        f"| 멤버당 리밸런싱 횟수 최대 / 실패 | {s['rebalances_per_member_max']} / {s['failed_rebalances']} |",
        f"| 트리거→첫 콜백 / 트리거→마지막 콜백 | {fmt_ms(s['first_callback_after_trigger_ms'])} / {fmt_ms(s['last_callback_after_trigger_ms'])} |",
        f"| revoke / assign / lost 파티션 수(콜백 합계) | {s['revoked_partitions']} / {s['assigned_partitions']} / {s['lost_partitions']} |",
        f"| 재할당된 파티션 수(구간 내 ASSIGNED에 등장) | {s['reassigned_partitions']} |",
        f"| 정지된 파티션 수(공백 > {PAUSE_THRESHOLD_MS}ms): 전체 (재할당 / 비재할당) | {s['paused_partitions']} ({s['paused_reassigned']} / {s['paused_not_reassigned']}) |",
        f"| 최대 정지 시간 / 정지 파티션·초 합 | {fmt_ms(s['max_pause_ms'])} / {s['pause_partition_sec']} |",
        f"| lag 기준값 / 피크 / 피크 시점 / 회복 시점 | {s['lag_baseline']} / {s['lag_peak']} / {fmt_ms(s['lag_peak_after_trigger_ms'])} / {fmt_ms(s['lag_recovered_after_trigger_ms'])} |",
        f"| 중복 건수(run 전체 / 관찰 구간) / 중복 발생 파티션 | {s['duplicates']} / {s['duplicates_in_window']} / {s['duplicate_partitions']} |",
        f"| 누락 오프셋 | {s['missing_offsets']} |",
        f"| 커밋 실패 이벤트 | {s['commit_failed']} |",
        "",
        "마커(트리거 기준):",
        "",
    ]
    lines += [f"- {fmt_ms(m['after_trigger_ms'])} {m['label']}" for m in s["markers"]]
    if s["errors"]:
        lines += ["", "오류:", ""] + [f"- {e}" for e in s["errors"]]
    return "\n".join(lines) + "\n"


def table_cmd(runs_dir):
    latest = {}
    for p in sorted(runs_dir.glob("*/summary.json")):
        s = json.loads(p.read_text())
        if s.get("setting") in SETTINGS and s.get("scenario") in SCENARIOS:
            latest[(s["setting"], s["scenario"])] = s

    def table(title, cell):
        out = [f"### {title}", "", "| 설정 | " + " | ".join(SCENARIOS) + " |", "|---" * (len(SCENARIOS) + 1) + "|"]
        for setting in SETTINGS:
            row = [setting]
            for scenario in SCENARIOS:
                s = latest.get((setting, scenario))
                row.append(cell(s) if s else "")
            out.append("| " + " | ".join(row) + " |")
        return "\n".join(out) + "\n"

    print(table("리밸런싱 시간 (그룹 비안정 누적 / 멤버별 latency-total 최대)",
                lambda s: f"{fmt_ms(s['group_unstable_ms'])} / {fmt_ms(s['rebalance_latency_total_ms_per_member_max'])}"))
    print(table("정지된 파티션 수: 전체(재할당/비재할당), 최대 정지 시간",
                lambda s: f"{s['paused_partitions']}({s['paused_reassigned']}/{s['paused_not_reassigned']}), {fmt_ms(s['max_pause_ms'])}"))
    print(table("lag 피크 (기준값)", lambda s: f"{s['lag_peak']} ({s['lag_baseline']})"))
    print(table("중복 건수", lambda s: f"{s['duplicates']}"))
    print("run 출처:\n")
    for (setting, scenario), s in sorted(latest.items()):
        print(f"- {setting} × {scenario}: `{s['run_id']}`")


def gaps_cmd(run_dir):
    """처리 기록의 파티션별 오프셋 구멍과 중복. 장애 훈련의 유실·재처리 판정에 쓴다."""
    seen = defaultdict(int)
    for f in sorted(run_dir.glob("processed-*.csv")):
        for row in read_csv(f):
            seen[(int(row["partition"]), int(row["offset"]))] += 1
    offsets = defaultdict(set)
    for (p, o) in seen:
        offsets[p].add(o)
    total_missing = 0
    total_dup = sum(c - 1 for c in seen.values() if c > 1)
    for p in sorted(offsets):
        os_ = sorted(offsets[p])
        holes = []
        for a, b in zip(os_, os_[1:]):
            if b - a > 1:
                holes.append((a + 1, b - 1))
        missing = sum(b - a + 1 for a, b in holes)
        total_missing += missing
        dup = sum(c - 1 for (pp, _), c in seen.items() if pp == p and c > 1)
        ranges = ", ".join(f"{a}-{b}" for a, b in holes[:3]) + (" ..." if len(holes) > 3 else "")
        print(f"p{p:<3} processed={len(os_):<7} missing={missing:<6} duplicates={dup:<6} {ranges}")
    print(f"total_missing={total_missing} total_duplicates={total_dup}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("run_dir", type=Path)
    g = sub.add_parser("gaps")
    g.add_argument("run_dir", type=Path)
    t = sub.add_parser("table")
    t.add_argument("--runs", type=Path, default=LAB_ROOT / "results" / "runs")
    a = p.parse_args()
    if a.cmd == "run":
        run_cmd(a.run_dir)
    elif a.cmd == "gaps":
        gaps_cmd(a.run_dir)
    else:
        table_cmd(a.runs)


if __name__ == "__main__":
    main()
