#!/usr/bin/env python3
"""소비 연속성 판정. processed-*.csv로 구간 안의 유실, 중복, 처리 공백을 센다.

  continuity.py <run_dir> [--from-marker 접두어] [--to-marker 접두어] [--max-gap-ms N]

마커를 주면 markers.csv에서 --from-marker는 처음 일치한 시각, --to-marker는 마지막 일치한 시각을 쓴다.
유실이 있거나 파티션 처리 공백이 --max-gap-ms를 넘으면 종료 코드 1이다.
"""
import argparse
import csv
import pathlib
import sys


def marker_times(run_dir, prefix):
    with open(run_dir / "markers.csv") as f:
        rows = [line.rstrip("\n").split(",", 1) for line in f if "," in line]
    return [int(ts) for ts, label in rows if ts.isdigit() and label.startswith(prefix)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", type=pathlib.Path)
    ap.add_argument("--from-marker")
    ap.add_argument("--to-marker")
    ap.add_argument("--max-gap-ms", type=int)
    args = ap.parse_args()

    start, end = 0, 2 ** 63
    if args.from_marker:
        found = marker_times(args.run_dir, args.from_marker)
        if not found:
            sys.exit("marker not found: " + args.from_marker)
        start = found[0]
    if args.to_marker:
        found = marker_times(args.run_dir, args.to_marker)
        if not found:
            sys.exit("marker not found: " + args.to_marker)
        end = found[-1]

    seen = {}
    times = {}
    for path in sorted(args.run_dir.glob("processed-*.csv")):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                try:
                    ts, partition, offset = int(row["ts"]), int(row["partition"]), int(row["offset"])
                except (TypeError, ValueError):
                    continue
                if start <= ts <= end:
                    seen.setdefault(partition, {})
                    seen[partition][offset] = seen[partition].get(offset, 0) + 1
                    times.setdefault(partition, []).append(ts)

    if not seen:
        print("records=0")
        sys.exit(1)
    # 커밋 뒤에 다시 읽은 레코드는 중복으로 세고, 범위 안에서 한 번도 처리되지 않은 오프셋만 유실로 센다.
    records = sum(sum(p.values()) for p in seen.values())
    duplicates = sum(c - 1 for p in seen.values() for c in p.values())
    missing = sum(max(p) - min(p) + 1 - len(p) for p in seen.values())
    partition_gap = max(max((b - a for a, b in zip(ts, ts[1:])), default=0) for ts in map(sorted, times.values()))
    merged = sorted(t for ts in times.values() for t in ts)
    group_gap = max((b - a for a, b in zip(merged, merged[1:])), default=0)
    print("records=%d missing=%d duplicates=%d max_partition_gap_ms=%d max_group_gap_ms=%d" % (
        records, missing, duplicates, partition_gap, group_gap))
    if missing or (args.max_gap_ms is not None and partition_gap > args.max_gap_ms):
        sys.exit(1)


if __name__ == "__main__":
    main()
