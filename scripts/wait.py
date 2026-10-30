#!/usr/bin/env python3
"""run 디렉터리의 CSV를 읽어 그룹 안정화, 멤버 할당 완료를 기다린다."""
import argparse
import csv
import sys
import time
from pathlib import Path


def last_row(path):
    if not path.exists():
        return None
    with path.open() as f:
        rows = list(csv.DictReader(f))
    return rows[-1] if rows else None


def group_ready(run_dir, members, partitions):
    row = last_row(run_dir / "group.csv")
    if row is None:
        return False, "group.csv empty"
    ok = (row["state"].lower() == "stable"
          and row["members"] == str(members)
          and row["assigned_partitions"] == str(partitions))
    return ok, f"type={row['type']} state={row['state']} members={row['members']} assigned={row['assigned_partitions']}"


def member_assigned(run_dir, member, since):
    path = run_dir / f"events-{member}.csv"
    if not path.exists():
        return False, "no events yet"
    started = False
    with path.open() as f:
        for row in csv.DictReader(f):
            if int(row["ts"]) < since:
                continue
            if row["event"] == "START":
                started = True
            elif started and row["event"] == "ASSIGNED" and row["detail"] != "owned=0":
                return True, f"{member} owns partitions ({row['detail']})"
    return False, f"{member} started={started}, waiting for partitions"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("what", choices=["group", "assigned"])
    p.add_argument("run_dir", type=Path)
    p.add_argument("--members", type=int, default=6)
    p.add_argument("--partitions", type=int, default=12)
    p.add_argument("--member")
    p.add_argument("--since", type=int, default=0)
    p.add_argument("--timeout", type=int, default=180)
    a = p.parse_args()

    deadline = time.time() + a.timeout
    last = ""
    while time.time() < deadline:
        if a.what == "group":
            ok, msg = group_ready(a.run_dir, a.members, a.partitions)
        else:
            ok, msg = member_assigned(a.run_dir, a.member, a.since)
        if ok:
            print(f"  ready: {msg}")
            return 0
        if msg != last:
            print(f"  waiting: {msg}")
            last = msg
        time.sleep(1)
    print(f"  timeout: {last}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
