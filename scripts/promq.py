#!/usr/bin/env python3
"""Prometheus 질의 도우미. 훈련 스크립트의 신호 판정에 쓴다.

  promq.py value '<promql>'                       벡터 값의 합(없으면 nan)
  promq.py wait '<promql>' '<op>' <n> [--timeout s] 조건이 참이 될 때까지 대기(op: > >= < <= ==)
  promq.py hold '<promql>' '<op>' <n> --for s      s초 동안 계속 참인지 확인
"""
import argparse
import json
import math
import operator
import os
import sys
import time
import urllib.parse
import urllib.request

URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
OPS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le, "==": operator.eq}


def value(expr):
    q = urllib.parse.urlencode({"query": expr})
    try:
        with urllib.request.urlopen(URL + "/api/v1/query?" + q, timeout=5) as r:
            data = json.load(r)["data"]
    except Exception:
        return math.nan
    if data["resultType"] == "scalar":
        return float(data["result"][1])
    vals = [float(s["value"][1]) for s in data["result"]]
    vals = [v for v in vals if not math.isnan(v)]
    return sum(vals) if vals else math.nan


def check(v, op, n):
    return not math.isnan(v) and OPS[op](v, n)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("cmd", choices=["value", "wait", "hold"])
    p.add_argument("expr")
    p.add_argument("op", nargs="?")
    p.add_argument("n", nargs="?", type=float)
    p.add_argument("--timeout", type=int, default=180)
    p.add_argument("--for", dest="hold_for", type=int, default=30)
    a = p.parse_args()

    if a.cmd == "value":
        v = value(a.expr)
        print("nan" if math.isnan(v) else ("%g" % v))
        return 0
    if a.cmd == "wait":
        deadline = time.time() + a.timeout
        while True:
            v = value(a.expr)
            if check(v, a.op, a.n):
                print("ok %g %s %g" % (v, a.op, a.n))
                return 0
            if time.time() >= deadline:
                print("timeout last=%s want %s %g" % (v, a.op, a.n))
                return 1
            time.sleep(3)
    deadline = time.time() + a.hold_for
    while time.time() < deadline:
        v = value(a.expr)
        if not check(v, a.op, a.n):
            print("broken %s (want %s %g)" % (v, a.op, a.n))
            return 1
        time.sleep(3)
    print("held %ds" % a.hold_for)
    return 0


if __name__ == "__main__":
    sys.exit(main())
