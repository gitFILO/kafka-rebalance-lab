#!/usr/bin/env python3
"""jfr print --json 결과에서 스레드별 샘플 수와 상위 프레임(포함 비율)을 집계한다.

  jfr_top.py <samples.json> [--thread main] [--top 25] [--filter org.apache.kafka]

jdk.ExecutionSample은 자바 코드를 실행 중인 스레드, jdk.NativeMethodSample은 네이티브(park, epoll 등)에
머무는 스레드를 잡는다. 둘을 따로 집계한다.
"""
import argparse
import collections
import json


def frames(event):
    for f in event["values"]["stackTrace"]["frames"]:
        m = f["method"]
        yield "%s.%s" % (m["type"]["name"].rsplit(".", 1)[-1], m["name"])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("path")
    p.add_argument("--thread", default="main")
    p.add_argument("--top", type=int, default=25)
    p.add_argument("--filter", default="")
    a = p.parse_args()

    events = json.load(open(a.path))["recording"]["events"]
    for kind in ("jdk.ExecutionSample", "jdk.NativeMethodSample"):
        summarize(kind, [e for e in events if e["type"] == kind], a)


def summarize(kind, events, a):
    by_thread = collections.Counter()
    inclusive = collections.Counter()
    leaf = collections.Counter()
    total = 0
    for e in events:
        t = e["values"]["sampledThread"]
        name = (t.get("javaName") or t.get("osName") or "?").split(" | ")[0]
        by_thread[name] += 1
        if name != a.thread:
            continue
        total += 1
        fs = list(frames(e))
        if fs:
            leaf[fs[0]] += 1
        # 재귀 호출이 같은 프레임을 여러 번 세지 않도록 샘플마다 한 번만 센다.
        for f in set(fs):
            if a.filter in f or not a.filter:
                inclusive[f] += 1

    print("######## %s (%d samples)" % (kind, len(events)))
    print("== samples by thread")
    for name, n in by_thread.most_common(8):
        print("%6d  %s" % (n, name))
    print()
    print("== thread '%s': %d samples, inclusive top %d" % (a.thread, total, a.top))
    for f, n in inclusive.most_common(a.top):
        print("%5.1f%%  %s" % (100.0 * n / max(total, 1), f))
    print()
    print("== thread '%s': leaf top 10" % a.thread)
    for f, n in leaf.most_common(10):
        print("%5.1f%%  %s" % (100.0 * n / max(total, 1), f))
    print()


if __name__ == "__main__":
    main()
