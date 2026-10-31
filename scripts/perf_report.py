#!/usr/bin/env python3
"""Stage 1 측정 결과 처리.

  perf_report.py parse <csv> <sweep> <label> <acks> <batch> <linger> <compression> <rps> <records> <bytes> <start> <end> <before> <after> <host> < perf.log
  perf_report.py report <csv>      report.md와 SVG 그래프를 csv와 같은 디렉터리에 만든다
"""
import csv
import math
import os
import re
import statistics
import sys

SUMMARY = re.compile(
    r"(\d+)(?: steady state)? records sent, ([\d.]+) records/sec \(([\d.]+) MB/sec\), ([\d.]+) ms avg latency, ([\d.]+) ms max latency, "
    r"(\d+) ms 50th, (\d+) ms 95th, (\d+) ms 99th")
METRIC = re.compile(r"^producer-metrics:([a-z-]+):\{client-id=[^}]*\}\s*:\s*(\S+)")
SNAPSHOT = re.compile(r'^(?:kafka_network_requestmetrics_([a-z]+)timems_(mean|count_total)\{request="Produce"\}'
                      r'|(process_cpu_seconds_total)) (\S+)')
STAGES = {"requestqueue": "broker_queue_ms", "local": "broker_local_ms", "remote": "broker_remote_ms",
          "responsequeue": "broker_respq_ms", "responsesend": "broker_send_ms", "total": "broker_total_ms"}
FIELDS = ["sweep", "label", "acks", "batch_size", "linger_ms", "compression", "target_rps", "records", "record_bytes",
          "rps", "mbps", "lat_avg", "lat_p50", "lat_p95", "lat_p99", "lat_max", "compression_rate", "batch_size_avg",
          "records_per_request", "request_latency_avg", "queue_time_avg", "io_wait_ratio", "io_ratio", "bufferpool_wait_ratio",
          "broker_queue_ms", "broker_local_ms", "broker_remote_ms", "broker_respq_ms", "broker_send_ms",
          "broker_total_ms", "broker_cpu_cores", "broker_cpu_s_per_gb", "producer_cpu_cores", "send_s", "duration_s",
          "start_ts", "end_ts", "host_state"]
# 노트북이 잠들면 측정이 수십 분으로 늘어난다. 정상 측정은 20초를 넘지 않으므로 이 값을 넘으면 버린다.
MAX_POINT_SEC = float(os.environ.get("PERF_MAX_POINT_SEC", 120))
CONFIG_FIELDS = {"sweep", "label", "acks", "batch_size", "linger_ms", "compression", "target_rps", "records", "record_bytes",
                 "host_state"}


def read_snapshot(path):
    brokers, cur = {}, None
    with open(path) as f:
        for line in f:
            if line.startswith("broker "):
                cur = brokers.setdefault(line.split()[1], {})
                continue
            m = SNAPSHOT.match(line)
            if m and cur is not None:
                cur[(m.group(1), m.group(2)) if m.group(1) else "cpu"] = float(m.group(4))
    return brokers


def broker_delta(before, after):
    """구간의 Produce 단계별 평균(ms)과 브로커 CPU 초. 측정 사이에는 Produce가 없으므로 전후 차이가 곧 이 구간이다."""
    sums, counts = dict.fromkeys(STAGES, 0.0), dict.fromkeys(STAGES, 0.0)
    cpu = 0.0
    for name, a in after.items():
        b = before.get(name, {})
        cpu += a.get("cpu", 0.0) - b.get("cpu", 0.0)
        for st in STAGES:
            ca, cb = a.get((st, "count_total"), 0.0), b.get((st, "count_total"), 0.0)
            sums[st] += a.get((st, "mean"), 0.0) * ca - b.get((st, "mean"), 0.0) * cb
            counts[st] += ca - cb
    return {STAGES[st]: sums[st] / counts[st] if counts[st] else math.nan for st in STAGES}, cpu


def fmt(v, digits=2):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return ("%." + str(digits) + "f") % v


def parse(argv):
    path, sweep, label, acks, batch, linger, comp, rps, records, size, start, end, before, after, host = argv
    text = sys.stdin.read()
    lines = [m for m in SUMMARY.finditer(text)]
    if not lines:
        sys.stderr.write("perf-test summary not found:\n" + text[-2000:])
        sys.exit(1)
    # --warmup-records를 쓰면 정상 상태 요약이 마지막에 나온다. 지연은 그 줄에서, 시간과 바이트는 전체 줄에서 읽는다.
    s, whole = lines[-1], lines[0]
    metrics = {}
    for line in text.splitlines():
        m = METRIC.match(line.strip())
        if m:
            try:
                metrics[m.group(1)] = float(m.group(2))
            except ValueError:
                metrics[m.group(1)] = math.nan
    start, end = float(start), float(end)
    dur = end - start
    send_s = int(whole.group(1)) / float(whole.group(2))
    if max(dur, send_s) > MAX_POINT_SEC:
        print("  WARN: 측정이 %.0f초 걸려 버린다(잠자기 등으로 끊긴 측정): %s %s" % (max(dur, send_s), sweep, label))
        return
    stages, cpu = broker_delta(read_snapshot(before), read_snapshot(after))
    usage = re.search(r"^cgroup_usage_usec (\d+)", text, re.M)
    gb = int(whole.group(1)) * int(size) / 1e9
    # 비율 메트릭(*-ratio)은 30초 창이 다 찬 것처럼 나누므로 짧은 측정에서 작게 나온다. 누적 시간으로 계산한다.
    busy = lambda name: metrics.get(name, math.nan) / 1e9 / send_s
    row = {
        "sweep": sweep, "label": label, "acks": acks, "batch_size": batch, "linger_ms": linger, "compression": comp,
        "target_rps": rps, "records": records, "record_bytes": size,
        "rps": s.group(2), "mbps": s.group(3), "lat_avg": s.group(4), "lat_max": s.group(5),
        "lat_p50": s.group(6), "lat_p95": s.group(7), "lat_p99": s.group(8),
        "compression_rate": fmt(metrics.get("compression-rate-avg"), 3),
        "batch_size_avg": fmt(metrics.get("batch-size-avg"), 0),
        "records_per_request": fmt(metrics.get("records-per-request-avg"), 1),
        "request_latency_avg": fmt(metrics.get("request-latency-avg"), 1),
        "queue_time_avg": fmt(metrics.get("record-queue-time-avg"), 1),
        "io_wait_ratio": fmt(busy("io-wait-time-ns-total"), 3),
        "io_ratio": fmt(busy("io-time-ns-total"), 3),
        "bufferpool_wait_ratio": fmt(busy("bufferpool-wait-time-ns-total"), 3),
        "broker_cpu_cores": fmt(cpu / send_s, 2), "broker_cpu_s_per_gb": fmt(cpu / gb, 1),
        "producer_cpu_cores": fmt(int(usage.group(1)) / 1e6 / dur, 2) if usage else "",
        "send_s": fmt(send_s, 1), "duration_s": fmt(dur, 1), "start_ts": int(start), "end_ts": int(end),
        "host_state": host,
    }
    row.update({k: fmt(v, 2) for k, v in stages.items()})
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)
    print("  %s rec/s, %s MB/s (%ss), p50 %s, p99 %s, rec/req %s, comp %s, remote %s ms, broker %s cores, producer %s cores" % (
        row["rps"], row["mbps"], row["send_s"], row["lat_p50"], row["lat_p99"], row["records_per_request"],
        row["compression_rate"] or "-", row["broker_remote_ms"] or "-", row["broker_cpu_cores"],
        row["producer_cpu_cores"] or "-"))


# ---- SVG ----

W, H, ML, MR, MT, MB = 640, 360, 70, 70, 40, 60
COLORS = ["#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c"]


def esc(t):
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def nice_max(v):
    if v <= 0:
        return 1
    e = 10 ** math.floor(math.log10(v))
    for m in (1, 2, 2.5, 5, 10):
        if v <= m * e:
            return m * e
    return 10 * e


def svg_chart(path, title, xlabel, series, ylabel, y2=None, xlog=False, bars=False, xcats=None, xfmt=None):
    """series: [(name, [(x, y)])]. y2: (label, [(name, [(x, y)])]) 오른쪽 축."""
    pw, ph = W - ML - MR, H - MT - MB
    xs = [x for _, pts in series for x, _ in pts]
    if xcats:
        xpos = {c: ML + pw * (i + 0.5) / len(xcats) for i, c in enumerate(xcats)}
        fx = lambda x: xpos[x]
    else:
        lo, hi = min(xs), max(xs)
        if xlog:
            lo, hi = math.log10(lo), math.log10(hi)
        span = (hi - lo) or 1
        fx = lambda x: ML + pw * (((math.log10(x) if xlog else x) - lo) / span)
    ymax = nice_max(max(y for _, pts in series for _, y in pts))
    fy = lambda y: MT + ph * (1 - y / ymax)
    out = ['<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" font-family="sans-serif" font-size="12">' % (W, H),
           '<rect width="100%" height="100%" fill="white"/>',
           '<text x="%d" y="22" font-size="15" font-weight="bold">%s</text>' % (ML, esc(title))]
    for i in range(6):
        y = ymax * i / 5
        out.append('<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#e5e7eb"/>' % (ML, fy(y), ML + pw, fy(y)))
        out.append('<text x="%d" y="%.1f" text-anchor="end">%g</text>' % (ML - 6, fy(y) + 4, round(y, 3)))
    out.append('<text x="16" y="%d" transform="rotate(-90 16 %d)" text-anchor="middle">%s</text>' % (MT + ph / 2, MT + ph / 2, esc(ylabel)))
    ticks = xcats or sorted({x for x, _ in series[0][1]})
    last = -1e9
    for x in ticks:
        # 로그 축 오른쪽 끝은 눈금이 몰려 글자가 겹친다.
        if fx(x) - last < 36:
            continue
        last = fx(x)
        out.append('<text x="%.1f" y="%d" text-anchor="middle">%s</text>' % (last, MT + ph + 18, esc(x if xcats else (xfmt(x) if xfmt else "%g" % x))))
    out.append('<text x="%d" y="%d" text-anchor="middle">%s</text>' % (ML + pw / 2, H - 12, esc(xlabel)))
    out.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#111"/>' % (ML, MT + ph, ML + pw, MT + ph))
    nb = len(series)
    for si, (name, pts) in enumerate(series):
        c = COLORS[si % len(COLORS)]
        if bars:
            bw = pw / len(ticks) / (nb + 1)
            for x, y in pts:
                cx = fx(x) - bw * nb / 2 + bw * si
                out.append('<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s"/>' % (cx, fy(y), bw * 0.9, MT + ph - fy(y), c))
        else:
            d = " ".join("%s%.1f,%.1f" % ("M" if i == 0 else "L", fx(x), fy(y)) for i, (x, y) in enumerate(pts))
            out.append('<path d="%s" fill="none" stroke="%s" stroke-width="2"/>' % (d, c))
            for x, y in pts:
                out.append('<circle cx="%.1f" cy="%.1f" r="3" fill="%s"/>' % (fx(x), fy(y), c))
        out.append('<text x="%d" y="%d" fill="%s">%s</text>' % (ML + 10 + si * 150, MT - 6, c, esc(name)))
    if y2:
        label2, series2 = y2
        y2max = nice_max(max(y for _, pts in series2 for _, y in pts))
        fy2 = lambda y: MT + ph * (1 - y / y2max)
        for i in range(6):
            y = y2max * i / 5
            out.append('<text x="%d" y="%.1f">%g</text>' % (ML + pw + 6, fy2(y) + 4, round(y, 3)))
        out.append('<text x="%d" y="%d" transform="rotate(90 %d %d)" text-anchor="middle">%s</text>' % (
            W - 14, MT + ph / 2, W - 14, MT + ph / 2, esc(label2)))
        for si, (name, pts) in enumerate(series2):
            c = COLORS[(si + len(series)) % len(COLORS)]
            # 범주 축은 값 사이가 이어지지 않으므로 점으로만 찍는다.
            if not xcats:
                d = " ".join("%s%.1f,%.1f" % ("M" if i == 0 else "L", fx(x), fy2(y)) for i, (x, y) in enumerate(pts))
                out.append('<path d="%s" fill="none" stroke="%s" stroke-width="2" stroke-dasharray="6 4"/>' % (d, c))
            for x, y in pts:
                out.append('<circle cx="%.1f" cy="%.1f" r="5" fill="%s"/>' % (fx(x), fy2(y), c))
            out.append('<text x="%d" y="%d" fill="%s">%s (오른쪽 축)</text>' % (ML + 10 + (si + len(series)) * 150, MT - 6, c, esc(name)))
    out.append("</svg>")
    with open(path, "w") as f:
        f.write("\n".join(out))


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return math.nan


def median_row(rows):
    """같은 점을 반복 측정한 줄을 열별 중앙값 한 줄로 묶는다. 처리량 편차는 (최대-최소)/중앙값이다."""
    out = dict(rows[-1])
    for k in FIELDS:
        if k in CONFIG_FIELDS:
            continue
        vals = [num(r.get(k)) for r in rows]
        vals = [v for v in vals if not math.isnan(v)]
        if vals:
            out[k] = ("%.3f" % statistics.median(vals)).rstrip("0").rstrip(".")
    rates = [num(r["rps"]) for r in rows]
    out["runs"] = str(len(rows))
    out["spread"] = "%.0f%%" % (100 * (max(rates) - min(rates)) / statistics.median(rates)) if len(rows) > 1 else "-"
    return out


def report(path):
    base = os.path.dirname(path)
    rows = list(csv.DictReader(open(path)))
    grouped = {}
    for r in rows:
        if r["sweep"] != "warmup":
            grouped.setdefault((r["sweep"], r["label"]), []).append(r)
    by = {}
    for (sweep, _), rs in grouped.items():
        by.setdefault(sweep, []).append(median_row(rs))
    md = ["# Stage 1 측정 결과", "", "`scripts/perf.sh report`가 만든 파일이다. 같은 점을 반복 측정하면 열마다 중앙값을 쓴다.", ""]
    states = {}
    for r in rows:
        states[r["host_state"]] = states.get(r["host_state"], 0) + 1
    if len(states) > 1:
        md += ["**경고: 측정 조건이 섞였다.** " + ", ".join("%s %d건" % kv for kv in sorted(states.items())) +
               ". 전원이나 덮개가 다른 측정은 비교하지 않는다.", ""]
    cols = [("label", "점"), ("runs", "횟수"), ("rps", "records/s"), ("mbps", "MB/s"), ("spread", "처리량 편차"),
            ("lat_p50", "p50 ms"), ("lat_p99", "p99 ms"), ("batch_size_avg", "평균 배치 B"),
            ("records_per_request", "요청당 레코드"), ("queue_time_avg", "큐 대기 ms"),
            ("request_latency_avg", "요청 지연 ms"), ("broker_queue_ms", "브로커 큐 ms"), ("broker_local_ms", "local ms"),
            ("broker_remote_ms", "remote ms"), ("compression_rate", "압축률"), ("broker_cpu_cores", "브로커 CPU"),
            ("broker_cpu_s_per_gb", "CPU초/GB"), ("producer_cpu_cores", "프로듀서 CPU"),
            ("io_wait_ratio", "Sender 대기"), ("bufferpool_wait_ratio", "버퍼 대기")]
    order = {"acks": 0, "acks-fixed": 1, "batch": 2, "linger": 3, "compression": 4, "compression-fixed": 5, "load": 6, "load-steady": 7,
             "load-steady-scrape": 8, "inflight": 9, "broker-cpu": 10, "payload": 11}
    for sweep in sorted(by, key=lambda s: order.get(s, 9)):
        rs = by[sweep]
        if sweep in ("batch", "linger"):
            rs.sort(key=lambda r: num(r["batch_size"] if sweep == "batch" else r["linger_ms"]))
        elif sweep == "load":
            rs.sort(key=lambda r: (num(r["target_rps"]) < 0, num(r["target_rps"])))
        md += ["## " + sweep, "", "| " + " | ".join(c[1] for c in cols) + " |", "|" + "---|" * len(cols)]
        for r in rs:
            md.append("| " + " | ".join(r.get(c[0], "") or "-" for c in cols) + " |")
        md.append("")
        svg = None
        if sweep == "acks":
            cats = [r["acks"] for r in rs]
            svg_chart(os.path.join(base, "acks.svg"), "acks별 최대 처리량과 p99", "acks",
                      [("MB/s", [(r["acks"], num(r["mbps"])) for r in rs])], "MB/s",
                      y2=("p99 ms", [("p99 ms", [(r["acks"], num(r["lat_p99"])) for r in rs])]), xcats=cats, bars=True)
            svg = "acks.svg"
        elif sweep == "batch":
            svg_chart(os.path.join(base, "batch.svg"), "batch.size별 최대 처리량", "batch.size (log)",
                      [("MB/s", [(num(r["batch_size"]), num(r["mbps"])) for r in rs])], "MB/s",
                      y2=("p99 ms", [("p99 ms", [(num(r["batch_size"]), num(r["lat_p99"])) for r in rs])]), xlog=True,
                      xfmt=lambda x: "%dKB" % round(x / 1024))
            svg = "batch.svg"
        elif sweep == "linger":
            svg_chart(os.path.join(base, "linger.svg"), "고정 부하 10k rec/s에서 linger.ms 효과", "linger.ms",
                      [("p50 ms", [(num(r["linger_ms"]), num(r["lat_p50"])) for r in rs]),
                       ("p99 ms", [(num(r["linger_ms"]), num(r["lat_p99"])) for r in rs])], "지연 ms",
                      y2=("요청당 레코드", [("요청당 레코드", [(num(r["linger_ms"]), num(r["records_per_request"])) for r in rs])]))
            svg = "linger.svg"
        elif sweep == "compression":
            cats = [r["compression"] for r in rs]
            svg_chart(os.path.join(base, "compression.svg"), "압축 코덱별 처리량과 압축률", "compression.type",
                      [("MB/s(압축 전)", [(r["compression"], num(r["mbps"])) for r in rs])], "MB/s",
                      y2=("압축률", [("압축률", [(r["compression"], num(r["compression_rate"]) if r["compression_rate"] else 1.0) for r in rs])]),
                      xcats=cats, bars=True)
            svg = "compression.svg"
        elif sweep == "load":
            pts = sorted(((num(r["rps"]), r) for r in rs), key=lambda t: t[0])
            series = [("p50", [(x, num(r["lat_p50"])) for x, r in pts]),
                      ("p99 (전체 구간)", [(x, num(r["lat_p99"])) for x, r in pts])]
            steady = sorted(((num(r["rps"]), r) for r in by.get("load-steady", [])), key=lambda t: t[0])
            if steady:
                series.append(("p99 (앞 절반 제외)", [(x, num(r["lat_p99"])) for x, r in steady]))
            svg_chart(os.path.join(base, "load.svg"), "처리량-지연 곡선(acks=all, linger 5)", "달성 처리량 (records/s, log)",
                      series, "지연 ms", xlog=True, xfmt=lambda x: "%dk" % round(x / 1000))
            svg = "load.svg"
        if svg:
            md += ["![%s](%s)" % (sweep, svg), ""]
    with open(os.path.join(base, "report.md"), "w") as f:
        f.write("\n".join(md))
    print(os.path.join(base, "report.md"))


if __name__ == "__main__":
    if sys.argv[1] == "parse":
        parse(sys.argv[2:])
    elif sys.argv[1] == "report":
        report(sys.argv[2])
    else:
        sys.exit(__doc__)
