#!/usr/bin/env python3
"""Collects per-cell server metrics from the in-cluster Prometheus.

For each row of OUT/retries.csv it evaluates instant queries at t_end over the
window [t_start, t_end] and appends one row per cell to OUT/metrics.csv.

usage: collect_metrics.py OUT_DIR [PROM_URL]
"""
import csv
import json
import math
import os
import sys
import urllib.parse
import urllib.request

out = sys.argv[1]
prom = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:19090"


def q(expr, t):
    url = f"{prom}/api/v1/query?" + urllib.parse.urlencode({"query": expr, "time": t})
    with urllib.request.urlopen(url, timeout=30) as r:
        res = json.load(r)
    if res.get("status") != "success":
        raise RuntimeError(f"{expr}: {res}")
    v = res["data"]["result"]
    if not v:
        return float("nan")
    return float(v[0]["value"][1])


def delta(sel, t0, t1, by="le"):
    """sum by(by)(sel) at t1 minus at t0, treating series born in the window as 0 at t0."""
    return (f"(sum by ({by}) ({sel}) - (sum by ({by}) ({sel} @ {t0}) "
            f"or sum by ({by}) ({sel}) * 0))")


def hq(phi, sel, t0, t1):
    return q(f"histogram_quantile({phi}, {delta(sel, t0, t1)})", t1)


def count(sel, t0, t1):
    return q(f"sum({sel}) - (sum({sel} @ {t0}) or vector(0))", t1)


def psi_max(sel, t0, t1, by):
    w = int(t1) - int(t0)
    return q(f"max(max_over_time((max by ({by}) (rate({sel}[1m])))[{w}s:15s])) * 100", t1)


def psi_mean(sel, t0, t1, by):
    w = int(t1) - int(t0)
    return q(f"avg(avg_over_time((sum by ({by}) (rate({sel}[1m])))[{w}s:15s])) * 100", t1)


bind = 'ate_scheduler_assignment_duration_seconds_bucket{ate_scheduler_outcome="assigned"}'
restore = 'ate_actor_restore_duration_seconds_bucket{ate_snapshot_phase="total"}'
restore_local = 'ate_actor_restore_duration_seconds_bucket{ate_snapshot_phase="total",ate_snapshot_kind="local"}'
restore_latest = 'ate_actor_restore_duration_seconds_bucket{ate_snapshot_phase="total",ate_snapshot_kind="latest"}'
crashes = "ate_actor_crashes_total"
restores_n = 'ate_actor_restore_duration_seconds_count{ate_snapshot_phase="total"}'
node_psi = 'container_pressure_cpu_waiting_seconds_total{container="node",kubernetes_io_hostname=~".*substrate-node-pool.*"}'
pod_psi = ('container_pressure_cpu_waiting_seconds_total{namespace="benchmark-workloads",container="",'
           'pod=~"pool-.*"}')

cols = ["scenario", "pattern", "policy", "seed", "atespace", "t_start", "t_end",
        "bind_p50_ms", "bind_p99_ms", "restore_p50_ms", "restore_p99_ms",
        "restore_local_p99_ms", "restore_latest_p99_ms", "restores",
        "crashes_metric", "node_psi_cpu_max_pct", "node_psi_cpu_mean_pct", "pod_psi_cpu_max_pct"]

done = set()
mpath = os.path.join(out, "metrics.csv")
if os.path.exists(mpath):
    with open(mpath) as f:
        for r in csv.DictReader(f):
            done.add((r["atespace"], r["t_start"]))

with open(os.path.join(out, "retries.csv")) as f:
    rows = list(csv.DictReader(f))

new = not os.path.exists(mpath)
with open(mpath, "a", newline="") as f:
    w = csv.writer(f)
    if new:
        w.writerow(cols)
    for r in rows:
        if (r["atespace"], r["t_start"]) in done:
            continue
        t0, t1 = r["t_start"], r["t_end"]
        if math.isnan(count(restores_n, t0, t1)):
            raise RuntimeError(f"no restore samples for {r['atespace']}; is Prometheus reachable?")
        ms = lambda x: round(x * 1000, 2)
        rec = [r["scenario"], r["pattern"], r["policy"], r["seed"], r["atespace"], t0, t1,
               ms(hq(0.5, bind, t0, t1)), ms(hq(0.99, bind, t0, t1)),
               ms(hq(0.5, restore, t0, t1)), ms(hq(0.99, restore, t0, t1)),
               ms(hq(0.99, restore_local, t0, t1)), ms(hq(0.99, restore_latest, t0, t1)),
               count(restores_n, t0, t1), count(crashes, t0, t1),
               round(psi_max(node_psi, t0, t1, "kubernetes_io_hostname"), 2),
               round(psi_mean(node_psi, t0, t1, "kubernetes_io_hostname"), 2),
               round(psi_max(pod_psi, t0, t1, "pod"), 2)]
        w.writerow(rec)
        print(dict(zip(cols, rec)))
