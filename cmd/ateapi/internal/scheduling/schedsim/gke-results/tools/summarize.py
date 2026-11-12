#!/usr/bin/env python3
"""Joins retries.csv, runs.csv and metrics.csv per run and prints a per-cell table.

usage: summarize.py OUT_DIR
"""
import csv
import math
import os
import statistics
import sys
from collections import defaultdict

out = sys.argv[1]


def load(name):
    p = os.path.join(out, name)
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


key = lambda r: (r["pattern"], r["policy"], r["seed"])
runs = {key(r): r for r in load("runs.csv")}
mets = {key(r): r for r in load("metrics.csv")}
# Runs whose log stream ended early but whose counts were re-read from Cloud
# Logging over the same window and matched.
verified = {r["atespace"] for r in load("stream_verified.csv")}
rows = []
for r in load("retries.csv"):
    k = key(r)
    if r["pattern"] == "smoke" or k not in runs:
        continue
    ru, me = runs[k], mets.get(k, {})
    f = lambda d, c: float(d.get(c, "nan") or "nan")
    resumes = f(ru, "resumes")
    crashes_m = f(me, "crashes_metric")
    valid = (f(ru, "other_errors") == 0 and (r["stream_ok"] == "yes" or r["atespace"] in verified) and r["ate_system_restarts"] == "0")
    rows.append({
        "pattern": r["pattern"], "policy": r["policy"], "seed": r["seed"], "atespace": r["atespace"],
        "valid": valid, "resumes": resumes, "unplaced": resumes - f(ru, "placed"),
        "retries_per_resume": int(r["filled_up"]) / resumes if resumes else math.nan,
        "filled_up": int(r["filled_up"]), "version_conflict": int(r["version_conflict"]),
        "detaches": int(r["detaches"]), "crashes_log": int(r["crashes_log"]),
        "crashes_metric": 0.0 if math.isnan(crashes_m) else crashes_m,
        "ttp_p50": f(ru, "ttp_p50_ms"), "ttp_p99": f(ru, "ttp_p99_ms"),
        "bind_p99": f(me, "bind_p99_ms"), "restore_p50": f(me, "restore_p50_ms"),
        "restore_p99": f(me, "restore_p99_ms"),
        "empty60": f(ru, "empty_drop_60"), "empty180": f(ru, "empty_drop_180"), "empty480": f(ru, "empty_drop_480"),
        "psi_max": f(me, "node_psi_cpu_max_pct"), "psi_mean": f(me, "node_psi_cpu_mean_pct"),
    })

cols = ["pattern", "policy", "seed", "valid", "resumes", "unplaced", "retries_per_resume", "detaches",
        "crashes_log", "crashes_metric", "ttp_p50", "ttp_p99", "bind_p99", "restore_p50", "restore_p99",
        "empty60", "empty180", "empty480", "psi_max", "psi_mean"]


def fmt(v):
    if isinstance(v, bool):
        return "yes" if v else "NO"
    if isinstance(v, float):
        if math.isnan(v):
            return "-"
        return f"{v:.4f}" if abs(v) < 1 and v != 0 else f"{v:.1f}" if v % 1 else f"{int(v)}"
    return str(v)


print("\t".join(cols))
for r in sorted(rows, key=lambda r: (r["pattern"], r["policy"], r["seed"])):
    print("\t".join(fmt(r[c]) for c in cols))

print("\nmedians over valid runs (values per run in parens)")
g = defaultdict(list)
for r in rows:
    if r["valid"]:
        g[(r["pattern"], r["policy"])].append(r)
for (p, pol), rs in sorted(g.items()):
    def m(c):
        vals = [r[c] for r in rs if not math.isnan(r[c])]
        if not vals:
            return "-"
        return f"{fmt(float(statistics.median(vals)))} ({', '.join(fmt(float(v)) for v in vals)})"
    print(f"{p:10s} {pol:16s} n={len(rs)} retries/resume={m('retries_per_resume')} ttp_p50={m('ttp_p50')} "
          f"ttp_p99={m('ttp_p99')} bind_p99={m('bind_p99')} restore_p99={m('restore_p99')} "
          f"crashes={m('crashes_metric')} detaches={m('detaches')} empty480={m('empty480')} psi_max={m('psi_max')}")
