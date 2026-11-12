#!/usr/bin/env python3
"""Prints the results-gke.md pattern tables from the phase-b and phase-b-70 CSVs.

Each cell shows the median of its valid runs, then every valid run in parens.
usage: report_tables.py
"""
import csv
import math
import os
import statistics
from collections import defaultdict

G = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOPOS = [("g-gke8", "phase-b", "~90% peak"), ("g-gke8-70", "phase-b-70", "~70% peak")]
POLICIES = [("p2c", "p2c"), ("bestofd:3", "bestofd-3"), ("prefixsat:1:0.2", "prefixsat")]


def load(d, name):
    p = os.path.join(G, d, name)
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


def rows_for(d):
    key = lambda r: (r["pattern"], r["policy"], r["seed"])
    runs = {key(r): r for r in load(d, "runs.csv")}
    mets = {key(r): r for r in load(d, "metrics.csv")}
    verified = {r["atespace"] for r in load(d, "stream_verified.csv")}
    out = []
    for r in load(d, "retries.csv"):
        k = key(r)
        if r["pattern"] == "smoke" or k not in runs:
            continue
        ru, me = runs[k], mets.get(k, {})
        f = lambda dd, c: float(dd.get(c, "nan") or "nan")
        valid = (f(ru, "other_errors") == 0 and r["ate_system_restarts"] == "0"
                 and (r["stream_ok"] == "yes" or r["atespace"] in verified))
        res = f(ru, "resumes")
        cm = f(me, "crashes_metric")
        out.append(dict(pattern=r["pattern"], policy=r["policy"], seed=r["seed"], valid=valid,
                        atespace=r["atespace"],
                        unplaced=res - f(ru, "placed"), rpr=int(r["filled_up"]) / res,
                        det=float(r["detaches"]), crash=max(float(r["crashes_log"]), 0 if math.isnan(cm) else cm),
                        bind=f(me, "bind_p99_ms"), p50=f(ru, "ttp_p50_ms"), p99=f(ru, "ttp_p99_ms"),
                        rest=f(me, "restore_p99_ms"), e480=f(ru, "empty_drop_480"),
                        psi=f(me, "node_psi_cpu_max_pct")))
    return out


def ms(v):
    return f"{v/1000:.1f}s" if v >= 10000 else f"{v:.0f}"


def cell(vals, kind):
    if not vals:
        return "not measured"
    m = statistics.median(vals)
    if kind == "rpr":
        return f"{m:.4f} ({', '.join(f'{v:.4f}' for v in vals)})"
    if kind == "ms":
        return f"{ms(m)}ms ({', '.join(ms(v) for v in vals)})"
    return f"{m:g} ({', '.join(f'{v:g}' for v in vals)})"


cols = ["Topology", "Algorithm", "Placed", "Retries per resume", "Bind p99", "ResumeActor p50",
        "ResumeActor p99", "Restore p99", "Crashes", "Detaches", "Empty +8 min"]

for pattern in ["ramp-down", "burst"]:
    table = []
    star_rows = {}
    for topo, d, label in TOPOS:
        rows = [r for r in rows_for(d) if r["pattern"] == pattern and r["valid"]]
        by = defaultdict(list)
        for r in rows:
            by[r["policy"]].append(r)
        med = {p: {k: statistics.median([r[k] for r in by[p]]) for k in ("p99", "crash", "e480")}
               for p, _ in POLICIES if by[p]}
        base = med.get("p2c")
        qual = []
        for p, _ in POLICIES:
            if p not in med or base is None:
                continue
            ok = (all(r["unplaced"] == 0 for r in by[p]) and med[p]["crash"] <= base["crash"]
                  and med[p]["p99"] <= 1.5 * base["p99"])
            if ok:
                qual.append(p)
        if qual:
            if pattern == "ramp-down":
                star = max(qual, key=lambda p: (med[p]["e480"], -med[p]["p99"]))
            else:
                star = min(qual, key=lambda p: med[p]["p99"])
            star_rows[topo] = star
        for i, (p, name) in enumerate(POLICIES):
            rs = by[p]
            unpl = sum(r["unplaced"] for r in rs)
            mark = "★ " if star_rows.get(topo) == p else "  "
            table.append([f"{topo} ({label})" if i == 0 else "", mark + name,
                          "all" if unpl == 0 else f"{unpl:g} unplaced",
                          cell([r["rpr"] for r in rs], "rpr"),
                          cell([r["bind"] for r in rs if not math.isnan(r["bind"])], "ms"),
                          cell([r["p50"] for r in rs], "ms"), cell([r["p99"] for r in rs], "ms"),
                          cell([r["rest"] for r in rs if not math.isnan(r["rest"])], "ms"),
                          cell([r["crash"] for r in rs], "n"), cell([r["det"] for r in rs], "n"),
                          cell([r["e480"] for r in rs], "n") if pattern == "ramp-down" else "-"])
        table.append(None)
    widths = [max(len(c), *(len(r[i]) for r in table if r)) for i, c in enumerate(cols)]
    sep = "─┼─".join("─" * w for w in widths)
    print(f"## {pattern}\n\n```")
    print(" │ ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print(sep)
    for r in table[:-1]:
        print(sep if r is None else " │ ".join(c.ljust(w) for c, w in zip(r, widths)))
    print("```\n")
