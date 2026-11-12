# Simulator calibration against the GKE runs

These runs check how well the simulator reproduces the GKE results when it is configured like the cluster: scenarios `g-gke8` and `g-gke8-70`, 10 seeds each.

- `a-nopin`: simulator defaults, with no node pinning.
- `b-wpn3`: `-workers-per-node 3`, as on the cluster. Default timing.
- `c-wpn3-gketiming`: `-workers-per-node 3 -detach-cost 514ms -replicas 3 -bind-base 7ms -bind-row 1500us -poll 200ms`.

Empty workers at +8 min on ramp-down, out of 24:

| Topology | Policy | GKE median | a-nopin | b-wpn3 | c-wpn3-gketiming |
|---|---|---|---|---|---|
| g-gke8 | p2c | 1 | 3 | 3 | 2 |
| | bestofd-3 | 14 | 10 | 13 | 14 |
| | prefixsat | 5 | **15** | 6 | 6 |
| g-gke8-70 | p2c | 5 | 3 | 3 | 5 |
| | bestofd-3 | 15 | 12 | 14 | 15 |
| | prefixsat | 7 | **17** | 11 | 9 |

Detaches per run:

| Cell | GKE median | b-wpn3 | c-wpn3-gketiming |
|---|---|---|---|
| g-gke8 ramp-down: p2c / bestofd-3 / prefixsat | 68 / 49 / 30 | 60 / 99 / 157 | 55 / 150 / 153 |
| g-gke8 burst | 0 / 14 / 16 | 5 / 16 / 72 | 1 / 22 / 61 |
| g-gke8-70 ramp-down | 0 / 7 / 3 | 1 / 19 / 70 | 0 / 26 / 76 |

Bind retries per resume:

| Cell | GKE median | a-nopin | b-wpn3 | c-wpn3-gketiming |
|---|---|---|---|---|
| g-gke8 ramp-down: p2c / bestofd-3 / prefixsat | .0006 / .0040 / .0066 | .001 / .012 / .003 | .001 / .004 / .003 | .004 / .033 / .025 |
| g-gke8 burst: bestofd-3 / prefixsat | .0087 / .0110 | .010 / .004 | .007 / .009 | .050 / .033 |

Raw output: `*/summary.csv` and `*/runs.csv` in this directory.

## d-new-defaults: the simulator after calibration

The simulator now charges each resume `-resume-overhead` plus a restore drawn by source:
- a pinned actor restores from its local snapshot;
- any other actor restores from its external snapshot: its first resume, an expired pin, or after a detach;
- a detach from a full node first spends `-detach-cost` uploading the local snapshot.

The defaults come from the GKE runs:
- `-workers-per-node 3`;
- `-resume-overhead 85ms`;
- `-restore-local 75ms` (p99 100ms);
- `-restore-external 80ms` (p99 250ms);
- `-detach-cost 350ms`, from detached resumes at 514 ms p50, minus the overhead and an external restore.

The restore medians come from Prometheus `ate.actor.restore.duration{phase="total"}`:
- local: p50 75 ms, p99 100 ms;
- external ("latest"): p50 78 ms, p90 100 ms. Its p99 of 378 ms over the whole session includes stall outliers, so the default p99 is 250 ms.

The simulator also reports `resume_p50_ms`, `resume_p99_ms`, `restores_local`, `restores_external` and `peak_fill`. A new opt-in pattern, `ramp-down-long` (a 45 minute low phase), reports `empty_drop_1500` and `empty_drop_2400`, before and after most pins expire. It runs only when `-patterns` names it, and has not been run.

| Cell | GKE ResumeActor p50 / p99 | Simulator resume p50 / p99 | GKE empty +8 | Simulator empty +8 | GKE detaches | Simulator detaches |
|---|---|---|---|---|---|---|
| g-gke8 ramp-down p2c | 162 / 527 | 165 / 563 | 1 | 2 | 68 | 79 |
| g-gke8 ramp-down bestofd-3 | 162 / 622 | 165 / 562 | 14 | 14 | 49 | 107 |
| g-gke8 ramp-down prefixsat | 162 / 552 | 166 / 584 | 5 | 7 | 30 | 135 |
| g-gke8 burst p2c | 160 / 208 | 165 / 335 | - | - | 0 | 4 |
| g-gke8 burst bestofd-3 | 163 / 742 | 166 / 563 | - | - | 14 | 21 |
| g-gke8 burst prefixsat | 162 / 560 | 168 / 609 | - | - | 16 | 68 |
| g-gke8-70 ramp-down p2c | 160 / 201 | 164 / 268 | 5 | 5 | 0 | 1 |
| g-gke8-70 ramp-down bestofd-3 | 162 / 224 | 165 / 520 | 15 | 15 | 7 | 26 |
| g-gke8-70 ramp-down prefixsat | 162 / 205 | 165 / 561 | 7 | 10 | 3 | 75 |
| g-gke8-70 burst p2c | 157 / 228 | 165 / 310 | - | - | 0 | 0 |
| g-gke8-70 burst bestofd-3 | 161 / 279 | 165 / 318 | - | - | 3 | 1 |
| g-gke8-70 burst prefixsat | 160 / 247 | 167 / 574 | - | - | 1 | 32 |

What matches and what doesn't:
- ResumeActor p50, empty workers and peak fill now match GKE.
- p99 matches at about 90% peak.
- The simulator still overpredicts detaches for the packing policies, prefixsat most: 75 against 3 at 70% ramp-down. So it overstates their p99 at 70% peak.
- Pins did not expire within any run, in the simulator or on GKE, so pin lifetime does not explain the gap. It is still open.

## e-prefixpack: prefixsat that packs inside a pinned node

`prefixpack` is prefixsat, except that an actor pinned to a node (`RequiredNodes`) goes to the fullest worker with room there. The run uses the calibrated defaults and 10 seeds. Empty workers at +8 min on ramp-down are the median, with the range across seeds:

| Topology | p2c | bestofd-3 | prefixsat | prefixpack |
|---|---|---|---|---|
| g-gke8 (~90% peak) | 2 [0-5] | 14 [11-15] | 7 [5-11] | **13 [11-14]** |
| g-gke8-70 (~70% peak) | 5 [1-8] | 15 [12-17] | 10 [8-11] | **15 [13-16]** |

- prefixpack closes the empty-worker gap to bestofd-3 but does not beat it.
- Resume p99 and detaches stay at prefixsat's level, which the simulator overpredicts against GKE.
- A cluster run would settle the latency side.
