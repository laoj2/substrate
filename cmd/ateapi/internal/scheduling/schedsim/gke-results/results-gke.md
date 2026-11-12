# Scheduling policy comparison on GKE: real gVisor data plane (Phase B)

Driven with `schedsim -kind` against cluster `sched-exp` (project `gke-luizaoj-test-dev`, `us-west1-c`, GKE `1.37.2-gke.1042000`). The cluster runs real atelet and gVisor workers (gVisor `release-20261102.0`). It was installed with `ate-setup` from `laoj2/substrate` branch `scheduler` at `ef59cb7a7`, plus the driver patch in [driver.patch](driver.patch). The patch changes only `schedsim` and the scripts, not `ate-api-server`. Phase A, the fake data plane at 1000 workers, was not run: the user scoped this session to Phase B. Each cell is the median of its valid runs, followed by every valid run in parentheses. Cells have 2 or 3 runs.

## Called out first: more than 2× off the predictions in section 4

- **Empty workers: prefixsat frees fewer than bestofd-3, not more.**
  - Simulator at 200 workers: prefixsat 109 (55% of the fleet), bestofd-3 63 (32%).
  - GKE: prefixsat **5 of 24 (21%)** at about 90% peak and **7 (29%)** at about 70%.
  - GKE: bestofd-3 **14 (58%)** and **15 (63%)** at the same peaks.
- **Detaches rank the opposite way from the pinning simulation.**
  - Simulator: p2c 22 and prefixsat 4,266 per 148,000 resumes, about 0.0001 and 0.029 per resume.
  - GKE, about 90% peak: p2c **68 per ~1,800 resumes (0.038)** and prefixsat **30 (0.017)**.
  - At 70% peak all policies detach rarely: 0 to 18 per run.
- **A detach costs about 10× the simulator's guess.**
  - Simulator: 50 ms.
  - Measured ResumeActor latency of resumes that detached: **p50 514 ms, p90 788 ms** (707 detached resumes at about 90% peak). At 70% peak: p50 577 ms, p90 789 ms (52).
  - The joins are from `resumes.csv` and the `events_*.log` lines.
- **bestofd-3 retries are lower than predicted.**
  - Predicted: 0.018 (200-worker simulator) and 0.058 (GKE-like timing).
  - Measured: **0.0040** on ramp-down and **0.0087** on burst at about 90% peak.

## Deviations from the plan and incidents

- **The plan's actor template could not run on a real data plane.**
  - `glutton` requires `--data-dir`, which the plan's `sched-250m` template omits. The actor exited immediately, and the golden snapshot failed with `runsc checkpoint ... no checkpointable filesystems`.
  - `gke-setup.sh` now passes `--data-dir=/var/lib/glutton` and a `/readyz` wakeup probe, as `benchmarking/workloads/manifests/glutton-template.yaml.tmpl` does.
- **Worker memory.** Real workers report the pod's memory minus 64Mi. Pools use 4160Mi, so each worker reports 4Gi, and 8 actors of 250m and 512Mi fill it by CPU.
- **ate-api-server rollout.** With 3 replicas of 2 CPU, each on a 4-CPU system node, the default `maxSurge: 1` could not schedule its surge pod. The Deployment uses `maxSurge: 0, maxUnavailable: 1`.
- **Exchange stall after every policy rollout.** Details: [logs/exchange-stall.md](logs/exchange-stall.md).
  - objectstore-plugin's exchange client keeps a channel to the headless `svc/api`. After an `ate-api-server` rollout that channel still holds the old pods' IPs.
  - The first snapshot transfer on each node then waits out grpc-go's 20 s `minConnectTimeout`.
  - In the g-gke8 runs, excluding run 16, it caused 16 detached resumes of 10 s or more, all within minutes of a rollout.
  - It made run 16 (burst, bestofd-3, seed 3) invalid: 3 DeadlineExceeded and p99 20.8 s. Seed 4 (run 18) replaced it.
  - From the 70% set on, `gke-run-cell.sh` restarts the atelet DaemonSet (which carries the plugin) after each rollout. The 70% set had no resume above 5 s, and one above 1 s.
  - This is a data-plane bug, independent of the scheduling policy.
- **Wrong-cluster incident.**
  - During run 10 the default kubectl context switched to another cluster in the project (`autoscale-exp`). Run 10's setup created a `benchmark-workloads` namespace and a pool there before failing.
  - The driver never ran there, so no data is affected. The namespace is still on `autoscale-exp`, and another client manages the pool now.
  - `phase-b.sh` now pins every call to a dedicated kubeconfig. Run 13 replaced run 10.
- **Log-stream gap.** Run c70-05's log stream ended early (`stream_ok=no`). Its counts were re-read from Cloud Logging over the same window and match: 5 retries, 7 detaches, 0 crashes. On c70-04 Cloud Logging and the stream agreed (0 / 2 / 0). This confirms the plan's open question: Cloud Logging works for counting these lines. Recorded in `phase-b-70/stream_verified.csv`.
- **Reruns under the 30% rule.** Cells whose first two seeds disagreed by more than 30% on retries or empty workers got a third seed:
  - g-gke8: ramp-down p2c, ramp-down prefixsat, burst prefixsat.
  - g-gke8-70: ramp-down for all three policies, plus burst prefixsat.
  - Burst bestofd-3 at g-gke8 also has 3 runs, because run 18 replaced invalid run 16.
- **Peak fill.** The plan's sizing rule aims ramp-down's peak at about 89% of capacity. Measured:
  - g-gke8: mean 0.82 over minutes 2 to 5, max 0.84 to 1.00. One prefixsat run reached 192 of 192.
  - So the user asked for a second topology peaking near 70%: `g-gke8-70`, mean 0.63, max 0.68 to 0.81.

## Columns

- **Placed**: resumes that returned within the driver's 30 s client timeout. `all` means every resume in every run.
- **Retries per resume**: `schedexp assignment retry` lines with `reason=filled_up`, divided by resumes. Counted from the live log stream of every ate-api-server replica.
- **Bind p99**: p99 of `ate.scheduler.assignment.duration` with `outcome=assigned`. That's one attempt to assign a worker, scraped from ate-api-server over the run's window.
- **ResumeActor p50 / p99**: the driver's time from the first `ResumeActor` call to its return, retries included. On a real data plane this includes the gVisor restore and the wakeup probe.
- **Restore p99**: p99 of `ate.actor.restore.duration{phase="total"}` from atelet.
- **Crashes**: `ate.actor.crashes`, and `Actor crashed` log lines. Both were 0 everywhere.
- **Detaches**: `Detached actor from its full node to resume it on another node` lines. Each is a resume whose snapshot node was full, so it uploaded the snapshot and restored on another node.
- **Empty +8 min**: workers with no actor 8 minutes after the ramp-down drop.

## Topologies

| Scenario | Workers | Slots per worker | Worker CPU / memory | Actor | Nodes × workers per node | Rate scale | Population |
|---|---|---|---|---|---|---|---|
| g-gke8 (~90% peak) | 24 | 1000 reported, 8 by CPU | 2 / 4160Mi (4Gi reported) | 250m / 512Mi | 8 × 3 (`c3-standard-8`) | 0.30 | 240 |
| g-gke8-70 (~70% peak) | 24 | same | same | same | same | 0.23 | 240 |

## Setup

- **ate-api-server:** 3 replicas with a fixed 2 CPU and 4Gi each. One per `ate-system-infra` node (`e2-standard-4`).
- **Postgres:** bundled, on its own `e2-standard-4` node, requesting 2 CPU with a 16 CPU limit. Probes at `timeoutSeconds: 5` and `failureThreshold: 6`. Pool max connections: 20.
- **Data plane:** real atelet with default snapshot detach (30 m) and upload (5 m) delays, and gVisor workers.
- **Monitoring:** Prometheus from `benchmarking/monitoring.yaml`, plus a scrape job for `ate-system`, all on the system nodes.
- **Measured bind time:** p50 8.5 ms, p99 24.6 ms, for every policy and topology.
- **Clean starts:** each run gets a fresh WorkerPool and a fresh 240-actor atespace. Actors start suspended from the golden snapshot.

## Load patterns

Actor resumes per second at each scenario's rate. Each resumed actor holds 20 to 40 s, then suspends. In ramp-down, 3% of actors hold 5 to 10 minutes.

| Pattern | Steps | g-gke8 (×0.30) | g-gke8-70 (×0.23) |
|---|---|---|---|
| ramp-down | 5 min high, 8 min low, 3 min zero | 4.5/s, then 0.9/s, then 0 | 3.45/s, then 0.69/s, then 0 |
| burst | 2 min low, 3 s spike, 3 min low | 1.5/s, then 45/s, then 1.5/s | 1.15/s, then 34.5/s, then 1.15/s |

## Policies

- **p2c**: the less loaded of 2 random workers. This is the current policy.
- **bestofd-3**: the fullest of 3 random workers.
- **prefixsat**: `prefixsat:1:0.2`, the shortest oldest-first prefix with at least 1 worker free and 20% headroom, then p2c inside it.

## Star rule

★ marks the best row per topology.
- A row qualifies when every resume is placed, no run had errors or restarts, it has no more crashes than p2c, and its ResumeActor p99 is at most 1.5× p2c's (the Phase B gate in section 7). Medians are compared.
- Among qualifying rows, ramp-down picks the most empty workers at +8 min. Burst has no drop, so it picks the lowest ResumeActor p99.

## ramp-down

```
Topology              │ Algorithm   │ Placed │ Retries per resume              │ Bind p99          │ ResumeActor p50       │ ResumeActor p99        │ Restore p99           │ Crashes     │ Detaches         │ Empty +8 min
──────────────────────┼─────────────┼────────┼─────────────────────────────────┼───────────────────┼───────────────────────┼────────────────────────┼───────────────────────┼─────────────┼──────────────────┼────────────────
g-gke8 (~90% peak)    │   p2c       │ all    │ 0.0006 (0.0006, 0.0039, 0.0006) │ 25ms (25, 25, 25) │ 162ms (162, 163, 162) │ 527ms (527, 554, 248)  │ 233ms (236, 233, 176) │ 0 (0, 0, 0) │ 68 (124, 68, 17) │ 1 (1, 2, 1)
                      │ ★ bestofd-3 │ all    │ 0.0040 (0.0040, 0.0040)         │ 25ms (25, 25)     │ 162ms (162, 162)      │ 622ms (706, 537)       │ 219ms (243, 196)      │ 0 (0, 0)    │ 49 (77, 21)      │ 14 (15, 13)
                      │   prefixsat │ all    │ 0.0066 (0.0066, 0.0023, 0.0102) │ 25ms (25, 25, 24) │ 162ms (162, 162, 162) │ 552ms (513, 552, 1548) │ 256ms (209, 256, 362) │ 0 (0, 0, 0) │ 30 (29, 30, 251) │ 5 (5, 8, 2)
──────────────────────┼─────────────┼────────┼─────────────────────────────────┼───────────────────┼───────────────────────┼────────────────────────┼───────────────────────┼─────────────┼──────────────────┼────────────────
g-gke8-70 (~70% peak) │   p2c       │ all    │ 0.0000 (0.0000, 0.0000, 0.0000) │ 25ms (25, 25, 25) │ 160ms (159, 160, 162) │ 201ms (295, 196, 201)  │ 100ms (100, 100, 100) │ 0 (0, 0, 0) │ 0 (0, 2, 0)      │ 5 (6, 3, 5)
                      │ ★ bestofd-3 │ all    │ 0.0037 (0.0037, 0.0066, 0.0030) │ 25ms (25, 25, 25) │ 162ms (162, 163, 161) │ 224ms (224, 517, 208)  │ 121ms (100, 195, 121) │ 0 (0, 0, 0) │ 7 (7, 18, 6)     │ 15 (13, 15, 15)
                      │   prefixsat │ all    │ 0.0007 (0.0000, 0.0044, 0.0007) │ 25ms (25, 24, 25) │ 162ms (164, 162, 162) │ 205ms (200, 208, 205)  │ 100ms (100, 100, 115) │ 0 (0, 0, 0) │ 3 (2, 5, 3)      │ 7 (8, 6, 7)
```

- Run 10 (g-gke8 prefixsat, seed 2) never ran because of the wrong-cluster incident, and run 13 replaced it.
- The third seeds in this table come from the 30% rule above.
- prefixsat's 1,548 ms p99 run (seed 3) is the one whose fleet hit 192 of 192 slots, with 251 detaches.
- Resumes of 10 s or more, all from the exchange stall: bestofd-3 seed 1 had 4 and prefixsat seed 2 had 6. All other runs in this table had none.
- No node was empty at +8 min in any g-gke8 run. In the 70% set, one p2c run (seed 3) had 1 empty node.

## burst

```
Topology              │ Algorithm   │ Placed │ Retries per resume              │ Bind p99          │ ResumeActor p50       │ ResumeActor p99       │ Restore p99           │ Crashes     │ Detaches        │ Empty +8 min
──────────────────────┼─────────────┼────────┼─────────────────────────────────┼───────────────────┼───────────────────────┼───────────────────────┼───────────────────────┼─────────────┼─────────────────┼─────────────
g-gke8 (~90% peak)    │ ★ p2c       │ all    │ 0.0000 (0.0000, 0.0000)         │ 24ms (24, 24)     │ 160ms (159, 161)      │ 208ms (192, 224)      │ 131ms (100, 162)      │ 0 (0, 0)    │ 0 (0, 0)        │ -
                      │   bestofd-3 │ all    │ 0.0087 (0.0122, 0.0087, 0.0069) │ 25ms (25, 24, 26) │ 163ms (165, 160, 163) │ 742ms (742, 772, 572) │ 264ms (340, 264, 243) │ 0 (0, 0, 0) │ 14 (14, 11, 18) │ -
                      │   prefixsat │ all    │ 0.0110 (0.0121, 0.0052, 0.0110) │ 25ms (24, 25, 25) │ 162ms (160, 164, 162) │ 560ms (767, 524, 560) │ 237ms (247, 234, 237) │ 0 (0, 0, 0) │ 16 (14, 17, 16) │ -
──────────────────────┼─────────────┼────────┼─────────────────────────────────┼───────────────────┼───────────────────────┼───────────────────────┼───────────────────────┼─────────────┼─────────────────┼─────────────
g-gke8-70 (~70% peak) │ ★ p2c       │ all    │ 0.0000 (0.0000, 0.0000)         │ 25ms (24, 25)     │ 157ms (157, 158)      │ 228ms (235, 220)      │ 162ms (182, 142)      │ 0 (0, 0)    │ 0 (0, 0)        │ -
                      │   bestofd-3 │ all    │ 0.0066 (0.0065, 0.0066)         │ 25ms (25, 25)     │ 161ms (161, 161)      │ 279ms (251, 306)      │ 204ms (194, 214)      │ 0 (0, 0)    │ 3 (3, 3)        │ -
                      │   prefixsat │ all    │ 0.0000 (0.0000, 0.0023, 0.0000) │ 25ms (25, 25, 24) │ 160ms (160, 161, 160) │ 247ms (224, 247, 254) │ 216ms (151, 216, 231) │ 0 (0, 0, 0) │ 1 (0, 1, 2)     │ -
```

- g-gke8 bestofd-3: run 16 (seed 3) is excluded as invalid, and run 18 (seed 4) is its third value.

## Takeaways

- **No policy crashed an actor**, on any pattern or topology, at either fill level. Peak node CPU PSI stayed at or below 1.2%. Because this driver's actors sit idle, hot workers never made actors compete for CPU here.
- **Near full (about 90% peak), packing fails the latency gate on burst.**
  - Burst ResumeActor p99: bestofd-3 742 ms and prefixsat 560 ms, against p2c's 208 ms, which is 3.6× and 2.7×.
  - The slow resumes are detaches. Each costs about 0.5 s p50, because a pinned node is full and the snapshot must move.
  - On ramp-down all three policies are within the gate.
- **With headroom (about 70% peak), packing costs nothing on latency.**
  - Burst p99: bestofd-3 279 ms and prefixsat 247 ms, against p2c's 228 ms.
  - Ramp-down p99: bestofd-3 224 ms, prefixsat 205 ms, p2c 201 ms.
  - Detaches drop to 0 to 18 per run.
- **bestofd-3 frees the most workers in every topology, and prefixsat doesn't beat it.**
  - Empty at +8 min: bestofd-3 14 of 24 and 15 of 24. prefixsat 5 and 7. p2c 1 and 5.
  - prefixsat misses the proposed pass rule (at least 1.5× bestofd-3's empty workers) by a wide margin, the reverse of the simulator.
  - Likely cause, unverified: suspended actors stay pinned to their node for 30 minutes, so after the drop most resumes choose only among the 3 workers on their pinned node. prefixsat's fleet-wide oldest-first window does not survive that. This is section 10's "node locality fights packing", stronger than the simulator modeled.
- **Data-plane finding unrelated to the policy:** every `ate-api-server` rollout makes the first snapshot transfer on each node stall 20 s ([logs/exchange-stall.md](logs/exchange-stall.md)). Packing policies hit it more because they detach and restore from object storage more often. Possible fixes: a ClusterIP Service, a shorter `MinConnectTimeout` plus keepalive, or re-resolving DNS when the channel leaves idle.

## Files

- `phase-b/`, `phase-b-70/`: `runs.csv`, `retries.csv`, `series.csv`, `nodes.csv` (actors per node every 5 s), `resumes.csv` (one row per resume), `metrics.csv` (Prometheus per run), `events_*.log` (streamed retry, detach and crash lines), `order.txt`, plus the driver, setup and orchestrator logs.
- `smoke/`: the setup check run.
- `logs/`: bootstrap, deploy and setup logs, and the exchange-stall investigation.
- `tools/`: `phase-b.sh` (orchestrator), `collect_metrics.py`, `summarize.py`, `report_tables.py` (generates the tables above), `monitoring-gke.yaml`.
- `driver.patch`: the scenarios added to `main.go` (`g-gke8`, `g-gke8-70`), the per-node and per-resume output in `kind.go`, `gke-setup.sh`, `gke-run-cell.sh`, and the tools.
- Cluster shape: `sched-exp` in `us-west1-c` with `substrate-node-pool` (8 × `c3-standard-8`), `ate-system-infra` (3 × `e2-standard-4`) and `ate-postgres` (1 × `e2-standard-4`). It was left running.
