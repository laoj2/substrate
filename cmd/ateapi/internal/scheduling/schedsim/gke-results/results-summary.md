# Scheduler experiment on GKE: results summary

Full report: [results-gke.md](results-gke.md). Raw data: `phase-b/` (about 90% peak) and `phase-b-70/` (about 70% peak).

**Setup:**
- Real gVisor data plane on GKE (`sched-exp`, `us-west1-c`): 8 nodes with 3 workers each, 24 workers of 8 actor slots, 192 slots in total.
- Compared p2c (the current policy), bestofd-3 and prefixsat (`prefixsat:1:0.2`).
- Two patterns, ramp-down and burst, at two load levels: peaks near 90% and near 70% of slots.
- 2 or 3 valid runs per cell, with a fresh pool and actor population for each run.
- Phase A (fake data plane, 1000 workers) was not run.

## Verdict

prefixsat does not meet the bar as it stands. bestofd-3 with spare capacity frees the most workers at no latency cost. p2c is safest when the fleet runs near full.

| | p2c | bestofd-3 | prefixsat |
|---|---|---|---|
| Actor crashes (all runs) | 0 | 0 | 0 |
| ~90% peak, burst ResumeActor p99 | **208 ms** ★ | 742 ms (fails gate) | 560 ms (fails gate) |
| ~90% peak, empty workers at +8 min | 1 / 24 | **14 / 24** ★ | 5 / 24 |
| ~70% peak, burst ResumeActor p99 | **228 ms** ★ | 279 ms | 247 ms |
| ~70% peak, ramp-down ResumeActor p99 | 201 ms | 224 ms | 205 ms |
| ~70% peak, empty workers at +8 min | 5 / 24 | **15 / 24** ★ | 7 / 24 |
| Bind p99 (all cases) | ~25 ms | ~25 ms | ~25 ms |

★ marks the winner under the plan's star rule. The gate is ResumeActor p99 at most 1.5× p2c's, and no more crashes than p2c.

## Findings

- **Packing only hurts latency near full.** At about 90% fill, a pinned actor's node is often full, so the resume detaches: it uploads the snapshot and restores on another node.
  - A detached resume took 514 ms at p50 and 788 ms at p90, against the simulator's guess of 50 ms.
  - At 70% fill, detaches nearly disappear and the three policies have about the same latency.
- **bestofd-3 frees more workers than prefixsat, the reverse of the simulator.** The cause is node pinning:
  - A suspended actor is pinned to its node, usually for 30 minutes. `schedulingConstraints` makes the pin a hard constraint (`RequiredNodes`), so the policy only sees the workers on that node.
  - prefixsat then builds its prefix from those 3 workers and picks the less loaded of two. In effect it runs p2c inside the node, which spreads actors.
  - bestofd-3 samples all 3 and takes the fullest. In effect it bin-packs inside the node, which empties the node's other workers.
  - After the drop most resumes are pinned, so the per-node behavior decides the outcome.
  - This comes from reading the code. The numbers fit, but per-node choices were not instrumented.
- **No node ever emptied completely,** apart from one empty node in one 70% p2c run. That matches the simulator.
- **The fuller the fleet, the more detaches.** In one prefixsat run the fleet hit 192 of 192 slots, giving 251 detaches and a ResumeActor p99 of 1.5 s.

## Problems found

- **The plan's actor template could not run on a real data plane.** It omits `glutton`'s required `--data-dir`, so the actor exited at once and its golden snapshot failed. Fixed in `gke-setup.sh`.
- **Data-plane bug, not caused by the policies.** After every `ate-api-server` rollout, the first snapshot transfer on each node stalls for 20 s ([logs/exchange-stall.md](logs/exchange-stall.md)).
  - objectstore-plugin's exchange client keeps a gRPC channel to the headless `svc/api` that still holds the old pods' IPs, and it waits out grpc-go's 20 s `minConnectTimeout`.
  - It made one run invalid (run 16, since replaced).
  - The experiment works around it by restarting atelet after each rollout. The product needs a real fix.
- **Wrong-cluster incident.** A kubectl context switch made run 10's setup create a namespace and pool on `autoscale-exp`. No data is affected. The scripts are now pinned to a dedicated kubeconfig.

## Next steps

- Make prefixsat pack inside the node when `RequiredNodes` is set, or order workers by node and then age. Then rerun it on the same cluster.
- Fix the exchange client's stale-endpoint stall.
- Repeat with the agent-session benchmark, which burns CPU in every actor. With idle actors, as here, hot workers never compete for CPU.
