# Resume stall after ate-api-server rollouts (found during Phase B)

Investigated on cell-16 (burst, bestofd:3, seed 3), 2026-11-12T02:20Z.

Symptom: about 8 resumes took ~21 s and 2 hit the driver's 30 s deadline.

Cause: objectstore-plugin's exchange client
(`internal/objectstoreplugin/exchangeclient/exchangeclient.go:93`, `grpc.NewClient`
with no connect params or keepalive) dials `api.ate-system.svc:8445`. `svc/api` is
headless. After an ate-api-server rollout, its idle channel still holds the old
pods' IPs. The first ExchangeActorJwt on each node dials the dead IPs and waits out
grpc-go's `minConnectTimeout` (20 s, grpc-go `clientconn.go:61`). Only then does it
re-resolve and connect. Every RPC queued behind that connect is released at the
same moment.

Evidence:
- atelet DetachActor took 19.9 to 20.6 s for every detach on node xv7s.
- All of them returned at 02:20:54.48 to 54.54, which is 20.0x s after the node's
  first exchange call at 02:20:33.96.
- The zstd uploads themselves took ~100 ms.
- A cached-credential upload on the same node completed in 208 ms during the stall.

The 3 failed resumes paid the stall twice: once in detach on the source node, then
again in restore manifest_fetch on a fresh target node.

Earlier outliers line up with rollouts:
- cell-03: 22:57 (rollout 22:54)
- cell-05: 23:27 (rollout 23:25)
- cell-11: 00:52 (rollout 00:49)
- cell-13: 01:24 to 01:27 (rollout 01:23)
- cell-16: 02:20 (rollout 02:17)

Not policy logic. Packing policies are exposed more: their detaches and "latest"
restores are snapshot transfers, and each can be the first exchange on its node.
p2c mostly restores from local snapshots.

Experiment mitigation (from cell c70-01 on): `gke-run-cell.sh` restarts the atelet
DaemonSet (which carries the plugin as a native sidecar) after every policy
rollout, before driving load.

Possible product fixes: a ClusterIP Service for the exchange, a short
MinConnectTimeout and keepalive in exchangeclient, or re-resolving when the channel
leaves idle.

Source: subagent conversation 13fd1323-7243-4e6b-a0e6-edc611da3ff5. Raw Cloud
Logging pulls are in that conversation's scratch dir.
