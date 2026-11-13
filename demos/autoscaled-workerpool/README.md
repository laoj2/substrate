---
title: "Autoscaled WorkerPool"
linkTitle: "Autoscaled WorkerPool"
weight: 8
estimated_time: "15 mins"
card_icon: "fa-solid fa-up-right-and-down-left-from-center"
description: >
  Scale a WorkerPool with a HorizontalPodAutoscaler on its utilization and on the demand of actors that wait for room.
---

# Autoscaled WorkerPool Demo

A HorizontalPodAutoscaler (HPA) scales a `WorkerPool` on two metrics of each
pool:

| Metric | Value | HPA target |
|---|---|---|
| `ate_workerpool_utilization` | The booked fraction of the pool. prometheus-adapter computes it from the `ate.worker.*.capacity` and `ate.worker.*.allocated` series of `ate-api-server`. | `Value: 650m` |
| `ate.workerpool.demand` | The workers that the booked actors and the actors waiting for room fill. A gauge of `ate-api-server`. It passes the size of the pool when resumes find no room. | `AverageValue: 1` |

Both take the fullest of the three resources:

```
used(r)     = booked(r) / capacity(r)
needed(r)   = (full(r) + waiting(r)) / capacity(r)
full(r)     = max(booked(r), capacity(r))

utilization = max(used(actors), used(cpu), used(memory))
demand      = workers * utilization                                      while no actor waits
demand      = workers * max(needed(actors), needed(cpu), needed(memory))  while actors wait
```

`booked` is what the actors on every worker of the pool book from their
limits, also on a draining worker. `capacity` and `workers` count only the
workers the scheduler can place on. `waiting` is what the actors that wait
for room would book. A waiting actor found room on no worker, so demand
counts the pool as full, and the waiting actors fill new workers. `full` is
the capacity, or the bookings when a draining worker pushes them above it. A
resource with no capacity reported is skipped. While a worker drains,
utilization can pass 1.

The HPA computes a replica count from each metric and takes the larger:

```
utilization: ceil(ready workers * utilization / 0.65)
demand:      ceil(demand)
```

Utilization keeps 35% of the pool free. Demand asks for the workers the
actors fill with no headroom, so it wins only while actors wait.

Demand alone never scales the pool down. With no actor waiting, it asks for
`ceil(workers * utilization)`, often fewer workers than the pool has. The HPA
takes the larger count, and utilization always asks for at least as many, so
utilization decides each scale-down. While actors wait, demand passes the
size of the pool and blocks a scale-down. With demand as the only metric, the
pool would shrink to what the bookings fill, and the next resume would wait.

The HPA skips a change while a metric is within a tolerance of its target,
10% by default. A waiting actor raises demand by only its share of a worker,
so a larger pool hides it. A large actor of 0.9 of a worker waiting on 10
workers reads `10.9 / 10`, within 10%, and the pool would not grow. The demo
sets the scale-up tolerance to 0, so any waiting actor adds a worker. Demand
passes the size of the pool only while actors wait, so this adds no noise.
Scale-down keeps the default.

This demo runs on kind only. It reads the metrics from the kind install's
Prometheus.

## Scaling signals

The CPU and memory use of the worker pods, the usual HPA signal, is a poor
signal for a `WorkerPool`. An actor books its limits on a worker
for as long as it runs, and actors are idle most of the time. A pool of idle
actors uses little CPU and still has no room for one more, so CPU use reads
low while resumes fail.

Both metrics read bookings instead. The scheduler places actors by the same
numbers.

| | `ate_workerpool_utilization` | `ate.workerpool.demand` |
|---|---|---|
| **Best for** | Headroom before the pool fills. It is a ratio, so one threshold fits pools of any size. | Sizing a scale-up once resumes fail, also for an actor that no single worker has room for. |
| **Before the pool fills** | Leads. A target of 0.65 adds workers before a resume fails. | Asks for fewer workers than utilization, since it keeps no headroom. |
| **Once the pool fills** | Reads the same whether 1 actor or 100 wait, so each step grows the pool by about 1/0.65. | Counts the pool as full and adds the waiting actors, so one step covers them. |
| **Lag** | None beyond the scrape and the HPA sync. | An actor counts as waiting only after a resume fails with `ResourceExhausted`, so the first request to each waiting actor fails or waits. |
| **Accuracy** | Every `ate-api-server` replica reports the same per-worker series from its worker cache, and the query takes one copy of each. | Can read low. Each replica counts only the actors that waited on it, and the adapter takes the largest reading. An actor drops out 10 seconds after its last failed resume, so a client has to keep trying. It can read high for up to 10 seconds when another replica places an actor that waited. |
| **Blind spots** | How much capacity a full pool lacks. | An actor that can run in more than one pool counts in each of them. An HPA on each pool adds workers for the same actor, so the pools together grow more than it needs. |

Limits:

- Utilization sums bookings and capacity across the pool. The scheduler
  spreads actors over the workers, so the free room is split among them. An
  actor can be larger than the room left on any one worker while utilization
  stays low. Demand counts the pool as full while that actor waits, so the
  HPA adds a worker. Step 4 under [How to Use](#how-to-use) shows it. The
  `ate_worker_*` series show the room on each worker.
- Both read what actors book, from their limits. Limits far above what the
  actors use keep the pool larger than its load needs.
- A pool with no worker that the scheduler can place on reports no value, so
  neither metric can scale a pool up from zero.
- The HPA cannot choose which worker to remove, even though
  `ate.worker.actors.allocated` shows which workers hold actors. See the
  warning under [Scale down](#5-scale-down).
- The HPA reads each value late. Prometheus scrapes every 15 seconds, and the
  HPA syncs every 15 seconds.

The [metrics table](../../docs/guides/observability.md#2-metrics) describes
the instruments.

## Architecture

```
ate-api-server :9090/metrics     ate_worker_*, ate_workerpool_demand
        │  scrape, every 15s
        ▼
Prometheus                       manifests/ate-install/kind/prometheus.yaml
        │  PromQL
        ▼
prometheus-adapter               demos/autoscaled-workerpool/prometheus-adapter.yaml
        │  external.metrics.k8s.io
        ▼
HorizontalPodAutoscaler          demos/autoscaled-workerpool/hpa-kind.yaml
        │  /scale, writes WorkerPool spec.replicas
        ▼
atecontroller  ──►  Deployment  ──►  worker pods
```

## Prerequisites

- A kind cluster with Agent Substrate installed as in the
  [Quickstart](../../docs/get-started/quickstart.md), with
  `export ATE_INSTALL_KIND=true` set in your shell.
- No other external metrics adapter in the cluster. A cluster has one
  `v1beta1.external.metrics.k8s.io` APIService, and the demo's
  prometheus-adapter takes it.
- Room for 6 workers on the kind node. Each requests 250m of CPU and 1280Mi
  of memory.

## How to Run on Agent Substrate

### 1. Build and Deploy

```bash
go run ./cmd/ate-setup deploy demo autoscaled-workerpool
```

This command will:

- Create the `ate-demo-autoscaled-workerpool` namespace and the `counter`
  `WorkerPool` with 2 workers. A worker holds 4 actors of the demo's template.
- Create the `ate-demo-autoscaled-workerpool` atespace and two actor
  templates in it, and wait for their golden snapshots. `counter` books 250m
  of CPU, a quarter of a worker. `counter-large` books 900m.
- Deploy `prometheus-adapter` into the namespace and the `counter` HPA, with 2
  to 6 replicas.

### 2. Check the external metrics

```bash
kubectl get apiservice v1beta1.external.metrics.k8s.io   # Available=True

selector='labelSelector=ate_workerpool_namespace%3Date-demo-autoscaled-workerpool,ate_workerpool_name%3Dcounter'
for metric in ate_workerpool_utilization ate_workerpool_demand; do
  kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1/namespaces/ate-demo-autoscaled-workerpool/${metric}?${selector}"
done
```

Both report 0 while the pool holds no actors.

## How to Use

### 1. Create more actors than the pool holds

```bash
# Install the CLI as a kubectl plugin if not already installed
go install ./cmd/kubectl-ate

# 16 actors for a pool that holds 8.
for i in $(seq -w 1 16); do
  kubectl ate create actor "c$i" -a ate-demo-autoscaled-workerpool --template counter
done
```

### 2. Resume them and keep trying

The first 8 resumes fill the pool, and the other 8 fail with
`ResourceExhausted`. An actor counts as waiting for 10 seconds after such a
failure, so the loop keeps trying:

```bash
while true; do
  for i in $(seq -w 1 16); do
    kubectl ate resume actor "c$i" -a ate-demo-autoscaled-workerpool >/dev/null 2>&1 &
  done
  wait
  sleep 2
done
```

### 3. Watch the HPA

In a second terminal:

```bash
kubectl -n ate-demo-autoscaled-workerpool get hpa counter -w
kubectl -n ate-demo-autoscaled-workerpool get workerpool counter -w
```

With 8 actors booked and 8 waiting on 2 workers:

| Metric | Value | Replicas the HPA computes |
|---|---|---|
| utilization | 1 | `ceil(2 * 1 / 0.65)` = 4 |
| demand | `(8 + 8) / 8 * 2` = 4 | `ceil(4)` = 4 |

The HPA takes 4, which holds all 16 actors. The pool is then full again, so
utilization asks for `ceil(4 * 1 / 0.65)` = 7, and the HPA stops at its
maximum of 6. Here the two metrics agree. With more actors waiting, demand
asks for more workers, while utilization still asks for 4.

The demand the HPA reads can be below 4 and arrives late. See
[Scaling signals](#scaling-signals).

Stop the loop once all 16 actors run.

### 4. Place a large actor in a pool with room

Suspend the actors and wait for the pool to shrink back to 2 workers, as in
[Scale down](#5-scale-down). Then resume 2 small actors. The scheduler puts
the next actor on the less loaded worker, so each worker gets one:

```bash
for i in 01 02; do
  kubectl ate resume actor "c$i" -a ate-demo-autoscaled-workerpool
done
```

Each worker now has 750m of CPU free, and the pool has 1500m. Create a
large actor and keep trying to resume it:

```bash
kubectl ate create actor big -a ate-demo-autoscaled-workerpool --template counter-large
until kubectl ate resume actor big -a ate-demo-autoscaled-workerpool >/dev/null 2>&1; do
  sleep 2
done
```

It needs 900m, so it fits on no worker:

| Metric | Value | Replicas the HPA computes |
|---|---|---|
| utilization | `500m / 2000m` = 0.25 | `ceil(2 * 0.25 / 0.65)` = 1, so the minimum of 2 |
| demand | `(2000m + 900m) / 2000m * 2` = 2.9 | `ceil(2.9)` = 3 |

The HPA adds a third worker, and the loop ends once the large actor runs on
it.

### 5. Scale down

Suspend the actors, and `big` if you created it in step 4:

```bash
for actor in $(seq -f 'c%02g' 1 16) big; do
  kubectl ate suspend actor "$actor" -a ate-demo-autoscaled-workerpool
done
```

Both metrics drop to 0. After the 300 second stabilization window the HPA
removes one worker a minute, down to 2.

> **Warning:** Scale-down is safe here only because no actor runs. The HPA
> lowers `spec.replicas`, and the Deployment picks the worker pods to delete.
> It can pick a worker that runs actors, and those actors crash.

## How to Uninstall

```bash
go run ./cmd/ate-setup delete demo autoscaled-workerpool
```
