// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/hardware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	workerpoolWorkersMetric     = "ate.workerpool.workers"
	workerCPUCapacityMetric     = "ate.worker.cpu.capacity"
	workerCPUAllocatedMetric    = "ate.worker.cpu.allocated"
	workerMemoryCapacityMetric  = "ate.worker.memory.capacity"
	workerMemoryAllocatedMetric = "ate.worker.memory.allocated"
	workerActorsCapacityMetric  = "ate.worker.actors.capacity"
	workerActorsAllocatedMetric = "ate.worker.actors.allocated"
	workerpoolDemandMetric      = "ate.workerpool.demand"
	lifecycleOpDurationMetric   = "ate.actor.lifecycle.operation.duration"
	schedulerAssignmentMetric   = "ate.scheduler.assignment.duration"
	actorCrashesMetric          = "ate.actor.crashes"
)

// waitingWindow is how long an actor counts as waiting after its resume found
// no room. It has to outlast the gap between two resume retries of a parked
// request, or the actor drops out of ate.workerpool.demand between them.
const waitingWindow = 10 * time.Second

var actorCrashesCounter metric.Int64Counter

// RegisterActorCrashes initializes the ate.actor.crashes counter instrument.
func RegisterActorCrashes(meter metric.Meter) error {
	counter, err := meter.Int64Counter(
		actorCrashesMetric,
		metric.WithUnit("{crash}"),
		metric.WithDescription("Number of times actors transitioned to ACTOR_STATE_CRASHED with failure reasons."),
	)
	if err != nil {
		return fmt.Errorf("create %s counter: %w", actorCrashesMetric, err)
	}
	actorCrashesCounter = counter
	return nil
}

// recordActorCrash records a crash event on ate.actor.crashes with low-cardinality attributes.
func recordActorCrash(ctx context.Context, attrs []attribute.KeyValue) {
	if actorCrashesCounter == nil || len(attrs) == 0 {
		return
	}
	actorCrashesCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RegisterWorkerCount wires the ate.workerpool.workers observable against workers
// (workercache.Cache.Workers in prod) and listPools (a WorkerPool lister's List,
// used to seed zero-valued series). Worker counts are spatially summable (over
// states = pool size, over pools = fleet), which is the UpDownCounter contract; a
// gauge would be wrong for a value meant to be summed.
func RegisterWorkerCount(meter metric.Meter, workers func() ([]*ateapi.Worker, error), listPools func(labels.Selector) ([]*atev1.WorkerPool, error)) error {
	counter, err := meter.Int64ObservableUpDownCounter(
		workerpoolWorkersMetric,
		metric.WithUnit("{worker}"),
		metric.WithDescription("Number of workers by pool namespace, pool, worker state, and sandbox class."),
	)
	if err != nil {
		return fmt.Errorf("create %s updowncounter: %w", workerpoolWorkersMetric, err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		ws, err := workers()
		if err != nil {
			// Worker cache unavailable (warmup/reconnect): skip the whole observation.
			return nil
		}
		type key struct{ namespace, pool, state, class string }
		tally := make(map[key]int64)
		// Seed every state at 0 for every known pool so a saturated or empty pool
		// reports 0, not an absent series that breaks idle==0 alerts. A failed
		// list just means no seeding this cycle, not a broken observation.
		if pools, err := listPools(labels.Everything()); err == nil {
			for _, p := range pools {
				class := string(p.Spec.DefaultSandboxClass())
				if class == "" {
					class = string(atev1.SandboxClassGvisor)
				}
				for _, state := range workerStates {
					tally[key{p.Namespace, p.Name, state, class}] = 0
				}
			}
		}
		for _, w := range ws {
			state := workerState(w)
			// A Worker has no sandbox class until its worker registers with
			// WorkerService. Report it as unknown, not as the pool's class: the
			// scheduler puts no actor on a worker that offers no class, thus
			// the pool's series must not count it as free capacity.
			class := ateattr.NormalizeSandboxClass(hardware.SandboxClass(w.GetStatus()))
			tally[key{w.GetWorkerNamespace(), w.GetWorkerPool(), state, class}]++
		}
		for k, n := range tally {
			o.ObserveInt64(counter, n, metric.WithAttributes(
				ateattr.WorkerPoolNamespaceKey.String(k.namespace),
				ateattr.WorkerPoolNameKey.String(k.pool),
				ateattr.WorkerStateKey.String(k.state),
				ateattr.SandboxClassKey.String(k.class),
			))
		}
		return nil
	}, counter)
	if err != nil {
		return fmt.Errorf("register %s callback: %w", workerpoolWorkersMetric, err)
	}
	return nil
}

var workerStates = []string{
	ateattr.WorkerStateIdle,
	ateattr.WorkerStatePartial,
	ateattr.WorkerStateAtCapacity,
	ateattr.WorkerStateUnschedulable,
}

// workerState counts actor slots only. A capacity not yet reported is
// unschedulable, not full.
func workerState(w *ateapi.Worker) string {
	capacity := w.GetStatus().GetCapacity().GetActors()
	if w.GetStatus().GetState() != ateapi.WorkerState_WORKER_STATE_ACTIVE || capacity <= 0 {
		return ateattr.WorkerStateUnschedulable
	}
	switch allocated := w.GetStatus().GetAllocated().GetActors(); {
	case allocated <= 0:
		return ateattr.WorkerStateIdle
	case allocated < capacity:
		return ateattr.WorkerStatePartial
	default:
		return ateattr.WorkerStateAtCapacity
	}
}

// RegisterWorkerResources wires the per-worker capacity and allocated
// instruments for CPU, memory and actors against workers
// (workercache.Cache.Workers in prod). Allocated is the sum of the limits of
// the actors on a worker, not their usage, and every worker reports it. Only a
// worker the scheduler can place on reports capacity, and only for a resource
// it reports. Thus a draining or new worker never reads as free room.
func RegisterWorkerResources(meter metric.Meter, workers func() ([]*ateapi.Worker, error)) error {
	var errs []error
	floatCounter := func(name, unit, description string) metric.Float64ObservableUpDownCounter {
		c, err := meter.Float64ObservableUpDownCounter(name, metric.WithUnit(unit), metric.WithDescription(description))
		if err != nil {
			errs = append(errs, fmt.Errorf("create %s updowncounter: %w", name, err))
		}
		return c
	}
	intCounter := func(name, unit, description string) metric.Int64ObservableUpDownCounter {
		c, err := meter.Int64ObservableUpDownCounter(name, metric.WithUnit(unit), metric.WithDescription(description))
		if err != nil {
			errs = append(errs, fmt.Errorf("create %s updowncounter: %w", name, err))
		}
		return c
	}
	cpuCapacity := floatCounter(workerCPUCapacityMetric, "{cpu}", "CPU cores of a worker that actors can book.")
	cpuAllocated := floatCounter(workerCPUAllocatedMetric, "{cpu}", "CPU cores that the actors on a worker book.")
	memoryCapacity := intCounter(workerMemoryCapacityMetric, "By", "Memory of a worker that actors can book.")
	memoryAllocated := intCounter(workerMemoryAllocatedMetric, "By", "Memory that the actors on a worker book.")
	actorsCapacity := intCounter(workerActorsCapacityMetric, "{actor}", "Most actors a worker can hold.")
	actorsAllocated := intCounter(workerActorsAllocatedMetric, "{actor}", "Actors on a worker.")
	if err := errors.Join(errs...); err != nil {
		return err
	}

	_, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		ws, err := workers()
		if err != nil {
			// Worker cache unavailable (warmup/reconnect): skip the whole observation.
			return nil
		}
		for _, w := range ws {
			attrs := metric.WithAttributes(
				ateattr.WorkerPoolNamespaceKey.String(w.GetWorkerNamespace()),
				ateattr.WorkerPoolNameKey.String(w.GetWorkerPool()),
				ateattr.WorkerNameKey.String(w.GetMetadata().GetName()),
				ateattr.WorkerPodNameKey.String(w.GetWorkerPod()),
				ateattr.WorkerNodeNameKey.String(w.GetNodeName()),
			)
			allocated := w.GetStatus().GetAllocated()
			o.ObserveFloat64(cpuAllocated, cores(allocated.GetCpuMillicores()), attrs)
			o.ObserveInt64(memoryAllocated, allocated.GetMemoryBytes(), attrs)
			o.ObserveInt64(actorsAllocated, int64(allocated.GetActors()), attrs)
			if !schedulable(w) {
				continue
			}
			capacity := w.GetStatus().GetCapacity()
			if c := capacity.GetCpuMillicores(); c > 0 {
				o.ObserveFloat64(cpuCapacity, cores(c), attrs)
			}
			if c := capacity.GetMemoryBytes(); c > 0 {
				o.ObserveInt64(memoryCapacity, c, attrs)
			}
			if c := capacity.GetActors(); c > 0 {
				o.ObserveInt64(actorsCapacity, int64(c), attrs)
			}
		}
		return nil
	}, cpuCapacity, cpuAllocated, memoryCapacity, memoryAllocated, actorsCapacity, actorsAllocated)
	if err != nil {
		return fmt.Errorf("register worker resource callback: %w", err)
	}
	return nil
}

func cores(millicores int64) float64 { return float64(millicores) / 1000 }

// schedulable reports whether the scheduler can place an actor on w.
func schedulable(w *ateapi.Worker) bool {
	return w.GetStatus().GetState() == ateapi.WorkerState_WORKER_STATE_ACTIVE && scheduling.Registered(w)
}

// waitingActor is an actor whose latest resume found no room, with what it
// would book in each pool it can run in.
type waitingActor struct {
	at       time.Time
	bookings map[poolKey]*ateapi.Resources
}

// waitingActors holds the actors that wait for room, by actor UID.
type waitingActors struct {
	mu     sync.Mutex
	actors map[string]waitingActor
}

func (w *waitingActors) add(uid string, actor waitingActor) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.actors[uid] = actor
}

func (w *waitingActors) remove(uid string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.actors, uid)
}

// since returns the actors that found no room at cutoff or later, and forgets
// the rest.
func (w *waitingActors) since(cutoff time.Time) []waitingActor {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []waitingActor
	for uid, actor := range w.actors {
		if actor.at.Before(cutoff) {
			delete(w.actors, uid)
			continue
		}
		out = append(out, actor)
	}
	return out
}

type poolKey struct{ namespace, name string }

// poolDemand returns the workers that the booked and the waiting actors of
// each pool fill. Bookings count on every worker of the pool, like
// ate.worker.*.allocated. Capacity and the number of workers count only the
// workers the scheduler can place on, like ate.worker.*.capacity. A waiting
// actor found room on no worker, so while one waits the pool counts as full
// and the waiting actors fill new workers. A pool with no such worker has no
// demand to report.
func poolDemand(workers []*ateapi.Worker, waiting []waitingActor) map[poolKey]float64 {
	// Each array holds actors, CPU millicores and memory bytes.
	type sums struct {
		workers                   float64
		capacity, booked, waiting [3]float64
	}
	byPool := make(map[poolKey]*sums)
	for _, w := range workers {
		key := poolKey{w.GetWorkerNamespace(), w.GetWorkerPool()}
		s := byPool[key]
		if s == nil {
			s = &sums{}
			byPool[key] = s
		}
		booked := w.GetStatus().GetAllocated()
		s.booked[0] += float64(booked.GetActors())
		s.booked[1] += float64(booked.GetCpuMillicores())
		s.booked[2] += float64(booked.GetMemoryBytes())
		if !schedulable(w) {
			continue
		}
		s.workers++
		capacity := w.GetStatus().GetCapacity()
		s.capacity[0] += float64(capacity.GetActors())
		s.capacity[1] += float64(capacity.GetCpuMillicores())
		s.capacity[2] += float64(capacity.GetMemoryBytes())
	}
	for _, actor := range waiting {
		for key, booking := range actor.bookings {
			s := byPool[key]
			if s == nil {
				continue
			}
			s.waiting[0]++
			s.waiting[1] += float64(booking.GetCpuMillicores())
			s.waiting[2] += float64(booking.GetMemoryBytes())
		}
	}
	demand := make(map[poolKey]float64, len(byPool))
	for key, s := range byPool {
		if s.workers == 0 {
			continue
		}
		var d float64
		for i, capacity := range s.capacity {
			// A resource the workers do not report is unconstrained.
			if capacity <= 0 {
				continue
			}
			booked := s.booked[i]
			if s.waiting[0] > 0 {
				booked = max(booked, capacity)
			}
			d = max(d, (booked+s.waiting[i])/capacity*s.workers)
		}
		demand[key] = d
	}
	return demand
}

// RegisterWorkerPoolDemand wires the ate.workerpool.demand gauge against
// workers (workercache.Cache.Workers in prod) and the actors that instruments
// saw find no room.
func RegisterWorkerPoolDemand(meter metric.Meter, workers func() ([]*ateapi.Worker, error), instruments *Instruments) error {
	demand, err := meter.Float64ObservableGauge(
		workerpoolDemandMetric,
		metric.WithUnit("{worker}"),
		metric.WithDescription("Number of workers that the booked actors of a pool and the actors that wait for room in it fill."),
	)
	if err != nil {
		return fmt.Errorf("create %s gauge: %w", workerpoolDemandMetric, err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		ws, err := workers()
		if err != nil {
			// Worker cache unavailable (warmup/reconnect): skip the whole observation.
			return nil
		}
		cutoff := time.Now().Add(-waitingWindow)
		for key, d := range poolDemand(ws, instruments.waiting.since(cutoff)) {
			o.ObserveFloat64(demand, d, metric.WithAttributes(ateattr.WorkerPoolAttributes(key.namespace, key.name)...))
		}
		return nil
	}, demand)
	if err != nil {
		return fmt.Errorf("register %s callback: %w", workerpoolDemandMetric, err)
	}
	return nil
}

// Instruments holds ateapi's actor-lifecycle and scheduler duration
// histograms, and the actors that wait for room. A nil *Instruments is a valid
// no-op, so call sites need no guard. The observables are registered
// separately (RegisterWorkerCount, RegisterWorkerResources,
// RegisterWorkerPoolDemand): they are callback-driven, not synchronous
// instruments.
type Instruments struct {
	lifecycleOpDuration         metric.Float64Histogram
	schedulerAssignmentDuration metric.Float64Histogram
	waiting                     waitingActors
}

// NewInstruments builds the two histograms against meter. Assignment buckets are
// finer than lifecycle's (a cache pick plus a few store writes, not a
// multi-second restore) but reach 5s so store latency spikes stay measurable.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	lifecycleOpDuration, err := meter.Float64Histogram(
		lifecycleOpDurationMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of an actor lifecycle operation (create, resume, suspend, delete, revert) handled by ateapi."),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1, 2.5, 5, 10, 30),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", lifecycleOpDurationMetric, err)
	}

	schedulerAssignmentDuration, err := meter.Float64Histogram(
		schedulerAssignmentMetric,
		metric.WithUnit("s"),
		metric.WithDescription("Duration of the scheduler's worker-assignment step during actor resume."),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
	)
	if err != nil {
		return nil, fmt.Errorf("create %s histogram: %w", schedulerAssignmentMetric, err)
	}

	return &Instruments{
		lifecycleOpDuration:         lifecycleOpDuration,
		schedulerAssignmentDuration: schedulerAssignmentDuration,
		waiting:                     waitingActors{actors: make(map[string]waitingActor)},
	}, nil
}

// recordWaiting counts the actor toward ate.workerpool.demand of each pool in
// bookings until waitingWindow passes or clearWaiting is called. bookings
// holds what the actor takes from a worker of each pool.
func (i *Instruments) recordWaiting(actorUID string, bookings map[poolKey]*ateapi.Resources) {
	if i == nil {
		return
	}
	i.waiting.add(actorUID, waitingActor{at: time.Now(), bookings: bookings})
}

// clearWaiting stops counting an actor that got a worker, or that a new
// worker would not help.
func (i *Instruments) clearWaiting(actorUID string) {
	if i == nil {
		return
	}
	i.waiting.remove(actorUID)
}

// recordLifecycleOp records op's duration. A non-nil err is classified onto
// error.type as the code the caller gets; error.type's absence marks success, so
// there is no parallel failure counter. extraAttrs carries the per-operation
// dimensions (template, pool, class, snapshot kind).
func (i *Instruments) recordLifecycleOp(ctx context.Context, op string, start time.Time, err error, extraAttrs ...attribute.KeyValue) {
	if i == nil || i.lifecycleOpDuration == nil {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(extraAttrs)+2)
	attrs = append(attrs, ateattr.ActorOperationNameKey.String(op))
	attrs = append(attrs, extraAttrs...)
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(apierror.Code(err).String()))
	}
	i.lifecycleOpDuration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attrs...))
}

// lifecycleOpAttrs builds the resume/suspend/revert dimensions from
// workflow state. Nil-safe, and omits the template, pool, sandbox-class,
// snapshot-kind and snapshot-fidelity labels until they are known so a failure
// before the load/assign/restore steps never emits an empty-string series.
// snapshotKind is empty for suspend, which does not restore; snapshotScope
// applies to resume and suspend and separates a full restore from a data-only
// one of the same kind.
// The pool keys are set together or not at all; see ateattr.WorkerPoolAttributes.
func lifecycleOpAttrs(actor *ateapi.Actor, snapshotKind, snapshotScope string) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	if actor != nil {
		attrs = append(attrs,
			ateattr.TemplateNameKey.String(actor.GetActorTemplate().GetName()),
			ateattr.TemplateAtespaceKey.String(actor.GetActorTemplate().GetAtespace()),
		)
	}
	ass := actor.GetStatus().GetWorkerAssignment()
	attrs = append(attrs, ateattr.WorkerPoolAttributes(ass.GetWorkerNamespace(), ass.GetWorkerPool())...)
	class := ass.GetSandboxRuntime().GetSandboxClass()
	if class == "" {
		class = snapshotAtLatestGeneration(actor.GetStatus()).GetSandboxRuntime().GetSandboxClass()
	}
	if class != "" {
		attrs = append(attrs, ateattr.SandboxClassAttribute(class))
	}
	if snapshotKind != "" {
		attrs = append(attrs, ateattr.SnapshotKindKey.String(snapshotKind))
	}
	if snapshotScope != "" {
		attrs = append(attrs, ateattr.SnapshotFidelityKey.String(snapshotScope))
	}
	return attrs
}

// recordSchedulerAssignment records one assignment attempt. pool is set only
// when a worker was assigned and error.type only for the Error outcome, so
// no_capacity (a capacity signal, not a failure) carries neither. class is
// set on every outcome it is known for, so no_capacity names the capacity
// that ran out and stays comparable with assigned.
// The pool keys are set together or not at all; see ateattr.WorkerPoolAttributes.
// elapsed is the time that the attempt took.
func (i *Instruments) recordSchedulerAssignment(ctx context.Context, elapsed time.Duration, outcome, poolNamespace, pool, class string, err error) {
	if i == nil || i.schedulerAssignmentDuration == nil {
		return
	}
	attrs := make([]attribute.KeyValue, 0, 5)
	attrs = append(attrs, ateattr.SchedulerOutcomeKey.String(outcome))
	attrs = append(attrs, ateattr.WorkerPoolAttributes(poolNamespace, pool)...)
	if class != "" {
		attrs = append(attrs, ateattr.SandboxClassAttribute(class))
	}
	if outcome == ateattr.SchedulerOutcomeError && err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(apierror.Code(err).String()))
	}
	i.schedulerAssignmentDuration.Record(ctx, elapsed.Seconds(), metric.WithAttributes(attrs...))
}
