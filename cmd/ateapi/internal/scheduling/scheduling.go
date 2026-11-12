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

// Package scheduling decides which worker should host an actor.
package scheduling

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/labels"
)

// Constraints describes what a worker must satisfy to host an actor.
type Constraints struct {
	// SandboxClass, when set, is the sandbox class of the snapshot the actor
	// restores from, and the worker's must equal it. A snapshot's files, its
	// durable volumes included, are written by one class's worker and read
	// back by the same, so unlike RequiredRuntime this holds for every
	// fidelity and is never relaxed. Empty for an actor with no snapshot,
	// which boots on whichever class its selectors pick.
	SandboxClass string

	// RequiredRuntime, when it names a runtime, restricts placement to
	// workers whose DefaultRuntime or RestorableRuntimes can restore a
	// snapshot stamped with it. Prepared once per Schedule, since Applies
	// asks it of every candidate.
	RequiredRuntime hardware.Requirement

	// SnapshotRuntime is the runtime of the snapshot the actor restores from,
	// or the zero Requirement when it starts on a worker's default runtime.
	// Unlike RequiredRuntime it is never relaxed: it picks the worker runtime
	// the actor runs under, and so the overhead it is booked with.
	SnapshotRuntime hardware.Requirement

	// TemplateSelector and ActorSelector must both match the worker's labels.
	TemplateSelector labels.Selector
	ActorSelector    labels.Selector

	// RequiredNodes, when non-empty, restricts placement to workers running
	// on one of these nodes. Used when the actor's latest snapshot is local
	// to specific node VMs.
	RequiredNodes []string

	// Limits are the actor's declared resource limits, in the units a Worker
	// reports its capacity in, so the two subtract.
	Limits *ateapi.Resources

	// LongLived is a hint that the actor is expected to run much longer than
	// most. EXPERIMENT ONLY: only the prefix policy reads it.
	LongLived bool
}

// ErrNoCapacity is returned by Schedule when no worker that meets the
// constraints has room. The cause can be full workers, or no worker that meets
// the constraints at all.
var ErrNoCapacity = errors.New("no worker that meets the constraints has room")

// Scheduler answers placement questions against the current worker fleet.
type Scheduler interface {
	// Schedule returns a worker that meets constraints and has room.
	// Returns ErrNoCapacity when no worker that meets constraints has room.
	Schedule(ctx context.Context, constraints Constraints) (*ateapi.Worker, error)

	// Applies reports whether worker satisfies non-capacity constraints. Capacity
	// is excluded so an existing assignment does not make its Worker ineligible.
	Applies(worker *ateapi.Worker, constraints Constraints) bool

	// HasRoom reports whether worker's remaining capacity admits one more actor
	// of this size, in every dimension. Independent of Applies.
	HasRoom(worker *ateapi.Worker, constraints Constraints) bool
}

// WorkerSource provides the whole fleet of workers.
type WorkerSource interface {
	Workers() ([]*ateapi.Worker, error)
}

type scheduler struct {
	source WorkerSource
	// intn returns a uniformly distributed random value in [0,n).
	// Defaults to the global math/rand source
	intn   func(n int) int
	policy Policy
}

// Option configures the Scheduler returned by New.
type Option func(*scheduler)

// Policy picks among the candidates with room. EXPERIMENT ONLY: it exists to
// compare fragmentation against P2C and is not meant for upstream.
type Policy struct {
	// Name is one of p2c (the default), random, mostallocated, bestofd, topk,
	// watermark, firstfit, oldestofd, or a prefix policy: prefix, prefixsat,
	// prefixbest or prefixpack.
	Name string
	// K is the sample size for bestofd, and the pool to pick from at random
	// for topk, watermark and firstfit. For prefix it is the least free
	// capacity the window keeps, in workers: 1 is one empty worker's worth.
	K int
	// Watermark is the highest post-booking load watermark packs to. For
	// prefix it is the free capacity the window keeps, as a fraction of what
	// the window has allocated.
	Watermark float64
}

// PolicyFromEnv parses ATE_API_SCHEDULING_POLICY as name[:K[:watermark]],
// for example "topk:3". Unset means p2c. EXPERIMENT ONLY.
func PolicyFromEnv() Policy {
	v := os.Getenv("ATE_API_SCHEDULING_POLICY")
	parts := strings.Split(v, ":")
	p := Policy{Name: parts[0]}
	if len(parts) > 1 {
		p.K, _ = strconv.Atoi(parts[1])
	}
	if len(parts) > 2 {
		p.Watermark, _ = strconv.ParseFloat(parts[2], 64)
	}
	slog.Info("scheduling policy", slog.String("name", p.Name), slog.Int("k", p.K), slog.Float64("watermark", p.Watermark))
	return p
}

// WithPolicy sets the placement policy. EXPERIMENT ONLY.
func WithPolicy(p Policy) Option {
	return func(s *scheduler) { s.policy = p }
}

// WithIntn overrides the random source used to sample candidate pairs: when
// n >= 2 candidates exist, Schedule calls intn(n) and then intn(n-1).
func WithIntn(intn func(n int) int) Option {
	return func(s *scheduler) { s.intn = intn }
}

// New returns a Scheduler placing onto workers reported by source.
func New(source WorkerSource, opts ...Option) Scheduler {
	s := &scheduler{source: source, intn: rand.Intn}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Schedule filters the fleet for eligible candidates with room, samples two at
// random (power of two choices), and returns the less-loaded one, where load is
// the higher of actor-slot and compute-resource utilization. Spreading across
// the warm pool avoids hotspots until autoscaling reclaims idle workers.
func (s *scheduler) Schedule(ctx context.Context, constraints Constraints) (*ateapi.Worker, error) {
	workers, err := s.source.Workers()
	if err != nil {
		return nil, fmt.Errorf("while listing workers: %w", err)
	}

	if strings.HasPrefix(s.policy.Name, "prefix") {
		return s.schedulePrefix(workers, constraints)
	}

	var candidates []candidate
	for _, worker := range workers {
		if !s.Applies(worker, constraints) {
			continue
		}
		booking, ok := Booking(worker, constraints)
		if !ok {
			continue
		}
		if cand, ok := checkRoom(worker, booking); ok {
			cand.after = loadAfter(worker, booking)
			candidates = append(candidates, cand)
		}
	}

	if len(candidates) == 0 {
		return nil, ErrNoCapacity
	}
	if len(candidates) == 1 {
		return candidates[0].worker, nil
	}

	switch s.policy.Name {
	case "", "p2c":
	case "random":
		return candidates[s.intn(len(candidates))].worker, nil
	case "mostallocated":
		best := 0
		for i := range candidates {
			if candidates[i].after > candidates[best].after {
				best = i
			}
		}
		return candidates[best].worker, nil
	case "bestofd":
		d := min(max(s.policy.K, 2), len(candidates))
		// Partial Fisher-Yates: the first d entries become a uniform sample.
		best := -1
		for i := range d {
			j := i + s.intn(len(candidates)-i)
			candidates[i], candidates[j] = candidates[j], candidates[i]
			if best < 0 || candidates[i].after > candidates[best].after {
				best = i
			}
		}
		return candidates[best].worker, nil
	case "oldestofd":
		// Sample d and take the oldest, so the newest workers, which a
		// ReplicaSet scales down first, rarely get actors. With Watermark set,
		// prefer the oldest sample that stays at or below it.
		d := min(max(s.policy.K, 2), len(candidates))
		best, bestUnder := -1, -1
		for i := range d {
			j := i + s.intn(len(candidates)-i)
			candidates[i], candidates[j] = candidates[j], candidates[i]
			if best < 0 || olderThan(&candidates[i], &candidates[best]) {
				best = i
			}
			if s.policy.Watermark > 0 && candidates[i].after <= s.policy.Watermark &&
				(bestUnder < 0 || olderThan(&candidates[i], &candidates[bestUnder])) {
				bestUnder = i
			}
		}
		if bestUnder >= 0 {
			return candidates[bestUnder].worker, nil
		}
		return candidates[best].worker, nil
	case "topk":
		slices.SortStableFunc(candidates, func(a, b candidate) int { return cmpDesc(a.after, b.after) })
		return candidates[s.intn(min(max(s.policy.K, 1), len(candidates)))].worker, nil
	case "watermark":
		var under []candidate
		for _, c := range candidates {
			if c.after <= s.policy.Watermark {
				under = append(under, c)
			}
		}
		if len(under) > 0 {
			slices.SortStableFunc(under, func(a, b candidate) int { return cmpDesc(a.after, b.after) })
			return under[s.intn(min(max(s.policy.K, 1), len(under)))].worker, nil
		}
	case "firstfit":
		slices.SortStableFunc(candidates, func(a, b candidate) int {
			at := a.worker.GetMetadata().GetCreateTime().AsTime()
			bt := b.worker.GetMetadata().GetCreateTime().AsTime()
			if c := at.Compare(bt); c != 0 {
				return c
			}
			return cmp.Compare(a.worker.GetWorkerPod(), b.worker.GetWorkerPod())
		})
		return candidates[s.intn(min(max(s.policy.K, 1), len(candidates)))].worker, nil
	default:
		return nil, fmt.Errorf("unknown scheduling policy %q", s.policy.Name)
	}

	i := s.intn(len(candidates))
	j := s.intn(len(candidates) - 1)
	if j >= i {
		j++
	}
	if lessLoaded(&candidates[j], &candidates[i]) {
		return candidates[j].worker, nil
	}
	return candidates[i].worker, nil
}

// schedulePrefix orders the eligible workers oldest first and takes the
// shortest prefix whose free capacity is at least max(K, Watermark*allocated).
// Both are sums of each worker's load, so they count in workers and do not
// depend on --max-actors. It returns the less loaded of two random workers
// with room in that prefix. Workers past the prefix get no new actors, so they
// empty as their actors suspend. A LongLived actor goes to one of the two
// oldest workers with room instead. prefixpack sends an actor pinned to a node
// (RequiredNodes) to the fullest worker with room there. EXPERIMENT ONLY.
func (s *scheduler) schedulePrefix(workers []*ateapi.Worker, constraints Constraints) (*ateapi.Worker, error) {
	type entry struct {
		cand    candidate
		hasRoom bool
	}
	var eligible []entry
	for _, worker := range workers {
		if !s.Applies(worker, constraints) {
			continue
		}
		e := entry{cand: candidate{worker: worker}}
		if booking, ok := Booking(worker, constraints); ok {
			if cand, ok := checkRoom(worker, booking); ok {
				cand.after = loadAfter(worker, booking)
				e.cand, e.hasRoom = cand, true
			}
		}
		eligible = append(eligible, e)
	}
	// A pinned actor's window holds only its node's few workers, and picking
	// the less loaded of two there keeps all of them busy.
	if s.policy.Name == "prefixpack" && len(constraints.RequiredNodes) > 0 {
		var best *candidate
		for i := range eligible {
			if e := &eligible[i]; e.hasRoom && (best == nil || e.cand.after > best.after) {
				best = &e.cand
			}
		}
		if best == nil {
			return nil, ErrNoCapacity
		}
		return best.worker, nil
	}
	slices.SortFunc(eligible, func(a, b entry) int {
		if olderThan(&a.cand, &b.cand) {
			return -1
		}
		return 1
	})

	var window []candidate
	var used, free float64
	// saturated is set when no prefix has the free capacity asked for.
	saturated := !constraints.LongLived
	for _, e := range eligible {
		load := loadNow(e.cand.worker)
		used += load
		free += 1 - load
		if e.hasRoom {
			window = append(window, e.cand)
		}
		if constraints.LongLived && len(window) == 2 {
			break
		}
		if !constraints.LongLived && len(window) >= 2 && free >= max(float64(s.policy.K), s.policy.Watermark*used) {
			saturated = false
			break
		}
	}
	switch len(window) {
	case 0:
		return nil, ErrNoCapacity
	case 1:
		return window[0].worker, nil
	}
	i := s.intn(len(window))
	if constraints.LongLived {
		return window[i].worker, nil
	}
	j := s.intn(len(window) - 1)
	if j >= i {
		j++
	}
	a, b := &window[i], &window[j]
	if lessLoaded(b, a) {
		a, b = b, a
	}
	// a is the less loaded. prefixbest always takes the fuller one. prefixsat
	// and prefixpack take it only when saturated, where spreading would leave
	// no worker with room for a large actor.
	if s.policy.Name == "prefixbest" || ((s.policy.Name == "prefixsat" || s.policy.Name == "prefixpack") && saturated) {
		return b.worker, nil
	}
	return a.worker, nil
}

// loadNow returns the highest of slot, CPU and memory utilization.
// Dimensions the worker does not report are skipped.
func loadNow(w *ateapi.Worker) float64 {
	capacity := w.GetStatus().GetCapacity()
	used := w.GetStatus().GetAllocated()
	load := float64(used.GetActors()) / float64(capacity.GetActors())
	if c := capacity.GetCpuMillicores(); c > 0 {
		load = max(load, float64(used.GetCpuMillicores())/float64(c))
	}
	if c := capacity.GetMemoryBytes(); c > 0 {
		load = max(load, float64(used.GetMemoryBytes())/float64(c))
	}
	return min(load, 1)
}

// candidate pairs an eligible worker with its cached compute utilization.
type candidate struct {
	worker *ateapi.Worker
	// resUtil is the worker's dominant compute-resource utilization in [0,1],
	// or -1 if not yet computed.
	resUtil float64
	// after is the worker's dominant utilization once this actor is booked.
	after float64
}

func cmpDesc(a, b float64) int { return cmp.Compare(b, a) }

// olderThan orders workers by create_time, then by pod name.
func olderThan(a, b *candidate) bool {
	at := a.worker.GetMetadata().GetCreateTime().AsTime()
	bt := b.worker.GetMetadata().GetCreateTime().AsTime()
	if c := at.Compare(bt); c != 0 {
		return c < 0
	}
	return a.worker.GetWorkerPod() < b.worker.GetWorkerPod()
}

// loadAfter returns the highest of slot, CPU and memory utilization once
// booking is added. Dimensions the worker does not report are skipped.
func loadAfter(w *ateapi.Worker, booking *ateapi.Resources) float64 {
	capacity := w.GetStatus().GetCapacity()
	used := w.GetStatus().GetAllocated()
	load := float64(used.GetActors()+1) / float64(capacity.GetActors())
	if c := capacity.GetCpuMillicores(); c > 0 {
		load = max(load, float64(used.GetCpuMillicores()+booking.GetCpuMillicores())/float64(c))
	}
	if c := capacity.GetMemoryBytes(); c > 0 {
		load = max(load, float64(used.GetMemoryBytes()+booking.GetMemoryBytes())/float64(c))
	}
	return load
}

func (c *candidate) resourceUtilization() float64 {
	if c.resUtil < 0 {
		c.resUtil = workerResourceUtilization(c.worker)
	}
	return c.resUtil
}

// lessLoaded compares dominant utilization — the higher of actor-slot
// utilization (allocated/capacity) and compute-resource utilization — so either
// dimension can mark a worker as hot. Ties fall back to actor-slot utilization,
// then to remaining actor slots.
func lessLoaded(a, b *candidate) bool {
	aAlloc := int64(a.worker.GetStatus().GetAllocated().GetActors())
	bAlloc := int64(b.worker.GetStatus().GetAllocated().GetActors())
	// checkRoom admits only workers with allocated < capacity, so capacity >= 1.
	aCap := int64(a.worker.GetStatus().GetCapacity().GetActors())
	bCap := int64(b.worker.GetStatus().GetCapacity().GetActors())

	aLoad := max(float64(aAlloc)/float64(aCap), a.resourceUtilization())
	bLoad := max(float64(bAlloc)/float64(bCap), b.resourceUtilization())
	if aLoad != bLoad {
		return aLoad < bLoad
	}

	// Compare slot utilization exactly via cross-multiplication.
	if lhs, rhs := aAlloc*bCap, bAlloc*aCap; lhs != rhs {
		return lhs < rhs
	}

	return aCap-aAlloc > bCap-bAlloc
}

// workerResourceUtilization returns the highest allocated/capacity ratio across
// compute dimensions, or 1 when capacity is unreported.
func workerResourceUtilization(w *ateapi.Worker) float64 {
	return utilization(w.GetStatus().GetCapacity(), w.GetStatus().GetAllocated())
}

func utilization(capacity, used *ateapi.WorkerResources) float64 {
	var (
		maxRatio    float64
		hasPositive bool
	)
	for _, d := range [][2]int64{
		{capacity.GetCpuMillicores(), used.GetCpuMillicores()},
		{capacity.GetMemoryBytes(), used.GetMemoryBytes()},
	} {
		if d[0] > 0 {
			hasPositive = true
			maxRatio = max(maxRatio, float64(d[1])/float64(d[0]))
		}
	}
	if !hasPositive {
		return 1
	}
	return maxRatio
}

// Registered reports whether worker's current worker process has registered
// and reported its sandbox class and runtimes. The assignment records the
// runtime and atelet requires it on Run/Restore, so a worker without one
// cannot host anything yet.
func Registered(worker *ateapi.Worker) bool {
	return worker.GetStatus().GetRegisteredEpoch() != 0 && hardware.SandboxClass(worker.GetStatus()) != ""
}

func (s *scheduler) Applies(worker *ateapi.Worker, constraints Constraints) bool {
	if !Registered(worker) {
		return false
	}
	workerClass := hardware.SandboxClass(worker.GetStatus())
	if constraints.SandboxClass != "" && workerClass != constraints.SandboxClass {
		return false
	}

	if worker.GetStatus().GetState() != ateapi.WorkerState_WORKER_STATE_ACTIVE {
		return false
	}

	if len(constraints.RequiredNodes) > 0 && !slices.Contains(constraints.RequiredNodes, worker.GetNodeName()) {
		return false
	}

	if !constraints.RequiredRuntime.WorkerMatches(worker.GetStatus()) {
		return false
	}

	set := labels.Set(worker.GetLabels())
	if constraints.TemplateSelector != nil && !constraints.TemplateSelector.Matches(set) {
		return false
	}
	return constraints.ActorSelector == nil || constraints.ActorSelector.Matches(set)
}

// HasRoom reports whether what the worker has left admits one more actor of
// this size. A dimension the worker does not report is unconstrained, so
// placement is never blocked by missing data.
func (s *scheduler) HasRoom(worker *ateapi.Worker, constraints Constraints) bool {
	booking, ok := Booking(worker, constraints)
	if !ok {
		return false
	}
	_, ok = checkRoom(worker, booking)
	return ok
}

// Booking is what an actor takes from the worker's capacity: its limits plus
// the overhead of the worker runtime it would run under there. It reports
// false when that doesn't fit in an int64, and the actor doesn't fit on the worker.
func Booking(worker *ateapi.Worker, constraints Constraints) (*ateapi.Resources, bool) {
	overhead := constraints.SnapshotRuntime.WorkerRuntimeFor(worker.GetStatus()).GetActorOverhead()
	if overhead == nil {
		return constraints.Limits, true
	}
	limit := constraints.Limits.GetMemoryBytes()
	scaled, ok := perGiB(limit, overhead.GetMemoryBytesPerGib())
	if !ok {
		return nil, false
	}
	memory, ok := sum(limit, overhead.GetMemoryBytes(), scaled)
	if !ok {
		return nil, false
	}
	return &ateapi.Resources{CpuMillicores: constraints.Limits.GetCpuMillicores(), MemoryBytes: memory}, true
}

// perGiB scales rate bytes per GiB to n bytes, and reports false on overflow.
func perGiB(n, rate int64) (int64, bool) {
	if n < 0 || rate < 0 {
		return 0, false
	}
	hi, lo := bits.Mul64(uint64(n), uint64(rate))
	// divide the 128-bit product by 1<<30
	if hi>>30 != 0 {
		return 0, false
	}
	v := hi<<34 | lo>>30
	return int64(v), v <= math.MaxInt64
}

// sum adds non-negative values, and reports false on overflow.
func sum(vs ...int64) (int64, bool) {
	var total int64
	for _, v := range vs {
		if v < 0 || v > math.MaxInt64-total {
			return 0, false
		}
		total += v
	}
	return total, true
}

func checkRoom(worker *ateapi.Worker, want *ateapi.Resources) (candidate, bool) {
	capacity := worker.GetStatus().GetCapacity()
	used := worker.GetStatus().GetAllocated()

	// No per-actor size to compare: every assignment costs one, so a worker at
	// its limit has no room however small the next actor is.
	if used.GetActors() >= capacity.GetActors() {
		return candidate{}, false
	}

	if want.GetCpuMillicores() == 0 && want.GetMemoryBytes() == 0 {
		return candidate{worker: worker, resUtil: -1}, true
	}
	if !resources.Covers(resources.Free(capacity, used), want) {
		return candidate{}, false
	}
	resUtil := utilization(capacity, used)
	return candidate{worker: worker, resUtil: resUtil}, true
}
