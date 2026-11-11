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

// Command schedsim replays resume/suspend load through scheduling.New to
// compare placement policies. EXPERIMENT ONLY.
//
// It models two ateapi replicas, each with its own worker cache. A replica sees
// its own commits at once and the other replica's on its next outbox poll. A
// bind locks the worker row, re-checks HasRoom on the fresh row, and retries
// with assignmentBackoff when the worker filled up. A resume that finds no
// capacity, or exhausts its bind retries, is retried by the client every
// second until clientTimeout.
package main

import (
	"container/heap"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/scheduling"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	gi            = int64(1) << 30
	mi            = int64(1) << 20
	clientRetry   = time.Second
	clientTimeout = 30 * time.Second
	sampleEvery   = time.Second
)

var (
	outDir         = flag.String("out", "/tmp/schedsim", "output directory")
	seeds          = flag.Int("seeds", 10, "repetitions per cell")
	largeSeeds     = flag.Int("large-seeds", 3, "repetitions per cell for the large scenario")
	bindBase       = flag.Duration("bind-base", 3*time.Millisecond, "bind latency outside the row lock")
	bindRow        = flag.Duration("bind-row", time.Millisecond, "time a bind holds the worker row lock")
	pollEvery      = flag.Duration("poll", 50*time.Millisecond, "outbox poll interval of the other replica")
	only           = flag.String("only", "", "comma-separated scenario names to run (default all)")
	replicasN      = flag.Int("replicas", 2, "ate-api-server replicas, each with its own worker cache")
	workersPerNode = flag.Int("workers-per-node", 0, "workers per node; 0 puts every worker on one node and disables snapshot pinning")
	detachDelay    = flag.Duration("detach-delay", 30*time.Minute, "how long a suspended actor stays pinned to its node (atelet --snapshot-detach-delay)")
	detachCost     = flag.Duration("detach-cost", 50*time.Millisecond, "time a detach from a full node adds to the resume")
	longFrac       = flag.Float64("long-frac", 0.03, "ramp-down: fraction of actors holding 5 to 10 minutes; rates are scaled to keep the same occupancy")
	holdDist       = flag.String("hold", "uniform", "hold time distribution: uniform (20 to 40s, plus ramp-down's long tail) or lognormal (mean 30s, sigma 1.5, capped at 30m)")
	fillNoWait     = flag.Bool("fill-no-backoff", false, "a bind that finds the worker full retries at once instead of sleeping assignmentBackoff; attempts still count toward its limit")
	onlyPolicies   = flag.String("policies", "", "comma-separated policy names, as printed in the output, to run (default all)")
	onlyPatterns   = flag.String("patterns", "", "comma-separated pattern names to run (default all)")
	observeOnFill  = flag.Bool("observe-on-fill", false, "a bind that finds the worker full puts the row it read into the replica's cache")
	retryP2C       = flag.Bool("retry-p2c", false, "after a bind finds the worker full, the retries pick with p2c over the whole fleet")
	lifetimeHint   = flag.Float64("lifetime-hint", 0, "accuracy of the LongLived hint given to the scheduler, in (0,1]; 0 gives no hint")
	longAfter      = flag.Duration("long-after", 2*time.Minute, "hold time from which an actor counts as long-lived for the hint")
	scaleTarget    = flag.Float64("scale-target", 0.7, "utilization the simulated autoscaler sizes each pool to")

	kindMode   = flag.Bool("kind", false, "drive a live cluster instead of simulating")
	kindSetup  = flag.Bool("setup", false, "kind mode: create and cool the actor population, then exit")
	kubeconfig = flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "kind mode: kubeconfig path")
	kubeCtx    = flag.String("context", "", "kind mode: kubeconfig context")
	kScenario  = flag.String("scenario", "a-homogeneous", "kind mode: scenario")
	kPattern   = flag.String("pattern", "constant", "kind mode: pattern")
	kPolicy    = flag.String("policy", "", "kind mode: policy label for the output; set the policy on ate-api-server")
	kAtespace  = flag.String("atespace", "schedexp", "kind mode: atespace of the actors")
	kSeed      = flag.Int64("seed", 1, "kind mode: seed")
)

// ---- scenarios, patterns, policies ----

type poolSpec struct {
	name    string
	workers int
	slots   int32
	cpu     int64
	mem     int64
}

type actorSize struct {
	frac float64
	cpu  int64
	mem  int64
}

type scenario struct {
	name       string
	pools      []poolSpec
	sizes      []actorSize
	population int
	rateScale  float64
	selector   labels.Selector
}

type step struct {
	dur  time.Duration
	rate float64
}

type pattern struct {
	name  string
	steps []step
	// longTail makes 3% of actors hold 5 to 10 minutes.
	longTail bool
	// dropAt is when load drops, for drain measurements. Zero when none.
	dropAt time.Duration
	// dropRate is the steady rate after the drop.
	dropRate float64
	// scaleAt is when the scale-down is simulated. Defaults to the end.
	scaleAt time.Duration
}

func scenarios() []scenario {
	sel := labels.SelectorFromSet(labels.Set{"workload": "benchmark-worker"})
	return []scenario{
		{
			name:       "a-homogeneous",
			pools:      []poolSpec{{"pool", 40, 16, 4000, 8 * gi}},
			sizes:      []actorSize{{1, 250, 512 * mi}},
			population: 800,
			rateScale:  1,
		},
		{
			name: "b-mixed",
			pools: []poolSpec{
				{"big", 6, 32, 8000, 16 * gi},
				{"small", 20, 8, 2000, 4 * gi},
			},
			sizes:      []actorSize{{1, 250, 512 * mi}},
			population: 450,
			rateScale:  0.55,
			selector:   sel,
		},
		{
			name:       "c-hetero-actors",
			pools:      []poolSpec{{"pool", 40, 32, 4000, 8 * gi}},
			sizes:      []actorSize{{0.8, 125, 256 * mi}, {0.2, 1000, 2 * gi}},
			population: 700,
			rateScale:  0.83,
		},
		{
			name:       "e-ratio",
			pools:      []poolSpec{{"pool", 40, 32, 4000, 16 * gi}},
			sizes:      []actorSize{{0.5, 500, 512 * mi}, {0.5, 125, 2 * gi}},
			population: 700,
			rateScale:  0.8,
		},
		{
			name:       "d-large",
			pools:      []poolSpec{{"pool", 1000, 16, 4000, 8 * gi}},
			sizes:      []actorSize{{1, 250, 512 * mi}},
			population: 20000,
			rateScale:  25,
		},
		{
			// Sized for a laptop kind cluster on the fake data plane.
			name:       "f-kind200",
			pools:      []poolSpec{{"pool", 200, 16, 4000, 8 * gi}},
			sizes:      []actorSize{{1, 250, 512 * mi}},
			population: 4000,
			rateScale:  5,
		},
	}
}

func patterns() []pattern {
	m := time.Minute
	s := time.Second
	var ramp []step
	for i := range 9 {
		ramp = append(ramp, step{80 * s, 2 + float64(i)*18/8})
	}
	var square []step
	for range 4 {
		square = append(square, step{2 * m, 4}, step{2 * m, 18})
	}
	var sine []step
	for i := range 32 {
		t := (float64(i) + 0.5) * 30
		sine = append(sine, step{30 * s, 11 + 7*math.Sin(2*math.Pi*t/240)})
	}
	return []pattern{
		{name: "constant", steps: []step{{12 * m, 12}}},
		{name: "ramp", steps: ramp},
		{name: "square-wave", steps: square},
		{name: "sine-wave", steps: sine},
		{name: "spike", steps: []step{{2 * m, 5}, {15 * s, 30}, {5 * m, 5}}},
		{name: "burst", steps: []step{{2 * m, 5}, {3 * s, 150}, {3 * m, 5}}},
		{
			name:     "ramp-down",
			steps:    []step{{5 * m, 15}, {8 * m, 3}, {3 * m, 0}},
			longTail: true,
			dropAt:   5 * m,
			dropRate: 3,
			scaleAt:  13 * m,
		},
	}
}

func policies() []scheduling.Policy {
	return []scheduling.Policy{
		{Name: "p2c"},
		{Name: "random"},
		{Name: "mostallocated"},
		{Name: "bestofd", K: 3},
		{Name: "bestofd", K: 4},
		{Name: "topk", K: 3},
		{Name: "watermark", K: 3, Watermark: 0.8},
		{Name: "firstfit", K: 2},
		{Name: "oldestofd", K: 2},
		{Name: "oldestofd", K: 3},
		{Name: "oldestofd", K: 3, Watermark: 0.9},
		{Name: "prefix", K: 1, Watermark: 0.2},
		{Name: "prefixsat", K: 1, Watermark: 0.1},
		{Name: "prefixsat", K: 1, Watermark: 0.2},
		{Name: "prefixsat", K: 1, Watermark: 0.3},
		{Name: "prefixsat", K: 2, Watermark: 0.2},
		{Name: "prefixbest", K: 1, Watermark: 0.2},
	}
}

func policyName(p scheduling.Policy) string {
	switch p.Name {
	case "bestofd", "topk", "firstfit":
		return fmt.Sprintf("%s-%d", p.Name, p.K)
	case "oldestofd":
		if p.Watermark > 0 {
			return fmt.Sprintf("oldestofd-%d-wm%.0f", p.K, p.Watermark*100)
		}
		return fmt.Sprintf("oldestofd-%d", p.K)
	case "watermark":
		return fmt.Sprintf("watermark-%.0f", p.Watermark*100)
	case "prefix", "prefixsat", "prefixbest":
		return fmt.Sprintf("%s-%d-%.0f", p.Name, p.K, p.Watermark*100)
	}
	return p.Name
}

// ---- event queue ----

type event struct {
	at  time.Duration
	seq int64
	fn  func()
}

type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// ---- simulation state ----

type replica struct {
	workers []*ateapi.Worker
	byPod   map[string]*ateapi.Worker
	sched   scheduling.Scheduler
	// fallback is p2c over the whole fleet, for -retry-p2c.
	fallback scheduling.Scheduler
	phase    time.Duration
}

func (r *replica) Workers() ([]*ateapi.Worker, error) { return r.workers, nil }

type actor struct {
	id       int
	size     int
	limits   *ateapi.Resources
	awake    bool
	worker   string
	pending  bool
	firstReq time.Duration
	// lastNode and suspendedAt pin a suspended actor to the node holding its
	// snapshot until detachDelay passes.
	lastNode    string
	suspendedAt time.Duration
	// hold is this resume's hold time, drawn at arrival when -lifetime-hint
	// is set. long is the hint the scheduler gets.
	hold time.Duration
	long bool
}

type request struct {
	a         *actor
	attemptAt time.Duration
	attempt   int
	backoff   time.Duration
	sawNoCap  bool
	bindTries int
}

type sim struct {
	sc     scenario
	pat    pattern
	pol    scheduling.Policy
	rng    *rand.Rand
	now    time.Duration
	seq    int64
	events eventHeap

	truth     []*ateapi.Worker
	truthBy   map[string]*ateapi.Worker
	poolOf    map[string]string
	created   map[string]int
	nodeOf    map[string]string
	nodes     []string
	rowFree   map[string]time.Duration
	replicas  []*replica
	actors    []*actor
	sleeping  []*actor
	stepIdx   int
	stepGen   int
	endAt     time.Duration
	placeBins map[[2]int64]int // (worker idx, second) -> placements

	m metrics
}

type metrics struct {
	resumes        int
	placed         int
	noCapResponses int
	fragNoCap      int // ErrNoCapacity while the fleet had room in total for this actor
	aborted        int // bind retries exhausted
	failed         int // gave up after clientTimeout
	filledUp       int
	timeToPlace    []float64 // ms
	bindLatency    []float64 // ms, pick to commit of the attempt that bound
	waitedPlaced   int       // placed after at least one ErrNoCapacity
	scheduleCalls  int
	scheduleWall   time.Duration

	// time-averaged over samples taken while any actor is awake
	samples       int
	sumHeldRatio  float64
	heldSamples   int
	sumEmptyFrac  float64
	sumCV         float64
	sumHot        float64
	sumBigShare   float64
	bigSamples    int
	sumNonEmptyLB float64
	lbSamples     int

	// drain, for patterns with a drop
	emptyAtDrop      []int // empty workers at drop+0,+60s,+180s,+480s
	emptyNodesAtDrop []int // nodes whose workers are all empty, same times
	detaches         int
	lowHeldRatio     float64
	lowSamples       int
	scaleKeep        int
	scaleNewest      int // actors displaced deleting newest first
	scaleEmptiest    int // actors displaced deleting emptiest first
	emptyAtScale     int
	// the same scale-down, sized to -scale-target instead of the lower bound
	targetKeep      int
	targetNewest    int
	targetEmptiest  int
	removableNewest int // workers deletable newest first before one has an actor
	sumEmptyLow     float64

	peakPlacements int

	series [][]string
}

func newSim(sc scenario, pat pattern, pol scheduling.Policy, seed int64) *sim {
	s := &sim{
		sc:        sc,
		pat:       pat,
		pol:       pol,
		rng:       rand.New(rand.NewSource(seed)),
		truthBy:   map[string]*ateapi.Worker{},
		poolOf:    map[string]string{},
		created:   map[string]int{},
		nodeOf:    map[string]string{},
		rowFree:   map[string]time.Duration{},
		placeBins: map[[2]int64]int{},
	}
	// Interleave pool creation so firstfit's create_time order does not
	// favor one pool by construction.
	type slot struct {
		pool poolSpec
		i    int
	}
	var order []slot
	for _, p := range sc.pools {
		for i := range p.workers {
			order = append(order, slot{p, i})
		}
	}
	s.rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	nodeFor := func(idx int) string {
		if *workersPerNode <= 0 {
			return "node-0"
		}
		return fmt.Sprintf("node-%03d", idx / *workersPerNode)
	}
	for idx, o := range order {
		pod := fmt.Sprintf("%s-%04d", o.pool.name, o.i)
		w := mkWorker(pod, o.pool, base.Add(time.Duration(idx)*time.Second), nodeFor(idx))
		if !slices.Contains(s.nodes, w.NodeName) {
			s.nodes = append(s.nodes, w.NodeName)
		}
		s.nodeOf[pod] = w.NodeName
		s.truth = append(s.truth, w)
		s.truthBy[pod] = w
		s.poolOf[pod] = o.pool.name
		s.created[pod] = idx
	}
	for range *replicasN {
		r := &replica{byPod: map[string]*ateapi.Worker{}, phase: time.Duration(s.rng.Int63n(int64(*pollEvery)))}
		for idx, o := range order {
			pod := fmt.Sprintf("%s-%04d", o.pool.name, o.i)
			w := mkWorker(pod, o.pool, base.Add(time.Duration(idx)*time.Second), nodeFor(idx))
			r.workers = append(r.workers, w)
			r.byPod[pod] = w
		}
		r.sched = scheduling.New(r, scheduling.WithIntn(s.rng.Intn), scheduling.WithPolicy(pol))
		r.fallback = scheduling.New(r, scheduling.WithIntn(s.rng.Intn))
		s.replicas = append(s.replicas, r)
	}
	for i := range sc.population {
		size := pickSize(sc.sizes, float64(i)/float64(sc.population))
		a := &actor{id: i, size: size, limits: &ateapi.Resources{CpuMillicores: sc.sizes[size].cpu, MemoryBytes: sc.sizes[size].mem}}
		s.actors = append(s.actors, a)
		s.sleeping = append(s.sleeping, a)
	}
	for _, st := range pat.steps {
		s.endAt += st.dur
	}
	return s
}

func pickSize(sizes []actorSize, q float64) int {
	acc := 0.0
	for i, sz := range sizes {
		acc += sz.frac
		if q < acc {
			return i
		}
	}
	return len(sizes) - 1
}

func mkWorker(pod string, p poolSpec, created time.Time, node string) *ateapi.Worker {
	return &ateapi.Worker{
		Metadata:   &ateapi.ResourceMetadata{CreateTime: timestamppb.New(created)},
		WorkerPool: p.name,
		WorkerPod:  pod,
		NodeName:   node,
		Labels:     map[string]string{"workload": "benchmark-worker", "pool": p.name},
		Status: &ateapi.WorkerStatus{
			State:           ateapi.WorkerState_WORKER_STATE_ACTIVE,
			RegisteredEpoch: 1,
			Capacity:        &ateapi.WorkerResources{Actors: p.slots, CpuMillicores: p.cpu, MemoryBytes: p.mem},
			SandboxClasses: []*ateapi.WorkerSandboxClass{{
				SandboxClass:   "gvisor",
				DefaultRuntime: &ateapi.WorkerSandboxRuntime{},
			}},
		},
	}
}

func (s *sim) at(t time.Duration, fn func()) {
	s.seq++
	heap.Push(&s.events, &event{at: t, seq: s.seq, fn: fn})
}

func (s *sim) constraints(a *actor) scheduling.Constraints {
	c := scheduling.Constraints{ActorSelector: s.sc.selector, Limits: a.limits, LongLived: a.long}
	if s.pinned(a) {
		c.RequiredNodes = []string{a.lastNode}
	}
	return c
}

func (s *sim) pinned(a *actor) bool {
	return *workersPerNode > 0 && a.lastNode != "" && s.now-a.suspendedAt < *detachDelay
}

func (s *sim) replica() int { return s.rng.Intn(len(s.replicas)) }

func apply(w *ateapi.Worker, booking *ateapi.Resources, sign int64) {
	w.Status.Allocated = resources.AddToAllocated(w.Status.Allocated, &ateapi.ActorAssignment{Resources: booking}, sign)
}

// commit applies a delta to the truth, to the replica that made it at once,
// and to the other replica on its next poll.
//
// Replicas cache whole rows, as workercache does: a delivered row replaces the
// cached one only when its version is newer.
func (s *sim) commit(pod string, booking *ateapi.Resources, sign int64, by int) {
	tw := s.truthBy[pod]
	apply(tw, booking, sign)
	tw.Metadata.Version++
	alloc, version := cloneAlloc(tw.Status.Allocated), tw.Metadata.Version
	observe(s.replicas[by].byPod[pod], alloc, version)
	poll := *pollEvery
	for i, other := range s.replicas {
		if i == by {
			continue
		}
		next := other.phase + ((s.now-other.phase)/poll+1)*poll
		s.at(next, func() { observe(other.byPod[pod], alloc, version) })
	}
}

func cloneAlloc(a *ateapi.WorkerResources) *ateapi.WorkerResources {
	if a == nil {
		return nil
	}
	return &ateapi.WorkerResources{CpuMillicores: a.CpuMillicores, MemoryBytes: a.MemoryBytes, Actors: a.Actors}
}

func observe(w *ateapi.Worker, alloc *ateapi.WorkerResources, version int64) {
	if version > w.Metadata.Version {
		w.Status.Allocated = alloc
		w.Metadata.Version = version
	}
}

func (s *sim) run() {
	s.startStep(0)
	for t := time.Duration(0); t <= s.endAt; t += sampleEvery {
		s.at(t, s.sample)
	}
	if s.pat.dropAt > 0 {
		for _, off := range []time.Duration{0, time.Minute, 3 * time.Minute, 8 * time.Minute} {
			s.at(s.pat.dropAt+off, func() {
				s.m.emptyAtDrop = append(s.m.emptyAtDrop, s.emptyWorkers())
				s.m.emptyNodesAtDrop = append(s.m.emptyNodesAtDrop, s.emptyNodes())
			})
		}
	}
	scaleAt := s.pat.scaleAt
	if scaleAt == 0 {
		scaleAt = s.endAt
	}
	s.at(scaleAt, s.scaleDown)
	for s.events.Len() > 0 {
		e := heap.Pop(&s.events).(*event)
		if e.at > s.endAt+clientTimeout {
			break
		}
		s.now = e.at
		e.fn()
	}
	for _, n := range s.placeBins {
		s.m.peakPlacements = max(s.m.peakPlacements, n)
	}
}

func (s *sim) startStep(i int) {
	s.stepGen++
	if i >= len(s.pat.steps) {
		return
	}
	s.stepIdx = i
	gen := s.stepGen
	stepEnd := s.now + s.pat.steps[i].dur
	s.at(stepEnd, func() { s.startStep(i + 1) })
	s.scheduleArrival(gen, stepEnd)
}

func (s *sim) scheduleArrival(gen int, stepEnd time.Duration) {
	rate := s.pat.steps[s.stepIdx].rate * s.sc.rateScale * s.longTailScale()
	if rate <= 0 {
		return
	}
	next := s.now + time.Duration(s.rng.ExpFloat64()/rate*float64(time.Second))
	if next >= stepEnd {
		return
	}
	s.at(next, func() {
		if gen != s.stepGen {
			return
		}
		s.arrive()
		s.scheduleArrival(gen, stepEnd)
	})
}

func (s *sim) arrive() {
	if len(s.sleeping) == 0 {
		return
	}
	i := s.rng.Intn(len(s.sleeping))
	a := s.sleeping[i]
	s.sleeping[i] = s.sleeping[len(s.sleeping)-1]
	s.sleeping = s.sleeping[:len(s.sleeping)-1]
	a.pending = true
	a.firstReq = s.now
	if *lifetimeHint > 0 {
		a.hold = s.hold()
		a.long = a.hold >= *longAfter
		if s.rng.Float64() > *lifetimeHint {
			a.long = !a.long
		}
	}
	s.m.resumes++
	s.attempt(&request{a: a}, s.replica())
}

func (s *sim) attempt(req *request, rep int) {
	r := s.replicas[rep]
	c := s.constraints(req.a)
	req.attemptAt = s.now
	start := time.Now()
	sched := r.sched
	if *retryP2C && req.backoff > 0 {
		sched = r.fallback
	}
	w, err := sched.Schedule(context.Background(), c)
	s.m.scheduleWall += time.Since(start)
	s.m.scheduleCalls++
	if errors.Is(err, scheduling.ErrNoCapacity) && len(c.RequiredNodes) > 0 {
		// detachFromFullNode: when another node has room, atelet drops the
		// local snapshot and the resume continues unpinned.
		free := c
		free.RequiredNodes = nil
		if _, ferr := r.sched.Schedule(context.Background(), free); ferr == nil {
			s.m.detaches++
			req.a.lastNode = ""
			s.at(s.now+*detachCost, func() { s.attempt(req, rep) })
			return
		}
	}
	if errors.Is(err, scheduling.ErrNoCapacity) {
		s.m.noCapResponses++
		if s.fleetHasRoomFor(req.a) {
			s.m.fragNoCap++
		}
		req.sawNoCap = true
		s.clientRetry(req)
		return
	}
	if err != nil {
		panic(err)
	}
	pod := w.GetWorkerPod()
	commitAt := max(s.now+*bindBase, s.rowFree[pod]) + *bindRow
	s.rowFree[pod] = commitAt
	s.at(commitAt, func() { s.bind(req, rep, pod) })
}

func (s *sim) clientRetry(req *request) {
	if s.now+clientRetry-req.a.firstReq > clientTimeout {
		s.m.failed++
		s.wake(req.a, false)
		return
	}
	s.at(s.now+clientRetry, func() {
		req.attempt, req.backoff = 0, 0
		s.attempt(req, s.replica())
	})
}

// fleetHasRoomFor reports whether the summed free capacity of eligible
// workers would admit a, so a no-capacity answer is due to fragmentation.
func (s *sim) fleetHasRoomFor(a *actor) bool {
	var slots int64
	var cpu, mem int64
	for _, w := range s.truth {
		cap, used := w.Status.Capacity, w.Status.Allocated
		slots += int64(cap.Actors - used.GetActors())
		cpu += cap.CpuMillicores - used.GetCpuMillicores()
		mem += cap.MemoryBytes - used.GetMemoryBytes()
	}
	return slots >= 1 && cpu >= a.limits.CpuMillicores && mem >= a.limits.MemoryBytes
}

func (s *sim) bind(req *request, rep int, pod string) {
	r := s.replicas[rep]
	c := s.constraints(req.a)
	tw := s.truthBy[pod]
	if !r.sched.HasRoom(tw, c) {
		if *observeOnFill {
			observe(r.byPod[pod], cloneAlloc(tw.Status.Allocated), tw.Metadata.Version)
		}
		s.m.filledUp++
		req.bindTries++
		// assignmentBackoff: 15ms doubling with full jitter, ending once the
		// delay passes its 250ms cap.
		if req.backoff == 0 {
			req.backoff = 15 * time.Millisecond
		}
		d := req.backoff
		if d > 250*time.Millisecond {
			s.m.aborted++
			s.clientRetry(req)
			return
		}
		req.backoff *= 2
		jittered := d + time.Duration(s.rng.Float64()*float64(d))
		if *fillNoWait {
			jittered = 0
		}
		s.at(s.now+jittered, func() { s.attempt(req, rep) })
		return
	}
	booking, _ := scheduling.Booking(tw, c)
	s.commit(pod, booking, +1, rep)
	a := req.a
	a.worker = pod
	s.m.placed++
	if req.sawNoCap {
		s.m.waitedPlaced++
	}
	s.m.timeToPlace = append(s.m.timeToPlace, float64(s.now-a.firstReq)/float64(time.Millisecond))
	s.m.bindLatency = append(s.m.bindLatency, float64(s.now-req.attemptAt)/float64(time.Millisecond))
	s.placeBins[[2]int64{int64(s.created[pod]), int64(s.now / time.Second)}]++
	s.wake(a, true)
	hold := a.hold
	if *lifetimeHint == 0 {
		hold = s.hold()
	}
	s.at(s.now+hold, func() { s.suspend(a, booking) })
}

func (s *sim) hold() time.Duration {
	if *holdDist == "lognormal" {
		const sigma = 1.5
		median := 30 * time.Second.Seconds() / math.Exp(sigma*sigma/2)
		h := time.Duration(median * math.Exp(sigma*s.rng.NormFloat64()) * float64(time.Second))
		return min(h, 30*time.Minute)
	}
	h := 20*time.Second + time.Duration(s.rng.Float64()*float64(20*time.Second))
	if s.pat.longTail && s.rng.Float64() < *longFrac {
		h = 5*time.Minute + time.Duration(s.rng.Float64()*float64(5*time.Minute))
	}
	return h
}

// longTailScale keeps ramp-down's occupancy near the default as long-frac
// changes: awake actors grow with 30s*(1-f) + 300s*f per resume/s.
func (s *sim) longTailScale() float64 {
	if !s.pat.longTail || *holdDist == "lognormal" {
		return 1
	}
	return (30 + 270*0.03) / (30 + 270**longFrac)
}

func (s *sim) emptyNodes() int {
	busy := map[string]bool{}
	for _, w := range s.truth {
		if w.Status.Allocated.GetActors() > 0 {
			busy[w.NodeName] = true
		}
	}
	return len(s.nodes) - len(busy)
}

func (s *sim) wake(a *actor, placed bool) {
	a.pending = false
	a.awake = placed
	if !placed {
		s.sleeping = append(s.sleeping, a)
	}
}

func (s *sim) suspend(a *actor, booking *ateapi.Resources) {
	s.commit(a.worker, booking, -1, s.replica())
	a.lastNode = s.nodeOf[a.worker]
	a.suspendedAt = s.now
	a.worker = ""
	a.awake = false
	s.sleeping = append(s.sleeping, a)
}

func (s *sim) emptyWorkers() int {
	n := 0
	for _, w := range s.truth {
		if w.Status.Allocated.GetActors() == 0 {
			n++
		}
	}
	return n
}

// heldRatio is the capacity held by non-empty workers over what is
// allocated, in the most-utilized dimension. 1 is perfect packing.
func (s *sim) heldRatio() (float64, bool) {
	var heldS, allocS, heldC, allocC, heldM, allocM float64
	for _, w := range s.truth {
		used := w.Status.Allocated
		if used.GetActors() == 0 {
			continue
		}
		cap := w.Status.Capacity
		heldS += float64(cap.Actors)
		heldC += float64(cap.CpuMillicores)
		heldM += float64(cap.MemoryBytes)
		allocS += float64(used.GetActors())
		allocC += float64(used.GetCpuMillicores())
		allocM += float64(used.GetMemoryBytes())
	}
	if allocS == 0 {
		return 0, false
	}
	return min(heldS/allocS, heldC/allocC, heldM/allocM), true
}

// lowerBound is, per pool, the fewest workers that could hold that pool's
// allocation.
func (s *sim) lowerBound() map[string]int {
	type tot struct{ s, c, m float64 }
	sum := map[string]*tot{}
	for _, w := range s.truth {
		p := s.poolOf[w.WorkerPod]
		if sum[p] == nil {
			sum[p] = &tot{}
		}
		u := w.Status.Allocated
		sum[p].s += float64(u.GetActors())
		sum[p].c += float64(u.GetCpuMillicores())
		sum[p].m += float64(u.GetMemoryBytes())
	}
	out := map[string]int{}
	for _, ps := range s.sc.pools {
		t := sum[ps.name]
		out[ps.name] = int(math.Ceil(max(t.s/float64(ps.slots), t.c/float64(ps.cpu), t.m/float64(ps.mem)) - 1e-9))
	}
	return out
}

func (s *sim) sample() {
	var awake int
	var nonEmpty int
	var utils []float64
	var hot, bigActors, allActors float64
	for _, w := range s.truth {
		u := w.Status.Allocated
		cap := w.Status.Capacity
		n := u.GetActors()
		awake += int(n)
		if n > 0 {
			nonEmpty++
		}
		util := max(float64(n)/float64(cap.Actors), float64(u.GetCpuMillicores())/float64(cap.CpuMillicores), float64(u.GetMemoryBytes())/float64(cap.MemoryBytes))
		utils = append(utils, util)
		if util >= 0.9 {
			hot += float64(n)
		}
		allActors += float64(n)
		if s.poolOf[w.WorkerPod] == "big" {
			bigActors += float64(n)
		}
	}
	lb := 0
	for _, v := range s.lowerBound() {
		lb += v
	}
	held, ok := s.heldRatio()
	s.m.series = append(s.m.series, []string{
		strconv.Itoa(int(s.now / time.Second)), strconv.Itoa(awake), strconv.Itoa(nonEmpty), strconv.Itoa(lb), strconv.Itoa(len(s.truth) - nonEmpty), fmt.Sprintf("%.3f", held),
	})
	if awake == 0 {
		return
	}
	s.m.samples++
	if ok {
		s.m.sumHeldRatio += held
		s.m.heldSamples++
		if s.pat.dropAt > 0 && s.now >= s.pat.dropAt+time.Minute && s.now <= s.pat.scaleAt {
			s.m.lowHeldRatio += held
			s.m.lowSamples++
			s.m.sumEmptyLow += float64(len(s.truth) - nonEmpty)
		}
	}
	if lb > 0 {
		s.m.sumNonEmptyLB += float64(nonEmpty) / float64(lb)
		s.m.lbSamples++
	}
	s.m.sumEmptyFrac += float64(len(s.truth)-nonEmpty) / float64(len(s.truth))
	mean, sd := meanSD(utils)
	if mean > 0 {
		s.m.sumCV += sd / mean
	}
	s.m.sumHot += hot / allActors
	if len(s.sc.pools) > 1 {
		s.m.sumBigShare += bigActors / allActors
		s.m.bigSamples++
	}
}

// scaleDown simulates shrinking each pool to its lower bound plus one, and
// counts the actors on the workers removed, newest first (the ReplicaSet
// default) and emptiest first (what pod-deletion-cost would give).
func (s *sim) scaleDown() {
	lb := s.lowerBound()
	s.m.emptyAtScale = s.emptyWorkers()
	for _, ps := range s.sc.pools {
		keep := lb[ps.name] + 1
		s.m.scaleKeep += keep
		var ws []*ateapi.Worker
		for _, w := range s.truth {
			if s.poolOf[w.WorkerPod] == ps.name {
				ws = append(ws, w)
			}
		}
		remove := max(len(ws)-keep, 0)
		newest := slices.Clone(ws)
		sort.Slice(newest, func(i, j int) bool { return s.created[newest[i].WorkerPod] > s.created[newest[j].WorkerPod] })
		emptiest := slices.Clone(ws)
		sort.Slice(emptiest, func(i, j int) bool {
			return emptiest[i].Status.Allocated.GetActors() < emptiest[j].Status.Allocated.GetActors()
		})
		for i := range remove {
			s.m.scaleNewest += int(newest[i].Status.Allocated.GetActors())
			s.m.scaleEmptiest += int(emptiest[i].Status.Allocated.GetActors())
		}
		for _, w := range newest {
			if w.Status.Allocated.GetActors() > 0 {
				break
			}
			s.m.removableNewest++
		}
		target := min(len(ws), int(math.Ceil(float64(lb[ps.name]) / *scaleTarget)))
		s.m.targetKeep += target
		for i := range len(ws) - target {
			s.m.targetNewest += int(newest[i].Status.Allocated.GetActors())
			s.m.targetEmptiest += int(emptiest[i].Status.Allocated.GetActors())
		}
	}
}

func meanSD(xs []float64) (float64, float64) {
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(len(xs))
	var v float64
	for _, x := range xs {
		v += (x - mean) * (x - mean)
	}
	return mean, math.Sqrt(v / float64(len(xs)))
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return math.NaN()
	}
	ys := slices.Clone(xs)
	slices.Sort(ys)
	return ys[min(len(ys)-1, int(p*float64(len(ys))))]
}

// ---- driver ----

type result struct {
	scenario, pattern, policy string
	seed                      int
	row                       map[string]float64
	series                    [][]string
}

var columns = []string{
	"resumes", "placed", "no_capacity", "frag_no_capacity", "aborted", "failed", "waited_placed",
	"bind_retries_per_resume", "bind_p99_ms", "ttp_p50_ms", "ttp_p95_ms", "ttp_p99_ms",
	"held_ratio", "nonempty_over_lb", "empty_frac", "util_cv", "hot_frac", "big_pool_share",
	"low_held_ratio", "empty_drop_0", "empty_drop_60", "empty_drop_180", "empty_drop_480",
	"empty_at_scale", "scale_keep", "displaced_newest", "displaced_emptiest",
	"peak_placements_per_worker_sec", "schedule_us",
	"detaches", "nodes", "empty_nodes_drop_180", "empty_nodes_drop_480",
	"ttp_p999_ms", "ttp_max_ms", "empty_low", "target_keep", "target_displaced_newest", "target_displaced_emptiest", "removable_newest",
}

func (s *sim) result(seed int) result {
	m := &s.m
	f := func(a, b float64) float64 {
		if b == 0 {
			return math.NaN()
		}
		return a / b
	}
	row := map[string]float64{
		"resumes":                        float64(m.resumes),
		"placed":                         float64(m.placed),
		"no_capacity":                    float64(m.noCapResponses),
		"frag_no_capacity":               float64(m.fragNoCap),
		"aborted":                        float64(m.aborted),
		"failed":                         float64(m.failed),
		"waited_placed":                  float64(m.waitedPlaced),
		"bind_retries_per_resume":        f(float64(m.filledUp), float64(m.resumes)),
		"bind_p99_ms":                    pct(m.bindLatency, 0.99),
		"ttp_p50_ms":                     pct(m.timeToPlace, 0.5),
		"ttp_p95_ms":                     pct(m.timeToPlace, 0.95),
		"ttp_p99_ms":                     pct(m.timeToPlace, 0.99),
		"held_ratio":                     f(m.sumHeldRatio, float64(m.heldSamples)),
		"nonempty_over_lb":               f(m.sumNonEmptyLB, float64(m.lbSamples)),
		"empty_frac":                     f(m.sumEmptyFrac, float64(m.samples)),
		"util_cv":                        f(m.sumCV, float64(m.samples)),
		"hot_frac":                       f(m.sumHot, float64(m.samples)),
		"big_pool_share":                 f(m.sumBigShare, float64(m.bigSamples)),
		"low_held_ratio":                 f(m.lowHeldRatio, float64(m.lowSamples)),
		"empty_at_scale":                 float64(m.emptyAtScale),
		"scale_keep":                     float64(m.scaleKeep),
		"displaced_newest":               float64(m.scaleNewest),
		"displaced_emptiest":             float64(m.scaleEmptiest),
		"peak_placements_per_worker_sec": float64(m.peakPlacements),
		"schedule_us":                    f(float64(m.scheduleWall/time.Microsecond), float64(m.scheduleCalls)),
		"detaches":                       float64(m.detaches),
		"nodes":                          float64(len(s.nodes)),
		"ttp_p999_ms":                    pct(m.timeToPlace, 0.999),
		"ttp_max_ms":                     pct(m.timeToPlace, 1),
		"empty_low":                      f(m.sumEmptyLow, float64(m.lowSamples)),
		"target_keep":                    float64(m.targetKeep),
		"target_displaced_newest":        float64(m.targetNewest),
		"target_displaced_emptiest":      float64(m.targetEmptiest),
		"removable_newest":               float64(m.removableNewest),
		"empty_nodes_drop_180":           math.NaN(),
		"empty_nodes_drop_480":           math.NaN(),
	}
	if len(m.emptyNodesAtDrop) == 4 {
		row["empty_nodes_drop_180"] = float64(m.emptyNodesAtDrop[2])
		row["empty_nodes_drop_480"] = float64(m.emptyNodesAtDrop[3])
	}
	for i, k := range []string{"empty_drop_0", "empty_drop_60", "empty_drop_180", "empty_drop_480"} {
		row[k] = math.NaN()
		if i < len(m.emptyAtDrop) {
			row[k] = float64(m.emptyAtDrop[i])
		}
	}
	return result{s.sc.name, s.pat.name, policyName(s.pol), seed, row, m.series}
}

func main() {
	flag.Parse()
	if *kindMode {
		runKind(kindFlags{kubeconfig: *kubeconfig, context: *kubeCtx, scenario: *kScenario, pattern: *kPattern, policy: *kPolicy, atespace: *kAtespace, out: *outDir, setup: *kindSetup, seed: *kSeed})
		return
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		panic(err)
	}
	type job struct {
		sc   scenario
		pat  pattern
		pol  scheduling.Policy
		seed int
	}
	var jobs []job
	want := map[string]bool{}
	for _, n := range strings.Split(*only, ",") {
		if n != "" {
			want[n] = true
		}
	}
	for _, sc := range scenarios() {
		if len(want) > 0 && !want[sc.name] {
			continue
		}
		n := *seeds
		if sc.name == "d-large" {
			n = *largeSeeds
		}
		for _, pat := range patterns() {
			if *onlyPatterns != "" && !slices.Contains(strings.Split(*onlyPatterns, ","), pat.name) {
				continue
			}
			for _, pol := range policies() {
				if *onlyPolicies != "" && !slices.Contains(strings.Split(*onlyPolicies, ","), policyName(pol)) {
					continue
				}
				for seed := range n {
					jobs = append(jobs, job{sc, pat, pol, seed})
				}
			}
		}
	}
	results := make([]result, len(jobs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	var mu sync.Mutex
	done := 0
	for i, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s := newSim(j.sc, j.pat, j.pol, int64(j.seed)+1)
			s.run()
			results[i] = s.result(j.seed)
			if j.seed != 0 {
				results[i].series = nil
			}
			mu.Lock()
			done++
			if done%100 == 0 {
				fmt.Fprintf(os.Stderr, "%d/%d\n", done, len(jobs))
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	writeRuns(results)
	writeSeries(results)
	writeSummary(results)
}

func writeRuns(results []result) {
	f, err := os.Create(filepath.Join(*outDir, "runs.csv"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write(append([]string{"scenario", "pattern", "policy", "seed"}, columns...))
	for _, r := range results {
		rec := []string{r.scenario, r.pattern, r.policy, strconv.Itoa(r.seed)}
		for _, c := range columns {
			rec = append(rec, strconv.FormatFloat(r.row[c], 'f', 4, 64))
		}
		w.Write(rec)
	}
	w.Flush()
}

func writeSeries(results []result) {
	f, err := os.Create(filepath.Join(*outDir, "series.csv"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"scenario", "pattern", "policy", "t", "awake", "nonempty", "lower_bound", "empty", "held_ratio"})
	for _, r := range results {
		for _, row := range r.series {
			w.Write(append([]string{r.scenario, r.pattern, r.policy}, row...))
		}
	}
	w.Flush()
}

// writeSummary writes median [min-max] across seeds per cell.
func writeSummary(results []result) {
	type key struct{ sc, pat, pol string }
	groups := map[key][]result{}
	var keys []key
	for _, r := range results {
		k := key{r.scenario, r.pattern, r.policy}
		if _, ok := groups[k]; !ok {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], r)
	}
	f, err := os.Create(filepath.Join(*outDir, "summary.csv"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	hdr := []string{"scenario", "pattern", "policy"}
	for _, c := range columns {
		hdr = append(hdr, c, c+"_min", c+"_max")
	}
	w.Write(hdr)
	for _, k := range keys {
		rec := []string{k.sc, k.pat, k.pol}
		for _, c := range columns {
			var xs []float64
			for _, r := range groups[k] {
				if v := r.row[c]; !math.IsNaN(v) {
					xs = append(xs, v)
				}
			}
			if len(xs) == 0 {
				rec = append(rec, "", "", "")
				continue
			}
			slices.Sort(xs)
			rec = append(rec, fmtF(pct(xs, 0.5)), fmtF(xs[0]), fmtF(xs[len(xs)-1]))
		}
		w.Write(rec)
	}
	w.Flush()
}

func fmtF(v float64) string { return strconv.FormatFloat(v, 'f', 3, 64) }
