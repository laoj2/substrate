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

package main

// kind mode drives the same patterns against a live ate-api-server, one
// client per replica through a pod port-forward, and polls ListWorkers for
// the fleet metrics. EXPERIMENT ONLY.

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"maps"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/portforward"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

type kindFlags struct {
	kubeconfig, context, scenario, pattern, policy, atespace, out string
	setup                                                         bool
	seed                                                          int64
}

func scenarioByName(n string) scenario {
	for _, s := range scenarios() {
		if s.name == n {
			return s
		}
	}
	log.Fatalf("unknown scenario %q", n)
	return scenario{}
}

func patternByName(n string) pattern {
	if n == "smoke" {
		return pattern{name: "smoke", steps: []step{{time.Minute, 5}}}
	}
	for _, p := range patterns() {
		if p.name == n {
			return p
		}
	}
	log.Fatalf("unknown pattern %q", n)
	return pattern{}
}

// templateFor names the ActorTemplate kind-setup.sh creates for a size.
func templateFor(sz actorSize) string { return fmt.Sprintf("sched-%dm", sz.cpu) }

const templateAtespace = "benchmark-workloads"

type kindRun struct {
	f       kindFlags
	sc      scenario
	pat     pattern
	clients []*ateclient.Client
	rng     *rand.Rand
	rngMu   sync.Mutex

	mu       sync.Mutex
	sleeping []kindActor

	resumes, placed, noCap, waitedPlaced, failed, otherErr atomic.Int64
	ttpMu                                                  sync.Mutex
	ttp                                                    []float64
	errCodes                                               sync.Map // code -> *atomic.Int64

	seriesMu sync.Mutex
	series   [][]string
	nodes    [][]string
	stats    kindAgg

	resumeMu  sync.Mutex
	resumeLog [][]string
}

// logResume records one resume so its latency can be joined with server logs
// by actor and time.
func (r *kindRun) logResume(a kindActor, start time.Time, outcome string) {
	r.resumeMu.Lock()
	defer r.resumeMu.Unlock()
	r.resumeLog = append(r.resumeLog, []string{a.name, strconv.FormatInt(start.UnixMilli(), 10),
		fmt.Sprintf("%.1f", float64(time.Since(start))/float64(time.Millisecond)), outcome})
}

type kindActor struct {
	name string
	size int
}

type kindAgg struct {
	samples, heldSamples, lbSamples, bigSamples, lowSamples int
	sumHeld, sumNonEmptyLB, sumEmptyFrac, sumCV, sumHot     float64
	sumBig, lowHeld                                         float64
	emptyAtDrop                                             []int
	emptyAtScale, scaleKeep, scaleNewest, scaleEmptiest     int
}

func (r *kindRun) intn(n int) int {
	r.rngMu.Lock()
	defer r.rngMu.Unlock()
	return r.rng.Intn(n)
}

func (r *kindRun) float() float64 {
	r.rngMu.Lock()
	defer r.rngMu.Unlock()
	return r.rng.Float64()
}

func (r *kindRun) client() *ateclient.Client { return r.clients[r.intn(len(r.clients))] }

func runKind(f kindFlags) {
	ctx := context.Background()
	r := &kindRun{f: f, sc: scenarioByName(f.scenario), rng: rand.New(rand.NewSource(f.seed))}
	if !f.setup {
		r.pat = patternByName(f.pattern)
	}
	stop := r.connect(ctx)
	defer stop()
	if f.setup {
		r.setUp(ctx)
		return
	}
	r.reset(ctx)
	r.run(ctx)
	r.write()
}

// connect port-forwards to every ate-api-server pod and dials each, so
// resumes spread across replicas as they do behind the Service.
func (r *kindRun) connect(ctx context.Context) func() {
	cfg, err := ateclient.LoadKubeConfig(r.f.kubeconfig, r.f.context)
	if err != nil {
		log.Fatal(err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	svc, err := cs.CoreV1().Services("ate-system").Get(ctx, installdefaults.APIServiceName, metav1.GetOptions{})
	if err != nil {
		log.Fatal(err)
	}
	sel := metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: svc.Spec.Selector})
	pods, err := cs.CoreV1().Pods("ate-system").List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		log.Fatal(err)
	}
	var svcPort corev1.ServicePort
	for _, p := range svc.Spec.Ports {
		if p.Port == 443 {
			svcPort = p
		}
	}
	var stops []func()
	for _, p := range pods.Items {
		if !portforward.IsPodReady(&p) {
			continue
		}
		target := svcPort.TargetPort.IntVal
		if svcPort.TargetPort.Type == intstr.String {
			for _, c := range p.Spec.Containers {
				for _, cp := range c.Ports {
					if cp.Name == svcPort.TargetPort.StrVal {
						target = cp.ContainerPort
					}
				}
			}
		}
		if target == 0 {
			target = svcPort.Port
		}
		port, stopPF, err := portforward.PodPortForward(ctx, cfg, cs, "ate-system", p.Name, target)
		if err != nil {
			log.Fatal(err)
		}
		c, err := ateclient.NewClient(ctx, r.f.kubeconfig, r.f.context, fmt.Sprintf("127.0.0.1:%d", port), "", false)
		if err != nil {
			log.Fatal(err)
		}
		r.clients = append(r.clients, c)
		stops = append(stops, func() { c.Close(); stopPF() })
	}
	if len(r.clients) == 0 {
		log.Fatal("no ready ate-api-server pods")
	}
	log.Printf("connected to %d ate-api-server replicas", len(r.clients))
	return func() {
		for _, s := range stops {
			s()
		}
	}
}

func (r *kindRun) actorName(i int) string { return fmt.Sprintf("se-%05d", i) }

func (r *kindRun) ref(name string) *ateapi.ObjectRef {
	return &ateapi.ObjectRef{Atespace: r.f.atespace, Name: name}
}

func (r *kindRun) actorSizes() []kindActor {
	var out []kindActor
	for i := range r.sc.population {
		out = append(out, kindActor{r.actorName(i), pickSize(r.sc.sizes, float64(i)/float64(r.sc.population))})
	}
	return out
}

// setUp creates the atespace and the population, and makes every actor cold
// with one resume and suspend.
func (r *kindRun) setUp(ctx context.Context) {
	_, err := r.clients[0].CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: r.f.atespace}}})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatal(err)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	var done atomic.Int64
	for _, a := range r.actorSizes() {
		g.Go(func() error {
			c := r.client()
			_, err := c.CreateActor(gctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: r.f.atespace, Name: a.name},
				ActorTemplate: &ateapi.ObjectRef{Atespace: templateAtespace, Name: templateFor(r.sc.sizes[a.size])},
			}})
			if err != nil && status.Code(err) != codes.AlreadyExists {
				return fmt.Errorf("create %s: %w", a.name, err)
			}
			if n := done.Add(1); n%100 == 0 {
				log.Printf("created %d/%d", n, r.sc.population)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
	// reset brings never-run actors through one resume and suspend.
	r.reset(ctx)
}

// reset suspends every actor that is not suspended, resuming the ones that
// never ran so they hold a snapshot, and waits until all are SUSPENDED.
func (r *kindRun) reset(ctx context.Context) {
	for round := 0; ; round++ {
		actors := r.listActors(ctx)
		var notReady []*ateapi.Actor
		for _, a := range actors {
			if a.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
				notReady = append(notReady, a)
			}
		}
		log.Printf("reset round %d: %d/%d actors not suspended", round, len(notReady), len(actors))
		if len(notReady) == 0 {
			break
		}
		if round > 30 {
			log.Fatalf("%d actors still not suspended", len(notReady))
		}
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(16)
		for _, a := range notReady {
			g.Go(func() error {
				c := r.client()
				name := a.GetMetadata().GetName()
				switch a.GetStatus().GetState() {
				case ateapi.ActorState_ACTOR_STATE_RUNNING:
					_, _ = c.SuspendActor(gctx, &ateapi.SuspendActorRequest{Actor: r.ref(name)})
				case ateapi.ActorState_ACTOR_STATE_RESUMING, ateapi.ActorState_ACTOR_STATE_SUSPENDING:
				default:
					if _, err := c.ResumeActor(gctx, &ateapi.ResumeActorRequest{Actor: r.ref(name)}); err == nil {
						_, _ = c.SuspendActor(gctx, &ateapi.SuspendActorRequest{Actor: r.ref(name)})
					}
				}
				return nil
			})
		}
		_ = g.Wait()
		time.Sleep(2 * time.Second)
	}
	workers := r.listWorkers(ctx)
	for _, w := range workers {
		if n := w.GetStatus().GetAllocated().GetActors(); n != 0 {
			log.Fatalf("worker %s still has %d actors allocated after reset", w.GetWorkerPod(), n)
		}
	}
	log.Printf("reset done: %d workers, all empty", len(workers))
}

func (r *kindRun) listActors(ctx context.Context) []*ateapi.Actor {
	var out []*ateapi.Actor
	token := ""
	for {
		resp, err := r.clients[0].ListActors(ctx, &ateapi.ListActorsRequest{Atespace: r.f.atespace, Options: &ateapi.ListOptions{PageSize: 500, PageToken: token}})
		if err != nil {
			log.Fatal(err)
		}
		out = append(out, resp.GetActors()...)
		if token = resp.GetNextPageToken(); token == "" {
			return out
		}
	}
}

func (r *kindRun) listWorkers(ctx context.Context) []*ateapi.Worker {
	var out []*ateapi.Worker
	token := ""
	for {
		resp, err := r.clients[0].ListWorkers(ctx, &ateapi.ListWorkersRequest{Options: &ateapi.ListOptions{PageSize: 500, PageToken: token}})
		if err != nil {
			log.Printf("list workers: %v", err)
			return nil
		}
		for _, w := range resp.GetWorkers() {
			if w.GetStatus().GetState() == ateapi.WorkerState_WORKER_STATE_ACTIVE {
				out = append(out, w)
			}
		}
		if token = resp.GetNextPageToken(); token == "" {
			return out
		}
	}
}

// warmUp resumes and suspends one actor until a resume succeeds, so the
// load does not start while the data plane still dials replaced ateapi pods.
func (r *kindRun) warmUp(ctx context.Context) {
	ref := r.ref(r.actorName(0))
	for attempt := 0; ; attempt++ {
		_, err := r.client().ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref})
		if err == nil {
			if _, err := r.client().SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref}); err != nil {
				log.Fatalf("warm-up suspend: %v", err)
			}
			log.Printf("warm-up resume succeeded after %d failures", attempt)
			return
		}
		if attempt >= 60 {
			log.Fatalf("warm-up resume still failing: %v", err)
		}
		time.Sleep(time.Second)
	}
}

func (r *kindRun) run(ctx context.Context) {
	r.warmUp(ctx)
	r.reset(ctx)
	r.sleeping = r.actorSizes()
	var end time.Duration
	for _, st := range r.pat.steps {
		end += st.dur
	}
	start := time.Now()
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup

	// Poller.
	wg.Add(1)
	go func() {
		defer wg.Done()
		marks := []time.Duration{}
		if r.pat.dropAt > 0 {
			for _, off := range []time.Duration{0, time.Minute, 3 * time.Minute, 8 * time.Minute} {
				marks = append(marks, r.pat.dropAt+off)
			}
		}
		scaleAt := r.pat.scaleAt
		if scaleAt == 0 {
			scaleAt = end
		}
		scaled := false
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			el := time.Since(start)
			ws := r.listWorkers(ctx)
			if len(ws) > 0 {
				r.sample(el, ws)
				for len(marks) > 0 && el >= marks[0] {
					r.stats.emptyAtDrop = append(r.stats.emptyAtDrop, countEmpty(ws))
					marks = marks[1:]
				}
				if !scaled && el >= scaleAt {
					r.scaleDown(ws)
					scaled = true
				}
			}
			select {
			case <-runCtx.Done():
				return
			case <-tick.C:
			}
		}
	}()

	// Arrivals.
	var inflight sync.WaitGroup
	t := time.Duration(0)
	for _, st := range r.pat.steps {
		stepEnd := t + st.dur
		rate := st.rate * r.sc.rateScale
		for rate > 0 {
			gap := time.Duration(r.expFloat() / rate * float64(time.Second))
			t += gap
			if t >= stepEnd {
				break
			}
			time.Sleep(time.Until(start.Add(t)))
			if a, ok := r.take(); ok {
				inflight.Add(1)
				go func() {
					defer inflight.Done()
					r.cycle(ctx, a)
				}()
			}
		}
		t = stepEnd
		time.Sleep(time.Until(start.Add(t)))
	}
	log.Printf("load done after %s, waiting for in-flight cycles", time.Since(start).Round(time.Second))
	inflight.Wait()
	cancel()
	wg.Wait()
}

func (r *kindRun) expFloat() float64 {
	r.rngMu.Lock()
	defer r.rngMu.Unlock()
	return r.rng.ExpFloat64()
}

func (r *kindRun) take() (kindActor, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sleeping) == 0 {
		return kindActor{}, false
	}
	i := r.intn(len(r.sleeping))
	a := r.sleeping[i]
	r.sleeping[i] = r.sleeping[len(r.sleeping)-1]
	r.sleeping = r.sleeping[:len(r.sleeping)-1]
	return a, true
}

func (r *kindRun) giveBack(a kindActor) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sleeping = append(r.sleeping, a)
}

func (r *kindRun) countErr(err error) {
	v, _ := r.errCodes.LoadOrStore(status.Code(err).String(), &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

// cycle resumes a, retrying ResourceExhausted every second up to
// clientTimeout, holds it, then suspends it.
func (r *kindRun) cycle(ctx context.Context, a kindActor) {
	defer r.giveBack(a)
	r.resumes.Add(1)
	first := time.Now()
	sawNoCap := false
	for {
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := r.client().ResumeActor(rctx, &ateapi.ResumeActorRequest{Actor: r.ref(a.name)})
		cancel()
		if err == nil {
			break
		}
		if status.Code(err) == codes.ResourceExhausted {
			r.noCap.Add(1)
			sawNoCap = true
			if time.Since(first)+clientRetry > clientTimeout {
				r.failed.Add(1)
				r.logResume(a, first, "failed")
				return
			}
			time.Sleep(clientRetry)
			continue
		}
		r.otherErr.Add(1)
		r.countErr(err)
		r.logResume(a, first, "error:"+status.Code(err).String())
		log.Printf("resume %s: %v", a.name, err)
		_, _ = r.client().SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: r.ref(a.name)})
		return
	}
	r.placed.Add(1)
	r.logResume(a, first, "placed")
	if sawNoCap {
		r.waitedPlaced.Add(1)
	}
	r.ttpMu.Lock()
	r.ttp = append(r.ttp, float64(time.Since(first))/float64(time.Millisecond))
	r.ttpMu.Unlock()

	hold := 20*time.Second + time.Duration(r.float()*float64(20*time.Second))
	if r.pat.longTail && r.float() < 0.03 {
		hold = 5*time.Minute + time.Duration(r.float()*float64(5*time.Minute))
	}
	time.Sleep(hold)
	for attempt := 0; attempt < 5; attempt++ {
		_, err := r.client().SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: r.ref(a.name)})
		if err == nil {
			return
		}
		log.Printf("suspend %s: %v", a.name, err)
		time.Sleep(time.Second)
	}
}

func countEmpty(ws []*ateapi.Worker) int {
	n := 0
	for _, w := range ws {
		if w.GetStatus().GetAllocated().GetActors() == 0 {
			n++
		}
	}
	return n
}

func (r *kindRun) poolSpec(name string) (poolSpec, bool) {
	for _, p := range r.sc.pools {
		if p.name == name {
			return p, true
		}
	}
	return poolSpec{}, false
}

func (r *kindRun) sample(el time.Duration, ws []*ateapi.Worker) {
	var awake, nonEmpty int
	var utils []float64
	var hot, big, all float64
	var heldS, allocS, heldC, allocC, heldM, allocM float64
	type tot struct{ s, c, m float64 }
	per := map[string]*tot{}
	for _, w := range ws {
		u, c := w.GetStatus().GetAllocated(), w.GetStatus().GetCapacity()
		n := u.GetActors()
		awake += int(n)
		util := max(float64(n)/float64(c.GetActors()), float64(u.GetCpuMillicores())/float64(c.GetCpuMillicores()), float64(u.GetMemoryBytes())/float64(c.GetMemoryBytes()))
		utils = append(utils, util)
		if util >= 0.9 {
			hot += float64(n)
		}
		all += float64(n)
		if w.GetWorkerPool() == "big" {
			big += float64(n)
		}
		if per[w.GetWorkerPool()] == nil {
			per[w.GetWorkerPool()] = &tot{}
		}
		per[w.GetWorkerPool()].s += float64(n)
		per[w.GetWorkerPool()].c += float64(u.GetCpuMillicores())
		per[w.GetWorkerPool()].m += float64(u.GetMemoryBytes())
		if n > 0 {
			nonEmpty++
			heldS += float64(c.GetActors())
			heldC += float64(c.GetCpuMillicores())
			heldM += float64(c.GetMemoryBytes())
			allocS += float64(n)
			allocC += float64(u.GetCpuMillicores())
			allocM += float64(u.GetMemoryBytes())
		}
	}
	lb := 0
	for name, t := range per {
		if ps, ok := r.poolSpec(name); ok {
			lb += int(math.Ceil(max(t.s/float64(ps.slots), t.c/float64(ps.cpu), t.m/float64(ps.mem)) - 1e-9))
		}
	}
	held := math.NaN()
	if allocS > 0 {
		held = min(heldS/allocS, heldC/allocC, heldM/allocM)
	}
	type nodeTot struct{ actors, workers, nonEmpty int }
	byNode := map[string]*nodeTot{}
	for _, w := range ws {
		nt := byNode[w.GetNodeName()]
		if nt == nil {
			nt = &nodeTot{}
			byNode[w.GetNodeName()] = nt
		}
		n := int(w.GetStatus().GetAllocated().GetActors())
		nt.actors += n
		nt.workers++
		if n > 0 {
			nt.nonEmpty++
		}
	}
	r.seriesMu.Lock()
	defer r.seriesMu.Unlock()
	r.series = append(r.series, []string{strconv.Itoa(int(el / time.Second)), strconv.Itoa(awake), strconv.Itoa(nonEmpty), strconv.Itoa(lb), strconv.Itoa(len(ws) - nonEmpty), fmt.Sprintf("%.3f", held)})
	for _, node := range slices.Sorted(maps.Keys(byNode)) {
		nt := byNode[node]
		r.nodes = append(r.nodes, []string{strconv.Itoa(int(el / time.Second)), node, strconv.Itoa(nt.actors), strconv.Itoa(nt.workers), strconv.Itoa(nt.nonEmpty)})
	}
	if awake == 0 {
		return
	}
	a := &r.stats
	a.samples++
	if !math.IsNaN(held) {
		a.sumHeld += held
		a.heldSamples++
		if r.pat.dropAt > 0 && el >= r.pat.dropAt+time.Minute && el <= r.pat.scaleAt {
			a.lowHeld += held
			a.lowSamples++
		}
	}
	if lb > 0 {
		a.sumNonEmptyLB += float64(nonEmpty) / float64(lb)
		a.lbSamples++
	}
	a.sumEmptyFrac += float64(len(ws)-nonEmpty) / float64(len(ws))
	if mean, sd := meanSD(utils); mean > 0 {
		a.sumCV += sd / mean
	}
	a.sumHot += hot / all
	if len(r.sc.pools) > 1 {
		a.sumBig += big / all
		a.bigSamples++
	}
}

func (r *kindRun) scaleDown(ws []*ateapi.Worker) {
	a := &r.stats
	a.emptyAtScale = countEmpty(ws)
	byPool := map[string][]*ateapi.Worker{}
	for _, w := range ws {
		byPool[w.GetWorkerPool()] = append(byPool[w.GetWorkerPool()], w)
	}
	for _, ps := range r.sc.pools {
		pw := byPool[ps.name]
		var s, c, m float64
		for _, w := range pw {
			u := w.GetStatus().GetAllocated()
			s += float64(u.GetActors())
			c += float64(u.GetCpuMillicores())
			m += float64(u.GetMemoryBytes())
		}
		keep := int(math.Ceil(max(s/float64(ps.slots), c/float64(ps.cpu), m/float64(ps.mem))-1e-9)) + 1
		a.scaleKeep += keep
		remove := len(pw) - keep
		if remove <= 0 {
			continue
		}
		newest := slices.Clone(pw)
		sort.Slice(newest, func(i, j int) bool {
			return newest[i].GetMetadata().GetCreateTime().AsTime().After(newest[j].GetMetadata().GetCreateTime().AsTime())
		})
		emptiest := slices.Clone(pw)
		sort.Slice(emptiest, func(i, j int) bool {
			return emptiest[i].GetStatus().GetAllocated().GetActors() < emptiest[j].GetStatus().GetAllocated().GetActors()
		})
		for i := range remove {
			a.scaleNewest += int(newest[i].GetStatus().GetAllocated().GetActors())
			a.scaleEmptiest += int(emptiest[i].GetStatus().GetAllocated().GetActors())
		}
	}
}

var kindColumns = []string{
	"resumes", "placed", "no_capacity", "waited_placed", "failed", "other_errors",
	"ttp_p50_ms", "ttp_p95_ms", "ttp_p99_ms",
	"held_ratio", "nonempty_over_lb", "empty_frac", "util_cv", "hot_frac", "big_pool_share",
	"low_held_ratio", "empty_drop_0", "empty_drop_60", "empty_drop_180", "empty_drop_480",
	"empty_at_scale", "scale_keep", "displaced_newest", "displaced_emptiest",
}

func (r *kindRun) write() {
	if err := os.MkdirAll(r.f.out, 0o755); err != nil {
		log.Fatal(err)
	}
	a := &r.stats
	f := func(x float64, n int) float64 {
		if n == 0 {
			return math.NaN()
		}
		return x / float64(n)
	}
	row := map[string]float64{
		"resumes": float64(r.resumes.Load()), "placed": float64(r.placed.Load()), "no_capacity": float64(r.noCap.Load()),
		"waited_placed": float64(r.waitedPlaced.Load()), "failed": float64(r.failed.Load()), "other_errors": float64(r.otherErr.Load()),
		"ttp_p50_ms": pct(r.ttp, 0.5), "ttp_p95_ms": pct(r.ttp, 0.95), "ttp_p99_ms": pct(r.ttp, 0.99),
		"held_ratio": f(a.sumHeld, a.heldSamples), "nonempty_over_lb": f(a.sumNonEmptyLB, a.lbSamples),
		"empty_frac": f(a.sumEmptyFrac, a.samples), "util_cv": f(a.sumCV, a.samples), "hot_frac": f(a.sumHot, a.samples),
		"big_pool_share": f(a.sumBig, a.bigSamples), "low_held_ratio": f(a.lowHeld, a.lowSamples),
		"empty_at_scale": float64(a.emptyAtScale), "scale_keep": float64(a.scaleKeep),
		"displaced_newest": float64(a.scaleNewest), "displaced_emptiest": float64(a.scaleEmptiest),
	}
	for i, k := range []string{"empty_drop_0", "empty_drop_60", "empty_drop_180", "empty_drop_480"} {
		row[k] = math.NaN()
		if i < len(a.emptyAtDrop) {
			row[k] = float64(a.emptyAtDrop[i])
		}
	}
	appendCSV(filepath.Join(r.f.out, "runs.csv"), append([]string{"scenario", "pattern", "policy", "seed"}, kindColumns...), func(w *csv.Writer) {
		rec := []string{r.sc.name, r.pat.name, r.f.policy, strconv.FormatInt(r.f.seed, 10)}
		for _, c := range kindColumns {
			rec = append(rec, strconv.FormatFloat(row[c], 'f', 4, 64))
		}
		w.Write(rec)
	})
	appendCSV(filepath.Join(r.f.out, "series.csv"), []string{"scenario", "pattern", "policy", "seed", "t", "awake", "nonempty", "lower_bound", "empty", "held_ratio"}, func(w *csv.Writer) {
		for _, s := range r.series {
			w.Write(append([]string{r.sc.name, r.pat.name, r.f.policy, strconv.FormatInt(r.f.seed, 10)}, s...))
		}
	})
	appendCSV(filepath.Join(r.f.out, "nodes.csv"), []string{"scenario", "pattern", "policy", "seed", "t", "node", "actors", "workers", "nonempty_workers"}, func(w *csv.Writer) {
		for _, s := range r.nodes {
			w.Write(append([]string{r.sc.name, r.pat.name, r.f.policy, strconv.FormatInt(r.f.seed, 10)}, s...))
		}
	})
	appendCSV(filepath.Join(r.f.out, "resumes.csv"), []string{"scenario", "pattern", "policy", "seed", "actor", "start_unix_ms", "ttp_ms", "outcome"}, func(w *csv.Writer) {
		for _, s := range r.resumeLog {
			w.Write(append([]string{r.sc.name, r.pat.name, r.f.policy, strconv.FormatInt(r.f.seed, 10)}, s...))
		}
	})
	var errs []string
	r.errCodes.Range(func(k, v any) bool {
		errs = append(errs, fmt.Sprintf("%s=%d", k, v.(*atomic.Int64).Load()))
		return true
	})
	log.Printf("done: resumes=%d placed=%d no_capacity=%d waited=%d failed=%d other=%d %v ttp_p99=%.0fms held=%.2f empty_drop=%v",
		r.resumes.Load(), r.placed.Load(), r.noCap.Load(), r.waitedPlaced.Load(), r.failed.Load(), r.otherErr.Load(), errs,
		row["ttp_p99_ms"], row["held_ratio"], a.emptyAtDrop)
}

func appendCSV(path string, header []string, rows func(*csv.Writer)) {
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if os.IsNotExist(statErr) {
		w.Write(header)
	}
	rows(w)
	w.Flush()
}
