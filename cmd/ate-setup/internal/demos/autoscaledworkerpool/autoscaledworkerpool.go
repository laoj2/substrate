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

// Package autoscaledworkerpool installs the autoscaled-workerpool demo: the
// counter workload on a WorkerPool that a HorizontalPodAutoscaler scales.
//
// The demo is Kind only. The HPA reads its metrics from the Prometheus of the
// Kind install, through a prometheus-adapter the demo deploys.
package autoscaledworkerpool

import (
	"context"

	"go.ate.dev/substrate/cmd/ate-setup/internal/demos"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/cmd/ate-setup/internal/steps"
	"go.ate.dev/substrate/internal/resources"
)

// namespace holds the pool, the prometheus-adapter and the HPA; it doubles as
// the atespace holding the demo's ActorTemplate.
const namespace = "ate-demo-autoscaled-workerpool"

const (
	prometheusAdapterManifest = "demos/autoscaled-workerpool/prometheus-adapter.yaml"
	hpaManifest               = "demos/autoscaled-workerpool/hpa-kind.yaml"
)

type demo struct {
	demos.Substrate
}

func init() {
	demos.Register(&demo{Substrate: demos.Substrate{
		DemoName:           "demo-autoscaled-workerpool",
		Short:              "A WorkerPool scaled by an HPA on its utilization and demand (Kind only)",
		WorkerPoolManifest: "demos/autoscaled-workerpool/autoscaled-workerpool.yaml.tmpl",
		Deployments:        []steps.TemplateRef{{Atespace: namespace, Name: "counter"}},
		Templates: []demos.SubstrateTemplate{
			{
				Manifest: "demos/autoscaled-workerpool/autoscaled-workerpool-template.yaml.tmpl",
				Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "counter"},
			},
			{
				Manifest: "demos/autoscaled-workerpool/autoscaled-workerpool-large-template.yaml.tmpl",
				Ref:      resources.ActorTemplateRef{Atespace: namespace, Name: "counter-large"},
			},
		},
	}})
}

func (d *demo) Deploy(ctx context.Context, e *steps.Env) error {
	if err := e.RequireKind(d.DemoName); err != nil {
		return err
	}
	if err := d.Substrate.Deploy(ctx, e); err != nil {
		return err
	}

	log.Step("Deploying prometheus-adapter and the HPA...")
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Path(prometheusAdapterManifest)); err != nil {
		return err
	}
	timeout := e.Cfg.WaitTimeout(steps.BootstrapTimeout)
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, namespace, "prometheus-adapter", timeout); err != nil {
		return err
	}
	return e.Kube.ApplyPath(ctx, e.Cfg.Path(hpaManifest))
}

func (d *demo) Delete(ctx context.Context, e *steps.Env) ([]string, error) {
	// `delete all` runs this on every cluster. The adapter manifest names the
	// cluster's one external metrics APIService, and only a Kind install has
	// the demo's, so another cluster's is left alone.
	if e.Cfg.Kind {
		// The HPA goes first, so it cannot scale the pool while it is removed.
		if err := e.Kube.DeletePath(ctx, e.Cfg.Path(hpaManifest)); err != nil {
			return nil, err
		}
		if err := e.Kube.DeletePath(ctx, e.Cfg.Path(prometheusAdapterManifest)); err != nil {
			return nil, err
		}
	}
	return d.Substrate.Delete(ctx, e)
}
