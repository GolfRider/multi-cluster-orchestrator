# multi-cluster-orchestrator

A multi-cluster Kubernetes meta control plane that places stateless workloads across registered clusters with region-aware scheduling and automated failover.

Declare a workload once via a custom resource; the controller decides which clusters host it, propagates it to each via per-cluster kubeconfigs, and migrates it across clusters when failures occur.

## What this demonstrates

- **Hierarchical scheduling** — a custom meta-scheduler chooses *which cluster*; each cluster's kube-scheduler chooses *which node*.
- **Multi-cluster reconciliation** — one controller process holds clients for every registered cluster and converges remote state toward declared intent.
- **Automated failover** — cluster health is probed continuously; when a cluster goes unreachable, workloads migrate to surviving clusters in the declared region preference.
- **Lifecycle correctness** — finalizers ensure all child Deployments are cleaned up before a workload is fully deleted, even across cluster boundaries.

The system is scoped to **stateless workloads**: the architectural commitment that mobility freedom is bought by restricting tenants to recoverable state.

## Architecture

    ┌─────────────────────────────────────────────────────────┐
    │  Management Cluster (kind: mgmt)                        │
    │                                                         │
    │  Manager process:                                       │
    │   - GlobalWorkloadReconciler  (places + drives state)   │
    │   - ClusterRegistrationReconciler  (probes + reports)   │
    │                                                         │
    │  CRDs:  GlobalWorkload, ClusterRegistration             │
    └────────────────────┬────────────────────────────────────┘
                         │ K8s API client per target cluster
                ┌────────┴────────┐
                ▼                 ▼
       ┌──────────────┐   ┌──────────────┐
       │  region-a    │   │  region-b    │
       │  (us-west)   │   │  (us-east)   │
       │              │   │              │
       │  Deployments │   │  Deployments │
       │  created by  │   │  created by  │
       │  controller  │   │  controller  │
       └──────────────┘   └──────────────┘

The meta control plane runs in its own kind cluster. Each target cluster registers via a `ClusterRegistration` object pointing at a `Secret` containing its kubeconfig. The controller holds per-cluster clients via a managed cache, reads target-cluster state to probe health, and writes Deployments to target clusters as placement decisions dictate.

## Placement engine

For each `GlobalWorkload`, the engine runs three phases:

1. **Filter** — drop clusters that are unhealthy, outside the region preference, or lack capacity for one replica.
2. **Score** — rank surviving clusters by a weighted combination of region preference, capacity utilization (bin-pack / spread strategy), and a headroom penalty that keeps placements away from near-full clusters regardless of strategy.
3. **Distribute** — assign replicas across top-scored clusters. `Spread` divides across top-K clusters for fault tolerance; `BinPack` greedily fills the highest-scored cluster first.

The engine is a pure function: same inputs always produce the same plan. Plans are data — they can be logged, replayed, or simulated without applying them.

## Failover model

The `ClusterRegistrationReconciler` probes each registered cluster every 15 seconds by listing its nodes. Probe failure (timeout, connection refused, invalid credentials) flips the registration's `Healthy` status to false.

The `GlobalWorkloadReconciler` watches `ClusterRegistration` changes. A health flip enqueues every workload for re-evaluation. The placement engine excludes unhealthy clusters; the reconciler creates Deployments in the new target clusters.

When a cluster is unreachable during cleanup of an old placement, the reconciler logs and skips rather than blocking — preserving forward progress at the cost of leaving orphan Deployments in the dead cluster, which are reconciled on recovery.

## Running

Requires `kind`, `kubectl`, `docker`, Go ≥ 1.25.

```bash
# One-time setup: three kind clusters, CRDs installed, target clusters registered
make demo-setup

# Terminal 1: controller
make run

# Terminal 2: walk through scenarios
make demo            # apply workload, observe placement across two clusters
make demo-failover   # stop region-a, observe migration to region-b
make demo-recover    # restart region-a, observe redistribution
make demo-clean      # delete workload; finalizer cleans target Deployments
make demo-teardown   # destroy kind clusters
```

## Repo layout

```
api/v1alpha1/          CRD type definitions
internal/placement/    filter/score/distribute engine (pure functions)
internal/clusters/     multi-cluster client manager (cached, invalidatable)
internal/controller/   GlobalWorkload + ClusterRegistration reconcilers
cmd/main.go            composition root: wire controllers + manager
config/                kubebuilder-generated CRD and RBAC manifests
hack/                  demo manifests and kubeconfigs
DESIGN.md              detailed design rationale and production tradeoffs
```

## Key design decisions

A small set of choices shape the rest of the system. Each is discussed in `DESIGN.md`.

- **Two controllers, single-writer status fields.** The GlobalWorkload reconciler writes its own status and `AllocatedCapacity` on registrations. The ClusterRegistration reconciler writes `Healthy`, `ObservedCapacity`, `LastProbeTime`. No field is written by more than one controller.
- **Placement engine is a pure function.** Decisions are data. The reconciler is responsible for applying plans; the engine has no I/O.
- **Headroom is an unconditional penalty.** Drift safety (don't pick near-full clusters) is applied regardless of placement strategy. Structural, not strategic.
- **Cluster unreachability is recoverable, not fatal.** Probe failures and cleanup failures on unreachable target clusters log-and-continue rather than block.
- **Finalizers gate deletion.** A `GlobalWorkload` cannot complete deletion until child Deployments across all target clusters are cleaned up.
- **Direct K8s API client per cluster.** The prototype uses Mode 1 (direct API client); production patterns like hub-and-spoke agents are discussed as graduation paths in `DESIGN.md`.

## What this is not

- **Not a production system.** Single-replica controller (no leader election), no observability beyond logs, no admission validation beyond OpenAPI schemas. Intended as a design exploration of multi-cluster control plane patterns.
- **Not stateful workload mobility.** The platform's freedom to move workloads relies on workloads being stateless. Stateful orchestration is a different system with different primitives (sticky placement, data migration, gravity awareness).
- **Not a kube-scheduler replacement.** Per-cluster pod placement remains kube-scheduler's job. This system makes cluster-level decisions and delegates node-level decisions downward.

See `DESIGN.md` for an exhaustive list of production extensions and the reasoning behind what was scoped in vs. out.

## Status

Prototype, working end-to-end on local `kind`. Demonstrates all goals listed above. Built as a design exploration; not intended for production deployment without the extensions noted in `DESIGN.md`.

