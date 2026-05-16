# DESIGN.md — multi-cluster-orchestrator

This document captures the design decisions behind the prototype, the alternatives considered, and the production extensions a real deployment would require. It complements the README, which describes *what* the system does. This document describes *why* it is shaped this way.

---

## 1. Goals and non-goals

### Goals

The prototype demonstrates five concrete capabilities, each observable in the demo:

1. **Declarative multi-cluster workload orchestration.** A single `GlobalWorkload` CRD declares image, replicas, resources, and region preference; the controller places the workload across one or more registered clusters without per-cluster configuration.
2. **Capacity-aware placement.** The placement engine considers each cluster's free capacity, scores clusters by region preference and headroom, and avoids placing workloads where they would not fit.
3. **Automated failover.** When a cluster transitions to unhealthy, the controller redistributes its workloads to other healthy clusters consistent with the region preference. The previous cluster's Deployments are removed when reachable.
4. **Idempotent reconciliation.** The controller can be killed and restarted mid-operation and will converge to the correct state without intervention. Status reflects current placement.
5. **Clean teardown.** When a `GlobalWorkload` is deleted, all child Deployments across target clusters are removed via finalizer before deletion completes.

### Non-goals

The following are deliberately out of scope. Each is discussed in section 7 as a production extension.

- **GPU-aware scheduling** — no real GPUs in the prototype environment; the placement engine treats CPU and memory only.
- **Predictive or demand-driven scaling** — only reactive placement on workload declaration. Production inference platforms need predictive scaling because cold-start cost is user-visible.
- **True hybrid cloud demonstration** — two local `kind` clusters simulate the architecture; cross-cloud identity, networking, and credential portability are not exercised.
- **Production observability** — structured logs only; no metrics, tracing, or dashboards.
- **Controller high availability** — single-replica controller; no leader election.
- **Capacity reservation API** — optimistic placement with a headroom buffer; production would add a reservation service to eliminate capacity drift.
- **Anti-affinity, gang scheduling, topology spread** — only basic region preference and bin-pack/spread strategy implemented.
- **Admission validation beyond OpenAPI schemas** — no webhook validation for cross-field invariants.

---

## 2. Architecture overview

### Three clusters, two reconcilers, one process

The system uses three kind clusters: one **management cluster** (`mgmt`) hosting the platform's state and controllers, and two **target clusters** (`region-a`, `region-b`) representing distinct regions.

The management cluster holds:

- The two CRDs: `GlobalWorkload` and `ClusterRegistration`
- `Secret` resources containing kubeconfigs for each target cluster
- The controller process containing two reconcilers

The controller talks to each target cluster as a standard Kubernetes client over HTTPS, using credentials loaded from Secrets. The target clusters know nothing about the platform — they hold tenant Deployments and serve standard Kubernetes APIs.

### Why this asymmetry matters

Separating management from target is deliberate. Alternatives considered:

- **Use one of the target clusters as also the management cluster.** Simpler to set up, but creates coupling — losing the management cluster also loses the platform's brain. With the demo's failover scenario, this would mean killing the controller as well as the target.
- **Separate management cluster (chosen).** Extra one-time setup. Mirrors production multi-cluster systems (Karmada's hub-and-spoke, Cluster API's management cluster, Open Cluster Management's hub) where the platform's state lives separately from tenant workloads.

The principle: **a platform's control plane should be a distinct failure domain from the workloads it manages.** A region failure should not cascade to the platform itself.

### Why two reconcilers, not one

Two reconcilers run in the same controller process:

- **GlobalWorkloadReconciler** — owns `GlobalWorkload.status.*` and writes `AllocatedCapacity` on registrations.
- **ClusterRegistrationReconciler** — owns `ClusterRegistration.status.Healthy`, `ObservedCapacity`, `LastProbeTime`.

Each reconciler runs on its own cadence (placement on workload change; health probing every 15 seconds). Each owns disjoint status fields — no two reconcilers write the same field, eliminating fight-loop risk.

Alternative considered: a single combined reconciler. Rejected because:

- The two concerns have different cadences (**event-driven vs. periodic**). Combining them means every placement reconcile re-probes every cluster (wasteful) or every probe re-evaluates every workload (also wasteful).
- Mixing logic for different concerns in one reconciler makes the code harder to reason about, harder to test, and harder to evolve independently.

The principle: **separate concerns at the controller level, communicate through shared CRD state.** This is the canonical Kubernetes pattern — other controllers (Deployment + ReplicaSet + Pod) follow it.

---

## 3. Data model

### CRD design discipline

Two CRDs: `GlobalWorkload` and `ClusterRegistration`. Both follow strict spec/status separation: spec is user-owned, status is controller-owned. Conditions follow the standard Kubernetes `metav1.Condition` shape.

Both CRDs are cluster-scoped. For the prototype this simplifies RBAC and the demo flow. In production, `GlobalWorkload` would likely be namespace-scoped to enable per-tenant quotas and multi-tenancy boundaries; `ClusterRegistration` remains cluster-scoped because clusters are infrastructure, not tenant-owned.

### `ClusterRegistration` — the cluster's registration card

```
spec:
  region: <string>                         # logical region for placement matching
  kubeconfigSecretRef: { name: <string> }  # Secret containing the kubeconfig

status:
  healthy: <bool>                          # written by health probe reconciler
  lastProbeTime: <timestamp>               # written by health probe reconciler
  observedCapacity: <Capacity>             # written by health probe reconciler
  allocatedCapacity: <Capacity>            # written by workload reconciler
  conditions: [<Condition>]                # standard Kubernetes conditions
```

The `Capacity` struct is an explicit `{CPU, Memory}` rather than reusing `corev1.ResourceList`. Tradeoff: less extensible (cannot add GPU without a code change) but clearer to read and validate. Production would use `ResourceList` to handle arbitrary resource types.

The status splits `observedCapacity` and `allocatedCapacity` into two fields with different writers — the single-writer rule applied concretely. Free capacity is computed on demand as `observed - allocated`. Storing free capacity as a third field would require coordination between writers; splitting eliminates the coordination need.

### `GlobalWorkload` — the placement request

```
spec:
  image: <string>
  replicas: <int32>                        # total across all clusters
  resources:
    cpu: <Quantity>
    memory: <Quantity>
  regionPreference: [<string>]             # ordered; earlier preferred
  placementStrategy: Spread | BinPack      # default Spread

status:
  observedGeneration: <int64>
  placements: [<Placement>]                # what was placed where
  conditions: [<Condition>]
```

Three details worth defending:

- **`regionPreference` is an ordered list, not a single region.** The list expresses both *acceptability* (which regions are allowed) and *preference* (in what order). A single-region field would force a separate `acceptableRegions` field for fallback.
- **`placementStrategy` is an enum with a default.** Enum validation rejects bad values at admission. The default (`Spread`) means a minimal YAML doesn't need to mention strategy.
- **`status.placements` includes `region` even though it's derivable from cluster name.** Denormalized for self-describing status — a user looking at workload status shouldn't have to cross-reference cluster registrations.

### What's missing from the data model

The data model does not include:

- **Topology constraints beyond region.** No anti-affinity, no max-per-region, no zone-aware spreading.
- **GPU resources.** The `ResourceRequirements` struct is CPU/memory only.
- **Sticky placement.** No way to express "keep this workload where it is unless forced to move."
- **Cost or priority.** No way to express scheduling priority beyond region rank, or cost-aware placement.

Each gap is intentional for prototype scope. Production extensions in section 7.

---

## 4. Placement engine

### The three-phase model

For each `GlobalWorkload`, the engine runs three phases:

1. **Filter** — eliminate clusters that cannot host the workload (unhealthy, region not in preference, insufficient capacity for one replica).
2. **Score** — rank surviving clusters by a weighted combination of region preference, capacity utilization, and headroom.
3. **Distribute** — assign replicas across top-scored clusters per the placement strategy.

This mirrors the kube-scheduler framework's Filter/Score model at the cluster-selection layer. Naming the pattern: this is the meta-scheduler in a hierarchical scheduling system. The local kube-scheduler in each target cluster runs the same pattern at the node-selection layer.

### Why scoring is a weighted linear combination

The score function adds independently-testable components:

```
score = (regionPreference × 10) + (capacity × 1) + headroomPenalty
```

Each component is a pure function returning a float. The weights encode policy: region preference dominates capacity 10:1, so region rank decides among feasible clusters. The headroom penalty (`-100`) is large enough to overpower the other components when a cluster crosses the 80% utilization threshold.

Alternatives considered:

- **Multiplicative scoring.** Tried briefly; rejected because zero scores annihilate the others, making the function brittle.
- **Hard-coded priority rules.** Simpler initially but does not extend cleanly when new factors (cost, latency-to-user, carbon) are added.
- **ML-based scoring.** Out of scope; would require training data the prototype does not have. Production schedulers like Borg use ML for some decisions but rule-based scoring for the core path.

The weighted linear approach is the same model kube-scheduler uses internally. Adding a new component is one function plus one line in the sum — additive, not invasive.

### Why headroom is a separate, unconditional penalty

The headroom penalty is *not* part of the capacity score. It is applied separately and runs regardless of placement strategy.

Reason: drift safety should be structural, not strategic. With `BinPack`, the strategy naturally prefers fuller clusters — but pushing a cluster past 80% utilization in our (slightly stale) view risks capacity drift in the cluster's real state. Without a separate guardrail, `BinPack` would happily place into a 78% cluster that is actually at 95%.

Folding headroom into the capacity score would couple drift safety to a specific strategy. By keeping it separate and additive, the guardrail applies to all strategies (current and future).

In general: **preventive safety should be structurally independent of feature logic.** Same principle as: input validation runs before business logic; security checks run before authorization decisions.

### Why the engine is pure

`ComputePlan(workload, clusters)` is a pure function — no I/O, no logging, no side effects. It takes data, returns data (a `Plan`).

This shape enables several things:

- **Unit testing is trivial.** Feed inputs, assert outputs. No Kubernetes API setup, no fakes, no mocks.
- **Plans are inspectable.** The reconciler can log the plan, compare to current state, decide whether to apply.
- **Plans are replayable.** Production schedulers benefit from "scheduler simulators" that replay historical workloads against new logic to detect regressions. The prototype could trivially add this.

The tradeoff: the engine has no built-in error logging. The reconciler is responsible for logging plans and reasons. This is the right separation — logging is a runtime concern, not a decision concern.

### Two-level scheduling explicit

The placement engine answers "which cluster?" Each cluster's kube-scheduler answers "which node?" This hierarchy is the same pattern used by Borg, Mesos, Peloton, and modern multi-cluster systems (Karmada, OCM).

Cost of two-level scheduling: capacity drift between layers. The meta engine sees aggregate cluster capacity that may be seconds-stale; by the time placement is applied, capacity may be gone. Mitigations:

- **Implemented:** headroom buffer (80% threshold) reduces the practical incidence of drift.
- **Discussed for production:** capacity reservation API on target clusters; two-phase commit; optimistic retry on apply failure.

The prototype takes the simplest mitigation (headroom) because production deployments routinely operate with this same buffer pattern.

---

## 5. Reconciliation model

### The five-phase reconciler

The `GlobalWorkloadReconciler.Reconcile` function has five named phases:

1. **Observe** — read the workload and all registered clusters.
2. **Decide** — call the placement engine to get a plan.
3. **Act** — converge target clusters toward the plan (create / update / remove Deployments).
4. **Status** — write what was observed and done.
5. **Requeue** — schedule the next reconcile.

Each phase has its own discipline:

- **Observe** is read-only against the management cluster.
- **Decide** is a pure function call, no I/O.
- **Act** writes to target clusters.
- **Status** writes only to `.status`, never `.spec`.
- **Requeue** is the only place that returns to controller-runtime.

Mixing phases is where bugs live. Reading state mid-act creates race conditions. Writing status before act creates "claim of success without success." Disciplined ordering prevents these.

### Level-triggered, not edge-triggered

The reconciler does not subscribe to "events" and react. It observes current state, compares to desired, takes one step. If reconciliation is interrupted, the next call sees the partial state and continues. There is no event log to replay, no missed events to recover.

The practical consequence: the controller can be killed and restarted at any time. On restart it observes the world, compares to declared intent, and converges. State is in the API server, not in the controller process.

This is the defining property of Kubernetes controllers and the reason they survive partitions, restarts, and arbitrary crashes. The prototype demonstrates this explicitly: deleting a target-cluster Deployment manually causes the reconciler to recreate it on the next pass.

### Single-writer rule across CRDs

The two reconcilers write disjoint fields. No coordination is required because no two writers ever touch the same field:

| Field | Writer |
|---|---|
| `GlobalWorkload.status.placements` | GlobalWorkloadReconciler |
| `GlobalWorkload.status.conditions` | GlobalWorkloadReconciler |
| `GlobalWorkload.status.observedGeneration` | GlobalWorkloadReconciler |
| `ClusterRegistration.status.healthy` | ClusterRegistrationReconciler |
| `ClusterRegistration.status.observedCapacity` | ClusterRegistrationReconciler |
| `ClusterRegistration.status.lastProbeTime` | ClusterRegistrationReconciler |
| `ClusterRegistration.status.conditions` | ClusterRegistrationReconciler |
| `ClusterRegistration.status.allocatedCapacity` | GlobalWorkloadReconciler |

This is the rule that makes complex platforms maintainable. The moment two controllers write the same field, you have a coordination problem that explodes in complexity. Sharp ownership keeps things simple.

### Watches drive failover

The `GlobalWorkloadReconciler` watches both `GlobalWorkload`s (its primary resource) and `ClusterRegistration`s (a secondary watch with a map function that enqueues all workloads when a registration changes).

This is what makes failover work: when the health watcher updates a `ClusterRegistration.status.healthy`, every workload gets a reconcile event, the placement engine excludes the unhealthy cluster, and the reconciler migrates workloads.

For the prototype, the watch enqueues all workloads regardless of relevance. Production would filter to workloads that actually reference the changed cluster's region.

### Finalizer protocol

`GlobalWorkload` carries a finalizer (`platform.platform.local/globalworkload-cleanup`). The reconciliation handles two paths:

- **Object alive (`deletionTimestamp` is zero):** add finalizer if missing, then run normal reconcile.
- **Object terminating (`deletionTimestamp` non-zero):** drain Deployments from every cluster in `status.placements`, then remove finalizer.

The finalizer is added *before* the first placement, eliminating the race window where a user could delete an object after Deployments are created but before cleanup is registered.

Cleanup failures keep the finalizer in place. The object stays in `Terminating` state until cleanup succeeds. This is the entire purpose of finalizers: turn deletion from fire-and-forget into a transactional guarantee.

The one exception: cleanup against an unreachable target cluster logs and skips. The alternative — block until the dead cluster returns — would freeze every workload that ever placed on a permanently-dead cluster. Skip-and-continue trades orphan Deployments for forward progress. Orphans are reclaimed when the cluster recovers or its registration is removed.

### Error handling discipline

The reconciler distinguishes three error categories:

- **Transient errors** (network blips, conflict on update) — return error to controller-runtime; it retries with exponential backoff.
- **Expected steady-state conditions** (no feasible clusters) — return `(Result{RequeueAfter}, nil)` with a longer requeue; not an error.
- **Unreachable target cluster errors** during cleanup — log and continue; the alternative is worse.

The general principle: errors are for things that should be retried fast; results are for things that should be retried slow; logged-and-skipped is for things that should not block forward progress.

---

## 6. Failure modes

### Capacity drift between meta and local schedulers

**What can go wrong:** the meta engine sees stale aggregate capacity for a cluster; placement decision is based on outdated info; by the time Deployments are applied, the local scheduler cannot place pods because real capacity has changed.

**Current mitigation:** headroom buffer (80% threshold) absorbs typical drift. Beyond 80%, the cluster gets a large score penalty regardless of strategy.

**Residual risk:** under high churn (many workloads placing simultaneously), drift can exceed the buffer. Pods stay `Pending` on the target cluster; the meta scheduler thinks placement succeeded.

**Production solution:** a capacity reservation API. Before placing, the meta scheduler asks the target cluster to reserve capacity for N replicas. If reservation succeeds, the cluster guarantees the capacity is held for a TTL. If reservation fails, the meta scheduler picks a different cluster. Drift is eliminated at the cost of an extra API and TTL-based GC.

### Cached client outliving its cluster

**What can go wrong:** the multi-cluster manager caches client objects per cluster. When a target cluster goes down, the cached client cannot be aware — its REST config is fine, but the network call fails at use time.

**Current mitigation:** the manager exposes `Invalidate(clusterName)`. The probe reconciler calls `Invalidate` on probe failures. The workload reconciler treats network-unreachable errors during cleanup the same as construction failures (`isClusterUnreachable` helper).

**Residual risk:** during the first failed call after a credential rotation, the call fails before invalidation happens. The next reconcile rebuilds the client correctly.

**Production solution:** watch the kubeconfig `Secret` directly and invalidate proactively on changes. This is a `controller-runtime` `Watches()` away — straightforward extension.

### Reconciliation fight loops

**What can go wrong:** two controllers writing the same field can fight indefinitely, each undoing the other's change.

**Current mitigation:** strict single-writer rule. Every field has exactly one writer documented in the data model.

**Residual risk:** subtle. If a future change accidentally introduces dual writes (e.g., a new feature that touches status from a new controller), the fight loop returns.

**Production safeguard:** server-side apply with field manager ownership, plus integration tests that verify only one controller writes each field.

### Thundering herd on cluster recovery

**What can go wrong:** a region returns from a long outage. Every workload that migrated away tries to migrate back simultaneously. The returning region is overwhelmed by the inrush, its local scheduler cannot keep up, and SLOs break.

**Current mitigation:** none. The prototype rebalances eagerly.

**Production solution:** hysteresis in the placement strategy. "Recently-recovered clusters are deprioritized for N minutes." Or rate-limited rebalancing: only migrate K workloads per minute back to a recovering cluster. Either pattern smooths the recovery curve.

### Cascading failover

**What can go wrong:** region A fails; workloads migrate to region B; region B becomes overloaded by the influx (it was sized for its own load, not A's also); region B's SLOs break; the platform tries to migrate to region C; cascade continues.

**Current mitigation:** none in the prototype.

**Production solution:** capacity headroom is the first line. Each region carries enough spare capacity to absorb a defined fraction of another region's load. Beyond that, circuit breakers: "do not migrate workloads into a region whose own SLOs are degraded." This requires tying placement decisions to SLO observability — out of scope for the prototype.

### Stale watch caches

**What can go wrong:** controller-runtime's informer cache desyncs from the API server during long disconnects. The reconciler reads stale data and acts on it.

**Current mitigation:** controller-runtime handles re-list and resync automatically. Status updates use the latest object version, so conflicts on update force a re-read.

**Residual risk:** under extreme load, the cache can lag. Eventual consistency wins — the next reconcile sees fresh state.

**Production safeguard:** monitor controller queue depth and reconcile latency; alert on prolonged elevations. This is observability work, not architectural.

### Permanently-dead clusters with orphan Deployments

**What can go wrong:** a target cluster suffers permanent loss (hardware failure, accidental deletion, cloud account closure). Its Deployments live on logically but are unreachable. Workloads that previously placed there have orphans in their history.

**Current mitigation:** the reconciler skips cleanup against unreachable clusters and logs. Orphans persist until the cluster recovers or its registration is removed.

**Production solution:** an "abandoned cluster" workflow. After N hours of unreachability, mark the cluster `Abandoned`; the workload reconciler treats this as confirmation that the orphans are unreclaimable and removes them from status. Cluster decommissioning is its own subsystem in production platforms.

---

## 7. Production extensions

This section enumerates the work that would convert the prototype into a production-grade system, organized by concern. Each item names the gap, the approach, and the rough effort.

### Scalability and HA

- **Leader election.** Run the controller process with 2-3 replicas using `controller-runtime`'s leader-election lease. Only one replica reconciles at a time; failover on lease expiry. Effort: small (~50 LOC plus configuration).
- **Sharded reconciliation.** At thousands of workloads, a single reconciler may not keep up. Shard by workload-name hash across replicas. Effort: medium; requires a separate sharding controller.
- **Etcd separation.** The management cluster's etcd holds platform state. At scale, consider a dedicated etcd or a separate database (Postgres) for high-cardinality status. Effort: substantial; design decision with operational implications.

### Scheduling depth

- **GPU-aware scheduling.** Extend `ResourceRequirements` to include GPU type and count. Cluster registration advertises GPU SKU inventory and NVLink topology. The placement engine adds a `gpuFit` filter and a `gpuTopology` score component. Effort: medium; the engine's shape doesn't change, just adds components.
- **Anti-affinity and topology spread.** Add `topologySpread` constraints to `GlobalWorkloadSpec` (max-per-region, max-per-zone). The placement distributor respects them. Effort: medium.
- **Gang scheduling at the meta level.** For workloads requiring all N replicas to be placed together (distributed inference with NVLink), implement all-or-nothing semantics: either all clusters can accommodate the workload's slice or the placement is rejected. Effort: medium.
- **Predictive scaling and warm pools.** A separate forecasting service produces traffic predictions; the platform pre-warms capacity. Effort: large; this is its own subsystem.

### Reliability

- **Capacity reservation API.** Each target cluster exposes a reservation endpoint (could itself be a CRD on the target cluster). The meta scheduler reserves before placing; the reservation has a TTL. Effort: medium-large; introduces a new control plane to manage reservation lifecycle.
- **Hysteresis on rebalancing.** When a cluster recovers, deprioritize it for N minutes to avoid thundering herd. Effort: small.
- **Circuit breakers in failover.** If the candidate target region is itself degraded, do not migrate into it. Effort: medium; requires SLO integration.
- **Server-side apply.** Replace imperative Get-then-Update with SSA. Cleaner conflict handling, explicit field ownership. Effort: small but propagates through the apply paths.

### Security and identity

- **Workload identity portability.** Replace kubeconfig Secrets with SPIFFE/SPIRE-mediated identity. The controller's identity is asserted via SVID; target clusters authenticate the controller without per-cluster credentials. Effort: large; introduces a cross-cluster trust system.
- **Admission webhooks.** Validate cross-field invariants on `GlobalWorkload` (e.g., region preference must contain at least one region the platform has registered). Effort: small.
- **OPA / Kyverno integration.** Tenant-level policies enforced at admission. Effort: medium.
- **RBAC on `GlobalWorkload`.** Namespace-scope the CRD and apply per-tenant RBAC. Effort: small (data model change plus RBAC manifests).

### Observability

- **Metrics.** Expose Prometheus metrics: placements per second, reconcile latency, probe success rate, capacity utilization per cluster, drift events. Effort: medium.
- **Tracing.** OpenTelemetry spans across reconcile phases. Useful for diagnosing slow reconciles. Effort: medium.
- **Structured events.** Emit Kubernetes Events on placement, migration, failover. Operators see the system's activity via `kubectl events`. Effort: small.
- **Dashboards.** Standard Grafana dashboards for the metrics above. Effort: small.

### Multi-cluster communication

The prototype uses **Mode 1: direct K8s API client per cluster.** This works for development and modest scale. Production patterns to consider:

- **Mode 2: Agent-pull (inverted direction).** Each target cluster runs an agent that pulls placements from the management cluster. Network direction is outbound from target clusters, which solves on-prem firewall traversal. Agents hold local state for autonomy during management plane outages.
- **Mode 3: Hub-and-spoke with custom protocol.** A formal control protocol (gRPC streaming) between hub and per-cluster klusterlets. Bidirectional state flow, versioned protocol. This is what Karmada, OCM, and ACM use at scale.
- **Mode 4: Shared substrate (GitOps).** Management plane writes intent to git or Kafka; agents watch and apply. Decoupling and audit trail benefits; latency cost.

The graduation path: Mode 1 → Mode 3 is the typical production trajectory. Mode 1 limits practical scale to a few dozen clusters before watch overhead becomes painful.

### Lifecycle and operations

- **Workload status propagation.** Read target Deployment `status.readyReplicas` and surface it in `GlobalWorkload.status.placements[].readyReplicas`. The prototype leaves this at zero. Effort: small.
- **Image / config rollout strategies.** Progressive rollout (canary, blue-green) across clusters. Today a workload spec change rolls out everywhere simultaneously. Effort: medium-large; introduces rollout state machinery.
- **Workload deletion across abandoned clusters.** An `Abandoned` state on `ClusterRegistration` with automated orphan reclamation. Effort: medium.
- **Cost-aware placement.** Cluster registrations advertise cost-per-unit-resource; placement balances cost against region preference. Effort: small (one more score component).

