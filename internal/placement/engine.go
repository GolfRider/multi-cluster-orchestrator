package placement

import (
	"fmt"
	"math"
	"sort"

	platformv1alpha1 "github.com/golfrider/global-workload-orchestrator/api/v1alpha1"
)

// Plan describes the placement decisions for a single GlobalWorkload.
//
// A Plan is data — it can be logged, compared, applied, or discarded.
// The reconciler is responsible for converging the cluster state toward a Plan.
type Plan struct {
	// Assignments lists how many replicas to place in each chosen cluster.
	Assignments []Assignment

	// Reason is a human-readable explanation, useful for status messages
	// when placement fails or is partial.
	Reason string
}

// Assignment is a single cluster's slice of the workload.
type Assignment struct {
	ClusterName string
	Region      string
	Replicas    int32
}

// Engine computes placement decisions. Stateless and safe to call concurrently.
type Engine struct {
	// HeadroomThreshold is the utilization fraction above which a cluster
	// receives a heavy score penalty. Default 0.80.
	HeadroomThreshold float64

	// HeadroomPenalty is subtracted from the score of clusters above the threshold.
	// Large enough to override most other score components.
	HeadroomPenalty float64
}

// NewEngine returns an engine with sensible defaults.
func NewEngine() *Engine {
	return &Engine{
		HeadroomThreshold: 0.80,
		HeadroomPenalty:   100.0,
	}
}

// ComputePlan is the single entry point. It produces a Plan or an error
// if no feasible placement exists.
func (e *Engine) ComputePlan(
	workload *platformv1alpha1.GlobalWorkload,
	clusters []platformv1alpha1.ClusterRegistration,
) (*Plan, error) {

	// Phase 1: Filter
	feasible := e.filter(workload, clusters)
	if len(feasible) == 0 {
		return nil, fmt.Errorf("no feasible clusters for workload %s", workload.Name)
	}

	// Phase 2: Score
	scored := e.score(workload, feasible)

	// Phase 3: Distribute
	plan := e.distribute(workload, scored)
	return plan, nil
}

// ----------------------------------------------------------------------------
// Phase 1: Filter — eliminate clusters that cannot host this workload.
// ----------------------------------------------------------------------------

func (e *Engine) filter(
	workload *platformv1alpha1.GlobalWorkload,
	clusters []platformv1alpha1.ClusterRegistration,
) []platformv1alpha1.ClusterRegistration {
	var feasible []platformv1alpha1.ClusterRegistration
	for _, c := range clusters {
		if e.isFeasible(workload, &c) {
			feasible = append(feasible, c)
		}
	}
	return feasible
}

func (e *Engine) isFeasible(
	workload *platformv1alpha1.GlobalWorkload,
	cluster *platformv1alpha1.ClusterRegistration,
) bool {
	// Hard constraint: cluster must be healthy.
	if !cluster.Status.Healthy {
		return false
	}
	// Hard constraint: cluster's region must be in the preference list.
	if !containsString(workload.Spec.RegionPreference, cluster.Spec.Region) {
		return false
	}
	// Hard constraint: cluster must have room for at least one replica.
	if !fitsAtLeastOneReplica(workload, cluster) {
		return false
	}
	return true
}

// ----------------------------------------------------------------------------
// Phase 2: Score — rank surviving clusters by a weighted sum of components.
// ----------------------------------------------------------------------------

type scoredCluster struct {
	Cluster platformv1alpha1.ClusterRegistration
	Score   float64
}

func (e *Engine) score(
	workload *platformv1alpha1.GlobalWorkload,
	clusters []platformv1alpha1.ClusterRegistration,
) []scoredCluster {
	scored := make([]scoredCluster, len(clusters))
	for i, c := range clusters {
		s := 0.0
		s += e.scoreRegionPreference(workload, &c) * 10.0 // weight: region matters most
		s += e.scoreCapacity(workload, &c) * 1.0          // weight: capacity is a tiebreaker
		s += e.scoreHeadroom(&c)                          // penalty: applied unconditionally
		scored[i] = scoredCluster{Cluster: c, Score: s}
	}
	// Sort descending by score.
	sort.SliceStable(scored, func(i, j int) bool {
		return scored[i].Score > scored[j].Score
	})
	return scored
}

// scoreRegionPreference returns 1.0 / (1 + rank): first-preference region scores
// 1.0, second scores 0.5, third scores 0.33, etc.
func (e *Engine) scoreRegionPreference(
	workload *platformv1alpha1.GlobalWorkload,
	cluster *platformv1alpha1.ClusterRegistration,
) float64 {
	for i, region := range workload.Spec.RegionPreference {
		if region == cluster.Spec.Region {
			return 1.0 / float64(i+1)
		}
	}
	return 0.0
}

// scoreCapacity returns a value reflecting the placement strategy.
// BinPack: prefer fuller clusters; Spread: prefer emptier ones.
func (e *Engine) scoreCapacity(
	workload *platformv1alpha1.GlobalWorkload,
	cluster *platformv1alpha1.ClusterRegistration,
) float64 {
	used := utilization(cluster)
	switch workload.Spec.PlacementStrategy {
	case platformv1alpha1.BinPack:
		return used
	case platformv1alpha1.Spread:
		return 1.0 - used
	default:
		return 1.0 - used // Spread is the safer default
	}
}

// scoreHeadroom returns a large negative penalty for clusters above the
// HeadroomThreshold. Applied unconditionally — independent of strategy.
// This is the drift-safety guardrail: even if BinPack would prefer a full
// cluster, the penalty keeps us away from the edge.
func (e *Engine) scoreHeadroom(cluster *platformv1alpha1.ClusterRegistration) float64 {
	if utilization(cluster) > e.HeadroomThreshold {
		return -e.HeadroomPenalty
	}
	return 0.0
}

// ----------------------------------------------------------------------------
// Phase 3: Distribute — assign replicas across the top scored clusters.
// ----------------------------------------------------------------------------

func (e *Engine) distribute(
	workload *platformv1alpha1.GlobalWorkload,
	scored []scoredCluster,
) *Plan {
	switch workload.Spec.PlacementStrategy {
	case platformv1alpha1.BinPack:
		return e.distributeBinPack(workload, scored)
	default:
		return e.distributeSpread(workload, scored)
	}
}

// distributeBinPack greedily fills the highest-scored cluster first;
// spills over to the next when the first is at capacity.
func (e *Engine) distributeBinPack(
	workload *platformv1alpha1.GlobalWorkload,
	scored []scoredCluster,
) *Plan {
	plan := &Plan{}
	remaining := workload.Spec.Replicas

	for _, sc := range scored {
		if remaining <= 0 {
			break
		}
		fitCount := replicasFittable(workload, &sc.Cluster)
		if fitCount <= 0 {
			continue
		}
		assign := minInt32(remaining, fitCount)
		plan.Assignments = append(plan.Assignments, Assignment{
			ClusterName: sc.Cluster.Name,
			Region:      sc.Cluster.Spec.Region,
			Replicas:    assign,
		})
		remaining -= assign
	}

	if remaining > 0 {
		plan.Reason = fmt.Sprintf("insufficient capacity: %d replicas unplaced", remaining)
	}
	return plan
}

// distributeSpread divides replicas approximately evenly across the top-K
// scored clusters, respecting per-cluster capacity. Leftover replicas spill
// into the next clusters.
func (e *Engine) distributeSpread(
	workload *platformv1alpha1.GlobalWorkload,
	scored []scoredCluster,
) *Plan {
	plan := &Plan{}

	// Choose top-K clusters. For now K = min(3, len(scored)); tunable.
	topK := scored
	if len(topK) > 3 {
		topK = topK[:3]
	}

	// Equal split with remainder distributed across the first few.
	perCluster := workload.Spec.Replicas / int32(len(topK))
	remainder := workload.Spec.Replicas % int32(len(topK))

	placed := int32(0)
	for i, sc := range topK {
		want := perCluster
		if int32(i) < remainder {
			want++
		}
		fitCount := replicasFittable(workload, &sc.Cluster)
		assign := minInt32(want, fitCount)
		if assign > 0 {
			plan.Assignments = append(plan.Assignments, Assignment{
				ClusterName: sc.Cluster.Name,
				Region:      sc.Cluster.Spec.Region,
				Replicas:    assign,
			})
			placed += assign
		}
	}

	// Spill leftover into next clusters beyond top-K.
	leftover := workload.Spec.Replicas - placed
	if leftover > 0 && len(scored) > len(topK) {
		for _, sc := range scored[len(topK):] {
			if leftover <= 0 {
				break
			}
			fitCount := replicasFittable(workload, &sc.Cluster)
			if fitCount <= 0 {
				continue
			}
			assign := minInt32(leftover, fitCount)
			plan.Assignments = append(plan.Assignments, Assignment{
				ClusterName: sc.Cluster.Name,
				Region:      sc.Cluster.Spec.Region,
				Replicas:    assign,
			})
			leftover -= assign
		}
	}

	if leftover > 0 {
		plan.Reason = fmt.Sprintf("insufficient capacity: %d replicas unplaced", leftover)
	}
	return plan
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func utilization(cluster *platformv1alpha1.ClusterRegistration) float64 {
	observed := cluster.Status.ObservedCapacity.CPU.AsApproximateFloat64()
	if observed <= 0 {
		return 1.0 // treat unknown capacity as full
	}
	allocated := cluster.Status.AllocatedCapacity.CPU.AsApproximateFloat64()
	return allocated / observed
}

func replicasFittable(
	workload *platformv1alpha1.GlobalWorkload,
	cluster *platformv1alpha1.ClusterRegistration,
) int32 {
	free := freeCapacity(cluster)
	perReplicaCPU := workload.Spec.Resources.CPU.AsApproximateFloat64()
	perReplicaMem := workload.Spec.Resources.Memory.AsApproximateFloat64()

	var cpuFits, memFits float64
	if perReplicaCPU > 0 {
		cpuFits = free.CPU / perReplicaCPU
	} else {
		cpuFits = math.MaxInt32
	}
	if perReplicaMem > 0 {
		memFits = free.Memory / perReplicaMem
	} else {
		memFits = math.MaxInt32
	}

	binding := math.Min(cpuFits, memFits)
	if binding < 0 {
		return 0
	}
	return int32(math.Floor(binding))
}

func fitsAtLeastOneReplica(
	workload *platformv1alpha1.GlobalWorkload,
	cluster *platformv1alpha1.ClusterRegistration,
) bool {
	return replicasFittable(workload, cluster) >= 1
}

type freeCap struct {
	CPU    float64
	Memory float64
}

func freeCapacity(cluster *platformv1alpha1.ClusterRegistration) freeCap {
	observedCPU := cluster.Status.ObservedCapacity.CPU.AsApproximateFloat64()
	observedMem := cluster.Status.ObservedCapacity.Memory.AsApproximateFloat64()
	allocatedCPU := cluster.Status.AllocatedCapacity.CPU.AsApproximateFloat64()
	allocatedMem := cluster.Status.AllocatedCapacity.Memory.AsApproximateFloat64()
	return freeCap{
		CPU:    observedCPU - allocatedCPU,
		Memory: observedMem - allocatedMem,
	}
}

func minInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
