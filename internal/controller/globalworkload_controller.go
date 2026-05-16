package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	platformv1alpha1 "github.com/golfrider/global-workload-orchestrator/api/v1alpha1"
	"github.com/golfrider/global-workload-orchestrator/internal/clusters"
	"github.com/golfrider/global-workload-orchestrator/internal/placement"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GlobalWorkloadReconciler reconciles a GlobalWorkload object.
type GlobalWorkloadReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	ClusterMgr clusters.Manager

	engine *placement.Engine
}

// +kubebuilder:rbac:groups=platform.platform.local,resources=globalworkloads,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=platform.platform.local,resources=globalworkloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=platform.platform.local,resources=globalworkloads/finalizers,verbs=update
// +kubebuilder:rbac:groups=platform.platform.local,resources=clusterregistrations,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile is the entry point invoked by controller-runtime whenever
// a watched object changes. It follows the pattern:
//
//	Observe → Decide → Act → Status → Requeue
//
// Each phase is intentionally separated for clarity and correctness.
func (r *GlobalWorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("globalworkload", req.NamespacedName)

	// ------------------------------------------------------------------
	// Phase 1: OBSERVE — read current state.
	// ------------------------------------------------------------------

	var workload platformv1alpha1.GlobalWorkload
	if err := r.Get(ctx, req.NamespacedName, &workload); err != nil {
		// If the GlobalWorkload was deleted, there's nothing to reconcile.
		// Returning IgnoreNotFound here keeps the reconciler quiet on delete events.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// ------------------------------------------------------------------
	// Phase 1a: LIFECYCLE — handle deletion or ensure finalizer.
	// ------------------------------------------------------------------

	if !workload.DeletionTimestamp.IsZero() {
		// Object is being deleted. Run cleanup, then remove our finalizer.
		return r.reconcileDelete(ctx, &workload)
	}

	// Object is alive. Ensure our finalizer is present before we create anything.
	if !controllerutil.ContainsFinalizer(&workload, finalizerName) {
		controllerutil.AddFinalizer(&workload, finalizerName)
		if err := r.Update(ctx, &workload); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		// Return after the update; the modification triggers another reconcile.
		return ctrl.Result{}, nil
	}

	// List all registered clusters. The placement engine considers all of them;
	// filtering for health and feasibility happens inside the engine.
	var clusterList platformv1alpha1.ClusterRegistrationList
	if err := r.List(ctx, &clusterList); err != nil {
		return ctrl.Result{}, fmt.Errorf("list ClusterRegistrations: %w", err)
	}

	// ------------------------------------------------------------------
	// Phase 2: DECIDE — compute the placement plan.
	// ------------------------------------------------------------------

	plan, err := r.engine.ComputePlan(&workload, clusterList.Items)
	if err != nil {
		// No feasible clusters: surface this in status and requeue with backoff.
		logger.Info("placement infeasible", "reason", err.Error())
		r.setCondition(&workload, "Scheduled", metav1.ConditionFalse, "NoFeasibleClusters", err.Error())
		if statusErr := r.Status().Update(ctx, &workload); statusErr != nil {
			return ctrl.Result{}, fmt.Errorf("update status after infeasible: %w", statusErr)
		}
		// Don't return the engine's error — that would trigger fast retries
		// with backoff, which is the wrong shape. Instead, requeue on a slow timer
		// so we re-check when cluster state may have changed.
		return ctrl.Result{RequeueAfter: requeueAfterInfeasible}, nil
	}

	logger.V(1).Info("computed plan", "assignments", len(plan.Assignments))

	// ------------------------------------------------------------------
	// Phase 3: ACT — converge cluster state toward the plan.
	// ------------------------------------------------------------------

	if err := r.applyPlan(ctx, &workload, plan); err != nil {
		// Apply errors return up to controller-runtime, which retries with backoff.
		return ctrl.Result{}, fmt.Errorf("apply plan: %w", err)
	}

	// ------------------------------------------------------------------
	// Phase 4: STATUS — write what we observed and did.
	// ------------------------------------------------------------------

	r.updatePlacementStatus(&workload, plan)
	if plan.Reason != "" {
		r.setCondition(&workload, "Scheduled", metav1.ConditionFalse, "PartialPlacement", plan.Reason)
	} else {
		r.setCondition(&workload, "Scheduled", metav1.ConditionTrue, "AllReplicasPlaced", "all replicas placed across target clusters")
	}
	workload.Status.ObservedGeneration = workload.Generation

	if err := r.Status().Update(ctx, &workload); err != nil {
		// Conflict means someone else updated the workload between our read and write.
		// Return the error; controller-runtime will requeue and we'll observe fresh state.
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}

	// ------------------------------------------------------------------
	// Phase 5: REQUEUE — schedule the next reconcile.
	// ------------------------------------------------------------------

	return ctrl.Result{RequeueAfter: requeueAfterSuccess}, nil
}

// ----------------------------------------------------------------------------
// applyPlan creates/updates target-cluster Deployments to match the plan,
// and removes any Deployments from clusters no longer in the plan.
// ----------------------------------------------------------------------------

func (r *GlobalWorkloadReconciler) applyPlan(
	ctx context.Context,
	workload *platformv1alpha1.GlobalWorkload,
	plan *placement.Plan,
) error {
	// Track which clusters the plan says should have a Deployment.
	planClusters := make(map[string]int32, len(plan.Assignments))
	for _, a := range plan.Assignments {
		planClusters[a.ClusterName] = a.Replicas
	}

	// 1. Ensure Deployments exist in clusters the plan targets.
	for _, assignment := range plan.Assignments {
		if err := r.ensureDeployment(ctx, workload, assignment.ClusterName, assignment.Replicas); err != nil {
			return fmt.Errorf("ensure Deployment in %s: %w", assignment.ClusterName, err)
		}
	}

	// 2. Drain Deployments from clusters that were previously placed but are no
	//    longer in the plan (this is how failover removes the old placement).
	for _, prior := range workload.Status.Placements {
		if _, stillPlanned := planClusters[prior.ClusterName]; stillPlanned {
			continue // still in the plan; leave it alone
		}
		if err := r.removeDeployment(ctx, workload, prior.ClusterName); err != nil {
			return fmt.Errorf("remove Deployment from %s: %w", prior.ClusterName, err)
		}
	}

	return nil
}

// ensureDeployment creates or updates a Deployment in the named target cluster
// to match the workload's desired image and replica count.
func (r *GlobalWorkloadReconciler) ensureDeployment(
	ctx context.Context,
	workload *platformv1alpha1.GlobalWorkload,
	clusterName string,
	replicas int32,
) error {
	targetClient, err := r.ClusterMgr.ClientFor(ctx, clusterName)
	if err != nil {
		return fmt.Errorf("client for %s: %w", clusterName, err)
	}

	desired := buildDeployment(workload, replicas)

	// Try to fetch; if not found, create. Otherwise, patch toward desired.
	var existing appsv1.Deployment
	key := types.NamespacedName{Namespace: desired.Namespace, Name: desired.Name}
	err = targetClient.Get(ctx, key, &existing)
	switch {
	case apierrors.IsNotFound(err):
		if err := targetClient.Create(ctx, desired); err != nil {
			return fmt.Errorf("create Deployment: %w", err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("get Deployment: %w", err)
	}

	// Update if replicas or image differ.
	needsUpdate := false
	if existing.Spec.Replicas == nil || *existing.Spec.Replicas != replicas {
		existing.Spec.Replicas = &replicas
		needsUpdate = true
	}
	if len(existing.Spec.Template.Spec.Containers) > 0 &&
		existing.Spec.Template.Spec.Containers[0].Image != workload.Spec.Image {
		existing.Spec.Template.Spec.Containers[0].Image = workload.Spec.Image
		needsUpdate = true
	}
	if !needsUpdate {
		return nil
	}
	if err := targetClient.Update(ctx, &existing); err != nil {
		return fmt.Errorf("update Deployment: %w", err)
	}
	return nil
}

// removeDeployment deletes the workload's Deployment from a target cluster.
// Used when failover or rescheduling drops a cluster from the plan.
//
// If the cluster is unreachable for any reason (kubeconfig invalid, network
// unreachable, API server down), we log and return nil. The alternative
// would be to block forever waiting for the dead cluster, which prevents
// failover from completing.
func (r *GlobalWorkloadReconciler) removeDeployment(
	ctx context.Context,
	workload *platformv1alpha1.GlobalWorkload,
	clusterName string,
) error {
	logger := log.FromContext(ctx)

	targetClient, err := r.ClusterMgr.ClientFor(ctx, clusterName)
	if err != nil {
		logger.Info("cannot reach cluster for deletion; skipping",
			"cluster", clusterName, "reason", err.Error())
		return nil
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultTargetNamespace,
			Name:      deploymentName(workload),
		},
	}

	err = targetClient.Delete(ctx, deployment)
	switch {
	case err == nil:
		return nil
	case apierrors.IsNotFound(err):
		// Already gone. Fine.
		return nil
	case isClusterUnreachable(err):
		// Network-level failure: cluster is unreachable. Treat the same as
		// ClientFor failure — log and skip. Invalidate the cached client so
		// the next attempt rebuilds from fresh credentials if they exist.
		logger.Info("cluster unreachable during deletion; skipping",
			"cluster", clusterName, "reason", err.Error())
		r.ClusterMgr.Invalidate(clusterName)
		return nil
	default:
		// Unknown error — return it so controller-runtime retries.
		return fmt.Errorf("delete Deployment in %s: %w", clusterName, err)
	}
}

// isClusterUnreachable returns true if the error indicates the target cluster's
// API server cannot be reached. We treat these as recoverable for cleanup
// operations — the cluster might come back, but we don't block the world
// waiting for it.
func isClusterUnreachable(err error) bool {
	if err == nil {
		return false
	}
	// Common indicators of network-level unreachability:
	//   - "connection refused" — API server down
	//   - "no such host" — DNS gone or stale endpoint
	//   - "i/o timeout" — network partition
	//   - "EOF" — connection cut mid-stream
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "TLS handshake timeout")
}

// reconcileDelete handles cleanup when a GlobalWorkload is being deleted.
// It removes Deployments from every cluster the workload was placed in, then
// removes the finalizer so Kubernetes can complete deletion.
func (r *GlobalWorkloadReconciler) reconcileDelete(
	ctx context.Context,
	workload *platformv1alpha1.GlobalWorkload,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("globalworkload", workload.Name)
	logger.Info("handling deletion")

	// Drain every cluster the workload was placed in.
	// We iterate over current status; if status was stale, the worst case is we
	// try to delete something that doesn't exist (handled gracefully by
	// removeDeployment via IsNotFound).
	for _, p := range workload.Status.Placements {
		if err := r.removeDeployment(ctx, workload, p.ClusterName); err != nil {
			// Cleanup error — don't remove the finalizer yet. Return the error
			// so controller-runtime retries.
			return ctrl.Result{}, fmt.Errorf("cleanup in cluster %s: %w", p.ClusterName, err)
		}
	}

	// All children removed. Drop our finalizer.
	controllerutil.RemoveFinalizer(workload, finalizerName)
	if err := r.Update(ctx, workload); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}

	logger.Info("deletion complete")
	return ctrl.Result{}, nil
}

// buildDeployment renders the desired Deployment object for a workload.
// Stateless, deterministic — same inputs always produce the same output.
func buildDeployment(workload *platformv1alpha1.GlobalWorkload, replicas int32) *appsv1.Deployment {
	labels := map[string]string{
		"platform.local/managed-by":    "global-workload-orchestrator",
		"platform.local/workload-name": workload.Name,
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: defaultTargetNamespace,
			Name:      deploymentName(workload),
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "main",
							Image: workload.Spec.Image,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    workload.Spec.Resources.CPU,
									corev1.ResourceMemory: workload.Spec.Resources.Memory,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    workload.Spec.Resources.CPU,
									corev1.ResourceMemory: workload.Spec.Resources.Memory,
								},
							},
						},
					},
				},
			},
		},
	}
}

// ----------------------------------------------------------------------------
// Status helpers
// ----------------------------------------------------------------------------

func (r *GlobalWorkloadReconciler) updatePlacementStatus(
	workload *platformv1alpha1.GlobalWorkload,
	plan *placement.Plan,
) {
	placements := make([]platformv1alpha1.Placement, 0, len(plan.Assignments))
	for _, a := range plan.Assignments {
		placements = append(placements, platformv1alpha1.Placement{
			ClusterName: a.ClusterName,
			Region:      a.Region,
			Replicas:    a.Replicas,
		})
	}
	workload.Status.Placements = placements
}

func (r *GlobalWorkloadReconciler) setCondition(
	workload *platformv1alpha1.GlobalWorkload,
	condType string,
	status metav1.ConditionStatus,
	reason, message string,
) {
	now := metav1.Now()
	// Find existing condition or append new.
	for i, c := range workload.Status.Conditions {
		if c.Type != condType {
			continue
		}
		if c.Status == status && c.Reason == reason {
			return // no change
		}
		workload.Status.Conditions[i] = metav1.Condition{
			Type:               condType,
			Status:             status,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: now,
		}
		return
	}
	workload.Status.Conditions = append(workload.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	})
}

// ----------------------------------------------------------------------------
// Setup
// ----------------------------------------------------------------------------

const (
	defaultTargetNamespace = "default"
	requeueAfterSuccess    = 60 * time.Second
	requeueAfterInfeasible = 30 * time.Second

	// finalizerName identifies our controller's cleanup responsibility on a
	// GlobalWorkload object. The name is namespaced to our domain to avoid
	// collision with other controllers that may operate on the same kind.
	finalizerName = "platform.platform.local/globalworkload-cleanup"
)

func deploymentName(workload *platformv1alpha1.GlobalWorkload) string {
	return "gwo-" + workload.Name
}

func (r *GlobalWorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.engine = placement.NewEngine()
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.GlobalWorkload{}).
		Watches(
			&platformv1alpha1.ClusterRegistration{},
			handler.EnqueueRequestsFromMapFunc(r.enqueueWorkloadsForCluster),
		).
		Complete(r)
}

// enqueueWorkloadsForCluster maps a ClusterRegistration change to all workloads,
// so that cluster health changes trigger re-evaluation of placement.
func (r *GlobalWorkloadReconciler) enqueueWorkloadsForCluster(ctx context.Context, obj client.Object) []ctrl.Request {
	var workloads platformv1alpha1.GlobalWorkloadList
	if err := r.List(ctx, &workloads); err != nil {
		return nil
	}
	requests := make([]ctrl.Request, 0, len(workloads.Items))
	for _, w := range workloads.Items {
		requests = append(requests, ctrl.Request{NamespacedName: types.NamespacedName{Name: w.Name}})
	}
	return requests
}
