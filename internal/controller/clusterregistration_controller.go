package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	platformv1alpha1 "github.com/golfrider/global-workload-orchestrator/api/v1alpha1"
	"github.com/golfrider/global-workload-orchestrator/internal/clusters"
)

// ClusterRegistrationReconciler reconciles a ClusterRegistration object.
//
// For each registration, it probes the target cluster's API server,
// observes node capacity, and updates the registration's status fields:
// Healthy, LastProbeTime, and ObservedCapacity.
//
// This controller is the SOLE writer of those status fields. The
// GlobalWorkloadReconciler writes AllocatedCapacity on registrations;
// no field is written by more than one controller.
type ClusterRegistrationReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	ClusterMgr clusters.Manager
}

// +kubebuilder:rbac:groups=platform.platform.local,resources=clusterregistrations,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=platform.platform.local,resources=clusterregistrations/status,verbs=get;update;patch

const (
	requeueAfterProbe   = 15 * time.Second
	requeueAfterFailure = 30 * time.Second
)

func (r *ClusterRegistrationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("clusterregistration", req.NamespacedName)

	// ------------------------------------------------------------------
	// Phase 1: OBSERVE
	// ------------------------------------------------------------------

	var registration platformv1alpha1.ClusterRegistration
	if err := r.Get(ctx, req.NamespacedName, &registration); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// ------------------------------------------------------------------
	// Phase 2: PROBE — attempt to reach the target cluster.
	// ------------------------------------------------------------------

	probeResult := r.probeCluster(ctx, registration.Name)

	// ------------------------------------------------------------------
	// Phase 3: STATUS — write what we observed.
	// ------------------------------------------------------------------

	now := metav1.Now()
	registration.Status.LastProbeTime = &now

	if probeResult.err != nil {
		// Probe failed — mark unhealthy, leave previous capacity in place.
		logger.Info("cluster probe failed", "error", probeResult.err.Error())
		registration.Status.Healthy = false
		r.setProbeCondition(&registration, metav1.ConditionFalse, "ProbeFailed", probeResult.err.Error())
		if err := r.Status().Update(ctx, &registration); err != nil {
			return ctrl.Result{}, fmt.Errorf("update status after probe failure: %w", err)
		}
		// Failure backoff is longer than success interval.
		return ctrl.Result{RequeueAfter: requeueAfterFailure}, nil
	}

	// Probe succeeded — update healthy state and observed capacity.
	registration.Status.Healthy = true
	registration.Status.ObservedCapacity = probeResult.capacity
	r.setProbeCondition(&registration, metav1.ConditionTrue, "ProbeSuccessful", "cluster reachable and responsive")

	if err := r.Status().Update(ctx, &registration); err != nil {
		return ctrl.Result{}, fmt.Errorf("update status: %w", err)
	}

	// ------------------------------------------------------------------
	// Phase 4: REQUEUE
	// ------------------------------------------------------------------

	return ctrl.Result{RequeueAfter: requeueAfterProbe}, nil
}

// probeResult bundles what a probe attempt observed.
type probeResult struct {
	capacity platformv1alpha1.Capacity
	err      error
}

// probeCluster attempts to list nodes in the target cluster and sum their
// allocatable capacity. Failure modes: client unreachable, list permission
// denied, network timeout. Any error indicates the cluster is unhealthy.
func (r *ClusterRegistrationReconciler) probeCluster(
	ctx context.Context,
	clusterName string,
) probeResult {
	targetClient, err := r.ClusterMgr.ClientFor(ctx, clusterName)
	if err != nil {
		return probeResult{err: fmt.Errorf("get client: %w", err)}
	}

	// Set a short timeout so probes don't hang the reconcile worker.
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var nodes corev1.NodeList
	if err := targetClient.List(probeCtx, &nodes); err != nil {
		// Invalidate the cached client so the next probe rebuilds it —
		// the old client may have stale credentials.
		r.ClusterMgr.Invalidate(clusterName)
		return probeResult{err: fmt.Errorf("list nodes: %w", err)}
	}

	// Sum allocatable resources across all nodes.
	totalCPU := resource.NewQuantity(0, resource.DecimalSI)
	totalMem := resource.NewQuantity(0, resource.BinarySI)
	for _, node := range nodes.Items {
		if cpu, ok := node.Status.Allocatable[corev1.ResourceCPU]; ok {
			totalCPU.Add(cpu)
		}
		if mem, ok := node.Status.Allocatable[corev1.ResourceMemory]; ok {
			totalMem.Add(mem)
		}
	}

	return probeResult{
		capacity: platformv1alpha1.Capacity{
			CPU:    *totalCPU,
			Memory: *totalMem,
		},
	}
}

// setProbeCondition updates the "Healthy" condition on the registration.
// Follows the same single-condition-per-type pattern used elsewhere.
func (r *ClusterRegistrationReconciler) setProbeCondition(
	registration *platformv1alpha1.ClusterRegistration,
	status metav1.ConditionStatus,
	reason, message string,
) {
	const condType = "Healthy"
	now := metav1.Now()
	for i, c := range registration.Status.Conditions {
		if c.Type != condType {
			continue
		}
		if c.Status == status && c.Reason == reason {
			return // no change
		}
		registration.Status.Conditions[i] = metav1.Condition{
			Type:               condType,
			Status:             status,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: now,
		}
		return
	}
	registration.Status.Conditions = append(registration.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	})
}

// SetupWithManager registers this controller with the manager.
func (r *ClusterRegistrationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&platformv1alpha1.ClusterRegistration{}).
		Complete(r)
}
