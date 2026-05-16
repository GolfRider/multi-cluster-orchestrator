package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterRegistrationSpec defines the desired state of a registered cluster.
type ClusterRegistrationSpec struct {
	// Region is the logical region this cluster belongs to.
	// Used by the placement engine for region preference matching.
	// +kubebuilder:validation:Required
	Region string `json:"region"`

	// KubeconfigSecretRef references a Secret in the same namespace
	// containing the kubeconfig for this cluster under the key "kubeconfig".
	// +kubebuilder:validation:Required
	KubeconfigSecretRef corev1.LocalObjectReference `json:"kubeconfigSecretRef"`
}

// ClusterRegistrationStatus defines the observed state of a registered cluster.
type ClusterRegistrationStatus struct {
	// Healthy indicates whether the cluster is reachable and operating normally.
	// Written by the health watcher controller.
	Healthy bool `json:"healthy"`

	// LastProbeTime is when the cluster's health was last checked.
	LastProbeTime *metav1.Time `json:"lastProbeTime,omitempty"`

	// ObservedCapacity is the total allocatable capacity across nodes in this cluster.
	// Written by the health watcher controller.
	ObservedCapacity Capacity `json:"observedCapacity,omitempty"`

	// AllocatedCapacity is the sum of resources requested by all GlobalWorkload
	// placements currently assigned to this cluster.
	// Written by the GlobalWorkload reconciler.
	AllocatedCapacity Capacity `json:"allocatedCapacity,omitempty"`

	// Conditions represent the latest available observations of the cluster's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Capacity describes a set of resource quantities.
type Capacity struct {
	CPU    resource.Quantity `json:"cpu,omitempty"`
	Memory resource.Quantity `json:"memory,omitempty"`
}

// ClusterRegistration is the Schema for the clusterregistrations API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Region",type=string,JSONPath=`.spec.region`
// +kubebuilder:printcolumn:name="Healthy",type=boolean,JSONPath=`.status.healthy`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ClusterRegistration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterRegistrationSpec   `json:"spec,omitempty"`
	Status ClusterRegistrationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ClusterRegistrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterRegistration `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterRegistration{}, &ClusterRegistrationList{})
}
