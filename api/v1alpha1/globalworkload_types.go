package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlacementStrategy determines how replicas are distributed across clusters.
type PlacementStrategy string

const (
	// Spread distributes replicas across multiple clusters for fault tolerance.
	Spread PlacementStrategy = "Spread"
	// BinPack concentrates replicas in the highest-scored clusters first.
	BinPack PlacementStrategy = "BinPack"
)

// GlobalWorkloadSpec defines the desired state of a GlobalWorkload.
type GlobalWorkloadSpec struct {
	// Image is the container image to run.
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Replicas is the desired total number of pods across all target clusters.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas"`

	// Resources describes the per-replica resource requirements.
	// +kubebuilder:validation:Required
	Resources ResourceRequirements `json:"resources"`

	// RegionPreference is an ordered list of regions. The placement engine
	// prefers earlier entries; clusters in regions not listed are ineligible.
	// +kubebuilder:validation:MinItems=1
	RegionPreference []string `json:"regionPreference"`

	// PlacementStrategy controls how replicas are distributed across eligible clusters.
	// +kubebuilder:validation:Enum=Spread;BinPack
	// +kubebuilder:default=Spread
	PlacementStrategy PlacementStrategy `json:"placementStrategy,omitempty"`
}

// ResourceRequirements describes resources needed per replica.
type ResourceRequirements struct {
	CPU    resource.Quantity `json:"cpu"`
	Memory resource.Quantity `json:"memory"`
}

// GlobalWorkloadStatus defines the observed state of a GlobalWorkload.
type GlobalWorkloadStatus struct {
	// ObservedGeneration is the .metadata.generation observed at the last reconcile.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Placements describes where replicas are currently assigned.
	Placements []Placement `json:"placements,omitempty"`

	// Conditions represent the latest available observations of the workload's state.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Placement represents replicas placed in a specific cluster.
type Placement struct {
	ClusterName   string `json:"clusterName"`
	Region        string `json:"region"`
	Replicas      int32  `json:"replicas"`
	ReadyReplicas int32  `json:"readyReplicas"`
}

// GlobalWorkload is the Schema for the globalworkloads API.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Strategy",type=string,JSONPath=`.spec.placementStrategy`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type GlobalWorkload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GlobalWorkloadSpec   `json:"spec,omitempty"`
	Status GlobalWorkloadStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type GlobalWorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []GlobalWorkload `json:"items"`
}

func init() {
	SchemeBuilder.Register(&GlobalWorkload{}, &GlobalWorkloadList{})
}
