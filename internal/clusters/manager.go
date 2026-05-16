package clusters

import (
	"context"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	platformv1alpha1 "github.com/golfrider/global-workload-orchestrator/api/v1alpha1"
)

// Manager builds and caches Kubernetes clients for registered target clusters.
//
// The manager reads ClusterRegistration objects and their referenced kubeconfig
// Secrets from the management cluster, then constructs a controller-runtime client
// per registered target cluster. Clients are cached by cluster name; the cache
// can be invalidated when credentials change.
type Manager interface {
	ClientFor(ctx context.Context, clusterName string) (client.Client, error)
	Invalidate(clusterName string)
}

// manager is the concrete implementation backed by a sync.Map cache.
type manager struct {
	// mgmtClient reads ClusterRegistrations and Secrets from the management cluster.
	mgmtClient client.Client

	// secretNamespace is where kubeconfig Secrets live. For this prototype,
	// that's the namespace the controller runs in; in production it would be
	// a dedicated cluster-registry namespace.
	secretNamespace string

	// scheme is the runtime.Scheme used when building per-cluster clients.
	// We use the same scheme as the management cluster so callers can work
	// with typed objects (Deployments, etc.) on remote clusters.
	scheme *runtime.Scheme

	// mu guards the cache.
	mu    sync.Mutex
	cache map[string]client.Client
}

// New constructs a Manager.
//
// mgmtClient is the controller's client to the management cluster, used to read
// ClusterRegistration objects and kubeconfig Secrets.
func New(mgmtClient client.Client, scheme *runtime.Scheme, secretNamespace string) Manager {
	return &manager{
		mgmtClient:      mgmtClient,
		scheme:          scheme,
		secretNamespace: secretNamespace,
		cache:           make(map[string]client.Client),
	}
}

// ClientFor returns a client for the named registered cluster.
//
// On cache miss: reads the ClusterRegistration, loads the referenced Secret,
// parses the kubeconfig, constructs a client, and caches it.
func (m *manager) ClientFor(ctx context.Context, clusterName string) (client.Client, error) {
	// Fast path: cache hit.
	m.mu.Lock()
	if c, ok := m.cache[clusterName]; ok {
		m.mu.Unlock()
		return c, nil
	}
	m.mu.Unlock()

	// Slow path: build a new client.
	c, err := m.buildClient(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("build client for %q: %w", clusterName, err)
	}

	// Populate cache. If another goroutine raced us, prefer theirs to avoid
	// duplicate connections — discard our build.
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.cache[clusterName]; ok {
		return existing, nil
	}
	m.cache[clusterName] = c
	return c, nil
}

// Invalidate drops the cached client. The next ClientFor call rebuilds it.
func (m *manager) Invalidate(clusterName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cache, clusterName)
}

// buildClient constructs a fresh client for a cluster by reading its
// registration, fetching the kubeconfig Secret, and parsing the rest.Config.
func (m *manager) buildClient(ctx context.Context, clusterName string) (client.Client, error) {
	// 1. Read the ClusterRegistration from the management cluster.
	var reg platformv1alpha1.ClusterRegistration
	if err := m.mgmtClient.Get(ctx, types.NamespacedName{Name: clusterName}, &reg); err != nil {
		return nil, fmt.Errorf("get ClusterRegistration: %w", err)
	}

	// 2. Read the referenced kubeconfig Secret.
	var secret corev1.Secret
	secretKey := types.NamespacedName{
		Namespace: m.secretNamespace,
		Name:      reg.Spec.KubeconfigSecretRef.Name,
	}
	if err := m.mgmtClient.Get(ctx, secretKey, &secret); err != nil {
		return nil, fmt.Errorf("get kubeconfig Secret %s: %w", secretKey, err)
	}

	// 3. Extract the kubeconfig bytes under the conventional key.
	kubeconfigBytes, ok := secret.Data["kubeconfig"]
	if !ok {
		return nil, fmt.Errorf("Secret %s missing 'kubeconfig' key", secretKey)
	}

	// 4. Parse into a rest.Config.
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigBytes)
	if err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}

	// 5. Build a controller-runtime client with our scheme so callers can
	//    work with typed objects (corev1, appsv1, etc.).
	c, err := client.New(restConfig, client.Options{Scheme: m.scheme})
	if err != nil {
		return nil, fmt.Errorf("build client: %w", err)
	}

	return c, nil
}
