package fake

import (
	"context"
	"fmt"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
)

var _ multicluster.Provider = &Provider{}

type indexRequest struct {
	obj          client.Object
	field        string
	extractValue client.IndexerFunc
}

// Provider is a fake implementation of multicluster.Provider for use in tests.
// Clusters are registered via Add before or after the provider is used.
// IndexField registrations are applied to all current and future clusters.
type Provider struct {
	mu       sync.RWMutex
	clusters map[multicluster.ClusterName]cluster.Cluster
	indexes  []indexRequest
}

// NewProvider returns an empty fake Provider.
func NewProvider() *Provider {
	return &Provider{
		clusters: make(map[multicluster.ClusterName]cluster.Cluster),
	}
}

// Add registers a cluster under the given name. If IndexField has already been
// called on the provider, the registered indexes are replayed on cl immediately.
func (p *Provider) Add(ctx context.Context, name multicluster.ClusterName, cl cluster.Cluster) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, idx := range p.indexes {
		if err := cl.GetFieldIndexer().IndexField(ctx, idx.obj, idx.field, idx.extractValue); err != nil {
			return fmt.Errorf("applying index %q to cluster %q: %w", idx.field, name, err)
		}
	}
	p.clusters[name] = cl
	return nil
}

// Get implements multicluster.Provider.
func (p *Provider) Get(_ context.Context, name multicluster.ClusterName) (cluster.Cluster, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	cl, ok := p.clusters[name]
	if !ok {
		return nil, fmt.Errorf("cluster %q: %w", name, multicluster.ErrClusterNotFound)
	}
	return cl, nil
}

// IndexField implements multicluster.Provider. It applies the index to all
// currently registered clusters and remembers it for clusters added later.
func (p *Provider) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.indexes = append(p.indexes, indexRequest{obj: obj, field: field, extractValue: extractValue})

	for name, cl := range p.clusters {
		if err := cl.GetFieldIndexer().IndexField(ctx, obj, field, extractValue); err != nil {
			return fmt.Errorf("indexing field %q on cluster %q: %w", field, name, err)
		}
	}
	return nil
}
