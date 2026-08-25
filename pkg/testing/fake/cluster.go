package fake

import (
	"context"
	"net/http"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
)

var _ cluster.Cluster = &Cluster{}

// Cluster is a fake implementation of cluster.Cluster backed by a fake client.
// Create one with NewCluster.
type Cluster struct {
	client client.Client
	cache  cache.Cache
	scheme *runtime.Scheme
	mapper meta.RESTMapper
}

// ClusterOption configures a Cluster.
type ClusterOption func(*Cluster)

// WithClientBuilder replaces the default fake client with one built from b.
func WithClientBuilder(b *fakeclient.ClientBuilder) ClusterOption {
	return func(c *Cluster) {
		c.client = b.Build()
	}
}

// WithClient allows to set a custom client for the Cluster.
// Incompatible with WithClientBuilder.
func WithClient(cl client.Client) ClusterOption {
	return func(c *Cluster) {
		c.client = cl
	}
}

// NewCluster creates a fake Cluster backed by a fake client and an in-memory
// cache. Use ClusterOption to customise it further.
func NewCluster(scheme *runtime.Scheme, opts ...ClusterOption) *Cluster {
	c := &Cluster{
		scheme: scheme,
		cache:  &informertest.FakeInformers{Scheme: scheme},
		mapper: meta.NewDefaultRESTMapper(nil),
	}
	for _, o := range opts {
		o(c)
	}
	if c.client == nil {
		c.client = fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	}
	return c
}

func (c *Cluster) GetClient() client.Client             { return c.client }
func (c *Cluster) GetCache() cache.Cache                { return c.cache }
func (c *Cluster) GetScheme() *runtime.Scheme           { return c.scheme }
func (c *Cluster) GetFieldIndexer() client.FieldIndexer { return c.cache }
func (c *Cluster) GetRESTMapper() meta.RESTMapper       { return c.mapper }
func (c *Cluster) GetConfig() *rest.Config              { return &rest.Config{} }
func (c *Cluster) GetHTTPClient() *http.Client          { return http.DefaultClient }
func (c *Cluster) GetAPIReader() client.Reader          { return c.client }
func (c *Cluster) Start(_ context.Context) error        { return nil }

// GetEventRecorderFor satisfies the deprecated recorder.Provider method.
func (c *Cluster) GetEventRecorderFor(name string) record.EventRecorder {
	return record.NewFakeRecorder(100)
}

// GetEventRecorder satisfies recorder.Provider.
func (c *Cluster) GetEventRecorder(name string) events.EventRecorder {
	return record.NewEventRecorderAdapter(record.NewFakeRecorder(100))
}
