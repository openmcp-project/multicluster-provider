package provider

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	"github.com/openmcp-project/controller-utils/pkg/clusters"
	ctrlutils "github.com/openmcp-project/controller-utils/pkg/controller"
	"github.com/openmcp-project/controller-utils/pkg/logging"
	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	clusteraccess "github.com/openmcp-project/openmcp-operator/lib/clusteraccess/advanced"
)

const (
	ProviderName = "OpenControlPlaneCluster"

	clusterKey = "cluster"
)

///////////
// TYPES //
///////////

type Provider struct {
	opts           Options
	log            logging.Logger
	platformClient client.Client
	car            clusteraccess.ClusterAccessReconciler

	lock     sync.RWMutex
	mcAware  multicluster.Aware
	clusters map[multicluster.ClusterName]*ClusterInstance
	// cancelFuncs map[multicluster.ClusterName]context.CancelFunc
	indexers []index
}

var _ multicluster.Provider = &Provider{}
var _ multicluster.ProviderRunnable = &Provider{}
var _ reconcile.Reconciler = &Provider{}

type index struct {
	object       client.Object
	field        string
	extractValue client.IndexerFunc
}

type ClusterInstance struct {
	*clusters.Cluster
	Resource  *clustersv1alpha1.Cluster
	disengage context.CancelFunc
}

type Options struct {
}

type Option interface {
	Apply(*Options)
}

///////////
// SETUP //
///////////

func New(opts ...Option) *Provider {
	// TODO
	return nil
}

// SetupWithManager sets up the controller with the Manager.
// It watches Cluster resources, but reacts to creation and deletion events only.
// It also watches Secrets which are owned by AccessRequests, here updates only. This is meant to requeue a Cluster when its kubeconfig token is rotated.
func (p *Provider) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).
		For(&clustersv1alpha1.Cluster{}, builder.WithPredicates(predicate.Or(
			ctrlutils.OnCreatePredicate(),
			ctrlutils.OnDeletePredicate(),
		))).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			owner := metav1.GetControllerOfNoCopy(obj)
			if owner != nil && owner.Kind == "AccessRequest" && owner.APIVersion == clustersv1alpha1.GroupVersion.String() {
				return []reconcile.Request{{
					NamespacedName: types.NamespacedName{
						Namespace: obj.GetNamespace(),
						Name:      owner.Name,
					},
				}}
			}
			return nil
		}), builder.WithPredicates(predicate.And(
			ctrlutils.OnUpdatePredicate(),
			predicate.NewPredicateFuncs(func(obj client.Object) bool {
				owner := metav1.GetControllerOfNoCopy(obj)
				return owner != nil && owner.Kind == "AccessRequest" && owner.APIVersion == clustersv1alpha1.GroupVersion.String()
			}),
		))).
		Complete(p)
}

///////////////////////////////////////
// PROVIDER INTERFACE IMPLEMENTATION //
///////////////////////////////////////

// Get implements [multicluster.Provider].
func (p *Provider) Get(ctx context.Context, clusterName multicluster.ClusterName) (cluster.Cluster, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if cl, ok := p.clusters[clusterName]; ok {
		return cl.Cluster.Cluster(), nil
	}

	return nil, fmt.Errorf("cluster '%s' not found", clusterName.String())
}

// IndexField implements [multicluster.Provider].
func (p *Provider) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// save for future clusters
	p.indexers = append(p.indexers, index{
		object:       obj,
		field:        field,
		extractValue: extractValue,
	})

	// apply to known clusters
	for key, ci := range p.clusters {
		if err := ci.Cluster.Cluster().GetCache().IndexField(ctx, obj, field, extractValue); err != nil {
			return fmt.Errorf("failed to index field '%s' on Cluster '%s': %w", field, key.String(), err)
		}
	}

	return nil
}

///////////////////////////////////////////////
// PROVIDERRUNNABLE INTERFACE IMPLEMENTATION //
///////////////////////////////////////////////

// Start implements [multicluster.ProviderRunnable].
func (p *Provider) Start(ctx context.Context, engager multicluster.Aware) error {
	panic("unimplemented")
}

/////////////////////////////////////////
// RECONCILER INTERFACE IMPLEMENTATION //
/////////////////////////////////////////

// Reconcile implements [reconcile.TypedReconciler].
func (p *Provider) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx).WithName(ProviderName).WithName("ClusterReconciler")
	ctx = logging.NewContext(ctx, log)
	log.Info("Starting reconcile")
	rr, err := p.reconcile(ctx, req)
	return rr, err
}

func (p *Provider) reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx)

	// fetch cluster
	cl := &clustersv1alpha1.Cluster{}
	cl.SetName(req.Name)
	cl.SetNamespace(req.Namespace)
	ci, isEngaged := p.clusters[ClusterName(cl)]
	if err := p.platformClient.Get(ctx, req.NamespacedName, cl); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Resource not found")
			if isEngaged {
				log.Info("Disengaging cluster")
				ci.disengage()
				delete(p.clusters, ClusterName(cl))
			}
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("unable to get resource '%s' from cluster: %w", req.String(), err)
	}

	log.Debug("Reconciling cluster access")
	rr, err := p.car.Reconcile(ctx, req)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to reconcile cluster access for Cluster '%s': %w", req.String(), err)
	}
	if rr.RequeueAfter > 0 {
		log.Info("Requeuing to wait for cluster access", "requeueAfter", rr.RequeueAfter, "requeueAt", time.Now().Add(rr.RequeueAfter).Format(time.RFC3339))
		return rr, nil
	}

	log.Debug("Getting cluster access")
	access, err := p.car.Access(ctx, req, clusterKey)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to get cluster access for Cluster '%s': %w", req.String(), err)
	}

	if isEngaged {
		// check if the cluster's kubeconfig has changed (token rotation)
		if !reflect.DeepEqual(ci.RESTConfig(), access.RESTConfig()) {
			log.Info("Cluster kubeconfig has changed, disengaging cluster to prepare for re-engagement")
			ci.disengage()
			delete(p.clusters, ClusterName(cl))
			isEngaged = false
		} else {
			log.Debug("Cluster is already engaged, nothing to do")
			return reconcile.Result{}, nil
		}
	}

	log.Info("Engaging cluster")
	log.Debug("Adding indexers")
	p.lock.Lock() // we don't want any new indexers to be added until we have engaged the cluster, otherwise they will be lost
	defer p.lock.Unlock()
	for _, indexer := range p.indexers {
		if err := access.Cluster().GetCache().IndexField(ctx, indexer.object, indexer.field, indexer.extractValue); err != nil {
			return reconcile.Result{}, fmt.Errorf("failed to index field '%s' on Cluster '%s': %w", indexer.field, req.String(), err)
		}
	}

	cCtx, cancel := context.WithCancel(ctx)
	ci = &ClusterInstance{
		Cluster:   access,
		Resource:  cl,
		disengage: cancel,
	}

	if err := p.mcAware.Engage(cCtx, ClusterName(cl), access.Cluster()); err != nil {
		cancel()
		return reconcile.Result{}, fmt.Errorf("failed to engage cluster '%s': %w", req.String(), err)
	}
	p.clusters[ClusterName(cl)] = ci
	log.Debug("Cluster successfully engaged")

	return reconcile.Result{}, nil
}

////////////////////////
// ADDITIONAL METHODS //
////////////////////////

// GetClusterInstance returns the ClusterInstance for the given ClusterName.
// Returns nil if the cluster is not engaged.
// Note that this returns a pointer to the internally used ClusterInstance, any modifications to the returned object are strongly discouraged and may lead to undefined behavior.
func (p *Provider) GetClusterInstance(clusterName multicluster.ClusterName) *ClusterInstance {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.clusters[clusterName]
}
