// nolint:revive
package provider

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mcclusters "sigs.k8s.io/multicluster-runtime/pkg/clusters"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	ctrlutils "github.com/openmcp-project/controller-utils/pkg/controller"
	"github.com/openmcp-project/controller-utils/pkg/logging"
	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"

	clusterctrl "github.com/openmcp-project/multicluster-provider/pkg/cluster"
)

const (
	ProviderName = "OpenControlPlaneCluster"
)

///////////
// TYPES //
///////////

type Provider struct {
	opts            *Options
	platformCluster cluster.Cluster

	clusters mcclusters.Clusters[*ClusterInstance]

	lock                      sync.RWMutex
	mcAware                   multicluster.Aware
	accessRequests            map[reconcile.Request]commonapi.ObjectReference // maps AccessRequests to their cluster reference, is kept in sync with clusters (required to remove clusters from deleted AccessRequests)
	scheme                    *runtime.Scheme
	hostingPlatformClusterRef *commonapi.ObjectReference // reference to the cluster which is the hosting platform cluster, if any
}

var _ multicluster.Provider = &Provider{}
var _ multicluster.ProviderRunnable = &Provider{}
var _ reconcile.Reconciler = &Provider{}

type ClusterInstance struct {
	cluster.Cluster
	AccessRequest *clustersv1alpha1.AccessRequest
	Kubeconfig    []byte
}

///////////
// SETUP //
///////////

// New creates a new Provider instance.
// platformCluster is the cluster object for the platform cluster.
// scheme is the scheme which will be used for the clients retrieved from the provider
// opts allows to set various options, most prominently, the label selector for AccessRequests which must be set, otherwise the provider will not engage any clusters.
//
// It is recommended to use NewWithClusterController instead, which will automatically wire the provider to a controller which creates the AccessRequests and allows to react to cluster lifecycle events.
func New(platformCluster cluster.Cluster, scheme *runtime.Scheme, opts ...Option) *Provider {
	p := &Provider{
		opts:            &Options{},
		platformCluster: platformCluster,
		accessRequests:  map[reconcile.Request]commonapi.ObjectReference{},
		clusters:        mcclusters.New[*ClusterInstance](),
		scheme:          scheme,
	}

	for _, opt := range opts {
		opt.Apply(p.opts)
	}

	return p
}

// NewWithClusterController combines the creation of a new Provider with a controller managing AccessRequests for clusters, which is required for the provider to work anyway.
// In addition, the cluster controller can also execute custom logic via the given handler.
// The provider will automatically be configured with the correct label selector to watch AccessRequests created by the cluster controller.
// DO NOT CHANGE THE LABEL SELECTOR, otherwise you risk breaking the wiring between provider and cluster controller.

// Arguments:
// - platformCluster: The cluster object for the platform cluster.
// - providerName: This must be a k8s label value compliant string, and it must be unique across all operators using this library in the same platform cluster. It is recommended to use the PlatformService (or ServiceProvider)'s name for this.
// - scheme: The scheme used for the clients retrieved from the provider.
// - tokenConfig: The configuration for the kubeconfig token to be used for the AccessRequests created by the cluster controller.
// - handler: A custom handler which will be called on cluster lifecycle events (engage/disengage). If not desired, putting in an empty cluster.Funcs{} struct works as a no-op handler.
func NewWithClusterController(platformCluster cluster.Cluster, providerName string, scheme *runtime.Scheme, tokenConfig *clustersv1alpha1.TokenConfig, handler clusterctrl.ClusterHandler, opts ...Option) (*Provider, *clusterctrl.ClusterController) {
	opts = append(opts, WithAccessRequestSelectors(clusterctrl.LabelSelectorForProvider(providerName)))
	prov := New(platformCluster, scheme, opts...)
	cctrl := clusterctrl.NewClusterController(platformCluster, handler, prov, providerName, tokenConfig)
	return prov, cctrl
}

// SetupWithManager sets up the controller with the Manager.
// It watches Cluster resources, but reacts to creation and deletion events only.
// It also watches Secrets which are owned by AccessRequests, here updates only. This is meant to requeue a Cluster when its kubeconfig token is rotated.
func (p *Provider) SetupWithManager(mgr manager.Manager) error {
	return builder.ControllerManagedBy(mgr).
		// watches AccessRequests on the platform cluster
		For(&clustersv1alpha1.AccessRequest{}, builder.WithPredicates(predicate.Or(
			// react to all deletion events, to be sure to disengage clusters when their AccessRequest is deleted
			ctrlutils.OnDeletePredicate(),
			// apart from that, we only care about granted AccessRequests, and only in the following situations:
			// - labels changed (might match or not match the selectors anymore)
			// - status changed (might have been granted)
			predicate.And(
				// react to granted AccessRequests only
				predicate.NewPredicateFuncs(func(obj client.Object) bool {
					ar, ok := obj.(*clustersv1alpha1.AccessRequest)
					if !ok {
						return false
					}
					return ar.Status.Phase == clustersv1alpha1.REQUEST_GRANTED
				}),
				predicate.Or(
					predicate.LabelChangedPredicate{},
					ctrlutils.StatusChangedPredicate{},
				),
			),
		))).
		// watches Secrets belonging to AccessRequests (on the platform cluster)
		// reacts to creation and updates only
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
			predicate.Or(
				ctrlutils.OnCreatePredicate(),
				ctrlutils.OnUpdatePredicate(),
			),
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
	if clusterName == HostingPlatformCluster {
		if p.hostingPlatformClusterRef != nil {
			clusterName = ClusterNameFromReference(p.hostingPlatformClusterRef)
		} else {
			return p.platformCluster, nil
		}
	}
	return p.clusters.Get(ctx, clusterName)
}

// IndexField implements [multicluster.Provider].
func (p *Provider) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	return p.clusters.IndexField(ctx, obj, field, extractValue)
}

///////////////////////////////////////////////
// PROVIDERRUNNABLE INTERFACE IMPLEMENTATION //
///////////////////////////////////////////////

// Start implements [multicluster.ProviderRunnable].
func (p *Provider) Start(ctx context.Context, aware multicluster.Aware) error {
	log := logging.FromContextOrDiscard(ctx).WithName(ProviderName)
	log.Info("Starting provider")

	p.lock.Lock()
	p.mcAware = aware
	p.lock.Unlock()

	err := p.platformCluster.Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to start platform cluster: %w", err)
	}

	log.Info("Stopping provider")
	return nil
}

/////////////////////////////////////////
// RECONCILER INTERFACE IMPLEMENTATION //
/////////////////////////////////////////

// Reconcile implements [reconcile.TypedReconciler].
func (p *Provider) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx).WithName(ProviderName)
	ctx = logging.NewContext(ctx, log)
	log.Info("Starting reconcile")
	rr, err := p.reconcile(ctx, req)
	return rr, err
}

func (p *Provider) reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx)

	p.lock.RLock()
	aw := p.mcAware
	p.lock.RUnlock()
	if aw == nil {
		log.Info("Requeuing because provider has not been started yet")
		return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// check internal state
	cRef, isEngaged := p.accessRequests[req]
	var ci *ClusterInstance
	if isEngaged {
		var err error
		ci, err = p.clusters.GetTyped(ctx, ClusterNameFromReference(&cRef))
		if err != nil {
			return reconcile.Result{}, fmt.Errorf("internal state inconsistency: AccessRequest in internal map, but corresponding cluster is not, this is not supposed to happen: %w", err)
		}
		log = log.WithValues("clusterName", cRef.Name, "clusterNamespace", cRef.Namespace)
		ctx = logging.NewContext(ctx, log)
	}
	if ci != nil && ci.AccessRequest != nil && (ci.AccessRequest.Name != req.Name || ci.AccessRequest.Namespace != req.Namespace) {
		log.Error(nil, "This AccessRequest belongs to a Cluster for which another AccessRequest is already registered, the cause is likely a wrong configuration. The AccessRequest will be ignored.", "conflictingName", ci.AccessRequest.Name, "conflictingNamespace", ci.AccessRequest.Namespace)
		return reconcile.Result{}, nil
	}

	// fetch AccessRequest
	ar := &clustersv1alpha1.AccessRequest{}
	ar.SetName(req.Name)
	ar.SetNamespace(req.Namespace)
	if err := p.platformCluster.GetClient().Get(ctx, req.NamespacedName, ar); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("Resource not found")
			if isEngaged {
				p.disengage(log, req)
			}
		}
		return reconcile.Result{}, fmt.Errorf("unable to get resource '%s' from cluster: %w", req.String(), err)
	}

	if ar.Status.Phase != clustersv1alpha1.REQUEST_GRANTED {
		return reconcile.Result{}, fmt.Errorf("AccessRequest '%s' is not granted", req.String())
	}
	if ar.Status.SecretRef == nil {
		return reconcile.Result{}, fmt.Errorf("AccessRequest '%s' is granted, but does not have a secret reference", req.String())
	}
	if ar.Spec.ClusterRef == nil {
		return reconcile.Result{}, fmt.Errorf("AccessRequest '%s' is granted, but does not have a cluster reference", req.String())
	}

	log.Debug("Fetching referenced Cluster for metadata")
	clu := &clustersv1alpha1.Cluster{}
	clu.SetName(ar.Spec.ClusterRef.Name)
	clu.SetNamespace(ar.Spec.ClusterRef.Namespace)
	if err := p.platformCluster.GetClient().Get(ctx, client.ObjectKeyFromObject(clu), clu); err != nil {
		return reconcile.Result{}, fmt.Errorf("unable to get Cluster '%s/%s' referenced by AccessRequest '%s': %w", ar.Spec.ClusterRef.Namespace, ar.Spec.ClusterRef.Name, req.String(), err)
	}

	log.Debug("Getting cluster access")
	sec := &corev1.Secret{}
	sec.SetName(ar.Status.SecretRef.Name)
	sec.SetNamespace(ar.Namespace)
	if err := p.platformCluster.GetClient().Get(ctx, client.ObjectKeyFromObject(sec), sec); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("secret referenced by AccessRequest '%s' not found: %w", req.String(), err)
		} else {
			return reconcile.Result{}, fmt.Errorf("unable to get Secret referenced by AccessRequest '%s': %w", req.String(), err)
		}
	}
	kcfgData, ok := sec.Data[clustersv1alpha1.SecretKeyKubeconfig]
	if !ok {
		return reconcile.Result{}, fmt.Errorf("secret referenced by AccessRequest '%s' does not contain a '%s' key", req.String(), clustersv1alpha1.SecretKeyKubeconfig)
	}

	if isEngaged && ci != nil {
		// check if the cluster's kubeconfig has changed (token rotation)
		if !bytes.Equal(ci.Kubeconfig, kcfgData) {
			log.Info("Cluster kubeconfig has changed, disengaging cluster to prepare for re-engagement")
			p.disengage(log, req)
			// isEngaged = false // commented out, because the linter complains otherwise, needs to be put in if isEngaged is used again below
		} else {
			log.Debug("Cluster is already engaged, nothing to do")
			return reconcile.Result{}, nil
		}
	}

	log.Debug("Creating cluster client")
	restCfg, err := clientcmd.RESTConfigFromKubeConfig(kcfgData)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to create REST config from kubeconfig: %w", err)
	}
	cl, err := cluster.New(restCfg, func(o *cluster.Options) {
		o.Scheme = p.scheme
		o.Cache.Scheme = p.scheme
	})
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to create cluster client: %w", err)
	}

	log.Info("Engaging cluster")
	p.lock.Lock()
	defer p.lock.Unlock()

	ci = &ClusterInstance{
		Cluster:       cl,
		AccessRequest: ar,
	}

	p.accessRequests[req] = cRef
	if err := p.clusters.AddOrReplace(ctx, ClusterNameFromReference(&cRef), ci, p.mcAware); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to engage cluster '%s/%s': %w", cRef.Namespace, cRef.Name, err)
	}
	log.Debug("Cluster successfully engaged")

	if slices.Contains(clu.Spec.Purposes, clustersv1alpha1.PURPOSE_PLATFORM) && cl.GetConfig().Host == p.platformCluster.GetConfig().Host {
		log.Info("Hosting platform cluster detected")
		p.hostingPlatformClusterRef = &cRef
	}

	return reconcile.Result{}, nil
}

////////////////////////
// ADDITIONAL METHODS //
////////////////////////

// disengage is a internal helper function to disengage a cluster and clean up the internal state.
func (p *Provider) disengage(log logging.Logger, arReq reconcile.Request) {
	log.Info("Disengaging cluster")
	p.lock.Lock()
	defer p.lock.Unlock()
	cRef, ok := p.accessRequests[arReq]
	if !ok {
		log.Debug("Cluster not engaged, nothing to do (trying to remove it anyway, just in case)")
		return
	}

	p.clusters.Remove(ClusterNameFromReference(&cRef))
	delete(p.accessRequests, arReq)
	if p.hostingPlatformClusterRef != nil && p.hostingPlatformClusterRef.Namespace == cRef.Namespace && p.hostingPlatformClusterRef.Name == cRef.Name {
		log.Info("Hosting platform cluster has been disengaged")
		p.hostingPlatformClusterRef = nil
	}
}

// getOptions returns the current Options, wrapped in a read lock.
func (p *Provider) getOptions() *Options {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.opts
}

// GetOptions returns a deep copy of the current Options.
func (p *Provider) GetOptions() *Options {
	return p.getOptions().DeepCopy()
}

// SetOptions applies the given options to the provider.
// This is safe to use while the provider is running, therefore only implementations of DynamicOption are supported.
// Returns an error as soon as the first option fails to apply, in which case the provider's options may be partially updated.
func (p *Provider) SetOptions(opts ...DynamicOption) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, opt := range opts {
		if err := opt.ApplyDynamic(p.opts); err != nil {
			return fmt.Errorf("failed to apply dynamic option: %w", err)
		}
	}

	return nil
}
