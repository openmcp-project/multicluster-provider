package cluster

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mcbuilder "sigs.k8s.io/multicluster-runtime/pkg/builder"
	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	ctrlutils "github.com/openmcp-project/controller-utils/pkg/controller"
	"github.com/openmcp-project/controller-utils/pkg/logging"
	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	clusteraccess "github.com/openmcp-project/openmcp-operator/lib/clusteraccess/advanced"
)

const (
	ControllerName = "ClusterHandler"

	LogNameIsResponsibleFor     = "IsResponsibleFor"
	LogNameHandleCreateOrUpdate = "HandleCreateOrUpdate"
	LogNameHandleDelete         = "HandleDelete"
	LogNameAfterDeletion        = "AfterDeletion"

	clusterKey = "mcaccess"

	MulticlusterIDLabelKey = "multicluster.open-control-plane.io/id"
)

type ClusterController struct {
	platformCluster cluster.Cluster
	Handler         ClusterHandler
	car             clusteraccess.ClusterAccessReconciler
	prov            multicluster.Provider // uses the interface to avoid import cycles, but this must be the provider implementation from this repo
}

var _ mcreconcile.Reconciler = &ClusterController{}

// NewClusterController creates a new ClusterController.
// The controller reconciles Cluster resources, manages access to them, and calls the given handler's methods at the appropriate times.
// The given provider must be the provider implementation from this repo, and it must use the label selector returned by LabelSelectorForProvider(providerName) to select the clusters it manages.
// The providerName argument must be unique among all operators in the same platform cluster using this controller. It must be valid to be used as a label value.
//
// The returned controller must be registered with a multicluster manager (working on the platform cluster) using its SetupWithMulticlusterManager method.
//
// It is recommended to use the NewWithClusterController constructor in the provider package instead, which creates provider and controller together.
func NewClusterController(platformCluster cluster.Cluster, handler ClusterHandler, prov multicluster.Provider, providerName string, tokenConfig *clustersv1alpha1.TokenConfig) *ClusterController {
	res := &ClusterController{
		platformCluster: platformCluster,
		Handler:         handler,
		car: clusteraccess.NewClusterAccessReconciler(platformCluster.GetClient(), providerName).WithManagedLabels(func(controllerName string, req reconcile.Request, reg clusteraccess.ClusterRegistration) (string, string, map[string]string) {
			managedBy, managedPurpose, additionalLabels := clusteraccess.DefaultManagedLabelGenerator(controllerName, req, reg)
			if additionalLabels == nil {
				additionalLabels = map[string]string{}
			}
			additionalLabels[MulticlusterIDLabelKey] = providerName
			return managedBy, managedPurpose, additionalLabels
		}),
		prov: prov,
	}
	res.car.Register(clusteraccess.ExistingCluster(clusterKey, "", clusteraccess.IdentityReferenceGenerator).
		WithNamespaceGenerator(clusteraccess.RequestNamespaceGenerator).
		WithTokenAccess(tokenConfig).
		Build())

	return res
}

// LabelSelectorForProvider returns a label selector which should be used in the provider when using the ClusterController.
func LabelSelectorForProvider(providerName string) labels.Selector {
	sel := labels.NewSelector()
	idReq, err := labels.NewRequirement(MulticlusterIDLabelKey, selection.Equals, []string{providerName})
	if err != nil {
		panic(fmt.Sprintf("error creating multicluster ID requirement: %v", err))
	}
	return sel.Add(*idReq)
}

func (cc *ClusterController) Reconcile(ctx context.Context, req mcreconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx).WithName(ControllerName)
	ctx = logging.NewContext(ctx, log)
	log.Info("Starting reconcile")
	return cc.reconcile(ctx, req)
}

func (cc *ClusterController) reconcile(ctx context.Context, req mcreconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx)

	// fetch cluster
	cl := &clustersv1alpha1.Cluster{}
	cl.Name = req.Name
	cl.Namespace = req.Namespace
	if err := cc.platformCluster.GetClient().Get(ctx, client.ObjectKeyFromObject(cl), cl); err != nil {
		if !apierrors.IsNotFound(err) {
			return reconcile.Result{}, fmt.Errorf("error fetching cluster '%s': %w", req.String(), err)
		}

		// cluster not found, call AfterDeletion
		return cc.callAfterDeletion(ctx, req)
	}

	isResponsible := cc.callIsResponsibleFor(ctx, req, cl)
	if !isResponsible {
		// somewhat ugly, but not possible any other way
		_, err := cc.car.AccessRequest(ctx, standardRequestFromMulticlusterRequest(req), clusterKey)
		if apierrors.IsNotFound(err) {
			// There is no AccessRequest for this Cluster, so it was not handled before (or has already been 'unhandled'), so we do nothing.
			log.Debug("No AccessRequest found for cluster, nothing to do")
			return reconcile.Result{}, nil
		}

		log.Debug("Found an AccessRequest for the cluster, running deletion logic")
	}

	log.Debug("Reconciling cluster access")
	res, err := cc.car.Reconcile(ctx, standardRequestFromMulticlusterRequest(req))
	if err != nil {
		return res, fmt.Errorf("error reconciling cluster access: %w", err)
	}
	if res.RequeueAfter > 0 {
		return res, nil
	}
	access, err := cc.prov.Get(ctx, req.ClusterName)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("error getting cluster access: %w", err)
	}

	if cl.DeletionTimestamp.IsZero() && isResponsible {
		// handle create or update
		res, err = cc.callHandleCreateOrUpdate(ctx, req, cl, access)
	} else {
		// handle delete
		res, err = cc.callHandleDelete(ctx, req, cl, access)
		if err != nil {
			return res, err
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}

		// content deletion logic is done, remove the access
		log.Debug("Reconciling cluster access deletion")
		res, err = cc.car.ReconcileDelete(ctx, standardRequestFromMulticlusterRequest(req))
		if err != nil {
			return res, fmt.Errorf("error reconciling cluster access deletion: %w", err)
		}
		if res.RequeueAfter > 0 {
			return res, nil
		}

		res, err = cc.callAfterDeletion(ctx, req)
	}

	return res, err
}

func (cc *ClusterController) SetupWithMulticlusterManager(mgr mcmanager.Manager) error {
	return mcbuilder.ControllerManagedBy(mgr).
		For(&clustersv1alpha1.Cluster{}, mcbuilder.WithPredicates(
			predicate.Or(
				ctrlutils.OnCreatePredicate(),
				ctrlutils.OnDeletePredicate(),
				predicate.GenerationChangedPredicate{},
				predicate.LabelChangedPredicate{},
				ctrlutils.DeletionTimestampChangedPredicate{},
			),
		)).
		Complete(cc)
}

func (cc *ClusterController) callIsResponsibleFor(ctx context.Context, req mcreconcile.Request, cluster *clustersv1alpha1.Cluster) bool {
	log := logging.FromContextOrPanic(ctx)
	log.Debug("Start: IsResponsibleFor")
	res := cc.Handler.IsResponsibleFor(logging.NewContext(ctx, log.WithName(LogNameIsResponsibleFor)), req, cc.platformCluster.GetClient(), cluster)
	log.Debug("End: IsResponsibleFor", "result", res)
	return res
}

func (cc *ClusterController) callHandleCreateOrUpdate(ctx context.Context, req mcreconcile.Request, cluster *clustersv1alpha1.Cluster, access cluster.Cluster) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx)
	log.Debug("Start: HandleCreateOrUpdate")
	res, err := cc.Handler.HandleCreateOrUpdate(logging.NewContext(ctx, log.WithName(LogNameHandleCreateOrUpdate)), req, cc.platformCluster.GetClient(), cluster, access)
	log.Debug("End: HandleCreateOrUpdate", "requeueAfter", res.RequeueAfter, "error", err)
	return res, err
}

func (cc *ClusterController) callHandleDelete(ctx context.Context, req mcreconcile.Request, cluster *clustersv1alpha1.Cluster, access cluster.Cluster) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx)
	log.Debug("Start: HandleDelete")
	res, err := cc.Handler.HandleDelete(logging.NewContext(ctx, log.WithName(LogNameHandleDelete)), req, cc.platformCluster.GetClient(), cluster, access)
	log.Debug("End: HandleDelete", "requeueAfter", res.RequeueAfter, "error", err)
	return res, err
}

func (cc *ClusterController) callAfterDeletion(ctx context.Context, req mcreconcile.Request) (reconcile.Result, error) {
	log := logging.FromContextOrPanic(ctx)
	log.Debug("Start: AfterDeletion")
	res, err := cc.Handler.AfterDeletion(logging.NewContext(ctx, log.WithName(LogNameAfterDeletion)), req, cc.platformCluster.GetClient())
	log.Debug("End: AfterDeletion", "requeueAfter", res.RequeueAfter, "error", err)
	return res, err
}

func standardRequestFromMulticlusterRequest(req mcreconcile.Request) reconcile.Request {
	return reconcile.Request{
		NamespacedName: req.NamespacedName,
	}
}
