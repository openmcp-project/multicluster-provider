package cluster

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
)

type ClusterHandler interface {
	// IsResponsibleFor allows to use cluster selectors.
	// It is called after the cluster has been fetched from the platform cluster, but before the controller determines whether to handle creation/update or deletion of the cluster.
	// If this returns false, the controller
	// - does nothing, if the cluster has not been handled before
	// - calls HandleDelete and AfterDeletion afterwards, if the cluster has been handled before (which would require this method to have returned true at least once before)
	IsResponsibleFor(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster) bool

	// HandleCreateOrUpdate is called when a cluster is reconciled which matches the IsResponsibleFor selector and does not have a DeletionTimestamp set.
	// If this returns a reconcile result with RequeueAfter > 0, the controller will requeue the request after the specified duration.
	HandleCreateOrUpdate(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster, access cluster.Cluster) (reconcile.Result, error)

	// HandleDelete is called when a cluster is reconciled which matches the IsResponsibleFor selector and has a DeletionTimestamp set.
	// It is also called when a cluster is reconciled which does not match the IsResponsibleFor selector, but has matched it before.
	// This is expected to only return a reconcile result with RequeueAfter == 0 and no error if the deletion has been handled successfully.
	HandleDelete(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster, access cluster.Cluster) (reconcile.Result, error)

	// AfterDeletion is called after the deletion process of a cluster has been completed.
	// It is called whenever the reconciliation request points to a non-existing cluster, regardless of whether the cluster has been handled before or not.
	// It is also called during the deletion logic, after HandleDelete and after the access has been removed. Note that the Cluster resource does still exist in this case.
	// It is likely that this method is called multiple times for the same cluster.
	AfterDeletion(ctx context.Context, req mcreconcile.Request, platformClient client.Client) (reconcile.Result, error)
}

// Funcs is a helper type that allows to implement the ClusterHandler interface by providing functions for each method.
// If IsResponsibleForFunc is not provided, it defaults to a function that always returns true.
// All other functions default to no-ops that return a zero-value reconcile.Result and nil error, if not set.
type Funcs struct {
	IsResponsibleForFunc     func(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster) bool
	HandleCreateOrUpdateFunc func(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster, access cluster.Cluster) (reconcile.Result, error)
	HandleDeleteFunc         func(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster, access cluster.Cluster) (reconcile.Result, error)
	AfterDeletionFunc        func(ctx context.Context, req mcreconcile.Request, platformClient client.Client) (reconcile.Result, error)
}

var _ ClusterHandler = &Funcs{}

func (f *Funcs) IsResponsibleFor(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster) bool {
	if f.IsResponsibleForFunc == nil {
		return true
	}
	return f.IsResponsibleForFunc(ctx, req, platformClient, cluster)
}

func (f *Funcs) HandleCreateOrUpdate(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster, access cluster.Cluster) (reconcile.Result, error) {
	if f.HandleCreateOrUpdateFunc == nil {
		return reconcile.Result{}, nil
	}
	return f.HandleCreateOrUpdateFunc(ctx, req, platformClient, cluster, access)
}

func (f *Funcs) HandleDelete(ctx context.Context, req mcreconcile.Request, platformClient client.Client, cluster *clustersv1alpha1.Cluster, access cluster.Cluster) (reconcile.Result, error) {
	if f.HandleDeleteFunc == nil {
		return reconcile.Result{}, nil
	}
	return f.HandleDeleteFunc(ctx, req, platformClient, cluster, access)
}

func (f *Funcs) AfterDeletion(ctx context.Context, req mcreconcile.Request, platformClient client.Client) (reconcile.Result, error) {
	if f.AfterDeletionFunc == nil {
		return reconcile.Result{}, nil
	}
	return f.AfterDeletionFunc(ctx, req, platformClient)
}
