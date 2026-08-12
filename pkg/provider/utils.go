package provider

import (
	"fmt"

	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
)

// ClusterName returns the multicluster.ClusterName for a given namespace and name.
func ClusterName(namespace, name string) multicluster.ClusterName {
	return multicluster.ClusterName(fmt.Sprintf("%s/%s", namespace, name))
}

// ClusterNameFromReference returns the multicluster.ClusterNameFromCluster for a given common.ObjectReference object.
func ClusterNameFromReference(ref commonapi.ObjectReference) multicluster.ClusterName {
	return ClusterName(ref.Namespace, ref.Name)
}

// ClusterNameFromCluster returns the multicluster.ClusterNameFromCluster for a given clustersv1alpha1.Cluster object.
func ClusterNameFromCluster(cluster *clustersv1alpha1.Cluster) multicluster.ClusterName {
	return ClusterName(cluster.Namespace, cluster.Name)
}
