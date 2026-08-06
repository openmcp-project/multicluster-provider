package provider

import (
	"fmt"

	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
)

// ClusterName returns the multicluster.ClusterName for a given clustersv1alpha1.Cluster object.
func ClusterName(cluster *clustersv1alpha1.Cluster) multicluster.ClusterName {
	return multicluster.ClusterName(fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name))
}
