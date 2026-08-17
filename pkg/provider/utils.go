package provider

import (
	"fmt"

	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"
	commonapi "github.com/openmcp-project/openmcp-operator/api/common"
)

const (
	// HostingPlatformCluster is a special cluster name which refers to the platform cluster hosting the provider.
	// Using this cluster name with the provider's Get method will return
	// - the access for the Cluster with purpose 'platform' and the same apiserver endpoint as the provider's platform cluster, if it exists, or
	// - the provider's platform cluster access otherwise.
	// The main difference between both cases is that the former case will use the permissions of the serviceaccount under which the provider itself is running,
	// whereas the latter case will use the permissions from the AccessRequest for the platform Cluster resource.
	HostingPlatformCluster multicluster.ClusterName = ""

	// ReasonClusterAccessError is a reason which can be used to indicate problems with getting access from the provider.
	ReasonClusterAccessError = "ClusterAccessError"
)

// ClusterName returns the multicluster.ClusterName for a given namespace and name.
// Returns the hosting platform cluster name if both namespace and name are empty.
func ClusterName(namespace, name string) multicluster.ClusterName {
	if namespace == "" && name == "" {
		return HostingPlatformCluster
	}
	return multicluster.ClusterName(fmt.Sprintf("%s/%s", namespace, name))
}

// ClusterNameFromReference returns the multicluster.ClusterNameFromCluster for a given common.ObjectReference object.
// Returns the hosting platform cluster name if the given reference is nil.
func ClusterNameFromReference(ref *commonapi.ObjectReference) multicluster.ClusterName {
	if ref == nil {
		return ClusterName("", "")
	}
	return ClusterName(ref.Namespace, ref.Name)
}

// ClusterNameFromCluster returns the multicluster.ClusterNameFromCluster for a given clustersv1alpha1.Cluster object.
// Returns the hosting platform cluster name if the given cluster is nil.
func ClusterNameFromCluster(cluster *clustersv1alpha1.Cluster) multicluster.ClusterName {
	if cluster == nil {
		return ClusterName("", "")
	}
	return ClusterName(cluster.Namespace, cluster.Name)
}
