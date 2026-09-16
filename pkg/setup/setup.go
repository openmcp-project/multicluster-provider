package setup

import (
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/cluster"

	clustersv1alpha1 "github.com/openmcp-project/openmcp-operator/api/clusters/v1alpha1"

	clusterctrl "github.com/openmcp-project/multicluster-provider/pkg/cluster"
	"github.com/openmcp-project/multicluster-provider/pkg/provider"
)

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
func NewWithClusterController(platformCluster cluster.Cluster, providerName string, scheme *runtime.Scheme, tokenConfig *clustersv1alpha1.TokenConfig, handler clusterctrl.ClusterHandler, opts ...provider.Option) (*provider.Provider, *clusterctrl.ClusterController) {
	opts = append(opts, provider.WithAccessRequestSelectors(clusterctrl.LabelSelectorForProvider(providerName)))
	prov := provider.New(platformCluster, scheme, opts...)
	cctrl := clusterctrl.NewClusterController(platformCluster, handler, prov, providerName, tokenConfig)
	return prov, cctrl
}
