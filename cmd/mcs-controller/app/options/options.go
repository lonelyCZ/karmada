/*
Copyright 2021 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package options

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	componentbaseconfig "k8s.io/component-base/config"

	"github.com/karmada-io/karmada/pkg/features"
	"github.com/karmada-io/karmada/pkg/sharedcli/profileflag"
	"github.com/karmada-io/karmada/pkg/sharedcli/ratelimiterflag"
	"github.com/karmada-io/karmada/pkg/util"
)

const (
	// DefaultKarmadaClusterNamespace defines the default namespace where the member cluster secrets are stored.
	DefaultKarmadaClusterNamespace = "karmada-cluster"
	defaultBindAddress             = "0.0.0.0"
	defaultPort                    = 10357
)

var (
	defaultElectionLeaseDuration = metav1.Duration{Duration: 15 * time.Second}
	defaultElectionRenewDeadline = metav1.Duration{Duration: 10 * time.Second}
	defaultElectionRetryPeriod   = metav1.Duration{Duration: 2 * time.Second}
)

// Options contains everything necessary to create and run controller-manager.
type Options struct {
	// Controllers contains all controller names.
	Controllers    []string
	LeaderElection componentbaseconfig.LeaderElectionConfiguration
	// BindAddress is the IP address on which to listen for the --secure-port port.
	BindAddress string
	// SecurePort is the port that the the server serves at.
	// Note: We hope support https in the future once controller-runtime provides the functionality.
	SecurePort        int
	KarmadaKubeConfig string
	// ClusterContext is the name of the cluster context in control plane KUBECONFIG file.
	// Default value is the current-context.
	KarmadaContext string
	ClusterName    string
	// ClusterNamespace holds the namespace name where the member cluster secrets are stored.
	ClusterNamespace string
	// ClusterAPIQPS is the QPS to use while talking with cluster kube-apiserver.
	ClusterAPIQPS float32
	// ClusterAPIBurst is the burst to allow while talking with cluster kube-apiserver.
	ClusterAPIBurst int
	// KubeAPIQPS is the QPS to use while talking with karmada-apiserver.
	KubeAPIQPS float32
	// KubeAPIBurst is the burst to allow while talking with karmada-apiserver.
	KubeAPIBurst int

	ClusterCacheSyncTimeout metav1.Duration
	// ResyncPeriod is the base frequency the informers are resynced.
	// Defaults to 0, which means the created informer will never do resyncs.
	ResyncPeriod metav1.Duration

	// ClusterAPIEndpoint holds the apiEndpoint of the cluster.
	ClusterAPIEndpoint string
	// ConcurrentClusterSyncs is the number of cluster objects that are
	// allowed to sync concurrently.
	ConcurrentClusterSyncs int
	// ConcurrentWorkSyncs is the number of work objects that are
	// allowed to sync concurrently.
	ConcurrentWorkSyncs int
	// MetricsBindAddress is the TCP address that the controller should bind to
	// for serving prometheus metrics.
	// It can be set to "0" to disable the metrics serving.
	// Defaults to ":8080".
	MetricsBindAddress string

	RateLimiterOpts ratelimiterflag.Options

	ProfileOpts profileflag.Options
}

// NewOptions builds an default scheduler options.
func NewOptions() *Options {
	return &Options{
		LeaderElection: componentbaseconfig.LeaderElectionConfiguration{
			LeaderElect:       true,
			ResourceLock:      resourcelock.LeasesResourceLock,
			ResourceNamespace: util.NamespaceKarmadaSystem,
		},
	}
}

// AddFlags adds flags of scheduler to the specified FlagSet
func (o *Options) AddFlags(fs *pflag.FlagSet, allControllers []string) {
	if o == nil {
		return
	}

	fs.StringSliceVar(&o.Controllers, "controllers", []string{"*"}, fmt.Sprintf(
		"A list of controllers to enable. '*' enables all on-by-default controllers, 'foo' enables the controller named 'foo', '-foo' disables the controller named 'foo'. All controllers: %s.",
		strings.Join(allControllers, ", "),
	))
	fs.StringVar(&o.BindAddress, "bind-address", defaultBindAddress,
		"The IP address on which to listen for the --secure-port port.")
	fs.IntVar(&o.SecurePort, "secure-port", defaultPort,
		"The secure port on which to serve HTTPS.")
	fs.BoolVar(&o.LeaderElection.LeaderElect, "leader-elect", true, "Start a leader election client and gain leadership before executing the main loop. Enable this when running replicated components for high availability.")
	fs.StringVar(&o.LeaderElection.ResourceNamespace, "leader-elect-resource-namespace", util.NamespaceKarmadaSystem, "The namespace of resource object that is used for locking during leader election.")
	fs.DurationVar(&o.LeaderElection.LeaseDuration.Duration, "leader-elect-lease-duration", defaultElectionLeaseDuration.Duration, ""+
		"The duration that non-leader candidates will wait after observing a leadership "+
		"renewal until attempting to acquire leadership of a led but unrenewed leader "+
		"slot. This is effectively the maximum duration that a leader can be stopped "+
		"before it is replaced by another candidate. This is only applicable if leader "+
		"election is enabled.")
	fs.DurationVar(&o.LeaderElection.RenewDeadline.Duration, "leader-elect-renew-deadline", defaultElectionRenewDeadline.Duration, ""+
		"The interval between attempts by the acting master to renew a leadership slot "+
		"before it stops leading. This must be less than or equal to the lease duration. "+
		"This is only applicable if leader election is enabled.")
	fs.DurationVar(&o.LeaderElection.RetryPeriod.Duration, "leader-elect-retry-period", defaultElectionRetryPeriod.Duration, ""+
		"The duration the clients should wait between attempting acquisition and renewal "+
		"of a leadership. This is only applicable if leader election is enabled.")
	fs.StringVar(&o.KarmadaKubeConfig, "karmada-kubeconfig", o.KarmadaKubeConfig, "Path to karmada control plane kubeconfig file.")
	fs.StringVar(&o.KarmadaContext, "karmada-context", "", "Name of the cluster context in karmada control plane kubeconfig file.")
	fs.StringVar(&o.ClusterName, "cluster-name", o.ClusterName, "Name of member cluster that the agent serves for.")
	fs.StringVar(&o.ClusterNamespace, "cluster-namespace", DefaultKarmadaClusterNamespace, "Namespace in the control plane where member cluster secrets are stored.")
	fs.Float32Var(&o.ClusterAPIQPS, "cluster-api-qps", 40.0, "QPS to use while talking with cluster kube-apiserver. Doesn't cover events and node heartbeat apis which rate limiting is controlled by a different set of flags.")
	fs.IntVar(&o.ClusterAPIBurst, "cluster-api-burst", 60, "Burst to use while talking with cluster kube-apiserver. Doesn't cover events and node heartbeat apis which rate limiting is controlled by a different set of flags.")
	fs.Float32Var(&o.KubeAPIQPS, "kube-api-qps", 40.0, "QPS to use while talking with karmada-apiserver. Doesn't cover events and node heartbeat apis which rate limiting is controlled by a different set of flags.")
	fs.IntVar(&o.KubeAPIBurst, "kube-api-burst", 60, "Burst to use while talking with karmada-apiserver. Doesn't cover events and node heartbeat apis which rate limiting is controlled by a different set of flags.")
	fs.DurationVar(&o.ClusterCacheSyncTimeout.Duration, "cluster-cache-sync-timeout", util.CacheSyncTimeout, "Timeout period waiting for cluster cache to sync.")
	fs.StringVar(&o.ClusterAPIEndpoint, "cluster-api-endpoint", o.ClusterAPIEndpoint, "APIEndpoint of the cluster.")
	fs.DurationVar(&o.ResyncPeriod.Duration, "resync-period", 0, "Base frequency the informers are resynced.")
	fs.IntVar(&o.ConcurrentClusterSyncs, "concurrent-cluster-syncs", 5, "The number of Clusters that are allowed to sync concurrently.")
	fs.IntVar(&o.ConcurrentWorkSyncs, "concurrent-work-syncs", 5, "The number of Works that are allowed to sync concurrently.")
	fs.StringVar(&o.MetricsBindAddress, "metrics-bind-address", ":8080", "The TCP address that the controller should bind to for serving prometheus metrics(e.g. 127.0.0.1:8080, :8080). It can be set to \"0\" to disable the metrics serving.")
	o.RateLimiterOpts.AddFlags(fs)
	features.FeatureGate.AddFlag(fs)
	o.ProfileOpts.AddFlags(fs)
}
