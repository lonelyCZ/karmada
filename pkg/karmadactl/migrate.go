package karmadactl

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	"github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	"github.com/karmada-io/karmada/pkg/karmadactl/options"
	"github.com/karmada-io/karmada/pkg/util/restmapper"
)

var (
	migrateShort   = `Migrate workloads from legacy clusters to karmada control plane.`
	migrateLong    = `Migrate workloads from legacy clusters to karmada control plane.`
	migrateExample = `
# Migrate deployment.v1.apps/demo from legacy clusters to karmada control plane.
%s migrate deployment.v1.apps demo -n default -c cluster1

# Migrate pod.v1/demo from legacy clusters to karmada control plane.
%s migrate pod.v1 demo -n default -c cluster1
`
)

// NewCmdMigrate defines the `migrate` command that migrate workloads from legacy clusters
func NewCmdMigrate(cmdOut io.Writer, karmadaConfig KarmadaConfig, parentCommand string) *cobra.Command {
	opts := CommandMigrateOption{}

	cmd := &cobra.Command{
		Use:          "migrate deployment.v1.apps demo -n default -c cluster1",
		Short:        migrateShort,
		Long:         migrateLong,
		Example:      getMigrateExample(parentCommand),
		SilenceUsage: true,
		RunE: func(cmmd *cobra.Command, args []string) error {
			if err := opts.Complete(args); err != nil {
				return err
			}
			if err := opts.Validate(); err != nil {
				return err
			}
			if err := RunMigrate(cmdOut, karmadaConfig, opts); err != nil {
				return err
			}
			return nil
		},
	}

	flag := cmd.Flags()
	opts.AddFlags(flag)

	return cmd
}

func getMigrateExample(parentCommand string) string {
	return fmt.Sprintf(migrateExample, parentCommand, parentCommand)
}

type CommandMigrateOption struct {
	options.GlobalCommandOptions

	// Cluster is the name of legacy cluster
	Cluster string

	// Namespace is the namespace of legacy resource
	Namespace string

	// ClusterContext is the cluster's context that we are going to join with.
	ClusterContext string

	// ClusterKubeConfig is the cluster's kubeconfig path.
	ClusterKubeConfig string

	name    string
	group   string
	version string
	kind    string
}

// AddFlags adds flags to the specified FlagSet.
func (o *CommandMigrateOption) AddFlags(flags *pflag.FlagSet) {
	o.GlobalCommandOptions.AddFlags(flags)

	flags.StringVarP(&o.Namespace, "namespace", "n", "default", "-n=namespace or -n namespace")
	flags.StringVarP(&o.Cluster, "cluster", "C", "", "the name of legacy cluster (eg -C=member1)")
	flags.StringVar(&o.ClusterContext, "cluster-context", "",
		"Context name of legacy cluster in kubeconfig. Only works when there are multiple contexts in the kubeconfig.")
	flags.StringVar(&o.ClusterKubeConfig, "cluster-kubeconfig", "",
		"Path of the legacy cluster's kubeconfig.")
}

// Complete ensures that options are valid and marshals them if necessary
func (o *CommandMigrateOption) Complete(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("incorrect command format, please use correct command format")
	}

	// parse gvk format
	if gvk := strings.Split(args[0], "."); len(gvk) == 3 {
		o.kind = gvk[0]
		o.version = gvk[1]
		o.group = gvk[2]
	} else if len(gvk) == 2 {
		o.kind = gvk[0]
		o.version = gvk[1]
	} else {
		return fmt.Errorf("incorrect gvk format")
	}

	o.name = args[1]

	return nil
}

// Validate checks to the MigrateOptions to see if there is sufficient information run the command
func (o *CommandMigrateOption) Validate() error {
	if o.Cluster == "" {
		return fmt.Errorf("the cluster cannot be empty")
	}

	// If '--cluster-context' not specified, take the cluster name as the context.
	if len(o.ClusterContext) == 0 {
		o.ClusterContext = o.Cluster
	}

	return nil
}

// RunMigrate migrate workloads from legacy clusters
func RunMigrate(_ io.Writer, karmadaConfig KarmadaConfig, opts CommandMigrateOption) error {
	// Get control plane karmada-apiserver client
	controlPlaneRestConfig, err := karmadaConfig.GetRestConfig(opts.KarmadaContext, opts.KubeConfig)
	if err != nil {
		return fmt.Errorf("failed to get control plane rest config. context: %s, kube-config: %s, error: %v",
			opts.KarmadaContext, opts.KubeConfig, err)
	}

	// Get cluster config
	clusterConfig, err := karmadaConfig.GetRestConfig(opts.ClusterContext, opts.ClusterKubeConfig)
	if err != nil {
		return fmt.Errorf("failed to get legacy cluster config. error: %v", err)
	}

	return migrate(controlPlaneRestConfig, clusterConfig, opts)
}

func migrate(controlPlaneRestConfig, memberConfig *rest.Config, opts CommandMigrateOption) error {
	memberClient := dynamic.NewForConfigOrDie(memberConfig)
	gvk := schema.GroupVersionKind{
		Group:   opts.group,
		Version: opts.version,
		Kind:    opts.kind,
	}
	mapper, err := apiutil.NewDynamicRESTMapper(memberConfig)
	if err != nil {
		return fmt.Errorf("Failed to create restmapper: %v", err)
	}
	gvr, err := restmapper.GetGroupVersionResource(mapper, gvk)
	if err != nil {
		return fmt.Errorf("Failed to get gvr from %q: %v", gvk, err)
	}
	obj, err := memberClient.Resource(gvr).Namespace(opts.Namespace).Get(context.TODO(), opts.name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Failed to get resource %q(%s/%s) in %s: %v", gvr, opts.Namespace, opts.name, opts.Cluster, err)
	}
	controlplaneDynamicClient := dynamic.NewForConfigOrDie(controlPlaneRestConfig)
	_, err = controlplaneDynamicClient.Resource(gvr).Namespace(opts.Namespace).Get(context.TODO(), opts.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			obj.SetResourceVersion("")
			_, err = controlplaneDynamicClient.Resource(gvr).Namespace(opts.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("Failed to create resource %q(%s/%s) in control plane: %v", gvr, opts.Namespace, opts.name, err)
			}
			karmadaClient := karmadaclientset.NewForConfigOrDie(controlPlaneRestConfig)
			if len(opts.Namespace) == 0 {
				return createOrUpdateClusterPropagationPolicy(karmadaClient, gvr, opts)
			}
			return createOrUpdatePropagationPolicy(karmadaClient, gvr, opts)
		}
		return fmt.Errorf("Failed to get resource %q(%s/%s) in control plane: %v", gvr, opts.Namespace, opts.name, err)
	}
	// TODO: adopt resource when it already exists in control plane
	return nil
}

func createOrUpdatePropagationPolicy(karmadaClient *versioned.Clientset, gvr schema.GroupVersionResource, opts CommandMigrateOption) error {
	name := opts.name + "-propagation"
	ns := opts.Namespace
	pp, err := karmadaClient.PolicyV1alpha1().PropagationPolicies(ns).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			pp = &policyv1alpha1.PropagationPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: ns,
				},
				Spec: policyv1alpha1.PropagationSpec{
					ResourceSelectors: []policyv1alpha1.ResourceSelector{
						{
							APIVersion: gvr.GroupVersion().String(),
							Kind:       opts.kind,
							Name:       opts.name,
						},
					},
					Placement: policyv1alpha1.Placement{
						ClusterAffinity: &policyv1alpha1.ClusterAffinity{
							ClusterNames: []string{opts.Cluster},
						},
					},
				},
			}
			_, err = karmadaClient.PolicyV1alpha1().PropagationPolicies(ns).Create(context.TODO(), pp, metav1.CreateOptions{})
			return err
		}
		return fmt.Errorf("Failed to get propagation policy(%s/%s) in control plane: %v", ns, name, err)
	}
	// edit propagation policy if it already exists
	// TODO: make it compatible with label selector and field selector!!!
	targetClusters := pp.Spec.Placement.ClusterAffinity.ClusterNames
	// append the cluster to ClusterAffinity.ClusterNames if not exists
	exists := false
	for _, cluster := range targetClusters {
		if cluster == opts.Cluster {
			exists = true
		}
	}
	if !exists {
		_ = append(targetClusters, opts.Cluster)
		_, err = karmadaClient.PolicyV1alpha1().PropagationPolicies(ns).Update(context.TODO(), pp, metav1.UpdateOptions{})
		return err
	}
	return nil
}

func createOrUpdateClusterPropagationPolicy(karmadaClient *versioned.Clientset, gvr schema.GroupVersionResource, opts CommandMigrateOption) error {
	name := opts.name + "-propagation"
	cpp, err := karmadaClient.PolicyV1alpha1().ClusterPropagationPolicies().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			cpp = &policyv1alpha1.ClusterPropagationPolicy{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
				Spec: policyv1alpha1.PropagationSpec{
					ResourceSelectors: []policyv1alpha1.ResourceSelector{
						{
							APIVersion: gvr.GroupVersion().String(),
							Kind:       opts.kind,
							Name:       opts.name,
						},
					},
					Placement: policyv1alpha1.Placement{
						ClusterAffinity: &policyv1alpha1.ClusterAffinity{
							ClusterNames: []string{opts.Cluster},
						},
					},
				},
			}
			_, err = karmadaClient.PolicyV1alpha1().ClusterPropagationPolicies().Create(context.TODO(), cpp, metav1.CreateOptions{})
			return err
		}
		return fmt.Errorf("Failed to get cluster propagation policy(%s) in control plane: %v", name, err)
	}
	// edit propagation policy if it already exists
	// TODO: make it compatible with label selector and field selector!!!
	targetClusters := cpp.Spec.Placement.ClusterAffinity.ClusterNames
	// append the cluster to ClusterAffinity.ClusterNames if not exists
	exists := false
	for _, cluster := range targetClusters {
		if cluster == opts.Cluster {
			exists = true
		}
	}
	if !exists {
		_ = append(targetClusters, opts.Cluster)
		_, err = karmadaClient.PolicyV1alpha1().ClusterPropagationPolicies().Update(context.TODO(), cpp, metav1.UpdateOptions{})
		return err
	}
	return nil
}
