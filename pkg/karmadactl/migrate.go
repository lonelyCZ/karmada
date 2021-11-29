package karmadactl

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/karmada-io/karmada/pkg/karmadactl/options"
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

	_, _ = controlPlaneRestConfig, clusterConfig

	return nil
}
