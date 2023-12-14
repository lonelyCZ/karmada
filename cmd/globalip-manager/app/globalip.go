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

package app

import (
	"context"
	"flag"
	"fmt"
	"net"
	"strconv"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/term"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	crtlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/karmada-io/karmada/cmd/globalip-manager/app/options"
	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	controllerscontext "github.com/karmada-io/karmada/pkg/controllers/context"
	"github.com/karmada-io/karmada/pkg/controllers/globalserviceip"
	"github.com/karmada-io/karmada/pkg/metrics"
	"github.com/karmada-io/karmada/pkg/resourceinterpreter"
	"github.com/karmada-io/karmada/pkg/sharedcli"
	"github.com/karmada-io/karmada/pkg/sharedcli/klogflag"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"github.com/karmada-io/karmada/pkg/util/gclient"
	"github.com/karmada-io/karmada/pkg/util/objectwatcher"
	"github.com/karmada-io/karmada/pkg/util/restmapper"
	"github.com/karmada-io/karmada/pkg/version/sharedcommand"
)

// NewGlobalIPCommand creates a *cobra.Command object with default parameters
func NewGlobalIPCommand(ctx context.Context) *cobra.Command {
	opts := options.NewOptions()

	cmd := &cobra.Command{
		Use:  "globalip-manager",
		Long: `The globalip-manager manages global service ip in member cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// validate options
			if errs := opts.Validate(); len(errs) != 0 {
				return errs.ToAggregate()
			}
			if err := run(ctx, opts); err != nil {
				return err
			}
			return nil
		},
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	fss := cliflag.NamedFlagSets{}

	genericFlagSet := fss.FlagSet("generic")
	genericFlagSet.AddGoFlagSet(flag.CommandLine)
	opts.AddFlags(genericFlagSet, controllers.ControllerNames())

	// Set klog flags
	logsFlagSet := fss.FlagSet("logs")
	klogflag.Add(logsFlagSet)

	cmd.AddCommand(sharedcommand.NewCmdVersion("globalip-manager"))
	cmd.Flags().AddFlagSet(genericFlagSet)
	cmd.Flags().AddFlagSet(logsFlagSet)

	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	sharedcli.SetUsageAndHelpFunc(cmd, fss, cols)
	return cmd
}

var controllers = make(controllerscontext.Initializers)

var controllersDisabledByDefault = sets.New(
	"",
)

func init() {
	controllers["globalcidrcollect"] = startGlobalCIDRCollectController
	controllers["globalipallocate"] = startGlobalIPAllocateController
}

func run(ctx context.Context, opts *options.Options) error {
	controlPlaneRestConfig, err := controllerruntime.GetConfig()
	if err != nil {
		return fmt.Errorf("error building kubeconfig of karmada control plane: %w", err)
	}
	controlPlaneRestConfig.QPS, controlPlaneRestConfig.Burst = opts.KubeAPIQPS, opts.KubeAPIBurst

	controllerManager, err := controllerruntime.NewManager(controlPlaneRestConfig, controllerruntime.Options{
		Logger:                     klog.Background(),
		Scheme:                     gclient.NewSchema(),
		SyncPeriod:                 &opts.ResyncPeriod.Duration,
		LeaderElection:             opts.LeaderElection.LeaderElect,
		LeaderElectionID:           "globalip-manager",
		LeaderElectionNamespace:    opts.LeaderElection.ResourceNamespace,
		LeaderElectionResourceLock: opts.LeaderElection.ResourceLock,
		LeaseDuration:              &opts.LeaderElection.LeaseDuration.Duration,
		RenewDeadline:              &opts.LeaderElection.RenewDeadline.Duration,
		RetryPeriod:                &opts.LeaderElection.RetryPeriod.Duration,
		HealthProbeBindAddress:     net.JoinHostPort(opts.BindAddress, strconv.Itoa(opts.SecurePort)),
		LivenessEndpointName:       "/healthz",
		MetricsBindAddress:         opts.MetricsBindAddress,
		MapperProvider:             restmapper.MapperProvider,
		BaseContext: func() context.Context {
			return ctx
		},
		Controller: config.Controller{
			GroupKindConcurrency: map[string]int{
				clusterv1alpha1.SchemeGroupVersion.WithKind("Cluster").GroupKind().String(): opts.ConcurrentClusterSyncs,
			},
			CacheSyncTimeout: opts.ClusterCacheSyncTimeout.Duration,
		},
		NewCache: func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
			opts.DefaultTransform = fedinformer.StripUnusedFields
			return cache.New(config, opts)
		},
	})
	if err != nil {
		klog.Errorf("Failed to build globalip manager: %v", err)
		return err
	}

	if err := controllerManager.AddHealthzCheck("ping", healthz.Ping); err != nil {
		klog.Errorf("Failed to add health check endpoint: %v", err)
		return err
	}

	crtlmetrics.Registry.MustRegister(metrics.ClusterCollectors()...)
	crtlmetrics.Registry.MustRegister(metrics.ResourceCollectors()...)
	crtlmetrics.Registry.MustRegister(metrics.PoolCollectors()...)

	setupControllers(controllerManager, opts, ctx.Done())

	// blocks until the context is done.
	if err := controllerManager.Start(ctx); err != nil {
		klog.Errorf("controller manager exits unexpectedly: %v", err)
		return err
	}

	return nil
}

// setupControllers initialize controllers and setup one by one.
func setupControllers(mgr controllerruntime.Manager, opts *options.Options, stopChan <-chan struct{}) {
	restConfig := mgr.GetConfig()
	dynamicClientSet := dynamic.NewForConfigOrDie(restConfig)
	kubeClientSet := kubeclientset.NewForConfigOrDie(restConfig)

	controlPlaneInformerManager := genericmanager.NewSingleClusterInformerManager(dynamicClientSet, 0, stopChan)
	// We need a service lister to build a resource interpreter with `ClusterIPServiceResolver`
	// witch allows connection to the customized interpreter webhook without a cluster DNS service.
	sharedFactory := informers.NewSharedInformerFactory(kubeClientSet, 0)
	serviceLister := sharedFactory.Core().V1().Services().Lister()
	sharedFactory.Start(stopChan)
	sharedFactory.WaitForCacheSync(stopChan)

	resourceInterpreter := resourceinterpreter.NewResourceInterpreter(controlPlaneInformerManager, serviceLister)
	if err := mgr.Add(resourceInterpreter); err != nil {
		klog.Fatalf("Failed to setup custom resource interpreter: %v", err)
	}

	objectWatcher := objectwatcher.NewObjectWatcher(mgr.GetClient(), mgr.GetRESTMapper(), util.NewClusterDynamicClientSet, resourceInterpreter)

	controllerContext := controllerscontext.Context{
		Mgr:           mgr,
		ObjectWatcher: objectWatcher,
		Opts: controllerscontext.Options{
			Controllers:             opts.Controllers,
			ClusterCacheSyncTimeout: opts.ClusterCacheSyncTimeout,
			ClusterAPIQPS:           opts.ClusterAPIQPS,
			ClusterAPIBurst:         opts.ClusterAPIBurst,
			ConcurrentWorkSyncs:     opts.ConcurrentWorkSyncs,
			RateLimiterOptions:      opts.RateLimiterOpts,
		},
		StopChan:                    stopChan,
		DynamicClientSet:            dynamicClientSet,
		KubeClientSet:               kubeClientSet,
		ControlPlaneInformerManager: controlPlaneInformerManager,
		ResourceInterpreter:         resourceInterpreter,
	}

	if err := controllers.StartControllers(controllerContext, controllersDisabledByDefault); err != nil {
		klog.Fatalf("error starting controllers: %v", err)
	}

	// Ensure the InformerManager stops when the stop channel closes
	go func() {
		<-stopChan
		genericmanager.StopInstance()
	}()
}

func startGlobalIPAllocateController(ctx controllerscontext.Context) (enable bool, err error) {
	globalIPAllocateController := &globalserviceip.GlobalIPAllocateController{
		Client:          ctx.Mgr.GetClient(),
		EventRecorder:   ctx.Mgr.GetEventRecorderFor(globalserviceip.GlobalIPAllocateControllerName),
		RESTMapper:      ctx.Mgr.GetRESTMapper(),
		InformerManager: genericmanager.GetInstance(),
	}
	if err := globalIPAllocateController.SetupWithManager(ctx.Mgr); err != nil {
		return false, err
	}
	return true, nil
}

func startGlobalCIDRCollectController(ctx controllerscontext.Context) (enabled bool, err error) {
	opts := ctx.Opts
	globalCIDRCollectController := &globalserviceip.GlobalCIDRCollectController{
		Client:                      ctx.Mgr.GetClient(),
		RESTMapper:                  ctx.Mgr.GetRESTMapper(),
		EventRecorder:               ctx.Mgr.GetEventRecorderFor(globalserviceip.GlobalCIDRCollectControllerName),
		InformerManager:             genericmanager.GetInstance(),
		ClusterDynamicClientSetFunc: util.NewClusterDynamicClientSet,
		ClusterCacheSyncTimeout:     opts.ClusterCacheSyncTimeout,
	}
	if err := globalCIDRCollectController.SetupWithManager(ctx.Mgr); err != nil {
		return false, err
	}
	return true, nil
}
