package globalserviceip

import (
	"context"
	"fmt"
	"sync"
	"time"

	submarinerv1 "github.com/submariner-io/submariner/pkg/apis/submariner.io/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"github.com/karmada-io/karmada/pkg/util/helper"
)

// GlobalCIDRCollectControllerName is the controller name that will be used when reporting events.
const GlobalCIDRCollectControllerName = "globalcidr-collect-controller"

// GlobalCIDRCollectController is to collect global cidr from member cluster.
type GlobalCIDRCollectController struct {
	client.Client
	EventRecorder               record.EventRecorder
	RESTMapper                  meta.RESTMapper
	InformerManager             genericmanager.MultiClusterInformerManager
	eventHandlers               sync.Map
	ClusterDynamicClientSetFunc func(clusterName string, client client.Client) (*util.DynamicClusterClient, error)

	ClusterCacheSyncTimeout metav1.Duration
}

var (
	subClusterGVR = submarinerv1.SchemeGroupVersion.WithResource("clusters")
)

// Reconcile performs a full reconciliation for the object referred to by the Request.
// The Controller will requeue the Request to be processed again if an error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (c *GlobalCIDRCollectController) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	klog.V(4).Infof("Collect glbal cidr from member cluster: %s", req.NamespacedName.Name)

	var err error

	cluster := &clusterv1alpha1.Cluster{}
	if err := c.Client.Get(ctx, req.NamespacedName, cluster); err != nil {
		// The resource may no longer exist, in which case we stop processing.
		if apierrors.IsNotFound(err) {
			return controllerruntime.Result{}, nil
		}

		return controllerruntime.Result{Requeue: true}, err
	}

	if !cluster.DeletionTimestamp.IsZero() {
		return controllerruntime.Result{}, nil
	}

	// 构建自己的informer
	if err = c.buildResourceInformers(ctx, cluster); err != nil {
		return controllerruntime.Result{Requeue: true}, err
	}

	if err = c.collectGlobalCidr(ctx, cluster.DeepCopy()); err != nil {
		return controllerruntime.Result{Requeue: true}, err
	}

	return controllerruntime.Result{RequeueAfter: 10 * time.Second}, nil
}

func (c *GlobalCIDRCollectController) buildResourceInformers(ctx context.Context, cluster *clusterv1alpha1.Cluster) error {
	if !util.IsClusterReady(&cluster.Status) {
		klog.Errorf("Stop collect global cidr for cluster(%s) as cluster not ready.", cluster.Name)
		return fmt.Errorf("cluster(%s) not ready", cluster.Name)
	}

	if err := c.registerInformersAndStart(cluster); err != nil {
		klog.Errorf("Failed to register informer for Cluster %s. Error: %v.", cluster.Name, err)
		return err
	}
	return nil
}

func (c *GlobalCIDRCollectController) registerInformersAndStart(cluster *clusterv1alpha1.Cluster) error {
	singleClusterInformerManager := c.InformerManager.GetSingleClusterManager(cluster.Name)
	if singleClusterInformerManager == nil {
		// 如果为空就创建informer
		dynamicClusterClient, err := c.ClusterDynamicClientSetFunc(cluster.Name, c.Client)
		if err != nil {
			klog.Errorf("Failed to build dynamic cluster client for cluster %s.", cluster.Name)
			return err
		}
		singleClusterInformerManager = c.InformerManager.ForCluster(dynamicClusterClient.ClusterName, dynamicClusterClient.DynamicClientSet, 0)
	}

	gvrTargets := []schema.GroupVersionResource{
		subClusterGVR,
	}

	allSynced := true
	for _, gvr := range gvrTargets {
		if !singleClusterInformerManager.IsInformerSynced(gvr) || !singleClusterInformerManager.IsHandlerExist(gvr, c.getEventHandler(cluster.Name)) {
			allSynced = false
			// 如果informer中没有监听sub cluster，则添加
			singleClusterInformerManager.ForResource(gvr, c.getEventHandler(cluster.Name))
		}
	}
	if allSynced {
		// 都监听了，不需要再操作了
		return nil
	}
	// 启动单集群informer
	c.InformerManager.Start(cluster.Name)

	if err := func() error {
		// 等待informer
		synced := c.InformerManager.WaitForCacheSyncWithTimeout(cluster.Name, c.ClusterCacheSyncTimeout.Duration)
		if synced == nil {
			return fmt.Errorf("no informerFactory for cluster %s exist", cluster.Name)
		}
		for _, gvr := range gvrTargets {
			if !synced[gvr] {
				return fmt.Errorf("informer for %s hasn't synced", gvr)
			}
		}
		return nil
	}(); err != nil {
		klog.Errorf("Failed to sync cache for cluster: %s, error: %v", cluster.Name, err)
		c.InformerManager.Stop(cluster.Name)
		return err
	}

	return nil
}

func (c *GlobalCIDRCollectController) collectGlobalCidr(ctx context.Context, cluster *clusterv1alpha1.Cluster) error {
	manager := c.InformerManager.GetSingleClusterManager(cluster.Name)
	if manager == nil {
		err := fmt.Errorf("failed to get informer manager for cluster %s", cluster.Name)
		klog.Errorf("%v", err)
		return err
	}

	subClusterObj, err := manager.Lister(subClusterGVR).ByNamespace("submariner-operator").Get(cluster.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		klog.Errorf("Failed to get submariner cluster(%s), Error: %v", cluster.Name, err)
		return err
	}

	// 转换为submariner cluster
	subCluster := &submarinerv1.Cluster{}
	if err = helper.ConvertToTypedObject(subClusterObj, subCluster); err != nil {
		klog.Errorf("Failed to convert object to submariner cluster, error: %v", err)
		return err
	}

	if len(subCluster.Spec.GlobalCIDR) == 0 {
		return nil
	}

	cluster.Annotations[ClusterGlobalCIDRAnnotation] = subCluster.Spec.GlobalCIDR[0]

	err = c.Client.Update(ctx, cluster)
	if err != nil {
		klog.Errorf("Failed to update cluster(%s) annotations, error: %v", cluster.Name, err)
		return err
	}

	return nil
}

// getEventHandler return callback function that knows how to handle events from the member cluster.
func (c *GlobalCIDRCollectController) getEventHandler(clusterName string) cache.ResourceEventHandler {
	if value, exists := c.eventHandlers.Load(clusterName); exists {
		return value.(cache.ResourceEventHandler)
	}

	eventHandler := fedinformer.NewHandlerOnEvents(c.genHandlerAddFunc(clusterName), c.genHandlerUpdateFunc(clusterName),
		c.genHandlerDeleteFunc(clusterName))
	c.eventHandlers.Store(clusterName, eventHandler)
	return eventHandler
}

func (c *GlobalCIDRCollectController) genHandlerAddFunc(clusterName string) func(obj interface{}) {
	return func(obj interface{}) {
	}
}

func (c *GlobalCIDRCollectController) genHandlerUpdateFunc(clusterName string) func(oldObj, newObj interface{}) {
	return func(oldObj, newObj interface{}) {
	}
}

func (c *GlobalCIDRCollectController) genHandlerDeleteFunc(clusterName string) func(obj interface{}) {
	return func(obj interface{}) {
	}
}

// SetupWithManager creates a controller and register to controller manager.
func (c *GlobalCIDRCollectController) SetupWithManager(mgr controllerruntime.Manager) error {
	// 将监听work和cluster
	return controllerruntime.NewControllerManagedBy(mgr).For(&clusterv1alpha1.Cluster{}).
		Complete(c)
}
