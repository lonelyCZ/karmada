/*
Copyright 2023 The Karmada Authors.

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

package multiclusterservice

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	networkingv1alpha1 "github.com/karmada-io/karmada/pkg/apis/networking/v1alpha1"
	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	"github.com/karmada-io/karmada/pkg/events"
	"github.com/karmada-io/karmada/pkg/sharedcli/ratelimiterflag"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/names"
)

// ControllerName is the controller name that will be used when reporting events.
const ControllerName = "multiclusterservice-controller"

// MCSController is to sync MultiClusterService.
type MCSController struct {
	client.Client
	EventRecorder      record.EventRecorder
	RateLimiterOptions ratelimiterflag.Options
}

var (
	serviceGVK = corev1.SchemeGroupVersion.WithKind("Service")
)

// Reconcile performs a full reconciliation for the object referred to by the Request.
// The Controller will requeue the Request to be processed again if an error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (c *MCSController) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	klog.V(4).InfoS("Reconciling MultiClusterService", "namespace", req.Namespace, "name", req.Name)

	mcs := &networkingv1alpha1.MultiClusterService{}
	if err := c.Client.Get(ctx, req.NamespacedName, mcs); err != nil {
		if apierrors.IsNotFound(err) {
			// The mcs no longer exist, in which case we stop processing.
			return controllerruntime.Result{}, nil
		}
		klog.ErrorS(err, "failed to get MultiClusterService object", "NamespacedName", req.NamespacedName)
		return controllerruntime.Result{}, err
	}

	if !mcs.DeletionTimestamp.IsZero() {
		return c.handleMCSDelete(ctx, mcs.DeepCopy())
	}

	var err error
	defer func() {
		if err != nil {
			// 如果出现错误，则设置状态为Failed
			_ = c.updateMCSStatus(mcs, metav1.ConditionFalse, "ServiceAppliedFailed", err.Error())
			c.EventRecorder.Eventf(mcs, corev1.EventTypeWarning, events.EventReasonSyncServiceWorkFailed, err.Error())
			return
		}
		// 如果成功则，则设置状态为Succeed
		_ = c.updateMCSStatus(mcs, metav1.ConditionTrue, "ServiceAppliedSucceed", "Service is propagated to target clusters.")
		c.EventRecorder.Eventf(mcs, corev1.EventTypeNormal, events.EventReasonSyncWorkSucceed, "Service is propagated to target clusters.")
	}()
	// 处理MCS的创建和更新
	if err = c.handleMCSCreateOrUpdate(ctx, mcs.DeepCopy()); err != nil {
		return controllerruntime.Result{}, err
	}
	return controllerruntime.Result{}, nil
}

func (c *MCSController) handleMCSDelete(ctx context.Context, mcs *networkingv1alpha1.MultiClusterService) (controllerruntime.Result, error) {
	klog.V(4).InfoS("Begin to handle MultiClusterService delete event", "namespace", mcs.Namespace, "name", mcs.Name)

	if err := c.deleteServiceWork(mcs, sets.New[string]()); err != nil {
		c.EventRecorder.Event(mcs, corev1.EventTypeWarning, events.EventReasonSyncServiceWorkFailed,
			fmt.Sprintf("failed to delete service work :%v", err))
		return controllerruntime.Result{}, err
	}

	if err := c.deleteMultiClusterServiceWork(mcs, true); err != nil {
		c.EventRecorder.Event(mcs, corev1.EventTypeWarning, events.EventReasonSyncServiceWorkFailed,
			fmt.Sprintf("failed to delete MultiClusterService work :%v", err))
		return controllerruntime.Result{}, err
	}

	finalizersUpdated := controllerutil.RemoveFinalizer(mcs, util.MCSControllerFinalizer)
	if finalizersUpdated {
		err := c.Client.Update(ctx, mcs)
		if err != nil {
			klog.V(4).ErrorS(err, "failed to update MultiClusterService with finalizer",
				"namespace", mcs.Namespace, "name", mcs.Name)
			return controllerruntime.Result{}, err
		}
	}

	klog.V(4).InfoS("Success to delete MultiClusterService", "namespace", mcs.Namespace, "name", mcs.Name)
	return controllerruntime.Result{}, nil
}

// 根据mcsID，删除对应的service work
func (c *MCSController) deleteServiceWork(mcs *networkingv1alpha1.MultiClusterService, retainClusters sets.Set[string]) error {

	mcsID := util.GetLabelValue(mcs.Labels, networkingv1alpha1.MultiClusterServicePermanentIDLabel)
	workList, err := helper.GetWorksByLabelsSet(c, labels.Set{
		networkingv1alpha1.MultiClusterServicePermanentIDLabel: mcsID,
	})
	if err != nil {
		klog.ErrorS(err, "failed to get work", "namespace", mcs.Namespace, "name", mcs.Name)
		return err
	}

	for _, work := range workList.Items {
		// 逐一删除service work
		if !helper.IsWorkContains(work.Spec.Workload.Manifests, serviceGVK) {
			continue
		}
		clusterName, err := names.GetClusterName(work.Namespace)
		if err != nil {
			klog.Errorf("Failed to get member cluster name for work %s/%s", work.Namespace, work.Name)
			continue
		}
		if retainClusters.Has(clusterName) {
			continue
		}

		if err = c.Client.Delete(context.TODO(), work.DeepCopy()); err != nil && !apierrors.IsNotFound(err) {
			klog.Errorf("Error while deleting work(%s/%s) deletion timestamp: %s",
				work.Namespace, work.Name, err)
			return err
		}
	}

	klog.V(4).InfoS("success to delete service work", "namespace", mcs.Namespace, "name", mcs.Name)
	return nil
}

// 删除创建的mcs类型的work
func (c *MCSController) deleteMultiClusterServiceWork(mcs *networkingv1alpha1.MultiClusterService, deleteAll bool) error {
	mcsID := util.GetLabelValue(mcs.Labels, networkingv1alpha1.MultiClusterServicePermanentIDLabel)
	workList, err := helper.GetWorksByLabelsSet(c, labels.Set{
		networkingv1alpha1.MultiClusterServicePermanentIDLabel: mcsID,
	})
	if err != nil {
		klog.Errorf("Failed to list work by MultiClusterService(%s/%s):%v", mcs.Namespace, mcs.Name, err)
		return err
	}
	// 获取提供服务的集群
	provisionClusters, err := helper.GetProvisionClusters(c.Client, mcs)
	if err != nil {
		klog.Errorf("Failed to get provision clusters by MultiClusterService(%s/%s):%v", mcs.Namespace, mcs.Name, err)
		return err
	}

	for _, work := range workList.Items {
		if !helper.IsWorkContains(work.Spec.Workload.Manifests, multiClusterServiceGVK) {
			continue
		}
		clusterName, err := names.GetClusterName(work.Namespace)
		if err != nil {
			klog.Errorf("Failed to get member cluster name for work %s/%s:%v", work.Namespace, work.Name, work)
			continue
		}

		if !deleteAll && provisionClusters.Has(clusterName) && c.IsClusterReady(clusterName) {
			// 不是删除所有，提供集群中有这个work的cluster，且cluster是ready状态，则不删除
			continue
		}
		// 清除掉endpointslice work
		if err := c.cleanProvisionEndpointSliceWork(work.DeepCopy()); err != nil {
			klog.Errorf("Failed to clean provision EndpointSlice work(%s/%s):%v", work.Namespace, work.Name, err)
			return err
		}

		if err = c.Client.Delete(context.TODO(), work.DeepCopy()); err != nil && !apierrors.IsNotFound(err) {
			klog.Errorf("Error while deleting work(%s/%s): %v", work.Namespace, work.Name, err)
			return err
		}
	}

	klog.V(4).InfoS("Success to delete MultiClusterService work", "namespace", mcs.Namespace, "name", mcs.Name)
	return nil
}

// 清理生产集群里的endpointslice
func (c *MCSController) cleanProvisionEndpointSliceWork(work *workv1alpha1.Work) error {
	workList := &workv1alpha1.WorkList{}
	if err := c.List(context.TODO(), workList, &client.ListOptions{
		Namespace: work.Namespace,
		LabelSelector: labels.SelectorFromSet(labels.Set{
			util.MultiClusterServiceNameLabel:      util.GetLabelValue(work.Labels, util.MultiClusterServiceNameLabel),
			util.MultiClusterServiceNamespaceLabel: util.GetLabelValue(work.Labels, util.MultiClusterServiceNamespaceLabel),
		}),
	}); err != nil {
		klog.Errorf("Failed to list workList reported by work(MultiClusterService)(%s/%s): %v", work.Namespace, work.Name, err)
		return err
	}

	var errs []error
	for _, work := range workList.Items {
		if !helper.IsWorkContains(work.Spec.Workload.Manifests, endpointSliceGVK) {
			continue
		}
		// This annotation is only added to the EndpointSlice work in consumption clusters' execution namespace
		// Here we only care about the EndpointSlice work in provision clusters' execution namespace
		if util.GetAnnotationValue(work.Annotations, util.EndpointSliceProvisionClusterAnnotation) != "" {
			continue
		}

		if err := c.Delete(context.TODO(), work.DeepCopy()); err != nil {
			klog.Errorf("Failed to delete work(%s/%s), Error: %v", work.Namespace, work.Name, err)
			errs = append(errs, err)
		}
	}
	if err := utilerrors.NewAggregate(errs); err != nil {
		return err
	}

	// TBD: This is needed because we add this finalizer in version 1.8.0, delete this in version 1.10.0
	if controllerutil.RemoveFinalizer(work, util.MCSEndpointSliceCollectControllerFinalizer) {
		if err := c.Update(context.TODO(), work); err != nil {
			klog.Errorf("Failed to update work(%s/%s), Error: %v", work.Namespace, work.Name, err)
			return err
		}
	}

	return nil
}

func (c *MCSController) handleMCSCreateOrUpdate(ctx context.Context, mcs *networkingv1alpha1.MultiClusterService) error {
	klog.V(4).InfoS("Begin to handle MultiClusterService create or update event",
		"namespace", mcs.Namespace, "name", mcs.Name)

	// 1. if mcs not contain CrossCluster type, delete service work if needed
	// 如果mcs不是跨集群类型，则删除service work和mcs work
	if !helper.MultiClusterServiceCrossClusterEnabled(mcs) {
		if err := c.deleteServiceWork(mcs, sets.New[string]()); err != nil {
			return err
		}
		if err := c.deleteMultiClusterServiceWork(mcs, true); err != nil {
			return err
		}
	}

	// 2. add finalizer if needed
	// 添加finalizer
	finalizersUpdated := controllerutil.AddFinalizer(mcs, util.MCSControllerFinalizer)
	if finalizersUpdated {
		err := c.Client.Update(ctx, mcs)
		if err != nil {
			klog.V(4).ErrorS(err, "failed to update mcs with finalizer", "namespace", mcs.Namespace, "name", mcs.Name)
			return err
		}
	}

	// 3. Generate the MCS work
	// 生成mcs work
	if err := c.ensureMultiClusterServiceWork(ctx, mcs); err != nil {
		return err
	}

	// 4. make sure service exist
	// 确保service存在于控制面
	svc := &corev1.Service{}
	err := c.Client.Get(ctx, types.NamespacedName{Namespace: mcs.Namespace, Name: mcs.Name}, svc)
	if err != nil && !apierrors.IsNotFound(err) {
		klog.ErrorS(err, "failed to get service", "namespace", mcs.Namespace, "name", mcs.Name)
		return err
	}

	// 5. if service not exist, delete service work if needed
	// 如果service不存在了，要删除掉对应的service work
	if apierrors.IsNotFound(err) {
		delErr := c.deleteServiceWork(mcs, sets.New[string]())
		if delErr != nil {
			klog.ErrorS(err, "failed to delete service work", "namespace", mcs.Namespace, "name", mcs.Name)
			c.EventRecorder.Event(mcs, corev1.EventTypeWarning, events.EventReasonSyncServiceWorkFailed,
				fmt.Sprintf("failed to delete service work :%v", err))
		}
		return err
	}

	// 6. if service exist, create or update corresponding work in clusters
	// 如果service存在，需要将service传播到成员集群中
	syncClusters, err := c.syncSVCWorkToClusters(ctx, mcs, svc)
	if err != nil {
		return err
	}

	// 7. delete MultiClusterService work not in provision clusters and in the unready clusters
	// 删除掉不在生产集群中和cluster未就绪集群中的mcs work
	if err = c.deleteMultiClusterServiceWork(mcs, false); err != nil {
		return err
	}

	// 8. delete service work not in need sync clusters
	// 删除掉不再需要同步集群中的service
	if err = c.deleteServiceWork(mcs, syncClusters); err != nil {
		return err
	}

	klog.V(4).InfoS("success to ensure service work", "namespace", mcs.Namespace, "name", mcs.Name)
	return nil
}

// mcs work只是提供给控制面识别，不下发到子集群中
func (c *MCSController) ensureMultiClusterServiceWork(ctx context.Context, mcs *networkingv1alpha1.MultiClusterService) error {
	// 获取到提供集群，
	provisionClusters, err := helper.GetProvisionClusters(c.Client, mcs)
	if err != nil {
		klog.Errorf("Failed to get provision clusters by MultiClusterService(%s/%s):%v", mcs.Namespace, mcs.Name, err)
		return err
	}

	for clusterName := range provisionClusters {
		if !c.IsClusterReady(clusterName) {
			continue
		}
		workMeta := metav1.ObjectMeta{
			Name:      names.GenerateMCSWorkName(mcs.Kind, mcs.Name, mcs.Namespace, clusterName),
			Namespace: names.GenerateExecutionSpaceName(clusterName),
			Labels: map[string]string{
				// We add this id in mutating webhook, let's just use it
				networkingv1alpha1.MultiClusterServicePermanentIDLabel: util.GetLabelValue(mcs.Labels, networkingv1alpha1.MultiClusterServicePermanentIDLabel),
				util.ManagedByKarmadaLabel:                             util.ManagedByKarmadaLabelValue,
				util.PropagationInstruction:                            util.PropagationInstructionSuppressed,
				util.MultiClusterServiceNamespaceLabel:                 mcs.Namespace,
				util.MultiClusterServiceNameLabel:                      mcs.Name,
			},
		}

		mcsObj, err := helper.ToUnstructured(mcs)
		if err != nil {
			klog.Errorf("Failed to convert MultiClusterService(%s/%s) to unstructured object, err is %v", mcs.Namespace, mcs.Name, err)
			return err
		}
		if err = helper.CreateOrUpdateWork(c, workMeta, mcsObj); err != nil {
			klog.Errorf("Failed to create or update MultiClusterService(%s/%s) work in the given member cluster %s, err is %v",
				mcs.Namespace, mcs.Name, clusterName, err)
			return err
		}
	}

	return nil
}

// 将svc同步到成员集群中，根据生产和消费集群的情况
func (c *MCSController) syncSVCWorkToClusters(
	ctx context.Context,
	mcs *networkingv1alpha1.MultiClusterService,
	svc *corev1.Service,
) (sets.Set[string], error) {
	syncClusters := sets.New[string]()
	clusters, err := util.GetClusterSet(c.Client)
	if err != nil {
		klog.ErrorS(err, "failed to list clusters")
		return syncClusters, err
	}

	serverLocations := sets.New[string](mcs.Spec.ServiceProvisionClusters...)
	clientLocations := sets.New[string](mcs.Spec.ServiceConsumptionClusters...)
	for clusterName := range clusters {
		// if ServerLocations or ClientLocations are empty, we will sync work to the all clusters
		// 如果没有设置生产或消费cluster，就需要传播到所有的成员集群中
		if len(serverLocations) == 0 || len(clientLocations) == 0 ||
			serverLocations.Has(clusterName) || clientLocations.Has(clusterName) {
			syncClusters.Insert(clusterName)
		}
	}

	svcObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(svc)
	if err != nil {
		return syncClusters, err
	}
	Unstructured := &unstructured.Unstructured{Object: svcObj}

	var errs []error
	for clusterName := range syncClusters {
		workMeta := metav1.ObjectMeta{
			Name:       names.GenerateMCSWorkName(svc.Kind, svc.Name, svc.Namespace, clusterName),
			Namespace:  names.GenerateExecutionSpaceName(clusterName),
			Finalizers: []string{util.ExecutionControllerFinalizer},
			Labels: map[string]string{
				// We add this id in mutating webhook, let's just use it
				networkingv1alpha1.MultiClusterServicePermanentIDLabel: util.GetLabelValue(mcs.Labels, networkingv1alpha1.MultiClusterServicePermanentIDLabel),
				util.ManagedByKarmadaLabel:                             util.ManagedByKarmadaLabelValue,
			},
		}

		// Add these two label as for work status synchronization
		// 需要同步woker的状态
		util.MergeLabel(Unstructured, workv1alpha1.WorkNameLabel, names.GenerateMCSWorkName(svc.Kind, svc.Name, svc.Namespace, clusterName))
		util.MergeLabel(Unstructured, workv1alpha1.WorkNamespaceLabel, names.GenerateExecutionSpaceName(clusterName))
		// 直接在此处创建work，不需要通过pp了
		if err = helper.CreateOrUpdateWork(c, workMeta, Unstructured); err != nil {
			klog.Errorf("Failed to create or update resource(%v/%v) in the given member cluster %s, err is %v",
				workMeta.GetNamespace(), workMeta.GetName(), clusterName, err)
			c.EventRecorder.Event(mcs, corev1.EventTypeWarning, events.EventReasonSyncServiceWorkFailed, fmt.Sprintf(
				"Failed to create or update resource(%v/%v) in member cluster(%s): %v",
				workMeta.GetNamespace(), workMeta.GetName(), clusterName, err))
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return syncClusters, utilerrors.NewAggregate(errs)
	}

	return syncClusters, nil
}

func (c *MCSController) updateMCSStatus(mcs *networkingv1alpha1.MultiClusterService, status metav1.ConditionStatus, reason, message string) error {
	serviceAppliedCondition := metav1.Condition{
		Type:               networkingv1alpha1.MCSServiceAppliedConditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() (err error) {
		meta.SetStatusCondition(&mcs.Status.Conditions, serviceAppliedCondition)
		updateErr := c.Status().Update(context.TODO(), mcs)
		if updateErr == nil {
			return nil
		}
		updated := &networkingv1alpha1.MultiClusterService{}
		if err = c.Get(context.TODO(), client.ObjectKey{Namespace: mcs.Namespace, Name: mcs.Name}, updated); err == nil {
			mcs = updated
		} else {
			klog.Errorf("Failed to get updated MultiClusterService %s/%s: %v", mcs.Namespace, mcs.Name, err)
		}
		return updateErr
	})
}

func (c *MCSController) IsClusterReady(clusterName string) bool {
	cluster := &clusterv1alpha1.Cluster{}
	if err := c.Client.Get(context.TODO(), types.NamespacedName{Name: clusterName}, cluster); err != nil {
		klog.ErrorS(err, "failed to get cluster object", "Name", clusterName)
		return false
	}

	return util.IsClusterReady(&cluster.Status)
}

// SetupWithManager creates a controller and register to controller manager.
func (c *MCSController) SetupWithManager(mgr controllerruntime.Manager) error {
	mcsPredicateFunc := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			mcs := e.Object.(*networkingv1alpha1.MultiClusterService)
			return helper.MultiClusterServiceCrossClusterEnabled(mcs)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			mcsOld := e.ObjectOld.(*networkingv1alpha1.MultiClusterService)
			mcsNew := e.ObjectNew.(*networkingv1alpha1.MultiClusterService)
			if !helper.MultiClusterServiceCrossClusterEnabled(mcsOld) && !helper.MultiClusterServiceCrossClusterEnabled(mcsNew) {
				return false
			}

			// We only care about the update events below:
			if equality.Semantic.DeepEqual(mcsOld.Annotations, mcsNew.Annotations) &&
				equality.Semantic.DeepEqual(mcsOld.Spec, mcsNew.Spec) &&
				equality.Semantic.DeepEqual(mcsOld.DeletionTimestamp.IsZero(), mcsNew.DeletionTimestamp.IsZero()) {
				return false
			}
			return true
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			// Since finalizer is added to the MultiClusterService object,
			// the delete event is processed by the update event.
			return false
		},
		GenericFunc: func(event.GenericEvent) bool {
			return true
		},
	}

	svcPredicateFunc := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			svcOld := e.ObjectOld.(*corev1.Service)
			svcNew := e.ObjectNew.(*corev1.Service)

			// We only care about the update events below:
			if equality.Semantic.DeepEqual(svcOld.Annotations, svcNew.Annotations) &&
				equality.Semantic.DeepEqual(svcOld.Spec, svcNew.Spec) {
				return false
			}
			return true
		},
		DeleteFunc: func(event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(event.GenericEvent) bool {
			return true
		},
	}

	svcMapFunc := handler.MapFunc(
		func(ctx context.Context, svcObj client.Object) []reconcile.Request {
			return []reconcile.Request{{
				NamespacedName: types.NamespacedName{
					Namespace: svcObj.GetNamespace(),
					Name:      svcObj.GetName(),
				},
			}}
		})

	return controllerruntime.NewControllerManagedBy(mgr).
		For(&networkingv1alpha1.MultiClusterService{}, builder.WithPredicates(mcsPredicateFunc)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(svcMapFunc), builder.WithPredicates(svcPredicateFunc)).
		Watches(&clusterv1alpha1.Cluster{}, handler.EnqueueRequestsFromMapFunc(c.clusterMapFunc())).
		WithOptions(controller.Options{RateLimiter: ratelimiterflag.DefaultControllerRateLimiter(c.RateLimiterOptions)}).
		Complete(c)
}

func (c *MCSController) clusterMapFunc() handler.MapFunc {
	return func(ctx context.Context, a client.Object) []reconcile.Request {
		var clusterName string
		switch t := a.(type) {
		case *clusterv1alpha1.Cluster:
			clusterName = t.Name
		default:
			return nil
		}

		klog.V(4).Infof("Begin to sync mcs with cluster %s.", clusterName)
		mcsList := &networkingv1alpha1.MultiClusterServiceList{}
		if err := c.Client.List(context.TODO(), mcsList, &client.ListOptions{}); err != nil {
			klog.Errorf("Failed to list MultiClusterService, error: %v", err)
			return nil
		}

		var requests []reconcile.Request
		for index := range mcsList.Items {
			if !needSyncMultiClusterService(&mcsList.Items[index], clusterName) {
				continue
			}

			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: mcsList.Items[index].Namespace,
				Name: mcsList.Items[index].Name}})
		}

		return requests
	}
}

func needSyncMultiClusterService(mcs *networkingv1alpha1.MultiClusterService, clusterName string) bool {
	if len(mcs.Spec.ServiceProvisionClusters) == 0 || len(mcs.Spec.ServiceConsumptionClusters) == 0 {
		return true
	}
	clusters := sets.New[string](mcs.Spec.ServiceProvisionClusters...)
	clusters.Insert(mcs.Spec.ServiceConsumptionClusters...)
	return clusters.Has(clusterName)
}
