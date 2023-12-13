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

package globalserviceip

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	mcsv1alpha1 "sigs.k8s.io/mcs-api/pkg/apis/v1alpha1"

	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/names"
)

// ControllerName is the controller name that will be used when reporting events.
const GlobalIPControllerName = "globalip-controller"

// GlobalIPController is to sync MultiClusterService.
type GlobalIPController struct {
	client.Client
	EventRecorder   record.EventRecorder
	RESTMapper      meta.RESTMapper
	InformerManager genericmanager.MultiClusterInformerManager
}

var (
	serviceGVK    = corev1.SchemeGroupVersion.WithKind("Service")
	deploymentGVK = appsv1.SchemeGroupVersion.WithKind("Deployment")

	ClusterGlobalCIDRAnnotation = "cluster.karmada.io/global-cidr"
	GlobalIPForWorkLabel        = "globalip.karmada.io/work-for"
)

// Reconcile performs a full reconciliation for the object referred to by the Request.
// The Controller will requeue the Request to be processed again if an error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (c *GlobalIPController) Reconcile(ctx context.Context, req controllerruntime.Request) (controllerruntime.Result, error) {
	klog.V(4).Infof("Reconciling Work %s", req.NamespacedName.String())

	work := &workv1alpha1.Work{}
	if err := c.Client.Get(ctx, req.NamespacedName, work); err != nil {
		if apierrors.IsNotFound(err) {
			return controllerruntime.Result{}, nil
		}
		return controllerruntime.Result{Requeue: true}, err
	}

	// 主要是监听deployment类型的work
	if !helper.IsWorkContains(work.Spec.Workload.Manifests, deploymentGVK) {
		return controllerruntime.Result{}, nil
	}

	// 只为设置了Global CIDR的成员集群中的Pod分配全局IP
	clusterName, err := names.GetClusterName(work.Namespace)
	if err != nil {
		klog.Errorf("Failed to get cluster name for work %s/%s", work.Namespace, work.Name)
		return controllerruntime.Result{}, err
	}
	cluster, err := util.GetCluster(c.Client, clusterName)
	if err != nil {
		klog.Errorf("Failed to get the given member cluster %s", clusterName)
		return controllerruntime.Result{}, err
	}
	if util.GetAnnotationValue(cluster.Annotations, ClusterGlobalCIDRAnnotation) == "" {
		return controllerruntime.Result{}, nil
	}

	if !work.DeletionTimestamp.IsZero() {
		// 如果该worker被标记删除了，则清除派生到成员集群中的service和serviceExport
		if err := c.cleanupGlobalIPFromCluster(ctx, work); err != nil {
			klog.Errorf("Failed to cleanup GlobalIP from cluster for work %s/%s:%v", work.Namespace, work.Name, err)
			return controllerruntime.Result{}, nil
		}
	}

	// 处理该work，判断是否要创建globalIP
	if err := c.allocateGlobalIP(ctx, work.DeepCopy(), clusterName); err != nil {
		return controllerruntime.Result{}, err
	}
	return controllerruntime.Result{}, nil
}

func (c *GlobalIPController) cleanupGlobalIPFromCluster(ctx context.Context, work *workv1alpha1.Work) error {
	workList := &workv1alpha1.WorkList{}
	err := c.Client.List(ctx, workList)
	if err != nil {
		klog.Errorf("Failed to list works serror: %v", err)
		return err
	}

	for _, item := range workList.Items {
		if util.GetLabelValue(item.Labels, GlobalIPForWorkLabel) != work.Name {
			continue
		}
		if err := c.Client.Delete(ctx, item.DeepCopy()); err != nil {
			return err
		}
	}

	return nil
}

func (c *GlobalIPController) allocateGlobalIP(ctx context.Context, work *workv1alpha1.Work, clusterName string) error {
	// 直接创建Service和ServiceExport，下发到成员集群
	manifest := work.Spec.Workload.Manifests[0]
	unstructuredObj := &unstructured.Unstructured{}
	if err := unstructuredObj.UnmarshalJSON(manifest.Raw); err != nil {
		klog.Errorf("Failed to unmarshal work manifest, error is: %v", err)
		return err
	}
	// 转化为deployment
	deployment := &appsv1.Deployment{}
	if err := helper.ConvertToTypedObject(unstructuredObj, deployment); err != nil {
		klog.Errorf("Failed to convert unstructured object to typed object, error is: %v", err)
		return err
	}

	// 生成全局Service，并添加名字后缀，作为全局IP的标识
	name := deployment.Name + "globalip"
	globalIPServiceWorkName := work.Name + "-globalips"
	globalIPService := &corev1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: corev1.SchemeGroupVersion.String(),
			Kind:       "Service",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: deployment.Namespace,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  deployment.Spec.Selector.MatchLabels,
		},
	}
	globalIPService.Labels = map[string]string{
		workv1alpha1.WorkNamespaceLabel: work.Namespace,
		workv1alpha1.WorkNameLabel:      globalIPServiceWorkName,
		util.ManagedByKarmadaLabel:      util.ManagedByKarmadaLabelValue,
	}
	// 添加selector label便于查找
	for k, v := range deployment.Spec.Selector.MatchLabels {
		globalIPService.Labels[k] = v
	}
	svcObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(globalIPService)
	if err != nil {
		return err
	}
	workMeta := metav1.ObjectMeta{
		Name:       globalIPServiceWorkName,
		Namespace:  work.Namespace,
		Finalizers: []string{util.ExecutionControllerFinalizer},
		Labels: map[string]string{
			util.ManagedByKarmadaLabel: util.ManagedByKarmadaLabelValue,
			GlobalIPForWorkLabel:       work.Name,
		},
	}
	unstructuredService := &unstructured.Unstructured{Object: svcObj}
	if err != nil {
		klog.Errorf("Failed to convert typed object to unstructured object, error is: %v", err)
		return err
	}
	if err := helper.CreateOrUpdateWork(c.Client, workMeta, unstructuredService); err != nil {
		klog.Errorf("Failed to allocate GlobalIPService %s/%s to member cluster %s:%v",
			work.GetNamespace(), work.GetName(), clusterName, err)
		return err
	}
	// 生成全局ServiceExport，并添加名字后缀，作为全局IP的标识
	globalIPServiceExportWorkName := work.Name + "-globalipse"
	globalIPServiceExport := &mcsv1alpha1.ServiceExport{
		TypeMeta: metav1.TypeMeta{
			APIVersion: mcsv1alpha1.SchemeGroupVersion.String(),
			Kind:       "ServiceExport",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: deployment.Namespace,
		},
	}
	globalIPServiceExport.Labels = map[string]string{
		workv1alpha1.WorkNamespaceLabel: work.Namespace,
		workv1alpha1.WorkNameLabel:      globalIPServiceExportWorkName,
		util.ManagedByKarmadaLabel:      util.ManagedByKarmadaLabelValue,
	}
	// 添加selector label便于查找
	for k, v := range deployment.Spec.Selector.MatchLabels {
		globalIPServiceExport.Labels[k] = v
	}
	svcExportObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(globalIPServiceExport)
	if err != nil {
		return err
	}
	workMeta = metav1.ObjectMeta{
		Name:       globalIPServiceExportWorkName,
		Namespace:  work.Namespace,
		Finalizers: []string{util.ExecutionControllerFinalizer},
		Labels: map[string]string{
			util.ManagedByKarmadaLabel: util.ManagedByKarmadaLabelValue,
			GlobalIPForWorkLabel:       work.Name,
		},
	}
	unstructuredServiceExport := &unstructured.Unstructured{Object: svcExportObj}
	if err != nil {
		klog.Errorf("Failed to convert typed object to unstructured object, error is: %v", err)
		return err
	}
	if err := helper.CreateOrUpdateWork(c.Client, workMeta, unstructuredServiceExport); err != nil {
		klog.Errorf("Failed to allocate GlobalIPServiceExport %s/%s to member cluster %s:%v",
			work.GetNamespace(), work.GetName(), clusterName, err)
		return err
	}

	return nil
}

// SetupWithManager creates a controller and register to controller manager.
func (c *GlobalIPController) SetupWithManager(mgr controllerruntime.Manager) error {
	// 将监听work和cluster
	return controllerruntime.NewControllerManagedBy(mgr).For(&workv1alpha1.Work{}).
		Complete(c)
}
