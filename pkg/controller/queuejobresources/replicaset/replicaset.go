/*
Copyright 2017 The Kubernetes Authors.

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

package replicaSet

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	qjobv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/apis/v1"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/controller/queuejobresources"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/schedulercache"
	"k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	//corev1informer "k8s.io/client-go/informers/core/v1"
	extv1beta1informer "k8s.io/client-go/informers/extensions/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	//extensionsclient "k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset/typed/extensions/internalversion"
	//corelisters "k8s.io/client-go/listers/core/v1"
	extv1beta1listers "k8s.io/client-go/listers/extensions/v1beta1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	//"k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/apis/extensions"
	"k8s.io/kubernetes/pkg/client/clientset_generated/internalclientset"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/kubectl"
)

var controllerKind = qjobv1.SchemeGroupVersion.WithKind("QueueJob")

const (
	// Timeout used in benchmarks, to eg: scale an rc
	DefaultTimeout = 1 * time.Minute
)

type QueueJobResReplicaSet struct {
	kubeClient        clientset.Interface
	internalClientset *internalclientset.Clientset
	//extensionsClientset *extensionsclient.ExtensionsClient

	// A TTLCache of replicaSet creates/deletes each rc expects to see
	expectations controller.ControllerExpectationsInterface

	// A store of replicaSets, populated by the replicaSetController
	replicaSetStore    extv1beta1listers.ReplicaSetLister
	replicaSetInformer extv1beta1informer.ReplicaSetInformer
	rtScheme           *runtime.Scheme
	jsonSerializer     *json.Serializer

	// Reference manager to manage membership of queuejob resource and its members
	refManager queuejobresources.RefManager
	recorder   record.EventRecorder
}

// Register registers a queue job resource type
func Register(regs *queuejobresources.RegisteredResources) {
	regs.Register(qjobv1.ResourceTypeReplicaSet, func(config *rest.Config) queuejobresources.Interface {
		return NewQueueJobResReplicaSet(config)
	})
}

func NewQueueJobResReplicaSet(config *rest.Config) queuejobresources.Interface {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)

	// create k8s clientset
	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		glog.Errorf("fail to create clientset")
		return nil
	}

	internalClientset, err := internalclientset.NewForConfig(config)
	if err != nil {
		glog.Errorf("fail to create internalClientset")
		return nil
	}

	//	extensionsClientset, err := extensionsclient.NewForConfig(config)
	//	if err != nil {
	//		glog.Errorf("fail to create extensionsClientset")
	//		return nil
	//	}

	qjrReplicaSet := &QueueJobResReplicaSet{
		kubeClient:        kubeClient,
		internalClientset: internalClientset,
		//		extensionsClientset: extensionsClientset,
		expectations: controller.NewControllerExpectations(),
		recorder:     eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "queuejob-controller"}),
	}

	// create informer for replicaSet information
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	qjrReplicaSet.replicaSetInformer = informerFactory.Extensions().V1beta1().ReplicaSets()
	qjrReplicaSet.replicaSetInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *v1beta1.ReplicaSet:
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    qjrReplicaSet.addReplicaSet,
				UpdateFunc: qjrReplicaSet.updateReplicaSet,
				DeleteFunc: qjrReplicaSet.deleteReplicaSet,
			},
		})

	qjrReplicaSet.rtScheme = runtime.NewScheme()
	v1.AddToScheme(qjrReplicaSet.rtScheme)
	v1beta1.AddToScheme(qjrReplicaSet.rtScheme)

	qjrReplicaSet.jsonSerializer = json.NewYAMLSerializer(json.DefaultMetaFactory, qjrReplicaSet.rtScheme, qjrReplicaSet.rtScheme)

	qjrReplicaSet.refManager = queuejobresources.NewLabelRefManager()

	return qjrReplicaSet
}

// Run the main goroutine responsible for watching and replicaSets.
func (qjrReplicaSet *QueueJobResReplicaSet) Run(stopCh <-chan struct{}) {

	qjrReplicaSet.replicaSetInformer.Informer().Run(stopCh)
}

func (qjrReplicaSet *QueueJobResReplicaSet) addReplicaSet(obj interface{}) {

	return
}

func (qjrReplicaSet *QueueJobResReplicaSet) updateReplicaSet(old, cur interface{}) {

	return
}

func (qjrReplicaSet *QueueJobResReplicaSet) deleteReplicaSet(obj interface{}) {

	return
}

// Parse queue job api object to get ReplicaSet template
func (qjrReplicaSet *QueueJobResReplicaSet) getReplicaSetTemplate(qjobRes *qjobv1.QueueJobResource) (*v1beta1.ReplicaSet, error) {

	replicaSetGVK := schema.GroupVersion{Group: v1beta1.GroupName, Version: "v1beta1"}.WithKind("ReplicaSet")

	obj, _, err := qjrReplicaSet.jsonSerializer.Decode(qjobRes.Template.Raw, &replicaSetGVK, nil)
	if err != nil {
		return nil, err
	}

	replicaSet, ok := obj.(*v1beta1.ReplicaSet)
	if !ok {
		return nil, fmt.Errorf("Queuejob resource not define a ReplicaSet")
	}

	return replicaSet, nil

}

func (qjrReplicaSet *QueueJobResReplicaSet) createReplicaSetWithControllerRef(namespace string, replicaSet *v1beta1.ReplicaSet, controllerRef *metav1.OwnerReference) error {

	fmt.Printf("==========create replicaSet: %s,  %+v \n", namespace, replicaSet)
	if controllerRef != nil {
		replicaSet.OwnerReferences = append(replicaSet.OwnerReferences, *controllerRef)
	}

	if _, err := qjrReplicaSet.kubeClient.ExtensionsV1beta1().ReplicaSets(namespace).Create(replicaSet); err != nil {
		return err
	}

	return nil
}

// ScaleRC scales the given rc to the given replicas.
func (qjrReplicaSet *QueueJobResReplicaSet) scaleRS(name, ns string, replicas int32) error {
	scaler, err := kubectl.ScalerFor(extensions.Kind("ReplicaSet"), qjrReplicaSet.internalClientset)
	if err != nil {
		return err
	}
	retry := &kubectl.RetryParams{Interval: 50 * time.Millisecond, Timeout: DefaultTimeout}
	waitForReplicas := &kubectl.RetryParams{Interval: 50 * time.Millisecond, Timeout: DefaultTimeout}
	err = scaler.Scale(ns, name, uint(replicas), nil, retry, waitForReplicas)
	if err != nil {
		return err
	}
	return nil
}

func (qjrReplicaSet *QueueJobResReplicaSet) delReplicaSet(namespace string, name string) error {

	fmt.Printf("==========delete replicaSet: %s,  %s \n", namespace, name)

	//if err := qjrReplicaSet.scaleRS(name, namespace, 0); err != nil {
	//	return err
	//}

	rsReaper, err := kubectl.ReaperFor(extensions.Kind("ReplicaSet"), qjrReplicaSet.internalClientset)
	if err != nil {
		return err
	}

	falseVar := false
	deleteOptions := &metav1.DeleteOptions{OrphanDependents: &falseVar}
	if err := rsReaper.Stop(namespace, name, DefaultTimeout, deleteOptions); err != nil {
		return err
	}
	//rsc := reaper.client.ReplicaSets(namespace)
	//return rsc.Delete(name, deleteOptions)

	//if err := qjrReplicaSet.kubeClient.ExtensionsV1beta1().ReplicaSets(namespace).Delete(name, nil); err != nil {
	//		return err
	//}

	return nil
}

// scale up queuejob resource to the desired number
func (qjrReplicaSet *QueueJobResReplicaSet) scaleUpQueueJobResource(diff int32, activeReplicaSets []*v1beta1.ReplicaSet, queuejob *qjobv1.QueueJob, qjobRes *qjobv1.QueueJobResource) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing queue job resource %q (%v)", qjobRes.Template, time.Now().Sub(startTime))
	}()

	template, err := qjrReplicaSet.getReplicaSetTemplate(qjobRes)
	if err != nil {
		return err
	}

	//TODO: need set reference after ReplicaSet has been really added
	tmpReplicaSet := v1beta1.ReplicaSet{}
	err = qjrReplicaSet.refManager.AddReference(qjobRes, &tmpReplicaSet)
	if err != nil {
		return err
	}

	if template.Labels == nil {

		template.Labels = map[string]string{}
	}
	for k, v := range tmpReplicaSet.Labels {
		template.Labels[k] = v
	}
	r := int32(qjobRes.AllocatedReplicas)
	template.Spec.Replicas = &r

	wait := sync.WaitGroup{}
	wait.Add(int(diff))
	for i := int32(0); i < diff; i++ {
		go func() {
			defer wait.Done()
			err := qjrReplicaSet.createReplicaSetWithControllerRef(queuejob.Namespace, template, metav1.NewControllerRef(queuejob, controllerKind))
			if err != nil && errors.IsTimeout(err) {
				return
			}
			if err != nil {
				defer utilruntime.HandleError(err)
			}
		}()
	}
	wait.Wait()

	return nil

}

// scale down queuejob resource to the desired number
func (qjrReplicaSet *QueueJobResReplicaSet) scaleDownQueueJobResource(diff int32, activeReplicaSets []*v1beta1.ReplicaSet, queuejob *qjobv1.QueueJob, qjobRes *qjobv1.QueueJobResource) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing queue job resource %q (%v)", qjobRes.Template, time.Now().Sub(startTime))
	}()

	wait := sync.WaitGroup{}
	wait.Add(int(diff))
	for i := int32(0); i < diff; i++ {
		go func(ix int32) {
			defer wait.Done()
			if err := qjrReplicaSet.delReplicaSet(queuejob.Namespace, activeReplicaSets[ix].Name); err != nil {
				defer utilruntime.HandleError(err)
			}
		}(i)
	}
	wait.Wait()
	return nil

}

func (qjrReplicaSet *QueueJobResReplicaSet) Sync(queuejob *qjobv1.QueueJob, qjobRes *qjobv1.QueueJobResource) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing queue job resource %q (%v)", qjobRes.Template, time.Now().Sub(startTime))
	}()

	activeReplicaSets, err := qjrReplicaSet.getReplicaSetsForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	numActiveReplicaSets := int32(len(activeReplicaSets))

	//if qjobRes.AllocatedReplicas < numActiveReplicaSets {
	if (qjobRes.AllocatedReplicas > 0) && (numActiveReplicaSets < 1) {

		qjrReplicaSet.scaleUpQueueJobResource(
			int32(1),
			activeReplicaSets, queuejob, qjobRes)
	} else if (qjobRes.AllocatedReplicas == 0) && (numActiveReplicaSets > 0) {

		qjrReplicaSet.scaleDownQueueJobResource(
			int32(0),
			activeReplicaSets, queuejob, qjobRes)
	}

	activeReplicaSets, err = qjrReplicaSet.getReplicaSetsForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	active := int32(len(activeReplicaSets))

	for i := int32(0); i < active; i++ {

		if err := qjrReplicaSet.scaleRS(activeReplicaSets[i].Name, queuejob.Namespace, qjobRes.AllocatedReplicas); err != nil {
			return err
		}

	}

	return nil
}

func (qjrReplicaSet *QueueJobResReplicaSet) getReplicaSetsForQueueJob(j *qjobv1.QueueJob) ([]*v1beta1.ReplicaSet, error) {
	replicaSetlist, err := qjrReplicaSet.kubeClient.ExtensionsV1beta1().ReplicaSets(j.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	fmt.Printf("==========get replicaSets: %+v \n", replicaSetlist)
	replicaSets := []*v1beta1.ReplicaSet{}
	for i, replicaSet := range replicaSetlist.Items {
		meta_replicaSet, err := meta.Accessor(&replicaSet)
		if err != nil {
			return nil, err
		}

		controllerRef := metav1.GetControllerOf(meta_replicaSet)
		fmt.Printf("==========get replicaSets cotlRef: %+v \n", controllerRef)
		if controllerRef != nil {
			fmt.Printf("==========get replicaSets cotlRef UID: %+v \n", controllerRef.UID)
			if controllerRef.UID == j.UID {
				replicaSets = append(replicaSets, &replicaSetlist.Items[i])
			}
		}
	}
	return replicaSets, nil

}

func (qjrReplicaSet *QueueJobResReplicaSet) getReplicaSetsForQueueJobRes(qjobRes *qjobv1.QueueJobResource, j *qjobv1.QueueJob) ([]*v1beta1.ReplicaSet, error) {

	replicaSets, err := qjrReplicaSet.getReplicaSetsForQueueJob(j)
	if err != nil {
		return nil, err
	}

	myReplicaSets := []*v1beta1.ReplicaSet{}
	for i, replicaSet := range replicaSets {
		if qjrReplicaSet.refManager.BelongTo(qjobRes, replicaSet) {
			myReplicaSets = append(myReplicaSets, replicaSets[i])
		}
	}

	return myReplicaSets, nil

}

func (qjrReplicaSet *QueueJobResReplicaSet) deleteQueueJobResReplicaSets(qjobRes *qjobv1.QueueJobResource, queuejob *qjobv1.QueueJob) error {

	job := *queuejob

	activeReplicaSets, err := qjrReplicaSet.getReplicaSetsForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	fmt.Printf("==========get replicaSets delete: %+v \n", activeReplicaSets)

	active := int32(len(activeReplicaSets))

	wait := sync.WaitGroup{}
	wait.Add(int(active))
	for i := int32(0); i < active; i++ {
		go func(ix int32) {
			defer wait.Done()
			if err := qjrReplicaSet.delReplicaSet(queuejob.Namespace, activeReplicaSets[ix].Name); err != nil {
				defer utilruntime.HandleError(err)
				glog.V(2).Infof("Failed to delete %v, queue job %q/%q deadline exceeded", activeReplicaSets[ix].Name, job.Namespace, job.Name)
			}
		}(i)
	}
	wait.Wait()

	return nil
}

func (qjrReplicaSet *QueueJobResReplicaSet) Cleanup(queuejob *qjobv1.QueueJob, qjobRes *qjobv1.QueueJobResource) error {

	return qjrReplicaSet.deleteQueueJobResReplicaSets(qjobRes, queuejob)
}

func (qjrReplicaSet *QueueJobResReplicaSet) GetResourceAllocated() *qjobv1.ResourceList {
	return nil
}

func (qjrReplicaSet *QueueJobResReplicaSet) GetResourceRequest() *schedulercache.Resource {
	return nil
}

func (qjrReplicaSet *QueueJobResReplicaSet) SetResourceAllocated(qjobv1.ResourceList) error {
	return nil
}
