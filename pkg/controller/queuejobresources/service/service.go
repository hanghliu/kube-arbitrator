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

package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	qjobv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/apis/v1"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/controller/queuejobresources"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/schedulercache"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	corev1informer "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"
)

var controllerKind = qjobv1.SchemeGroupVersion.WithKind("QueueJob")

type QueueJobResService struct {
	kubeClient clientset.Interface

	// A TTLCache of service creates/deletes each rc expects to see
	expectations controller.ControllerExpectationsInterface

	// A store of services, populated by the serviceController
	serviceStore    corelisters.ServiceLister
	serviceInformer corev1informer.ServiceInformer
	rtScheme        *runtime.Scheme
	jsonSerializer  *json.Serializer

	// Reference manager to manage membership of queuejob resource and its members
	refManager queuejobresources.RefManager
	recorder   record.EventRecorder
}

// Register registers a queue job resource type
func Register(regs *queuejobresources.RegisteredResources) {
	regs.Register(qjobv1.ResourceTypeService, func(config *rest.Config) queuejobresources.Interface {
		return NewQueueJobResService(config)
	})
}

func NewQueueJobResService(config *rest.Config) queuejobresources.Interface {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)

	// create k8s clientset
	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		glog.Errorf("fail to create clientset")
		return nil
	}

	qjrService := &QueueJobResService{
		kubeClient:   kubeClient,
		expectations: controller.NewControllerExpectations(),
		recorder:     eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "queuejob-controller"}),
	}

	// create informer for service information
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	qjrService.serviceInformer = informerFactory.Core().V1().Services()
	qjrService.serviceInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *v1.Service:
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    qjrService.addService,
				UpdateFunc: qjrService.updateService,
				DeleteFunc: qjrService.deleteService,
			},
		})

	qjrService.rtScheme = runtime.NewScheme()
	v1.AddToScheme(qjrService.rtScheme)

	qjrService.jsonSerializer = json.NewYAMLSerializer(json.DefaultMetaFactory, qjrService.rtScheme, qjrService.rtScheme)

	qjrService.refManager = queuejobresources.NewLabelRefManager()

	return qjrService
}

// Run the main goroutine responsible for watching and services.
func (qjrService *QueueJobResService) Run(stopCh <-chan struct{}) {

	qjrService.serviceInformer.Informer().Run(stopCh)
}

func (qjrService *QueueJobResService) addService(obj interface{}) {

	return
}

func (qjrService *QueueJobResService) updateService(old, cur interface{}) {

	return
}

func (qjrService *QueueJobResService) deleteService(obj interface{}) {

	return
}

// Parse queue job api object to get Service template
func (qjrService *QueueJobResService) getServiceTemplate(qjobRes *qjobv1.QueueJobResource) (*v1.Service, error) {

	serviceGVK := schema.GroupVersion{Group: v1.GroupName, Version: "v1"}.WithKind("Service")

	obj, _, err := qjrService.jsonSerializer.Decode(qjobRes.Template.Raw, &serviceGVK, nil)
	if err != nil {
		return nil, err
	}

	service, ok := obj.(*v1.Service)
	if !ok {
		return nil, fmt.Errorf("Queuejob resource not define a Service")
	}

	return service, nil

}

func (qjrService *QueueJobResService) createServiceWithControllerRef(namespace string, service *v1.Service, controllerRef *metav1.OwnerReference) error {

	fmt.Printf("==========create service: %s,  %+v \n", namespace, service)
	if controllerRef != nil {
		service.OwnerReferences = append(service.OwnerReferences, *controllerRef)
	}

	if _, err := qjrService.kubeClient.Core().Services(namespace).Create(service); err != nil {
		return err
	}

	return nil
}

func (qjrService *QueueJobResService) delService(namespace string, name string) error {

	fmt.Printf("==========delete service: %s,  %s \n", namespace, name)
	if err := qjrService.kubeClient.Core().Services(namespace).Delete(name, nil); err != nil {
		return err
	}

	return nil
}

// scale up queuejob resource to the desired number
func (qjrService *QueueJobResService) scaleUpQueueJobResource(diff int32, activeServices []*v1.Service, queuejob *qjobv1.QueueJob, qjobRes *qjobv1.QueueJobResource) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing queue job resource %q (%v)", qjobRes.Template, time.Now().Sub(startTime))
	}()

	template, err := qjrService.getServiceTemplate(qjobRes)
	if err != nil {
		return err
	}

	//TODO: need set reference after Service has been really added
	tmpService := v1.Service{}
	err = qjrService.refManager.AddReference(qjobRes, &tmpService)
	if err != nil {
		return err
	}

	if template.Labels == nil {

		template.Labels = map[string]string{}
	}
	for k, v := range tmpService.Labels {
		template.Labels[k] = v
	}

	wait := sync.WaitGroup{}
	wait.Add(int(diff))
	for i := int32(0); i < diff; i++ {
		go func() {
			defer wait.Done()
			err := qjrService.createServiceWithControllerRef(queuejob.Namespace, template, metav1.NewControllerRef(queuejob, controllerKind))
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
func (qjrService *QueueJobResService) scaleDownQueueJobResource(diff int32, activeServices []*v1.Service, queuejob *qjobv1.QueueJob, qjobRes *qjobv1.QueueJobResource) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing queue job resource %q (%v)", qjobRes.Template, time.Now().Sub(startTime))
	}()

	wait := sync.WaitGroup{}
	wait.Add(int(diff))
	for i := int32(0); i < diff; i++ {
		go func(ix int32) {
			defer wait.Done()
			if err := qjrService.delService(queuejob.Namespace, activeServices[ix].Name); err != nil {
				defer utilruntime.HandleError(err)
			}
		}(i)
	}
	wait.Wait()
	return nil

}

func (qjrService *QueueJobResService) Sync(queuejob *qjobv1.QueueJob, qjobRes *qjobv1.QueueJobResource) error {

	startTime := time.Now()
	defer func() {
		glog.V(4).Infof("Finished syncing queue job resource %q (%v)", qjobRes.Template, time.Now().Sub(startTime))
	}()

	activeServices, err := qjrService.getServicesForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	numActiveServices := int32(len(activeServices))

	if qjobRes.AllocatedReplicas < numActiveServices {

		qjrService.scaleDownQueueJobResource(
			int32(numActiveServices-qjobRes.AllocatedReplicas),
			activeServices, queuejob, qjobRes)
	} else if qjobRes.AllocatedReplicas > numActiveServices {

		qjrService.scaleUpQueueJobResource(
			int32(qjobRes.AllocatedReplicas-numActiveServices),
			activeServices, queuejob, qjobRes)
	}

	return nil
}

func (qjrService *QueueJobResService) getServicesForQueueJob(j *qjobv1.QueueJob) ([]*v1.Service, error) {
	servicelist, err := qjrService.kubeClient.CoreV1().Services(j.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	services := []*v1.Service{}
	for i, service := range servicelist.Items {
		meta_service, err := meta.Accessor(&service)
		if err != nil {
			return nil, err
		}

		controllerRef := metav1.GetControllerOf(meta_service)
		if controllerRef != nil {
			if controllerRef.UID == j.UID {
				services = append(services, &servicelist.Items[i])
			}
		}
	}
	return services, nil

}

func (qjrService *QueueJobResService) getServicesForQueueJobRes(qjobRes *qjobv1.QueueJobResource, j *qjobv1.QueueJob) ([]*v1.Service, error) {

	services, err := qjrService.getServicesForQueueJob(j)
	if err != nil {
		return nil, err
	}

	myServices := []*v1.Service{}
	for i, service := range services {
		if qjrService.refManager.BelongTo(qjobRes, service) {
			myServices = append(myServices, services[i])
		}
	}

	return myServices, nil

}

func (qjrService *QueueJobResService) deleteQueueJobResServices(qjobRes *qjobv1.QueueJobResource, queuejob *qjobv1.QueueJob) error {

	job := *queuejob

	activeServices, err := qjrService.getServicesForQueueJobRes(qjobRes, queuejob)
	if err != nil {
		return err
	}

	fmt.Printf("==========get services: %+v \n", activeServices)

	active := int32(len(activeServices))

	wait := sync.WaitGroup{}
	wait.Add(int(active))
	for i := int32(0); i < active; i++ {
		go func(ix int32) {
			defer wait.Done()
			if err := qjrService.delService(queuejob.Namespace, activeServices[ix].Name); err != nil {
				defer utilruntime.HandleError(err)
				glog.V(2).Infof("Failed to delete %v, queue job %q/%q deadline exceeded", activeServices[ix].Name, job.Namespace, job.Name)
			}
		}(i)
	}
	wait.Wait()

	return nil
}

func (qjrService *QueueJobResService) Cleanup(queuejob *qjobv1.QueueJob, qjobRes *qjobv1.QueueJobResource) error {

	return qjrService.deleteQueueJobResServices(qjobRes, queuejob)
}

func (qjrService *QueueJobResService) GetResourceAllocated() *qjobv1.ResourceList {
	return nil
}

func (qjrService *QueueJobResService) GetResourceRequest() *schedulercache.Resource {
	return nil
}

func (qjrService *QueueJobResService) SetResourceAllocated(qjobv1.ResourceList) error {
	return nil
}
