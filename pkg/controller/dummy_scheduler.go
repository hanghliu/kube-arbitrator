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

package controller

import (
	"fmt"
	//"strconv"
	"time"

	"github.com/golang/glog"
	qjobv1 "github.com/kubernetes-incubator/kube-arbitrator/pkg/apis/v1"
	qjobclient "github.com/kubernetes-incubator/kube-arbitrator/pkg/client"
	qInformerfactory "github.com/kubernetes-incubator/kube-arbitrator/pkg/client/informers"
	//qclient "github.com/kubernetes-incubator/kube-arbitrator/pkg/client/informers/queue/v1"
	qjobv1informer "github.com/kubernetes-incubator/kube-arbitrator/pkg/client/informers/queuejob/v1"
	qjobv1lister "github.com/kubernetes-incubator/kube-arbitrator/pkg/client/listers/queuejob/v1"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/controller/queuejobresources"
	//respod "github.com/kubernetes-incubator/kube-arbitrator/pkg/controller/queuejobresources/pod"
	//resreplicaset "github.com/kubernetes-incubator/kube-arbitrator/pkg/controller/queuejobresources/replicaset"
	//resservice "github.com/kubernetes-incubator/kube-arbitrator/pkg/controller/queuejobresources/service"
	"github.com/kubernetes-incubator/kube-arbitrator/pkg/schedulercache"
	"k8s.io/api/core/v1"
	//"k8s.io/apimachinery/pkg/api/errors"
	//"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"
)

//var controllerKind = qjobv1.SchemeGroupVersion.WithKind("QueueJob")

const (
	dmPriorityBar   = 100
	dmTotalReplicas = 10
)

type SubsetAmt struct {
	MinReplicas      int32
	DesiredReplicas  int32
	OldAllocReplicas int32
	NewAllocReplicas int32
	JobPtr           *qjobv1.QueueJob
}

type DummyScheduler struct {
	kubeClient              clientset.Interface
	qjobRegisteredResources queuejobresources.RegisteredResources
	qjobResControls         map[qjobv1.ResourceType]queuejobresources.Interface

	// Kubernetes restful client to operate queuejob
	qjobClient *rest.RESTClient

	// To allow injection of updateQueueJobStatus for testing.
	syncHandler func(queuejobKey string) error

	// A TTLCache of pod creates/deletes each rc expects to see
	expectations controller.ControllerExpectationsInterface

	// A store of queuejobs
	queueJobLister   qjobv1lister.QueueJobLister
	queueJobInformer qjobv1informer.QueueJobInformer

	// QueueJobs that need to be updated
	queue workqueue.RateLimitingInterface

	// Reference manager to manage membership of queuejob resource and its members
	refManager queuejobresources.RefManager

	recorder record.EventRecorder
}

func NewDummyScheduler(config *rest.Config, schCache schedulercache.Cache) *DummyScheduler {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)

	// create k8s clientset
	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		glog.Errorf("fail to create clientset")
		return nil
	}

	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.Core().RESTClient()).Events("")})

	qjobClient, _, err := qjobclient.NewQueueJobClient(config)

	dmsch := &DummyScheduler{
		kubeClient:   kubeClient,
		qjobClient:   qjobClient,
		expectations: controller.NewControllerExpectations(),
		queue:        workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "queuejob"),
		recorder:     eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "queuejob-controller"}),
	}

	// create informer for queuejob information
	qjobInformerFactory := qInformerfactory.NewSharedInformerFactory(qjobClient, 0)
	dmsch.queueJobInformer = qjobInformerFactory.QueueJob().QueueJobs()
	dmsch.queueJobInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *qjobv1.QueueJob:
					glog.V(4).Infof("filter queuejob name(%s) namespace(%s)\n", t.Name, t.Namespace)
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    dmsch.addQueueJob,
				DeleteFunc: dmsch.deleteQueueJob,
				UpdateFunc: dmsch.updateQueueJob,
			},
		})
	dmsch.queueJobLister = dmsch.queueJobInformer.Lister()

	dmsch.syncHandler = dmsch.syncQueueJob

	return dmsch
}

func (dmsch *DummyScheduler) runOnce() {
	dmsch.enqueueController("update")
}

// Run the main goroutine responsible for watching and syncing jobs.
func (dmsch *DummyScheduler) Run(workers int, stopCh <-chan struct{}) {

	go dmsch.queueJobInformer.Informer().Run(stopCh)

	defer utilruntime.HandleCrash()

	glog.Infof("Starting Dummy Scheduler")
	defer glog.Infof("Shutting down Dummy Scheduler")

	for i := 0; i < workers; i++ {
		go wait.Until(dmsch.worker, time.Second, stopCh)
	}

	go wait.Until(dmsch.runOnce, 2000*time.Millisecond, stopCh)

	<-stopCh
}

// obj could be an *QueueJob, or a DeletionFinalStateUnknown marker item.
func (dmsch *DummyScheduler) enqueueController(obj interface{}) {

	/*
		key, err := controller.KeyFunc(obj)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
			return
		}

		fmt.Printf("========= enqueue add %v \n", key)
	*/
	dmsch.queue.Add(obj)
}

// obj could be an *QueueJob, or a DeletionFinalStateUnknown marker item.
func (dmsch *DummyScheduler) addQueueJob(obj interface{}) {

	//dmsch.enqueueController(obj)
	return
}

func (dmsch *DummyScheduler) updateQueueJob(old, cur interface{}) {

	//dmsch.enqueueController(cur)
	return
}

func (dmsch *DummyScheduler) deleteQueueJob(obj interface{}) {

	//dmsch.enqueueController(obj)

}

// worker runs a worker thread that just dequeues items, processes them, and marks them done.
// It enforces that the syncHandler is never invoked concurrently with the same key.
func (dmsch *DummyScheduler) worker() {
	for dmsch.processNextWorkItem() {
	}
}

func (dmsch *DummyScheduler) processNextWorkItem() bool {

	key, quit := dmsch.queue.Get()
	if quit {
		return false
	}
	defer dmsch.queue.Done(key)

	err := dmsch.syncHandler(key.(string))
	if err == nil {
		dmsch.queue.Forget(key)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("Error syncing queueJob: %v", err))
	dmsch.queue.AddRateLimited(key)

	return true
}

func printAmts(amts []SubsetAmt) {

	for _, amt := range amts {

		fmt.Printf("    QueueJob:  %s \n", amt.JobPtr.Name)
		fmt.Printf("        MinReplicas:  %d \n", amt.MinReplicas)
		fmt.Printf("        DesiredReplicas:  %d \n", amt.DesiredReplicas)
		fmt.Printf("        OldAllocReplicas:  %d \n", amt.OldAllocReplicas)
		fmt.Printf("        NewAllocReplicas:  %d \n", amt.NewAllocReplicas)
	}
}

func reportStatus(low []SubsetAmt, high []SubsetAmt) {

	fmt.Printf("==================== Status ====================\n")
	fmt.Printf("High Priorty QueueJobs -->\n")
	printAmts(high)
	fmt.Printf("Low Priorty QueueJobs -->\n")
	printAmts(low)
	fmt.Printf("=========================== ====================\n")

}

func getSubsetAmts(jobs []*qjobv1.QueueJob) []SubsetAmt {

	amts := []SubsetAmt{}

	for _, job := range jobs {

		min := int32(0)
		desired := int32(0)
		for _, ar := range job.Spec.AggrResources.Items {
			desired += ar.DesiredReplicas
			min += ar.MinReplicas
		}

		amt := SubsetAmt{MinReplicas: min,
			DesiredReplicas:  desired,
			OldAllocReplicas: job.Status.AllocatedReplicas,
			NewAllocReplicas: 0,
			JobPtr:           job,
		}
		amts = append(amts, amt)
	}

	return amts

}

func (dmsch *DummyScheduler) updateJobAlloc(amt SubsetAmt) error {

	job := amt.JobPtr

	min := int32(0)
	desired := int32(0)
	for i, ar := range job.Spec.AggrResources.Items {
		desired += ar.DesiredReplicas
		job.Spec.AggrResources.Items[i].AllocatedReplicas = ar.DesiredReplicas
		min += ar.MinReplicas
	}

	last := len(job.Spec.AggrResources.Items) - 1
	if last >= 0 {
		lastAr := job.Spec.AggrResources.Items[last]
		fixedAlloc := desired - lastAr.DesiredReplicas
		newAlloc := amt.NewAllocReplicas - fixedAlloc
		job.Spec.AggrResources.Items[last].AllocatedReplicas = newAlloc

	}
	_, err := qjobclient.QueueJobUpdate(dmsch.qjobClient, job)
	if err != nil {
		return err
	}

	job.Status.AllocatedReplicas = amt.NewAllocReplicas
	_, err = qjobclient.QueueJobUpdateStatus(dmsch.qjobClient, job)
	if err != nil {
		return err
	}

	return nil

}

func getTotalMin(amts []SubsetAmt) int32 {

	sum := int32(0)
	for _, amt := range amts {
		sum += amt.MinReplicas
	}

	return sum
}

func getTotalDesired(amts []SubsetAmt) int32 {

	sum := int32(0)
	for _, amt := range amts {
		sum += amt.DesiredReplicas
	}

	return sum
}

func getTotalNewAlloc(amts []SubsetAmt) int32 {

	sum := int32(0)
	for _, amt := range amts {
		sum += amt.NewAllocReplicas
	}

	return sum
}

func avgNewAlloc(amts []SubsetAmt, left int32) {

	avg := left / int32(len(amts))
	for i, amt := range amts {
		amts[i].NewAllocReplicas = amt.MinReplicas + avg
	}

}

func fullDesiredNewAlloc(amts []SubsetAmt) {

	for i, amt := range amts {
		amts[i].NewAllocReplicas = amt.DesiredReplicas
	}

}

func getTotalExistMin(amts []SubsetAmt) int32 {

	//keep existing jobs
	exist_alloc := int32(0)
	for _, amt := range amts {
		if amt.OldAllocReplicas != 0 {
			exist_alloc += amt.MinReplicas
		}
	}
	return exist_alloc
}

func bestMinNoPreemptNewAlloc(amts []SubsetAmt, total int32) {

	//keep existing jobs
	exist_alloc := int32(0)
	for i, amt := range amts {
		if amt.OldAllocReplicas != 0 {
			exist_alloc += amt.MinReplicas
			amts[i].NewAllocReplicas = amt.MinReplicas
		}
	}

	left := total - exist_alloc
	for i, amt := range amts {
		if left >= amt.MinReplicas && amt.NewAllocReplicas == 0 {
			amts[i].NewAllocReplicas = amt.MinReplicas
			left -= amt.MinReplicas
		}
	}
}

func bestMinPreemptNewAlloc(amts []SubsetAmt, total int32) {

	left := total
	for i, amt := range amts {
		if left >= amt.MinReplicas && amt.OldAllocReplicas != 0 {
			amts[i].NewAllocReplicas = amt.MinReplicas
			left -= amt.MinReplicas
		} else {
			amts[i].NewAllocReplicas = 0
		}
	}
}

func (dmsch *DummyScheduler) syncQueueJob(key string) error {

	jobs, err := dmsch.queueJobLister.List(labels.Everything())
	if err != nil {
		return err
	}

	highPriJobs := []*qjobv1.QueueJob{}
	lowPriJobs := []*qjobv1.QueueJob{}

	for _, job := range jobs {
		if job.Spec.Priority > dmPriorityBar {
			highPriJobs = append(highPriJobs, job)
		} else {
			lowPriJobs = append(lowPriJobs, job)
		}

	}

	highPriAmts := getSubsetAmts(highPriJobs)
	lowPriAmts := getSubsetAmts(lowPriJobs)

	//for high priority
	totalHighDesired := getTotalDesired(highPriAmts)
	totalHighMin := getTotalMin(highPriAmts)
	if totalHighDesired < dmTotalReplicas {
		//enough resource
		fullDesiredNewAlloc(highPriAmts)

	} else if dmTotalReplicas > totalHighMin {

		//average to all highs
		avgNewAlloc(highPriAmts, dmTotalReplicas-totalHighMin)

	} else {

		bestMinNoPreemptNewAlloc(highPriAmts, dmTotalReplicas)

	}

	//for low priority
	totalLowDesired := getTotalDesired(lowPriAmts)
	totalLowMin := getTotalMin(lowPriAmts)
	totalLowExistMin := getTotalExistMin(lowPriAmts)
	leftForLow := dmTotalReplicas - totalHighDesired
	fmt.Printf("             ============ totalLowDesired  %d \n", totalLowDesired)
	fmt.Printf("             ============ totalLowMin  %d \n", totalLowMin)
	fmt.Printf("             ============ leftForLow  %d \n", leftForLow)

	if totalLowDesired < leftForLow {
		//enough resource
		fullDesiredNewAlloc(lowPriAmts)
	} else if leftForLow > totalLowMin {

		avgNewAlloc(lowPriAmts, leftForLow-totalLowMin)
	} else if leftForLow > totalLowExistMin {
		bestMinNoPreemptNewAlloc(lowPriAmts, leftForLow)
	} else {
		bestMinPreemptNewAlloc(lowPriAmts, leftForLow)
	}

	reportStatus(lowPriAmts, highPriAmts)

	//update queuejobs
	for _, amt := range highPriAmts {

		fmt.Printf("========= highPriAmts old %d New %d\n", amt.OldAllocReplicas, amt.NewAllocReplicas)
		if amt.NewAllocReplicas != amt.OldAllocReplicas {
			err := dmsch.updateJobAlloc(amt)
			if err != nil {
				return err
			}
		}
	}

	for _, amt := range lowPriAmts {

		fmt.Printf("========= lowPriAmts old %d New %d\n", amt.OldAllocReplicas, amt.NewAllocReplicas)
		if amt.NewAllocReplicas != amt.OldAllocReplicas {
			err := dmsch.updateJobAlloc(amt)
			if err != nil {
				return err
			}
		}
	}

	return nil

}
