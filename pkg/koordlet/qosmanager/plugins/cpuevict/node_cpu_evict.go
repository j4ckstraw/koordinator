package cpuevict

import (
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"

	apiext "github.com/koordinator-sh/koordinator/apis/extension"
	slov1alpha1 "github.com/koordinator-sh/koordinator/apis/slo/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/features"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/metriccache"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/qosmanager/helpers"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/resourceexecutor"
	koordletutil "github.com/koordinator-sh/koordinator/pkg/koordlet/util"
	"github.com/koordinator-sh/koordinator/pkg/util"
)

const (
	defaultNodeCPUUsageThresholdPercent = 65
	cpuReleaseBufferPercent             = 2
)

// evict pod by node utilization
// return true if evicted success
func (c *cpuEvictor) nodeCPUEvict() bool {
	klog.V(5).Infof("node cpu evict process start")

	nodeSLO := c.statesInformer.GetNodeSLO()
	// nodeCPUEvict also rely on featureGate BECPUEvict as cpuEvict
	if disabled, err := features.IsFeatureDisabled(nodeSLO, features.BECPUEvict); err != nil {
		klog.Warningf("nodeCPUEvict failed, cannot check the feature gate, err: %s", err)
		return false
	} else if disabled {
		klog.V(4).Infof("nodeCPUEvict skipped, nodeSLO disable the feature gate")
		return false
	}

	if time.Since(c.lastEvictTime) < c.evictCoolingInterval {
		klog.V(4).Infof("skip node CPU evict process, still in evict cool time")
		return false
	}

	thresholdConfig := nodeSLO.Spec.ResourceUsedThresholdWithBE
	windowSeconds := int64(c.metricCollectInterval.Seconds() * 2)
	if thresholdConfig.CPUEvictTimeWindowSeconds != nil && *thresholdConfig.CPUEvictTimeWindowSeconds > int64(c.metricCollectInterval.Seconds()) {
		windowSeconds = *thresholdConfig.CPUEvictTimeWindowSeconds
	}

	node := c.statesInformer.GetNode()
	if node == nil {
		klog.Warningf("cpuEvict failed, got nil node")
		return false
	}

	cpuCapacity := node.Status.Capacity.Cpu().Value()
	if cpuCapacity <= 0 {
		klog.Warningf("cpuEvict failed, node cpuCapacity not valid,value: %d", cpuCapacity)
		return false
	}

	result := c.evictByNodeUtilization(node, thresholdConfig, windowSeconds)
	klog.V(5).Info("node cpu evict process finished.")
	return result
}

func (c *cpuEvictor) evictByNodeUtilization(node *corev1.Node, thresholdConfig *slov1alpha1.ResourceThresholdStrategy, windowSeconds int64) bool {
	if !isNodeCPUEvictionConfigValid(thresholdConfig) {
		return false
	}

	milliRelease := c.calculateMilliReleaseByNodeUtilization(thresholdConfig, windowSeconds)
	if milliRelease > 0 {
		podInfos := c.getPodEvictInfoAndSortX(func(pod *corev1.Pod) bool {
			// we want to match Mid pod and BE pod
			priorityClass := apiext.GetPodPriorityClassWithDefault(pod)
			if priorityClass == apiext.PriorityBatch || priorityClass == apiext.PriorityMid {
				return true
			}

			return false
		})
		return c.killAndEvictBEPodsReleaseX(node, podInfos, milliRelease)
	}

	return false
}

func (c *cpuEvictor) calculateMilliReleaseByNodeUtilization(thresholdConfig *slov1alpha1.ResourceThresholdStrategy, windowSeconds int64) int64 {
	// get real node cpu capacity
	info, err := koordletutil.GetMachineInfo()
	if err != nil {
		klog.Error("get machine info error: %v", err)
		return 0
	}
	realCapacity := float64(info.NumCores) * 1000

	// get node cpu usage
	queryParam := helpers.GenerateQueryParamsAvg(windowSeconds)
	queryMeta, err := metriccache.NodeCPUUsageMetric.BuildQueryMeta(nil)
	if err != nil {
		klog.Warningf("get node metric queryMeta failed, error: %v", err)
		return 0
	}
	queryResult, err := helpers.CollectNodeMetrics(c.metricCache, *queryParam.Start, *queryParam.End, queryMeta)
	if err != nil {
		klog.Warningf("query node cpu metrics failed, error: %v", err)
		return 0
	}
	cpuUsage, err := queryResult.Value(queryParam.Aggregate)
	if err != nil {
		klog.Warningf("aggragate node cpu metric failed, error: %v", err)
		return 0
	}
	cpuUsage = cpuUsage * 1000

	// check threshold
	if !isNodeCPUUsageHighEnough(cpuUsage, realCapacity, thresholdConfig.CPUEvictThresholdPercent) {
		klog.V(5).Infof("cpuEvict by nodeUtilization skipped, current usage not enough, "+
			"Usage: %v, Capacity:%v", cpuUsage, realCapacity)
		return 0
	}

	// calculate eviction
	milliRelease := calculateResourceMilliToReleaseByNodeUtilization(cpuUsage, realCapacity, thresholdConfig)
	if milliRelease <= 0 {
		klog.Warningf("cpuEvict by nodeUtilization skipped, releaseByCurrent: %v", milliRelease)
		return 0
	}

	klog.V(4).Infof("cpuEvict by nodeUtilization start to evict, milliRelease: %v, current status (Usage:%v, Capacity:%v)",
		cpuUsage, realCapacity)
	return milliRelease
}

// copy from killAndEvictBEPodsRelease add return value
func (c *cpuEvictor) killAndEvictBEPodsReleaseX(node *corev1.Node, bePodInfos []*podEvictCPUInfo, cpuNeedMilliRelease int64) bool {
	message := fmt.Sprintf("killAndEvictBEPodsReleaseX for node(%s), need release milli CPU: %v",
		node.Name, cpuNeedMilliRelease)

	cpuMilliReleased := int64(0)
	hasKillPods := false
	for _, bePod := range bePodInfos {
		if cpuMilliReleased >= cpuNeedMilliRelease {
			break
		}

		ok := c.evictor.EvictPodIfNotEvicted(bePod.pod, node, resourceexecutor.EvictPodByNodeCPUUtilization, message)
		if ok {
			podKillMsg := fmt.Sprintf("%s, kill pod: %s", message, util.GetPodKey(bePod.pod))
			helpers.KillContainers(bePod.pod, podKillMsg)

			cpuMilliReleased = cpuMilliReleased + bePod.milliRequest
			klog.V(5).Infof("node cpu evict pick pod %s to evict", util.GetPodKey(bePod.pod))
			hasKillPods = true
		}
	}

	if hasKillPods {
		c.lastEvictTime = time.Now()
	}
	klog.V(5).Infof("killAndEvictBEPodsReleaseX finished! cpuNeedMilliRelease(%d) cpuMilliReleased(%d)",
		cpuNeedMilliRelease, cpuMilliReleased)
	return hasKillPods
}

// copy from getPodEvictInfoAndSort add filter function as parameter
func (c *cpuEvictor) getPodEvictInfoAndSortX(filter func(*corev1.Pod) bool) []*podEvictCPUInfo {
	var podInfos []*podEvictCPUInfo

	for _, podMeta := range c.statesInformer.GetAllPods() {
		pod := podMeta.Pod
		if filter(pod) {
			podInfo := &podEvictCPUInfo{pod: podMeta.Pod}
			queryMeta, err := metriccache.PodCPUUsageMetric.BuildQueryMeta(metriccache.MetricPropertiesFunc.Pod(string(pod.UID)))
			if err == nil {
				result, err := helpers.CollectPodMetricLast(c.metricCache, queryMeta, c.metricCollectInterval)
				if err == nil {
					podInfo.milliUsedCores = int64(result * 1000)
				}
			}

			milliRequestSum := int64(0)
			for _, container := range pod.Spec.Containers {
				containerCPUReq := util.GetContainerBatchMilliCPURequest(&container)
				if containerCPUReq > 0 {
					milliRequestSum = milliRequestSum + containerCPUReq
				}
			}

			podInfo.milliRequest = milliRequestSum
			if podInfo.milliRequest > 0 {
				podInfo.cpuUsage = float64(podInfo.milliUsedCores) / float64(podInfo.milliRequest)
			}

			podInfos = append(podInfos, podInfo)
		}
	}

	sort.Slice(podInfos, func(i, j int) bool {
		if podInfos[i].pod.Spec.Priority == nil || podInfos[j].pod.Spec.Priority == nil ||
			*podInfos[i].pod.Spec.Priority == *podInfos[j].pod.Spec.Priority {
			return podInfos[i].cpuUsage > podInfos[j].cpuUsage
		}
		// koord-batch is less than koord-mid, so evict BE pod first
		return *podInfos[i].pod.Spec.Priority < *podInfos[j].pod.Spec.Priority
	})

	return podInfos
}

func isNodeCPUUsageHighEnough(usage, capacity float64, thresholdPercent *int64) bool {
	if capacity <= 0 {
		klog.Warningf("node cpu evict skipped! capacity %v is no larger than zero!",
			capacity)
		return false
	}
	if capacity < 1000 {
		klog.Warningf("node cpu eviction: capacity %v is less than 1 core!", capacity)
		return true
	}
	rate := usage / capacity
	if thresholdPercent == nil {
		thresholdPercent = pointer.Int64(defaultNodeCPUUsageThresholdPercent)
	}
	if rate < float64(*thresholdPercent)/100 {
		klog.V(5).Infof("node cpu evict skipped! utiliization rate(%.2f) and thresholdPercent %d!", rate, *thresholdPercent)
		return false
	}

	klog.V(4).Infof("node cpu eviction: utilization rate(%.2f) >= thresholdPercent %d!", rate, *thresholdPercent)
	return true
}

func calculateResourceMilliToReleaseByNodeUtilization(usage, capacity float64, thresholdConfig *slov1alpha1.ResourceThresholdStrategy) int64 {
	// evict (usage - lower) quantity
	if capacity <= 0 {
		klog.V(5).Infof("node cpu evict skipped! node capacity is zero!")
		return 0
	}

	thresholdPercent := thresholdConfig.CPUEvictThresholdPercent
	lowerPercent := thresholdConfig.CPUEvictLowerPercent
	if thresholdPercent == nil {
		*thresholdPercent = defaultNodeCPUUsageThresholdPercent
	}
	if lowerPercent == nil {
		*lowerPercent = *thresholdPercent - cpuReleaseBufferPercent
	}

	lower := float64(*lowerPercent) / 100
	if lower <= 0 {
		klog.Warningf("node cpu evict skipped! lower percent (%f) is less than 0", lower)
		return 0
	}

	rate := usage / capacity
	if rate <= lower {
		klog.V(5).Infof("node cpu evict skipped! rate(%.2f) less than lower(%f)", rate, lower)
		return 0
	}

	rateGap := rate - lower
	milliRelease := capacity * rateGap
	return int64(milliRelease)
}

func isNodeCPUEvictionConfigValid(thresholdConfig *slov1alpha1.ResourceThresholdStrategy) bool {
	lowPercent := thresholdConfig.CPUEvictLowerPercent
	threshold := thresholdConfig.CPUEvictThresholdPercent
	if threshold == nil || *threshold <= 0 || *threshold >= 100 {
		klog.V(4).Infof("cpuEvict by node utilization skipped, CPUEvictThresholdPercent not config or less than 0")
		return false
	}

	if *lowPercent > *threshold || *lowPercent <= 0 {
		klog.V(4).Infof("cpuEvict by node utilization skipped, CPUEvictLowerPercent(%d) is not valid! must (0,%d]", *lowPercent, *threshold)
		return false
	}

	return true
}
