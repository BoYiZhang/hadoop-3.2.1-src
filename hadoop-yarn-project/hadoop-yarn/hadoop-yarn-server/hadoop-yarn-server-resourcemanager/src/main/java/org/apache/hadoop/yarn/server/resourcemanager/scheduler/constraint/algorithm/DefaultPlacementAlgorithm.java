/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintsUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutputCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingRequestWithPlacementAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.BatchedRequests;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.NodeCandidateSelector;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic placement algorithm.
 * Supports different Iterators at SchedulingRequest level including:
 * Serial, PopularTags
 */
public class DefaultPlacementAlgorithm implements ConstraintPlacementAlgorithm {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultPlacementAlgorithm.class);

  // Number of times to re-attempt placing a single scheduling request.
  private static final int RE_ATTEMPT_COUNT = 2;

  // 标签管理
  private LocalAllocationTagsManager tagsManager;

  // 放置
  private PlacementConstraintManager constraintManager;

  // node 选择器
  private NodeCandidateSelector nodeSelector;

  // 资源计算器
  private ResourceCalculator resourceCalculator;

  @Override
  public void init(RMContext rmContext) {
    // 初始化
    this.tagsManager = new LocalAllocationTagsManager(
        rmContext.getAllocationTagsManager());

    this.constraintManager = rmContext.getPlacementConstraintManager();

    this.resourceCalculator = rmContext.getScheduler().getResourceCalculator();

    this.nodeSelector =
        filter -> ((AbstractYarnScheduler) (rmContext).getScheduler())
            .getNodes(filter);
  }

  boolean attemptPlacementOnNode(ApplicationId appId,
      Resource availableResources, SchedulingRequest schedulingRequest,
      SchedulerNode schedulerNode, boolean ignoreResourceCheck)
      throws InvalidAllocationTagsQueryException {
    boolean fitsInNode = ignoreResourceCheck ||
        Resources.fitsIn(resourceCalculator,
            schedulingRequest.getResourceSizing().getResources(),
            availableResources);
    boolean constraintsSatisfied =
        PlacementConstraintsUtil.canSatisfyConstraints(appId,
        schedulingRequest, schedulerNode, constraintManager, tagsManager);
    return fitsInNode && constraintsSatisfied;
  }


  @Override
  public void place(ConstraintPlacementAlgorithmInput input, ConstraintPlacementAlgorithmOutputCollector collector) {
    BatchedRequests requests = (BatchedRequests) input;
    
    // 请求次数
    int placementAttempt = requests.getPlacementAttempt();
    
    
    // 构建输出信息
    ConstraintPlacementAlgorithmOutput resp =  new ConstraintPlacementAlgorithmOutput(requests.getApplicationId());
   
    // 获取SchedulerNode
    List<SchedulerNode> allNodes = nodeSelector.selectNodes(null);

    // 构建拒绝请求
    List<SchedulingRequest> rejectedRequests = new ArrayList<>();
    
    // 可用资源清单
    Map<NodeId, Resource> availResources = new HashMap<>();
    
    // 重试次数: 2
    int rePlacementCount = RE_ATTEMPT_COUNT;
    
    while (rePlacementCount > 0) {
      // 执行分配.
      doPlacement(requests, resp, allNodes, rejectedRequests, availResources);
      // Double check if placement constraints are really satisfied
      // 双重检查 , 放置位置是否满意.
      validatePlacement(requests.getApplicationId(), resp, rejectedRequests, availResources);

      // 如果拒绝 1, 2 次 可以直接重试..
      if (rejectedRequests.size() == 0 || rePlacementCount == 1) {
        break;
      }

      // 分配成功, 反馈请求.
      requests = new BatchedRequests(requests.getIteratorType(),
          requests.getApplicationId(), rejectedRequests,
          requests.getPlacementAttempt());
      rejectedRequests = new ArrayList<>();
      
      // 数量 -1 
      rePlacementCount--;
    }

    // 处理拒绝请求.
    resp.getRejectedRequests().addAll(
        rejectedRequests.stream().map(
            x -> new SchedulingRequestWithPlacementAttempt(
                placementAttempt, x)).collect(Collectors.toList()));
    
    collector.collect(resp);
    // 请求当前container tags
    // Clean current temp-container tags
    this.tagsManager.cleanTempContainers(requests.getApplicationId());
  }

  private void doPlacement(BatchedRequests requests,
      ConstraintPlacementAlgorithmOutput resp,
      List<SchedulerNode> allNodes,
      List<SchedulingRequest> rejectedRequests,
      Map<NodeId, Resource> availableResources) {

    // 获取调度请求.
    Iterator<SchedulingRequest> requestIterator = requests.iterator();

    // 获取所有的可用node
    Iterator<SchedulerNode> nIter = allNodes.iterator();
    SchedulerNode lastSatisfiedNode = null;

    // 循环分配
    while (requestIterator.hasNext()) {

      // 验证资源是否可用...
      if (allNodes.isEmpty()) {
        LOG.warn("No nodes available for placement at the moment !!");
        break;
      }
      // 获取分配请求.
      SchedulingRequest schedulingRequest = requestIterator.next();

      // 构建放置请求...
      PlacedSchedulingRequest placedReq =  new PlacedSchedulingRequest(schedulingRequest);
      placedReq.setPlacementAttempt(requests.getPlacementAttempt());
      resp.getPlacedRequests().add(placedReq);

      // 迭代检查其..
      CircularIterator<SchedulerNode> nodeIter = new CircularIterator(lastSatisfiedNode, nIter, allNodes);
      // 获取分配数量
      int numAllocs =  schedulingRequest.getResourceSizing().getNumAllocations();

      // 循环node
      while (nodeIter.hasNext() && numAllocs > 0) {
        SchedulerNode node = nodeIter.next();
        try {
          // 获取tag
          String tag = schedulingRequest.getAllocationTags() == null ? "" :
              schedulingRequest.getAllocationTags().iterator().next();

          // 获取资源
          Resource unallocatedResource =
              availableResources.computeIfAbsent(node.getNodeID(),
                  x -> Resource.newInstance(node.getUnallocatedResource()));


          if (!requests.getBlacklist(tag).contains(node.getNodeID()) &&
              attemptPlacementOnNode(
                  requests.getApplicationId(), unallocatedResource,
                  schedulingRequest, node, false)) {

            // 开始分配资源
            schedulingRequest.getResourceSizing()
                .setNumAllocations(--numAllocs);

            Resources.addTo(unallocatedResource,
                schedulingRequest.getResourceSizing().getResources());
            placedReq.getNodes().add(node);
            numAllocs =
                schedulingRequest.getResourceSizing().getNumAllocations();
            // Add temp-container tags for current placement cycle
            // 为当前的放置循环添加临时的 tag
            this.tagsManager.addTempTags(node.getNodeID(),
                requests.getApplicationId(),
                schedulingRequest.getAllocationTags());

            lastSatisfiedNode = node;
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("Got exception from TagManager !", e);
        }
      }
    }
    // 将numAllocations仍大于0的所有请求添加到拒绝列表中。
    // Add all requests whose numAllocations still > 0 to rejected list.
    requests.getSchedulingRequests().stream()
        .filter(sReq -> sReq.getResourceSizing().getNumAllocations() > 0)
        .forEach(rejReq -> rejectedRequests.add(cloneReq(rejReq)));
  }

  /**
   * During the placement phase, allocation tags are added to the node if the
   * constraint is satisfied, But depending on the order in which the
   * algorithm sees the request, it is possible that a constraint that happened
   * to be valid during placement of an earlier-seen request, might not be
   * valid after all subsequent requests have been placed.
   *
   * For eg:
   *   Assume nodes n1, n2, n3, n4 and n5
   *
   *   Consider the 2 constraints:
   *   1) "foo", anti-affinity with "foo"
   *   2) "bar", anti-affinity with "foo"
   *
   *   And 2 requests
   *   req1: NumAllocations = 4, allocTags = [foo]
   *   req2: NumAllocations = 1, allocTags = [bar]
   *
   *   If "req1" is seen first, the algorithm can place the 4 containers in
   *   n1, n2, n3 and n4. And when it gets to "req2", it will see that 4 nodes
   *   with the "foo" tag and will place on n5.
   *   But if "req2" is seem first, then "bar" will be placed on any node,
   *   since no node currently has "foo", and when it gets to "req1", since
   *   "foo" has not anti-affinity with "bar", the algorithm can end up placing
   *   "foo" on a node with "bar" violating the second constraint.
   *
   * To prevent the above, we need a validation step: after the placements for a
   * batch of requests are made, for each req, we remove its tags from the node
   * and try to see of constraints are still satisfied if the tag were to be
   * added back on the node.
   *
   *   When applied to the example above, after "req2" and "req1" are placed,
   *   we remove the "bar" tag from the node and try to add it back on the node.
   *   This time, constraint satisfaction will fail, since there is now a "foo"
   *   tag on the node and "bar" cannot be added. The algorithm will then
   *   retry placing "req2" on another node.
   *
   * @param applicationId
   * @param resp
   * @param rejectedRequests
   * @param availableResources
   */
  private void validatePlacement(ApplicationId applicationId,
      ConstraintPlacementAlgorithmOutput resp,
      List<SchedulingRequest> rejectedRequests,
      Map<NodeId, Resource> availableResources) {
    Iterator<PlacedSchedulingRequest> pReqIter =
        resp.getPlacedRequests().iterator();
    while (pReqIter.hasNext()) {
      PlacedSchedulingRequest pReq = pReqIter.next();
      Iterator<SchedulerNode> nodeIter = pReq.getNodes().iterator();
      // Assuming all reqs were satisfied.
      int num = 0;
      while (nodeIter.hasNext()) {
        SchedulerNode node = nodeIter.next();
        try {
          // Remove just the tags for this placement.
          this.tagsManager.removeTempTags(node.getNodeID(),
              applicationId, pReq.getSchedulingRequest().getAllocationTags());
          Resource availOnNode = availableResources.get(node.getNodeID());
          if (!attemptPlacementOnNode(applicationId, availOnNode,
              pReq.getSchedulingRequest(), node, true)) {
            nodeIter.remove();
            num++;
            Resources.subtractFrom(availOnNode,
                pReq.getSchedulingRequest().getResourceSizing().getResources());
          } else {
            // Add back the tags if everything is fine.
            this.tagsManager.addTempTags(node.getNodeID(),
                applicationId, pReq.getSchedulingRequest().getAllocationTags());
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("Got exception from TagManager !", e);
        }
      }
      if (num > 0) {
        SchedulingRequest sReq = cloneReq(pReq.getSchedulingRequest());
        sReq.getResourceSizing().setNumAllocations(num);
        rejectedRequests.add(sReq);
      }
      if (pReq.getNodes().isEmpty()) {
        pReqIter.remove();
      }
    }
  }

  private static SchedulingRequest cloneReq(SchedulingRequest sReq) {
    return SchedulingRequest.newInstance(
        sReq.getAllocationRequestId(), sReq.getPriority(),
        sReq.getExecutionType(), sReq.getAllocationTags(),
        ResourceSizing.newInstance(
            sReq.getResourceSizing().getNumAllocations(),
            sReq.getResourceSizing().getResources()),
        sReq.getPlacementConstraint());
  }

}
