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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.RejectionReason;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.DefaultPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingRequestWithPlacementAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * An ApplicationMasterServiceProcessor that performs Constrained placement of
 * Scheduling Requests.
 *
 * It does the following:
 * 1. All initialization.
 * 2. Intercepts placement constraints from the register call and adds it to
 *    the placement constraint manager.
 * 3. Dispatches Scheduling Requests to the Planner.
 */
public class PlacementConstraintProcessor extends AbstractPlacementProcessor {

  /**
   * Wrapper over the SchedulingResponse that wires in the placement attempt
   * and last attempted Node.
   */
  static final class Response extends SchedulingResponse {

    // 放置次数
    private final int placementAttempt;

    // 尝试的节点
    private final SchedulerNode attemptedNode;

    private Response(boolean isSuccess, ApplicationId applicationId,
        SchedulingRequest schedulingRequest, int placementAttempt,
        SchedulerNode attemptedNode) {
      super(isSuccess, applicationId, schedulingRequest);
      this.placementAttempt = placementAttempt;
      this.attemptedNode = attemptedNode;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementConstraintProcessor.class);

  // 调度线程池
  private ExecutorService schedulingThreadPool;

  // 重试次数,默认 3
  private int retryAttempts;

  // 重试请求.
  private Map<ApplicationId, List<BatchedRequests>> requestsToRetry = new ConcurrentHashMap<>();

  // 拒绝请求.
  private Map<ApplicationId, List<SchedulingRequest>> requestsToReject =   new ConcurrentHashMap<>();

  // 类型 串行? or 热门标签 ??
  private BatchedRequests.IteratorType iteratorType;

  // Dispatcher
  private PlacementDispatcher placementDispatcher;


  @Override
  public void init(ApplicationMasterServiceContext amsContext,   ApplicationMasterServiceProcessor nextProcessor) {
    LOG.info("Initializing Constraint Placement Processor:");
    super.init(amsContext, nextProcessor);
    //考虑第一个类-即使提供了逗号分隔的列表。
    // （这是为了简单起见，因为getInstances通过正确处理事情做了很多好事）

    // Only the first class is considered - even if a comma separated list is provided.
    // (This is for simplicity, since getInstances does a lot of good things by handling things correctly)

    // 根据配置参数,获取 ConstraintPlacementAlgorithm
    // yarn.resourcemanager.placement-constraints.algorithm.class
    List<ConstraintPlacementAlgorithm> instances =
        ((RMContextImpl) amsContext).getYarnConfiguration().getInstances(
            YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_CLASS,
            ConstraintPlacementAlgorithm.class);

    ConstraintPlacementAlgorithm algorithm = null;
    if (instances != null && !instances.isEmpty()) {
      algorithm = instances.get(0);
    } else {
      algorithm = new DefaultPlacementAlgorithm();
    }
    LOG.info("Placement Algorithm [{}]", algorithm.getClass().getName());

    // 获取 yarn.resourcemanager.placement-constraints.algorithm.iterator
    String iteratorName = ((RMContextImpl) amsContext).getYarnConfiguration()
        .get(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_ITERATOR,
            BatchedRequests.IteratorType.SERIAL.name());
    LOG.info("Placement Algorithm Iterator[{}]", iteratorName);
    try {
      // 获取类型
      iteratorType = BatchedRequests.IteratorType.valueOf(iteratorName);
    } catch (IllegalArgumentException e) {
      throw new YarnRuntimeException(
          "Could not instantiate Placement Algorithm Iterator: ", e);
    }

    // 算法池 大小 : 1
    // yarn.resourcemanager.placement-constraints.algorithm.pool-size : 1
    int algoPSize = ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_POOL_SIZE,
        YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_ALGORITHM_POOL_SIZE);

    // 构建Dispatcher
    this.placementDispatcher = new PlacementDispatcher();

    // 初始化Dispatcher
    this.placementDispatcher.init(
        ((RMContextImpl)amsContext), algorithm, algoPSize);
    LOG.info("Planning Algorithm pool size [{}]", algoPSize);


    // yarn.resourcemanager.placement-constraints.scheduler.pool-size : 1
    int schedPSize = ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_SCHEDULER_POOL_SIZE,
        YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_SCHEDULER_POOL_SIZE);

    // 创建线程池
    this.schedulingThreadPool = Executors.newFixedThreadPool(schedPSize);
    LOG.info("Scheduler pool size [{}]", schedPSize);

    // yarn.resourcemanager.设置重试次数
    // placement-constraints.retry-attempts: 3
    // Number of times a request that is not satisfied by the scheduler  can be retried.
    this.retryAttempts =
        ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
            YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS);
    LOG.info("Num retry attempts [{}]", this.retryAttempts);
  }

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    // Copy the scheduling request since we will clear it later after sending to dispatcher
    // 复制调度请求，因为我们稍后将其发送给调度程序后将其清除
    List<SchedulingRequest> schedulingRequests = new ArrayList<>(request.getSchedulingRequests());

    // 派送 放置请求.
    dispatchRequestsForPlacement(appAttemptId, schedulingRequests);

    // 分配重试请求.
    reDispatchRetryableRequests(appAttemptId);

    // 处理已经处理的请求.
    schedulePlacedRequests(appAttemptId);

    // Remove SchedulingRequest from AllocateRequest to avoid SchedulingRequest added to scheduler.
    // 移除调度请求.
    request.setSchedulingRequests(Collections.emptyList());

    // 执行 下一请求.
    nextAMSProcessor.allocate(appAttemptId, request, response);

    // 处理拒绝请求.
    handleRejectedRequests(appAttemptId, response);
  }

  private void dispatchRequestsForPlacement(ApplicationAttemptId appAttemptId,  List<SchedulingRequest> schedulingRequests) {

    if (schedulingRequests != null && !schedulingRequests.isEmpty()) {

      // 获取 调度请求.
      SchedulerApplicationAttempt appAttempt =  scheduler.getApplicationAttempt(appAttemptId);
      String queueName = null;

      if(appAttempt != null) {
        queueName = appAttempt.getQueueName();
      }

      // 获取队列最大资源
      Resource maxAllocation =  scheduler.getMaximumResourceCapability(queueName);
      // Normalize the Requests before dispatching

      // 处理请求信息? 干啥这是???
      schedulingRequests.forEach(req -> {
        // 获取请求的资源
        Resource reqResource = req.getResourceSizing().getResources();
        // 重新设置资源
        req.getResourceSizing().setResources(
            this.scheduler.getNormalizedResource(reqResource, maxAllocation));
      });

      // 处理分发请求.
      this.placementDispatcher.dispatch(new BatchedRequests(iteratorType,
          appAttemptId.getApplicationId(), schedulingRequests, 1));
    }
  }

  private void reDispatchRetryableRequests(ApplicationAttemptId appAttId) {
    // 获取重试
    List<BatchedRequests> reqsToRetry = this.requestsToRetry.get(appAttId.getApplicationId());
    if (reqsToRetry != null && !reqsToRetry.isEmpty()) {
      synchronized (reqsToRetry) {
        for (BatchedRequests bReq: reqsToRetry) {
          // 重新分发
          this.placementDispatcher.dispatch(bReq);
        }
        reqsToRetry.clear();
      }
    }
  }

  private void schedulePlacedRequests(ApplicationAttemptId appAttemptId) {
    // 获取applicationId
    ApplicationId applicationId = appAttemptId.getApplicationId();
    // 拉取请求
    List<PlacedSchedulingRequest> placedSchedulingRequests = this.placementDispatcher.pullPlacedRequests(applicationId);

    for (PlacedSchedulingRequest placedReq : placedSchedulingRequests) {
      // 获取请求信息
      SchedulingRequest sReq = placedReq.getSchedulingRequest();
      // 获取请求的节点
      for (SchedulerNode node : placedReq.getNodes()) {

        // 获取调度请求
        final SchedulingRequest sReqClone =
            SchedulingRequest.newInstance(sReq.getAllocationRequestId(),
                sReq.getPriority(), sReq.getExecutionType(),
                sReq.getAllocationTags(),
                ResourceSizing.newInstance(
                    sReq.getResourceSizing().getResources()),
                sReq.getPlacementConstraint());

        // 获取SchedulerApplicationAttempt
        SchedulerApplicationAttempt applicationAttempt =
            this.scheduler.getApplicationAttempt(appAttemptId);

        Runnable task = () -> {
          // 尝试分配
          boolean success =
              scheduler.attemptAllocationOnNode(applicationAttempt, sReqClone, node);
          if (!success) {
            LOG.warn("Unsuccessful allocation attempt [{}] for [{}]",
                placedReq.getPlacementAttempt(), sReqClone);
          }
          // 处理响应
          handleSchedulingResponse(
              new Response(success, applicationId, sReqClone,
              placedReq.getPlacementAttempt(), node));
        };
        // 提交任务...交由线程池执行...
        this.schedulingThreadPool.submit(task);
      }
    }
  }

  private void handleRejectedRequests(ApplicationAttemptId appAttemptId,
      AllocateResponse response) {

    // 获取拒绝的请求.
    List<SchedulingRequestWithPlacementAttempt> rejectedAlgoRequests =  this.placementDispatcher.pullRejectedRequests(  appAttemptId.getApplicationId());

    if (rejectedAlgoRequests != null && !rejectedAlgoRequests.isEmpty()) {
      LOG.warn("Following requests of [{}] were rejected by" +
              " the PlacementAlgorithmOutput Algorithm: {}",
          appAttemptId.getApplicationId(), rejectedAlgoRequests);
      rejectedAlgoRequests.stream()
          .filter(req -> req.getPlacementAttempt() < retryAttempts)
          .forEach(req -> handleSchedulingResponse(
              new Response(false, appAttemptId.getApplicationId(),
                  req.getSchedulingRequest(), req.getPlacementAttempt(),
                  null)));

      ApplicationMasterServiceUtils.addToRejectedSchedulingRequests(response,
          rejectedAlgoRequests.stream()
              .filter(req -> req.getPlacementAttempt() >= retryAttempts)
              .map(sr -> RejectedSchedulingRequest.newInstance(
                  RejectionReason.COULD_NOT_PLACE_ON_NODE,
                  sr.getSchedulingRequest()))
              .collect(Collectors.toList()));
    }

    // 获取决绝请求..
    List<SchedulingRequest> rejectedRequests =  this.requestsToReject.get(appAttemptId.getApplicationId());
    if (rejectedRequests != null && !rejectedRequests.isEmpty()) {
      synchronized (rejectedRequests) {
        LOG.warn("Following requests of [{}] exhausted all retry attempts " +
                "trying to schedule on placed node: {}",
            appAttemptId.getApplicationId(), rejectedRequests);
        ApplicationMasterServiceUtils.addToRejectedSchedulingRequests(response,
            rejectedRequests.stream()
                .map(sr -> RejectedSchedulingRequest.newInstance(
                    RejectionReason.COULD_NOT_SCHEDULE_ON_NODE, sr))
                .collect(Collectors.toList()));
        rejectedRequests.clear();
      }
    }
  }

  @Override
  public void finishApplicationMaster(ApplicationAttemptId appAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {

    // 清理各种遗留信息
    placementDispatcher.clearApplicationState(appAttemptId.getApplicationId());
    requestsToReject.remove(appAttemptId.getApplicationId());
    requestsToRetry.remove(appAttemptId.getApplicationId());

    // 调用父类的请求.
    super.finishApplicationMaster(appAttemptId, request, response);
  }

  private void handleSchedulingResponse(SchedulingResponse schedulerResponse) {
    int placementAttempt = ((Response)schedulerResponse).placementAttempt;
    // Retry this placement as it is not successful and we are still
    // under max retry. The req is batched with other unsuccessful
    // requests from the same app
    if (!schedulerResponse.isSuccess() && placementAttempt < retryAttempts) {
      List<BatchedRequests> reqsToRetry =
          requestsToRetry.computeIfAbsent(
              schedulerResponse.getApplicationId(),
              k -> new ArrayList<>());
      synchronized (reqsToRetry) {
        addToRetryList(schedulerResponse, placementAttempt, reqsToRetry);
      }
      LOG.warn("Going to retry request for application [{}] after [{}]" +
              " attempts: [{}]", schedulerResponse.getApplicationId(),
          placementAttempt, schedulerResponse.getSchedulingRequest());
    } else {
      if (!schedulerResponse.isSuccess()) {
        LOG.warn("Not retrying request for application [{}] after [{}]" +
                " attempts: [{}]", schedulerResponse.getApplicationId(),
            placementAttempt, schedulerResponse.getSchedulingRequest());
        List<SchedulingRequest> reqsToReject =
            requestsToReject.computeIfAbsent(
                schedulerResponse.getApplicationId(),
                k -> new ArrayList<>());
        synchronized (reqsToReject) {
          reqsToReject.add(schedulerResponse.getSchedulingRequest());
        }
      }
    }
  }

  private void addToRetryList(SchedulingResponse schedulerResponse,
      int placementAttempt, List<BatchedRequests> reqsToRetry) {
    boolean isAdded = false;
    for (BatchedRequests br : reqsToRetry) {
      if (br.getPlacementAttempt() == placementAttempt + 1) {
        br.addToBatch(schedulerResponse.getSchedulingRequest());
        br.addToBlacklist(
            schedulerResponse.getSchedulingRequest().getAllocationTags(),
            ((Response) schedulerResponse).attemptedNode);
        isAdded = true;
        break;
      }
    }
    if (!isAdded) {
      BatchedRequests br = new BatchedRequests(iteratorType,
          schedulerResponse.getApplicationId(),
          Lists.newArrayList(schedulerResponse.getSchedulingRequest()),
          placementAttempt + 1);
      reqsToRetry.add(br);
      br.addToBlacklist(
          schedulerResponse.getSchedulingRequest().getAllocationTags(),
          ((Response) schedulerResponse).attemptedNode);
    }
  }
}
