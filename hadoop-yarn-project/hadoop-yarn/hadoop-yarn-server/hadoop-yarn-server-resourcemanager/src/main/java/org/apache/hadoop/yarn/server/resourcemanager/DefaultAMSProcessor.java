/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceUtils;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException
        .InvalidResourceType;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResoureRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event
    .RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event
    .RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.InvalidResourceType
        .GREATER_THEN_MAX_ALLOCATION;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.InvalidResourceType.LESS_THAN_ZERO;

/**
 * This is the default Application Master Service processor. It has be the
 * last processor in the @{@link AMSProcessingChain}.
 */
final class DefaultAMSProcessor implements ApplicationMasterServiceProcessor {

  private static final Log LOG = LogFactory.getLog(DefaultAMSProcessor.class);

  //  空的Container 集合
  private final static List<Container> EMPTY_CONTAINER_LIST =  new ArrayList<Container>();

  // 构建一个 空的  Allocation  ??
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(  EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  // RM 上下文信息
  private RMContext rmContext;

  //  resource profiles
  private ResourceProfilesManager resourceProfilesManager;

  // 是否启用timelineServiceV2
  private boolean timelineServiceV2Enabled;
  // 是否启用node label
  private boolean nodelabelsEnabled;

  @Override
  public void init(ApplicationMasterServiceContext amsContext, ApplicationMasterServiceProcessor nextProcessor) {

    this.rmContext = (RMContext)amsContext;

    this.resourceProfilesManager = rmContext.getResourceProfilesManager();

    this.timelineServiceV2Enabled = YarnConfiguration. timelineServiceV2Enabled(rmContext.getYarnConfiguration());
    this.nodelabelsEnabled = YarnConfiguration .areNodeLabelsEnabled(rmContext.getYarnConfiguration());
  }

  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {

    // 获取 application
    RMApp app = getRmContext().getRMApps().get(  applicationAttemptId.getApplicationId());

    //向RMAppAttemptImpl发送一个RMAppAttemptEventType.registered事件
    LOG.info("AM registration " + applicationAttemptId);

    // !!!! 在这里通过Dispatcher获取对应事件的Handler处理任务.!!!!!
    getRmContext().getDispatcher().getEventHandler()
        .handle(
            new RMAppAttemptRegistrationEvent(applicationAttemptId, request
                .getHost(), request.getRpcPort(), request.getTrackingUrl()));

    // 日志审计
    RMAuditLogger.logSuccess(app.getUser(),
        RMAuditLogger.AuditConstants.REGISTER_AM,
        "ApplicationMasterService", app.getApplicationId(),
        applicationAttemptId);

    // 返回资源的最大容量
    response.setMaximumResourceCapability(getScheduler()
        .getMaximumResourceCapability(app.getQueue()));

    // 设置权限
    response.setApplicationACLs(app.getRMAppAttempt(applicationAttemptId)
        .getSubmissionContext().getAMContainerSpec().getApplicationACLs());

    // 设置队列
    response.setQueue(app.getQueue());
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Setting client token master key");
      response.setClientToAMTokenMasterKey(java.nio.ByteBuffer.wrap(
          getRmContext().getClientToAMTokenSecretManager()
          .getMasterKey(applicationAttemptId).getEncoded()));
    }


    // For work-preserving AM restart, retrieve previous attempts' containers and corresponding NM tokens.
    if (app.getApplicationSubmissionContext()  .getKeepContainersAcrossApplicationAttempts()) {

      // 获取需要处理的Container
      List<Container> transferredContainers = getScheduler().getTransferredContainers(applicationAttemptId);

      if (!transferredContainers.isEmpty()) {
        response.setContainersFromPreviousAttempts(transferredContainers);
        // Clear the node set remembered by the secret manager. Necessary
        // for UAM restart because we use the same attemptId.
        rmContext.getNMTokenSecretManager()
            .clearNodeSetForAttempt(applicationAttemptId);

        List<NMToken> nmTokens = new ArrayList<NMToken>();
        for (Container container : transferredContainers) {
          try {
            // 更新token操作
            NMToken token = getRmContext().getNMTokenSecretManager()
                .createAndGetNMToken(app.getUser(), applicationAttemptId,
                    container);
            if (null != token) {
              nmTokens.add(token);
            }
          } catch (IllegalArgumentException e) {
            // if it's a DNS issue, throw UnknowHostException directly and
            // that
            // will be automatically retried by RMProxy in RPC layer.
            if (e.getCause() instanceof UnknownHostException) {
              throw (UnknownHostException) e.getCause();
            }
          }
        }

        response.setNMTokensFromPreviousAttempts(nmTokens);
        LOG.info("Application " + app.getApplicationId() + " retrieved "
            + transferredContainers.size() + " containers from previous"
            + " attempts and " + nmTokens.size() + " NM tokens.");
      }
    }

    // 设置资源类型
    response.setSchedulerResourceTypes(getScheduler()
        .getSchedulingResourceTypes());

    // 设置资源类型 返回响应信息
    response.setResourceTypes(ResourceUtils.getResourcesTypeInfo());


    // yarn.resourcemanager.resource-profiles.enabled : false
    if (getRmContext().getYarnConfiguration().getBoolean(
        YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED,
        YarnConfiguration.DEFAULT_RM_RESOURCE_PROFILES_ENABLED)) {

      // 如果启用了, 返回信息
      response.setResourceProfiles(
          resourceProfilesManager.getResourceProfiles());
    }
  }

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    // 处理 请求/ 更新心跳
    handleProgress(appAttemptId, request);

    // 获取资源请求
    List<ResourceRequest> ask = request.getAskList();

    // 获取需要释放的 ContainerId
    List<ContainerId> release = request.getReleaseList();

    // 获取请求的黑名单
    ResourceBlacklistRequest blacklistRequest =  request.getResourceBlacklistRequest();
    List<String> blacklistAdditions =
        (blacklistRequest != null) ?
            blacklistRequest.getBlacklistAdditions() : Collections.emptyList();

    // 获取移除黑名单的数据
    List<String> blacklistRemovals =
        (blacklistRequest != null) ?
            blacklistRequest.getBlacklistRemovals() : Collections.emptyList();


    // 获取 RM application
    RMApp app =
        getRmContext().getRMApps().get(appAttemptId.getApplicationId());

    // set label expression for Resource Requests if resourceName=ANY
    // 根据资源请求 设置 label表达式
    ApplicationSubmissionContext asc = app.getApplicationSubmissionContext();
    for (ResourceRequest req : ask) {
      if (null == req.getNodeLabelExpression()
          && ResourceRequest.ANY.equals(req.getResourceName())) {
        req.setNodeLabelExpression(asc.getNodeLabelExpression());
      }
    }

    // 获取队列中的最大资源能力
    Resource maximumCapacity = getScheduler().getMaximumResourceCapability(app.getQueue());

    // 健全性检查
    // sanity check
    try {
      RMServerUtils.normalizeAndValidateRequests(ask,
          maximumCapacity, app.getQueue(),
          getScheduler(), getRmContext(), nodelabelsEnabled);
    } catch (InvalidResourceRequestException e) {
      RMAppAttempt rmAppAttempt = app.getRMAppAttempt(appAttemptId);
      handleInvalidResourceException(e, rmAppAttempt);
    }

    try {
      // 验证请求
      RMServerUtils.validateBlacklistRequest(blacklistRequest);
    }  catch (InvalidResourceBlacklistRequestException e) {
      LOG.warn("Invalid blacklist request by application " + appAttemptId, e);
      throw e;
    }
    // 在工作保持AM重启的情况下，AM有可能从先前的尝试中释放容器。
    // In the case of work-preserving AM restart, it's possible for the
    // AM to release containers from the earlier attempt.
    if (!app.getApplicationSubmissionContext()
        .getKeepContainersAcrossApplicationAttempts()) {
      try {
        RMServerUtils.validateContainerReleaseRequest(release, appAttemptId);
      } catch (InvalidContainerReleaseException e) {
        LOG.warn("Invalid container release by application " + appAttemptId,
            e);
        throw e;
      }
    }

    // 将更新资源请求拆分为增加和减少。
    // 这里没有抛出异常。所有更新错误都将聚合并返回给AM。
    // Split Update Resource Requests into increase and decrease.
    // No Exceptions are thrown here. All update errors are aggregated
    // and returned to the AM.
    List<UpdateContainerError> updateErrors = new ArrayList<>();
    ContainerUpdates containerUpdateRequests =
        RMServerUtils.validateAndSplitUpdateResourceRequests(
            getRmContext(), request, maximumCapacity, updateErrors);

    // 向appAttempt 发送新的requests
    // Send new requests to appAttempt.
    Allocation allocation;
    RMAppAttemptState state =
        app.getRMAppAttempt(appAttemptId).getAppAttemptState();

    if (state.equals(RMAppAttemptState.FINAL_SAVING) ||
        state.equals(RMAppAttemptState.FINISHING) ||
        app.isAppFinalStateStored()) {
      // 如果任务正处于保存中或者保存完成 , 不做操作,修改allocation状态为EMPTY_ALLOCATION
      LOG.warn(appAttemptId + " is in " + state +
          " state, ignore container allocate request.");
      allocation = EMPTY_ALLOCATION;
    } else {
      try {

        // 重点!!!  获取 YarnScheduler 处理 allocate
        allocation = getScheduler().allocate(appAttemptId, ask,
            request.getSchedulingRequests(), release,
            blacklistAdditions, blacklistRemovals, containerUpdateRequests);
      } catch (SchedulerInvalidResoureRequestException e) {
        LOG.warn("Exceptions caught when scheduler handling requests");
        throw new YarnException(e);
      }
    }

    if (!blacklistAdditions.isEmpty() || !blacklistRemovals.isEmpty()) {
      LOG.info("blacklist are updated in Scheduler." +
          "blacklistAdditions: " + blacklistAdditions + ", " +
          "blacklistRemovals: " + blacklistRemovals);
    }
    // 获取RMAppAttempt
    RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);

    if (allocation.getNMTokens() != null &&
        !allocation.getNMTokens().isEmpty()) {
      response.setNMTokens(allocation.getNMTokens());
    }

    // 通知AM的container更新错误信息
    // Notify the AM of container update errors
    ApplicationMasterServiceUtils.addToUpdateContainerErrors(
        response, updateErrors);

    // 使用节点状态更改的增量更新响应
    // update the response with the deltas of node status changes
    handleNodeUpdates(app, response);

    // 添加新分配的container...
    ApplicationMasterServiceUtils.addToAllocatedContainers(
        response, allocation.getContainers());

    // 设置已完成Container状态
    response.setCompletedContainersStatuses(appAttempt
        .pullJustFinishedContainers());
    // 设置可用资源
    response.setAvailableResources(allocation.getResourceLimit());

    // 设置Container变更
    addToContainerUpdates(response, allocation,
        ((AbstractYarnScheduler)getScheduler())
            .getApplicationAttempt(appAttemptId).pullUpdateContainerErrors());

    // 设置集群节点数量
    response.setNumClusterNodes(getScheduler().getNumClusterNodes());

    // add collector address for this application
    if (timelineServiceV2Enabled) {
      CollectorInfo collectorInfo = app.getCollectorInfo();
      if (collectorInfo != null) {
        response.setCollectorInfo(collectorInfo);
      }
    }

    // 向allocateResponse消息添加抢占（如果有）
    // add preemption to the allocateResponse message (if any)
    response.setPreemptionMessage(generatePreemptionMessage(allocation));

    //  设置application 优先级
    // Set application priority
    response.setApplicationPriority(app
        .getApplicationPriority());

    response.setContainersFromPreviousAttempts(
        allocation.getPreviousAttemptContainers());
  }

  private void handleInvalidResourceException(InvalidResourceRequestException e,
          RMAppAttempt rmAppAttempt) throws InvalidResourceRequestException {
    if (e.getInvalidResourceType() == LESS_THAN_ZERO ||
            e.getInvalidResourceType() == GREATER_THEN_MAX_ALLOCATION) {
      rmAppAttempt.updateAMLaunchDiagnostics(e.getMessage());
    }
    LOG.warn("Invalid resource ask by application " +
            rmAppAttempt.getAppAttemptId(), e);
    throw e;
  }

  private void handleNodeUpdates(RMApp app, AllocateResponse allocateResponse) {
    Map<RMNode, NodeUpdateType> updatedNodes = new HashMap<>();
    if(app.pullRMNodeUpdates(updatedNodes) > 0) {
      List<NodeReport> updatedNodeReports = new ArrayList<>();
      for(Map.Entry<RMNode, NodeUpdateType> rmNodeEntry :
          updatedNodes.entrySet()) {
        RMNode rmNode = rmNodeEntry.getKey();
        SchedulerNodeReport schedulerNodeReport =
            getScheduler().getNodeReport(rmNode.getNodeID());
        Resource used = BuilderUtils.newResource(0, 0);
        int numContainers = 0;
        if (schedulerNodeReport != null) {
          used = schedulerNodeReport.getUsedResource();
          numContainers = schedulerNodeReport.getNumContainers();
        }
        NodeId nodeId = rmNode.getNodeID();
        NodeReport report =
            BuilderUtils.newNodeReport(nodeId, rmNode.getState(),
                rmNode.getHttpAddress(), rmNode.getRackName(), used,
                rmNode.getTotalCapability(), numContainers,
                rmNode.getHealthReport(), rmNode.getLastHealthReportTime(),
                rmNode.getNodeLabels(), rmNode.getDecommissioningTimeout(),
                rmNodeEntry.getValue());

        updatedNodeReports.add(report);
      }
      allocateResponse.setUpdatedNodes(updatedNodeReports);
    }
  }

  private void handleProgress(ApplicationAttemptId appAttemptId,
      AllocateRequest request) {
    //filter illegal progress values
    float filteredProgress = request.getProgress();
    if (Float.isNaN(filteredProgress) ||
        filteredProgress == Float.NEGATIVE_INFINITY ||
        filteredProgress < 0) {
      request.setProgress(0);
    } else if (filteredProgress > 1 ||
        filteredProgress == Float.POSITIVE_INFINITY) {
      request.setProgress(1);
    }

    // Send the status update to the appAttempt.
    getRmContext().getDispatcher().getEventHandler().handle(
        new RMAppAttemptStatusupdateEvent(appAttemptId, request
            .getProgress(), request.getTrackingUrl()));
  }

  @Override
  public void finishApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {

    // 获取application
    RMApp app =
        getRmContext().getRMApps().get(applicationAttemptId.getApplicationId());
    // For UnmanagedAMs, return true so they don't retry
    response.setIsUnregistered(
            app.getApplicationSubmissionContext().getUnmanagedAM());

    // 通过Dispatcher处理对应的时间
    getRmContext().getDispatcher().getEventHandler().handle(
        new RMAppAttemptUnregistrationEvent(applicationAttemptId, request
            .getTrackingUrl(), request.getFinalApplicationStatus(), request
            .getDiagnostics()));
  }

  private PreemptionMessage generatePreemptionMessage(Allocation allocation){
    PreemptionMessage pMsg = null;
    // assemble strict preemption request
    if (allocation.getStrictContainerPreemptions() != null) {
      pMsg =
          recordFactory.newRecordInstance(PreemptionMessage.class);
      StrictPreemptionContract pStrict =
          recordFactory.newRecordInstance(StrictPreemptionContract.class);
      Set<PreemptionContainer> pCont = new HashSet<>();
      for (ContainerId cId : allocation.getStrictContainerPreemptions()) {
        PreemptionContainer pc =
            recordFactory.newRecordInstance(PreemptionContainer.class);
        pc.setId(cId);
        pCont.add(pc);
      }
      pStrict.setContainers(pCont);
      pMsg.setStrictContract(pStrict);
    }

    // assemble negotiable preemption request
    if (allocation.getResourcePreemptions() != null &&
        allocation.getResourcePreemptions().size() > 0 &&
        allocation.getContainerPreemptions() != null &&
        allocation.getContainerPreemptions().size() > 0) {
      if (pMsg == null) {
        pMsg =
            recordFactory.newRecordInstance(PreemptionMessage.class);
      }
      PreemptionContract contract =
          recordFactory.newRecordInstance(PreemptionContract.class);
      Set<PreemptionContainer> pCont = new HashSet<>();
      for (ContainerId cId : allocation.getContainerPreemptions()) {
        PreemptionContainer pc =
            recordFactory.newRecordInstance(PreemptionContainer.class);
        pc.setId(cId);
        pCont.add(pc);
      }
      List<PreemptionResourceRequest> pRes = new ArrayList<>();
      for (ResourceRequest crr : allocation.getResourcePreemptions()) {
        PreemptionResourceRequest prr =
            recordFactory.newRecordInstance(PreemptionResourceRequest.class);
        prr.setResourceRequest(crr);
        pRes.add(prr);
      }
      contract.setContainers(pCont);
      contract.setResourceRequest(pRes);
      pMsg.setContract(contract);
    }

    return pMsg;
  }

  protected RMContext getRmContext() {
    return rmContext;
  }

  protected YarnScheduler getScheduler() {
    return rmContext.getScheduler();
  }

  private static void addToContainerUpdates(AllocateResponse allocateResponse,
      Allocation allocation, List<UpdateContainerError> updateContainerErrors) {
    // Handling increased containers
    ApplicationMasterServiceUtils.addToUpdatedContainers(
        allocateResponse, ContainerUpdateType.INCREASE_RESOURCE,
        allocation.getIncreasedContainers());

    // Handling decreased containers
    ApplicationMasterServiceUtils.addToUpdatedContainers(
        allocateResponse, ContainerUpdateType.DECREASE_RESOURCE,
        allocation.getDecreasedContainers());

    // Handling promoted containers
    ApplicationMasterServiceUtils.addToUpdatedContainers(
        allocateResponse, ContainerUpdateType.PROMOTE_EXECUTION_TYPE,
        allocation.getPromotedContainers());

    // Handling demoted containers
    ApplicationMasterServiceUtils.addToUpdatedContainers(
        allocateResponse, ContainerUpdateType.DEMOTE_EXECUTION_TYPE,
        allocation.getDemotedContainers());

    ApplicationMasterServiceUtils.addToUpdateContainerErrors(
        allocateResponse, updateContainerErrors);
  }
}
