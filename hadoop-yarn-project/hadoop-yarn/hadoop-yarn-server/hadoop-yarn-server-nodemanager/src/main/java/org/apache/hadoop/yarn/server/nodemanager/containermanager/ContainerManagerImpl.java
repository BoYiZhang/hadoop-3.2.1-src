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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.UpdateContainerTokenEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerTokenUpdatedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEvent;
import org.apache.hadoop.yarn.server.nodemanager.recovery.RecoveryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.CommitResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ContainerUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReInitializeContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceLocalizationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RestartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RollbackResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidAuxServiceException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerException;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.FlowContextProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.AuxiliaryLocalPathHandler;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrUpdateContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrSignalContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl.FlowContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerReInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.AbstractContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.SignalContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.LogAggregationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.NonAggregatingLogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerScheduler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerSchedulerEventType;

import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredApplicationsState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerType;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import static org.apache.hadoop.service.Service.STATE.STARTED;

public class ContainerManagerImpl extends CompositeService implements
    ContainerManager {

  private enum ReInitOp {
    RE_INIT, COMMIT, ROLLBACK, LOCALIZE;
  }


  /**
   * 等待应用程序关闭的额外时间。
   * Extra duration to wait for applications to be killed on shutdown.
   */
  private static final int SHUTDOWN_CLEANUP_SLOP_MS = 1000;


  private static final Logger LOG =  LoggerFactory.getLogger(ContainerManagerImpl.class);


  public static final String INVALID_NMTOKEN_MSG = "Invalid NMToken";
  static final String INVALID_CONTAINERTOKEN_MSG =  "Invalid ContainerToken";


  // 上下文环境信息
  protected final Context context;

  // 容器监控
  private final ContainersMonitor containersMonitor;

  // 服务
  private Server server;

  // 资源本地化服务
  private final ResourceLocalizationService rsrcLocalizationSrvc;

  // 容器启动器
  private final AbstractContainersLauncher containersLauncher;

  // 辅助服务
  private final AuxServices auxiliaryServices;

  // 度量信息
  private final NodeManagerMetrics metrics;

  //节点状态更新程序
  protected final NodeStatusUpdater nodeStatusUpdater;

  // 本地目录处理程序
  protected LocalDirsHandlerService dirsHandler;

  // 异步调度器
  protected final AsyncDispatcher dispatcher;

  // 清理服务
  private final DeletionService deletionService;

  // 日志服务
  private LogHandler logHandler;

  // 服务是否停止标志.
  private boolean serviceStopped = false;

  // 读锁
  private final ReadLock readLock;

  // 写锁
  private final WriteLock writeLock;

  // AMRM 代理服务
  private AMRMProxyService amrmProxyService;

  //是否启用 AMRM 代理服务
  protected boolean amrmProxyEnabled = false;

  // Container 调度器
  private final ContainerScheduler containerScheduler;

  // 等待Container停止毫秒数
  private long waitForContainersOnShutdownMillis;

  // 仅当启用了timeline service v.2时，才会设置NM metrics publisher
  // NM metrics publisher is set only if the timeline service v.2 is enabled
  private NMTimelinePublisher nmMetricsPublisher;

  private boolean timelineServiceV2Enabled;

  public ContainerManagerImpl(Context context, ContainerExecutor exec,
      DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater,
      NodeManagerMetrics metrics, LocalDirsHandlerService dirsHandler) {

    super(ContainerManagerImpl.class.getName());

    this.context = context;
    // Service org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService in state org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService: NOTINITED
    this.dirsHandler = dirsHandler;

    // ContainerManager level dispatcher.
    dispatcher = new AsyncDispatcher("NM ContainerManager dispatcher");
    // Service org.apache.hadoop.yarn.server.nodemanager.DeletionService in state org.apache.hadoop.yarn.server.nodemanager.DeletionService: NOTINITED
    this.deletionService = deletionContext;

    this.metrics = metrics;
    // Service org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService in state org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService: NOTINITED
    rsrcLocalizationSrvc =  createResourceLocalizationService(exec, deletionContext, context,   metrics);

    addService(rsrcLocalizationSrvc);
    // Service containers-launcher in state containers-launcher: NOTINITED
    containersLauncher = createContainersLauncher(context, exec);
    addService(containersLauncher);
    this.nodeStatusUpdater = nodeStatusUpdater;
    // Service org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerScheduler in state org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerScheduler: NOTINITED
    this.containerScheduler = createContainerScheduler(context);
    addService(containerScheduler);

    AuxiliaryLocalPathHandler auxiliaryLocalPathHandler =new AuxiliaryLocalPathHandlerImpl(dirsHandler);

    // Start configurable services
    auxiliaryServices = new AuxServices(auxiliaryLocalPathHandler,this.context, this.deletionService);

    auxiliaryServices.registerServiceListener(this);
    addService(auxiliaryServices);

    // initialize the metrics publisher if the timeline service v.2 is enabled
    // and the system publisher is enabled
    Configuration conf = context.getConf();
    if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
      if (YarnConfiguration.systemMetricsPublisherEnabled(conf)) {
        LOG.info("YARN system metrics publishing service is enabled");
        nmMetricsPublisher = createNMTimelinePublisher(context);
        context.setNMTimelinePublisher(nmMetricsPublisher);
      }
      this.timelineServiceV2Enabled = true;
    }
    this.containersMonitor = createContainersMonitor(exec);
    addService(this.containersMonitor);

    dispatcher.register(ContainerEventType.class,
        new ContainerEventDispatcher());
    dispatcher.register(ApplicationEventType.class,
        createApplicationEventDispatcher());
    dispatcher.register(LocalizationEventType.class,
        new LocalizationEventHandlerWrapper(rsrcLocalizationSrvc,
            nmMetricsPublisher));
    dispatcher.register(AuxServicesEventType.class, auxiliaryServices);
    dispatcher.register(ContainersMonitorEventType.class, containersMonitor);
    dispatcher.register(ContainersLauncherEventType.class, containersLauncher);
    dispatcher.register(ContainerSchedulerEventType.class, containerScheduler);

    addService(dispatcher);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {

    logHandler =  createLogHandler(conf, this.context, this.deletionService);
    addIfService(logHandler);
    dispatcher.register(LogHandlerEventType.class, logHandler);
    
    // add the shared cache upload service (it will do nothing if the shared cache is disabled)
    // Service org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadService in state org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadService: NOTINITED
    SharedCacheUploadService sharedCacheUploader = createSharedCacheUploaderService();

    addService(sharedCacheUploader);
    dispatcher.register(SharedCacheUploadEventType.class, sharedCacheUploader);

    createAMRMProxyService(conf);
    // waitForContainersOnShutdownMillis : 6250
    waitForContainersOnShutdownMillis =
        conf.getLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
            YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS) +
        conf.getLong(YarnConfiguration.NM_PROCESS_KILL_WAIT_MS,
            YarnConfiguration.DEFAULT_NM_PROCESS_KILL_WAIT_MS) +
        SHUTDOWN_CLEANUP_SLOP_MS;

    super.serviceInit(conf);
    recover();
  }

  protected void createAMRMProxyService(Configuration conf) {
    this.amrmProxyEnabled =
        conf.getBoolean(YarnConfiguration.AMRM_PROXY_ENABLED,
            YarnConfiguration.DEFAULT_AMRM_PROXY_ENABLED) ||
            conf.getBoolean(YarnConfiguration.DIST_SCHEDULING_ENABLED,
                YarnConfiguration.DEFAULT_DIST_SCHEDULING_ENABLED);

    if (amrmProxyEnabled) {
      LOG.info("AMRMProxyService is enabled. "
          + "All the AM->RM requests will be intercepted by the proxy");
      this.setAMRMProxyService(
          new AMRMProxyService(this.context, this.dispatcher));
      addService(this.getAMRMProxyService());
    } else {
      LOG.info("AMRMProxyService is disabled");
    }
  }

  @VisibleForTesting
  protected ContainerScheduler createContainerScheduler(Context cntxt) {
    // Currently, this dispatcher is shared by the ContainerManager,
    // all the containers, the container monitor and all the container.
    // The ContainerScheduler may use its own dispatcher.
    return new ContainerScheduler(cntxt, dispatcher, metrics);
  }

  protected ContainersMonitor createContainersMonitor(ContainerExecutor exec) {
    return new ContainersMonitorImpl(exec, dispatcher, this.context);
  }

  @SuppressWarnings("unchecked")
  private void recover() throws IOException, URISyntaxException {
    NMStateStoreService stateStore = context.getNMStateStore();
    if (stateStore.canRecover()) {
      rsrcLocalizationSrvc.recoverLocalizedResources(
          stateStore.loadLocalizationState());

      RecoveredApplicationsState appsState = stateStore.loadApplicationsState();
      try (RecoveryIterator<ContainerManagerApplicationProto> rasIterator =
               appsState.getIterator()) {
        while (rasIterator.hasNext()) {
          ContainerManagerApplicationProto proto = rasIterator.next();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering application with state: " + proto.toString());
          }
          recoverApplication(proto);
        }
      }

      try (RecoveryIterator<RecoveredContainerState> rcsIterator =
               stateStore.getContainerStateIterator()) {
        while (rcsIterator.hasNext()) {
          RecoveredContainerState rcs = rcsIterator.next();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Recovering container with state: " + rcs);
          }
          recoverContainer(rcs);
        }
      }

      // Recovery AMRMProxy state after apps and containers are recovered
      if (this.amrmProxyEnabled) {
        this.getAMRMProxyService().recover();
      }

      //Dispatching the RECOVERY_COMPLETED event through the dispatcher
      //so that all the paused, scheduled and queued containers will
      //be scheduled for execution on availability of resources.
      dispatcher.getEventHandler().handle(
          new ContainerSchedulerEvent(null,
              ContainerSchedulerEventType.RECOVERY_COMPLETED));
    } else {
      LOG.info("Not a recoverable state store. Nothing to recover.");
    }
  }

  private void recoverApplication(ContainerManagerApplicationProto p)
      throws IOException {
    ApplicationId appId = new ApplicationIdPBImpl(p.getId());
    Credentials creds = new Credentials();
    creds.readTokenStorageStream(
        new DataInputStream(p.getCredentials().newInput()));

    List<ApplicationACLMapProto> aclProtoList = p.getAclsList();
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(aclProtoList.size());
    for (ApplicationACLMapProto aclProto : aclProtoList) {
      acls.put(ProtoUtils.convertFromProtoFormat(aclProto.getAccessType()),
          aclProto.getAcl());
    }

    LogAggregationContext logAggregationContext = null;
    if (p.getLogAggregationContext() != null) {
      logAggregationContext =
          new LogAggregationContextPBImpl(p.getLogAggregationContext());
    }

    FlowContext fc = null;
    if (p.getFlowContext() != null) {
      FlowContextProto fcp = p.getFlowContext();
      fc = new FlowContext(fcp.getFlowName(), fcp.getFlowVersion(),
          fcp.getFlowRunId());
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Recovering Flow context: " + fc + " for an application " + appId);
      }
    } else {
      // in upgrade situations, where there is no prior existing flow context,
      // default would be used.
      fc = new FlowContext(TimelineUtils.generateDefaultFlowName(null, appId),
          YarnConfiguration.DEFAULT_FLOW_VERSION, appId.getClusterTimestamp());
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "No prior existing flow context found. Using default Flow context: "
                + fc + " for an application " + appId);
      }
    }

    LOG.info("Recovering application " + appId);
    ApplicationImpl app = new ApplicationImpl(dispatcher, p.getUser(), fc,
        appId, creds, context, p.getAppLogAggregationInitedTime());
    context.getApplications().put(appId, app);
    app.handle(new ApplicationInitEvent(appId, acls, logAggregationContext));
  }

  private void recoverContainer(RecoveredContainerState rcs)
      throws IOException {
    StartContainerRequest req = rcs.getStartRequest();
    ContainerLaunchContext launchContext = req.getContainerLaunchContext();
    ContainerTokenIdentifier token;
    if(rcs.getCapability() != null) {
      ContainerTokenIdentifier originalToken =
          BuilderUtils.newContainerTokenIdentifier(req.getContainerToken());
      token = new ContainerTokenIdentifier(originalToken.getContainerID(),
          originalToken.getVersion(), originalToken.getNmHostAddress(),
          originalToken.getApplicationSubmitter(), rcs.getCapability(),
          originalToken.getExpiryTimeStamp(), originalToken.getMasterKeyId(),
          originalToken.getRMIdentifier(), originalToken.getPriority(),
          originalToken.getCreationTime(),
          originalToken.getLogAggregationContext(),
          originalToken.getNodeLabelExpression(),
          originalToken.getContainerType(), originalToken.getExecutionType(),
          originalToken.getAllocationRequestId(),
          originalToken.getAllcationTags());

    } else {
      token = BuilderUtils.newContainerTokenIdentifier(req.getContainerToken());
    }

    ContainerId containerId = token.getContainerID();
    ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();

    LOG.info("Recovering " + containerId + " in state " + rcs.getStatus()
        + " with exit code " + rcs.getExitCode());

    Application app = context.getApplications().get(appId);
    if (app != null) {
      recoverActiveContainer(app, launchContext, token, rcs);
      if (rcs.getRecoveryType() == RecoveredContainerType.KILL) {
        dispatcher.getEventHandler().handle(
            new ContainerKillEvent(containerId, ContainerExitStatus.ABORTED,
                "Due to invalid StateStore info container was killed"
                    + " during recovery"));
      }
    } else {
      if (rcs.getStatus() != RecoveredContainerStatus.COMPLETED) {
        LOG.warn(containerId + " has no corresponding application!");
      }
      LOG.info("Adding " + containerId + " to recently stopped containers");
      nodeStatusUpdater.addCompletedContainer(containerId);
    }
  }

  /**
   * Recover a running container.
   */
  @SuppressWarnings("unchecked")
  protected void recoverActiveContainer(Application app,
      ContainerLaunchContext launchContext, ContainerTokenIdentifier token,
      RecoveredContainerState rcs) throws IOException {
    Credentials credentials = YarnServerSecurityUtils.parseCredentials(
        launchContext);
    Container container = new ContainerImpl(getConfig(), dispatcher,
        launchContext, credentials, metrics, token, context, rcs);
    context.getContainers().put(token.getContainerID(), container);
    containerScheduler.recoverActiveContainer(container, rcs);
    app.handle(new ApplicationContainerInitEvent(container));
  }

  private void waitForRecoveredContainers() throws InterruptedException {
    final int sleepMsec = 100;
    int waitIterations = 100;
    List<ContainerId> newContainers = new ArrayList<ContainerId>();
    while (--waitIterations >= 0) {
      newContainers.clear();
      for (Container container : context.getContainers().values()) {
        if (container.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.NEW) {
          newContainers.add(container.getContainerId());
        }
      }
      if (newContainers.isEmpty()) {
        break;
      }
      LOG.info("Waiting for containers: " + newContainers);
      Thread.sleep(sleepMsec);
    }
    if (waitIterations < 0) {
      LOG.warn("Timeout waiting for recovered containers");
    }
  }

  protected LogHandler createLogHandler(Configuration conf, Context context,
      DeletionService deletionService) {
    if (conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      return new LogAggregationService(this.dispatcher, context,
          deletionService, dirsHandler);
    } else {
      return new NonAggregatingLogHandler(this.dispatcher, deletionService,
                                          dirsHandler,
                                          context.getNMStateStore());
    }
  }

  @Override
  public ContainersMonitor getContainersMonitor() {
    return this.containersMonitor;
  }

  protected ResourceLocalizationService createResourceLocalizationService(
      ContainerExecutor exec, DeletionService deletionContext,
      Context nmContext, NodeManagerMetrics nmMetrics) {
    return new ResourceLocalizationService(this.dispatcher, exec,
        deletionContext, dirsHandler, nmContext, nmMetrics);
  }

  protected SharedCacheUploadService createSharedCacheUploaderService() {
    return new SharedCacheUploadService();
  }

  @VisibleForTesting
  protected NMTimelinePublisher createNMTimelinePublisher(Context ctxt) {
    NMTimelinePublisher nmTimelinePublisherLocal =
        new NMTimelinePublisher(ctxt);
    addIfService(nmTimelinePublisherLocal);
    return nmTimelinePublisherLocal;
  }

  protected AbstractContainersLauncher createContainersLauncher(
      Context ctxt, ContainerExecutor exec) {
    Class<? extends AbstractContainersLauncher> containersLauncherClass =
        ctxt.getConf()
            .getClass(YarnConfiguration.NM_CONTAINERS_LAUNCHER_CLASS,
                ContainersLauncher.class, AbstractContainersLauncher.class);
    AbstractContainersLauncher launcher;
    try {
      launcher = ReflectionUtils.newInstance(containersLauncherClass,
          ctxt.getConf());
      launcher.init(ctxt, this.dispatcher, exec, dirsHandler, this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return launcher;
  }

  protected EventHandler<ApplicationEvent> createApplicationEventDispatcher() {
    return new ApplicationEventDispatcher();
  }

  @Override
  protected void serviceStart() throws Exception {

    // Enqueue user dirs in deletion context
    Configuration conf = getConfig();
    //  0.0.0.0/0.0.0.0:0
    final InetSocketAddress initialAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_PORT);
    boolean usingEphemeralPort = (initialAddress.getPort() == 0);
    if (context.getNMStateStore().canRecover() && usingEphemeralPort) {
      throw new IllegalArgumentException("Cannot support recovery with an "
          + "ephemeral server port. Check the setting of "
          + YarnConfiguration.NM_ADDRESS);
    }
    // If recovering then delay opening the RPC service until the recovery
    // of resources and containers have completed, otherwise requests from
    // clients during recovery can interfere with the recovery process.
    // false
    final boolean delayedRpcServerStart =    context.getNMStateStore().canRecover();

    Configuration serverConf = new Configuration(conf);

    // always enforce it to be token-based.
    serverConf.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      SaslRpcServer.AuthMethod.TOKEN.toString());
    
    YarnRPC rpc = YarnRPC.create(conf);
    // ProtobufRpcEngine
    server =
        rpc.getServer(ContainerManagementProtocol.class, this, initialAddress, 
            serverConf, this.context.getNMTokenSecretManager(),
            conf.getInt(YarnConfiguration.NM_CONTAINER_MGR_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_NM_CONTAINER_MGR_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, NMPolicyProvider.getInstance());
    }
    
    String bindHost = conf.get(YarnConfiguration.NM_BIND_HOST);
    String nmAddress = conf.getTrimmed(YarnConfiguration.NM_ADDRESS);
    String hostOverride = null;
    if (bindHost != null && !bindHost.isEmpty()
        && nmAddress != null && !nmAddress.isEmpty()) {
      //a bind-host case with an address, to support overriding the first
      //hostname found when querying for our hostname with the specified
      //address, combine the specified address with the actual port listened
      //on by the server
      hostOverride = nmAddress.split(":")[0];
    }

    // setup node ID
    InetSocketAddress connectAddress;
    if (delayedRpcServerStart) {
      connectAddress = NetUtils.getConnectAddress(initialAddress);
    } else {
      server.start();
      connectAddress = NetUtils.getConnectAddress(server);
    }
    NodeId nodeId = buildNodeId(connectAddress, hostOverride);  // boyi-pro.lan:54950
    ((NodeManager.NMContext)context).setNodeId(nodeId);
    this.context.getNMTokenSecretManager().setNodeId(nodeId);
    this.context.getContainerTokenSecretManager().setNodeId(nodeId);

    // start remaining services
    super.serviceStart();

    if (delayedRpcServerStart) {
      waitForRecoveredContainers();
      server.start();

      // check that the node ID is as previously advertised
      connectAddress = NetUtils.getConnectAddress(server);
      NodeId serverNode = buildNodeId(connectAddress, hostOverride);
      if (!serverNode.equals(nodeId)) {
        throw new IOException("Node mismatch after server started, expected '"
            + nodeId + "' but found '" + serverNode + "'");
      }
    }

    LOG.info("ContainerManager started at " + connectAddress);
    LOG.info("ContainerManager bound to " + initialAddress);
  }

  private NodeId buildNodeId(InetSocketAddress connectAddress,
      String hostOverride) {
    if (hostOverride != null) {
      connectAddress = NetUtils.getConnectAddress(
          new InetSocketAddress(hostOverride, connectAddress.getPort()));
    }
    return NodeId.newInstance(
        connectAddress.getAddress().getCanonicalHostName(),
        connectAddress.getPort());
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public void serviceStop() throws Exception {
    this.writeLock.lock();
    try {
      serviceStopped = true;
      if (context != null) {
        cleanUpApplicationsOnNMShutDown();
      }
    } finally {
      this.writeLock.unlock();
    }
    if (auxiliaryServices.getServiceState() == STARTED) {
      auxiliaryServices.unregisterServiceListener(this);
    }
    if (server != null) {
      server.stop();
    }
    super.serviceStop();
  }

  public void cleanUpApplicationsOnNMShutDown() {
    Map<ApplicationId, Application> applications =
        this.context.getApplications();
    if (applications.isEmpty()) {
      return;
    }
    LOG.info("Applications still running : " + applications.keySet());

    if (this.context.getNMStateStore().canRecover()
        && !this.context.getDecommissioned()) {
      if (getConfig().getBoolean(YarnConfiguration.NM_RECOVERY_SUPERVISED,
          YarnConfiguration.DEFAULT_NM_RECOVERY_SUPERVISED)) {
        // do not cleanup apps as they can be recovered on restart
        return;
      }
    }

    List<ApplicationId> appIds =
        new ArrayList<ApplicationId>(applications.keySet());
    this.handle(new CMgrCompletedAppsEvent(appIds,
            CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN));

    LOG.info("Waiting for Applications to be Finished");

    long waitStartTime = System.currentTimeMillis();
    while (!applications.isEmpty()
        && System.currentTimeMillis() - waitStartTime < waitForContainersOnShutdownMillis) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        LOG.warn(
          "Interrupted while sleeping on applications finish on shutdown", ex);
      }
    }

    // All applications Finished
    if (applications.isEmpty()) {
      LOG.info("All applications in FINISHED state");
    } else {
      LOG.info("Done waiting for Applications to be Finished. Still alive: "
          + applications.keySet());
    }
  }

  public void cleanupContainersOnNMResync() {
    Map<ContainerId, Container> containers = context.getContainers();
    if (containers.isEmpty()) {
      return;
    }
    LOG.info("Containers still running on "
        + CMgrCompletedContainersEvent.Reason.ON_NODEMANAGER_RESYNC + " : "
        + containers.keySet());

    List<ContainerId> containerIds =
      new ArrayList<ContainerId>(containers.keySet());

    LOG.info("Waiting for containers to be killed");

    this.handle(new CMgrCompletedContainersEvent(containerIds,
      CMgrCompletedContainersEvent.Reason.ON_NODEMANAGER_RESYNC));

    /*
     * We will wait till all the containers change their state to COMPLETE. We
     * will not remove the container statuses from nm context because these
     * are used while re-registering node manager with resource manager.
     */
    boolean allContainersCompleted = false;
    while (!containers.isEmpty() && !allContainersCompleted) {
      allContainersCompleted = true;
      for (Entry<ContainerId, Container> container : containers.entrySet()) {
        if (((ContainerImpl) container.getValue()).getCurrentState()
            != ContainerState.COMPLETE) {
          allContainersCompleted = false;
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            LOG.warn("Interrupted while sleeping on container kill on resync",
              ex);
          }
          break;
        }
      }
    }
    // All containers killed
    if (allContainersCompleted) {
      LOG.info("All containers in DONE state");
    } else {
      LOG.info("Done waiting for containers to be killed. Still alive: " +
        containers.keySet());
    }
  }

  // Get the remoteUGI corresponding to the api call.
  protected UserGroupInformation getRemoteUgi()
      throws YarnException {
    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg = "Cannot obtain the user-name. Got exception: "
          + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }
    return remoteUgi;
  }

  // Obtain the needed ContainerTokenIdentifier from the remote-UGI. RPC layer
  // currently sets only the required id, but iterate through anyways just to
  // be sure.
  @Private
  @VisibleForTesting
  protected NMTokenIdentifier selectNMTokenIdentifier(
      UserGroupInformation remoteUgi) {
    Set<TokenIdentifier> tokenIdentifiers = remoteUgi.getTokenIdentifiers();
    NMTokenIdentifier resultId = null;
    for (TokenIdentifier id : tokenIdentifiers) {
      if (id instanceof NMTokenIdentifier) {
        resultId = (NMTokenIdentifier) id;
        break;
      }
    }
    return resultId;
  }

  protected void authorizeUser(UserGroupInformation remoteUgi,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    if (nmTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    if (!remoteUgi.getUserName().equals(
      nmTokenIdentifier.getApplicationAttemptId().toString())) {
      throw RPCUtil.getRemoteException("Expected applicationAttemptId: "
          + remoteUgi.getUserName() + "Found: "
          + nmTokenIdentifier.getApplicationAttemptId());
    }
  }

  /**
   * @param containerTokenIdentifier
   *          of the container whose resource is to be started or increased
   * @throws YarnException
   */
  @Private
  @VisibleForTesting
  protected void authorizeStartAndResourceIncreaseRequest(
      NMTokenIdentifier nmTokenIdentifier,
      ContainerTokenIdentifier containerTokenIdentifier,
      boolean startRequest)
      throws YarnException {
    if (nmTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    if (containerTokenIdentifier == null) {
      throw RPCUtil.getRemoteException(INVALID_CONTAINERTOKEN_MSG);
    }
    /*
     * Check the following:
     * 1. The request comes from the same application attempt
     * 2. The request possess a container token that has not expired
     * 3. The request possess a container token that is granted by a known RM
     */
    ContainerId containerId = containerTokenIdentifier.getContainerID();
    String containerIDStr = containerId.toString();
    boolean unauthorized = false;
    StringBuilder messageBuilder =
        new StringBuilder("Unauthorized request to " + (startRequest ?
            "start container." : "increase container resource."));
    if (!nmTokenIdentifier.getApplicationAttemptId().getApplicationId().
        equals(containerId.getApplicationAttemptId().getApplicationId())) {
      unauthorized = true;
      messageBuilder.append("\nNMToken for application attempt : ")
        .append(nmTokenIdentifier.getApplicationAttemptId())
        .append(" was used for "
            + (startRequest ? "starting " : "increasing resource of ")
            + "container with container token")
        .append(" issued for application attempt : ")
        .append(containerId.getApplicationAttemptId());
    } else if (startRequest && !this.context.getContainerTokenSecretManager()
        .isValidStartContainerRequest(containerTokenIdentifier)) {
      // Is the container being relaunched? Or RPC layer let startCall with
      // tokens generated off old-secret through?
      unauthorized = true;
      messageBuilder.append("\n Attempt to relaunch the same ")
        .append("container with id ").append(containerIDStr).append(".");
    } else if (containerTokenIdentifier.getExpiryTimeStamp() < System
      .currentTimeMillis()) {
      // Ensure the token is not expired.
      unauthorized = true;
      messageBuilder.append("\nThis token is expired. current time is ")
        .append(System.currentTimeMillis()).append(" found ")
        .append(containerTokenIdentifier.getExpiryTimeStamp());
      messageBuilder.append("\nNote: System times on machines may be out of sync.")
        .append(" Check system time and time zones.");
    }
    if (unauthorized) {
      String msg = messageBuilder.toString();
      LOG.error(msg);
      throw RPCUtil.getRemoteException(msg);
    }
    if (containerTokenIdentifier.getRMIdentifier() != nodeStatusUpdater
        .getRMIdentifier()) {
      // Is the container coming from unknown RM
      StringBuilder sb = new StringBuilder("\nContainer ");
      sb.append(containerTokenIdentifier.getContainerID().toString())
        .append(" rejected as it is allocated by a previous RM");
      throw new InvalidContainerException(sb.toString());
    }
  }


  //
  //  start_container_request {
  //    container_launch_context {
  //      localResources {
  //        key: "__spark_conf__"
  //        value {
  //          resource {
  //            scheme: "hdfs"
  //            host: "localhost"
  //            port: 8020
  //            file: "/user/henghe/.sparkStaging/application_1611649876067_0001/__spark_conf__.zip"
  //          }
  //          size: 250024
  //          timestamp: 1611650748335
  //          type: ARCHIVE
  //          visibility: PRIVATE
  //        }
  //      }
  //      localResources {
  //        key: "__app__.jar"
  //        value {
  //          resource {
  //            scheme: "hdfs"
  //            host: "localhost"
  //            port: 8020
  //            file: "/user/henghe/.sparkStaging/application_1611649876067_0001/spark-examples_2.11-2.4.5.jar"
  //          }
  //          size: 1475072
  //          timestamp: 1611650747880
  //          type: FILE
  //          visibility: PRIVATE
  //        }
  //      }
  //      tokens: "HDTS\000\001\000\032\n\r\n\t\b\001\020\343\200\306\356\363.\020\001\020\211\261\343\254\370\377\377\377\377\001\024\367\0369&\223=\221M`\\\245\0018\374z\360\266\275+@\020YARN_AM_RM_TOKEN\000\000"
  //      environment {
  //        key: "SPARK_YARN_STAGING_DIR"
  //        value: "hdfs://localhost:8020/user/henghe/.sparkStaging/application_1611649876067_0001"
  //      }
  //      environment {
  //        key: "APPLICATION_WEB_PROXY_BASE"
  //        value: "/proxy/application_1611649876067_0001"
  //      }
  //      environment {
  //        key: "SPARK_USER"
  //        value: "henghe"
  //      }
  //      environment {
  //        key: "CLASSPATH"
  //        value: "{{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>$HADOOP_CONF_DIR<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/*<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/lib/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*<CPS>{{PWD}}/__spark_conf__/__hadoop_conf__"
  //      }
  //      environment {
  //        key: "PYTHONHASHSEED"
  //        value: "0"
  //      }
  //      environment {
  //        key: "APP_SUBMIT_TIME_ENV"
  //        value: "1611650782082"
  //      }
  //      command: "{{JAVA_HOME}}/bin/java"
  //      command: "-server"
  //      command: "-Xmx1024m"
  //      command: "-Djava.io.tmpdir={{PWD}}/tmp"
  //      command: "-Dspark.yarn.app.container.log.dir=<LOG_DIR>"
  //      command: "org.apache.spark.deploy.yarn.ApplicationMaster"
  //      command: "--class"
  //      command: "\'org.apache.spark.examples.SparkPi\'"
  //      command: "--jar"
  //      command: "file:/opt/tools/spark-2.4.5/examples/jars/spark-examples_2.11-2.4.5.jar"
  //      command: "--arg"
  //      command: "\'10\'"
  //      command: "--properties-file"
  //      command: "{{PWD}}/__spark_conf__/__spark_conf__.properties"
  //      command: "1>"
  //      command: "<LOG_DIR>/stdout"
  //      command: "2>"
  //      command: "<LOG_DIR>/stderr"
  //      application_ACLs {
  //        accessType: APPACCESS_VIEW_APP
  //        acl: "sysadmin,henghe "
  //      }
  //      application_ACLs {
  //        accessType: APPACCESS_MODIFY_APP
  //        acl: "sysadmin,henghe "
  //      }
  //    }
  //    container_token {
  //      identifier: "\n\021\022\r\n\t\b\001\020\343\200\306\356\363.\020\001\030\001\022\023192.168.8.188:59693\032\006henghe\"+\b\200\020\020\001\032\024\n\tmemory-mb\020\200\020\032\002Mi \000\032\016\n\006vcores\020\001\032\000 \000(\354\251\242\357\363.0\347\232\217\336\0068\343\200\306\356\363.B\002\b\000H\231\332\375\356\363.Z\000`\001h\001p\000x\377\377\377\377\377\377\377\377\377\001"
  //      password: "O\203up\251\241_)\016ml[\335\035\317\225\252,\021\204"
  //      kind: "ContainerToken"
  //      service: "192.168.8.188:59693"
  //    }
  //  }

  /**
   * Start a list of containers on this NodeManager.
   */
  @Override
  public StartContainersResponse startContainers(  StartContainersRequest requests) throws YarnException, IOException {


    UserGroupInformation remoteUgi = getRemoteUgi();

    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi, nmTokenIdentifier);


    //成功 succeededContainers
    List<ContainerId> succeededContainers = new ArrayList<ContainerId>();

    // 失败 failedContainers
    Map<ContainerId, SerializedException> failedContainers = new HashMap<ContainerId, SerializedException>();


    // Synchronize with NodeStatusUpdaterImpl#registerWithRM
    // to avoid race condition during NM-RM resync (due to RM restart) while a
    // container is being started, in particular when the container has not yet
    // been added to the containers map in NMContext.
    synchronized (this.context) {
      // 获取启动Container请求
      for (StartContainerRequest request : requests.getStartContainerRequests()) {

        ContainerId containerId = null;
        try {
          // 权限token校验 & 鉴权
          if (request.getContainerToken() == null
              || request.getContainerToken().getIdentifier() == null) {
            throw new IOException(INVALID_CONTAINERTOKEN_MSG);
          }
          ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());
          verifyAndGetContainerTokenIdentifier(request.getContainerToken(),containerTokenIdentifier);
          containerId = containerTokenIdentifier.getContainerID();


          // 仅当容器的类型为AM并且启用了AMRMProxy服务时，才初始化AMRMProxy服务实例
          // Initialize the AMRMProxy service instance only if the container is of
          // type AM and if the AMRMProxy service is enabled
          if (amrmProxyEnabled && containerTokenIdentifier.getContainerType().equals(ContainerType.APPLICATION_MASTER)) {
            this.getAMRMProxyService().processApplicationStartRequest(request);
          }

          // 执行Container启动前检查
          performContainerPreStartChecks(nmTokenIdentifier, request, containerTokenIdentifier);

          // 启动Container
          startContainerInternal(containerTokenIdentifier, request);

          // 构建启动成功的containerId
          succeededContainers.add(containerId);

        } catch (YarnException e) {
          // 构建启动失败的信息的containerId
          failedContainers.put(containerId, SerializedException.newInstance(e));
        } catch (InvalidToken ie) {
          // 构建启动失败的信息的containerId
          failedContainers .put(containerId, SerializedException.newInstance(ie));
          throw ie;
        } catch (IOException e) {
          throw RPCUtil.getRemoteException(e);
        }
      }
      // 返回响应数据
      return StartContainersResponse.newInstance(getAuxServiceMetaData(), succeededContainers, failedContainers);

    }
  }

  private void performContainerPreStartChecks(
      NMTokenIdentifier nmTokenIdentifier, StartContainerRequest request,
      ContainerTokenIdentifier containerTokenIdentifier)
      throws YarnException, InvalidToken {
    /*

     1)它应该将NMToken保存到NMTokenSecretManager中。
        这是在这里而不是RPC层完成的，因为在打开/验证连接时，它不知道用户将对其进行哪些RPC调用。
        另外，新的NMToken只在startContainer上发布（一旦更新）。


     2)它应该验证containerToken。需要检查下面的东西。（一）
        它由正确的主密钥（检索密码的一部分）签名。（二）
        它属于正确的节点管理器（检索密码的一部分）。（三）
        它有正确的识别器。d） 它没有过期。

   * 1) It should save the NMToken into NMTokenSecretManager. This is done
   * here instead of RPC layer because at the time of opening/authenticating
   * the connection it doesn't know what all RPC calls user will make on it.
   * Also new NMToken is issued only at startContainer (once it gets
   * renewed).
   *
   * 2) It should validate containerToken. Need to check below things. a) It
   * is signed by correct master key (part of retrieve password). b) It
   * belongs to correct Node Manager (part of retrieve password). c) It has
   * correct RMIdentifier. d) It is not expired.
   */
    authorizeStartAndResourceIncreaseRequest( nmTokenIdentifier, containerTokenIdentifier, true);
    // update NMToken
    updateNMTokenIdentifier(nmTokenIdentifier);

    ContainerLaunchContext launchContext = request.getContainerLaunchContext();

    Map<String, ByteBuffer> serviceData = getAuxServiceMetaData();

    if (launchContext.getServiceData()!=null && !launchContext.getServiceData().isEmpty()) {

      for (Entry<String, ByteBuffer> meta : launchContext.getServiceData() .entrySet()) {

        if (null == serviceData.get(meta.getKey())) {
          throw new InvalidAuxServiceException("The auxService:" + meta.getKey()
              + " does not exist");
        }
      }
    }
  }

  private ContainerManagerApplicationProto buildAppProto(ApplicationId appId,
      String user, Credentials credentials,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, FlowContext flowContext) {

    ContainerManagerApplicationProto.Builder builder =
        ContainerManagerApplicationProto.newBuilder();
    builder.setId(((ApplicationIdPBImpl) appId).getProto());
    builder.setUser(user);

    if (logAggregationContext != null) {
      builder.setLogAggregationContext((
          (LogAggregationContextPBImpl)logAggregationContext).getProto());
    }

    builder.clearCredentials();
    if (credentials != null) {
      DataOutputBuffer dob = new DataOutputBuffer();
      try {
        credentials.writeTokenStorageToStream(dob);
        builder.setCredentials(ByteString.copyFrom(dob.getData()));
      } catch (IOException e) {
        // should not occur
        LOG.error("Cannot serialize credentials", e);
      }
    }

    builder.clearAcls();
    if (appAcls != null) {
      for (Map.Entry<ApplicationAccessType, String> acl : appAcls.entrySet()) {
        ApplicationACLMapProto p = ApplicationACLMapProto.newBuilder()
            .setAccessType(ProtoUtils.convertToProtoFormat(acl.getKey()))
            .setAcl(acl.getValue())
            .build();
        builder.addAcls(p);
      }
    }

    builder.clearFlowContext();
    if (flowContext != null && flowContext.getFlowName() != null
        && flowContext.getFlowVersion() != null) {
      FlowContextProto fcp =
          FlowContextProto.newBuilder().setFlowName(flowContext.getFlowName())
              .setFlowVersion(flowContext.getFlowVersion())
              .setFlowRunId(flowContext.getFlowRunId()).build();
      builder.setFlowContext(fcp);
    }

    return builder.build();
  }

  @SuppressWarnings("unchecked")
  protected void startContainerInternal( ContainerTokenIdentifier containerTokenIdentifier, StartContainerRequest request) throws YarnException, IOException {

    // 获取containerId
    ContainerId containerId = containerTokenIdentifier.getContainerID();

    // 获取containerId 的字符串
    String containerIdStr = containerId.toString();

    // 获取提交人
    String user = containerTokenIdentifier.getApplicationSubmitter();

    LOG.info("Start request for " + containerIdStr + " by user " + user);

    ContainerLaunchContext launchContext = request.getContainerLaunchContext();

    // 本地资源的健全性检查
    // Sanity check for local resources
    for (Map.Entry<String, LocalResource> rsrc : launchContext.getLocalResources().entrySet()) {

      if (rsrc.getValue() == null || rsrc.getValue().getResource() == null) {
        throw new YarnException(
            "Null resource URL for local resource " + rsrc.getKey() + " : " + rsrc.getValue());
      } else if (rsrc.getValue().getType() == null) {
        throw new YarnException(
            "Null resource type for local resource " + rsrc.getKey() + " : " + rsrc.getValue());
      } else if (rsrc.getValue().getVisibility() == null) {
        throw new YarnException(
            "Null resource visibility for local resource " + rsrc.getKey() + " : " + rsrc.getValue());
      }
    }

    // 获取凭证信息
    Credentials credentials = YarnServerSecurityUtils.parseCredentials(launchContext);

    // Container 启动时间
    long containerStartTime = SystemClock.getInstance().getTime();

    // 构建 ContainerImpl  container_1611649876067_0001_01_000001
    Container container = new ContainerImpl(getConfig(), this.dispatcher,launchContext, credentials, metrics, containerTokenIdentifier,context, containerStartTime);

    //获取ApplicationId : application_1611649876067_0001
    ApplicationId applicationID =  containerId.getApplicationAttemptId().getApplicationId();
    if (context.getContainers().putIfAbsent(containerId, container) != null) {
      NMAuditLogger.logFailure(user, AuditConstants.START_CONTAINER,
        "ContainerManagerImpl", "Container already running on this node!",
        applicationID, containerId);
      throw RPCUtil.getRemoteException("Container " + containerIdStr
          + " already is running on this node!!");
    }

    this.readLock.lock();
    try {
      if (!isServiceStopped()) {
        // 应用程序是否包含applicationID
        if (!context.getApplications().containsKey(applicationID)) {
          // 在这里代表第一次启动

          // Create the application
          // populate the flow context from the launch context if the timeline
          // service v.2 is enabled

          // 如果 timeline 服务v2 启用的话, 从launch上下文中构建flow context

          FlowContext flowContext =  getFlowContext(launchContext, applicationID);

          // 构建Application
          Application application = new ApplicationImpl(dispatcher, user, flowContext, applicationID, credentials, context);

          // 添加 application 信息
          if (context.getApplications().putIfAbsent(applicationID, application) == null) {

            LOG.info("Creating a new application reference for app "  + applicationID);
            LogAggregationContext logAggregationContext = containerTokenIdentifier.getLogAggregationContext();
            Map<ApplicationAccessType, String> appAcls =  container.getLaunchContext().getApplicationACLs();

            // 存储Application
            context.getNMStateStore().storeApplication(applicationID, buildAppProto(applicationID, user, credentials, appAcls, logAggregationContext, flowContext));

            // 处理Application init 时间
            dispatcher.getEventHandler().handle(new ApplicationInitEvent(
                applicationID, appAcls, logAggregationContext));
          }
        } else if (containerTokenIdentifier.getContainerType() == ContainerType.APPLICATION_MASTER) {

          // Application 已经启动了.


          // 根据已有的launchContext和applicationID 构建 FlowContext
          FlowContext flowContext =  getFlowContext(launchContext, applicationID);

          if (flowContext != null) {

            // 获取 ApplicationImpl
            ApplicationImpl application = (ApplicationImpl) context.getApplications().get(applicationID);

            // update flowContext reference in ApplicationImpl
            application.setFlowContext(flowContext);

            // Required to update state store for recovery.
            // 更新存储
            context.getNMStateStore().storeApplication(applicationID,
                buildAppProto(applicationID, user, credentials,
                    container.getLaunchContext().getApplicationACLs(),
                    containerTokenIdentifier.getLogAggregationContext(),
                    flowContext));

            LOG.info(
                "Updated application reference with flowContext " + flowContext
                    + " for app " + applicationID);
          } else {
            LOG.info("TimelineService V2.0 is not enabled. Skipping updating "
                + "flowContext for application " + applicationID);
          }
        }

        // 存储 container 信息
        this.context.getNMStateStore().storeContainer(containerId, containerTokenIdentifier.getVersion(), containerStartTime, request);

        // 处理 ApplicationContainerInitEvent 事件
        dispatcher.getEventHandler().handle(      new ApplicationContainerInitEvent(container));


        // token鉴权
        this.context.getContainerTokenSecretManager().startContainerSuccessful( containerTokenIdentifier);

        // 审计记录日志
        NMAuditLogger.logSuccess(user, AuditConstants.START_CONTAINER, "ContainerManageImpl", applicationID, containerId);
        // TODO
        //  launchedContainer misplaced ->不一定意味着容器启动。
        //  完成的Application不会启动containers。

        //  launchedContainer misplaced -> doesn't necessarily mean a container launch.
        //
        // A finished Application will not launch containers.
        metrics.launchedContainer();
        metrics.allocateContainer(containerTokenIdentifier.getResource());
      } else {
        throw new YarnException(
            "Container start failed as the NodeManager is " +
            "in the process of shutting down");
      }
    } finally {
      this.readLock.unlock();
    }
  }

  private FlowContext getFlowContext(ContainerLaunchContext launchContext,
      ApplicationId applicationID) {
    FlowContext flowContext = null;

    if (timelineServiceV2Enabled) {

      String flowName = launchContext.getEnvironment() .get(TimelineUtils.FLOW_NAME_TAG_PREFIX);
      String flowVersion = launchContext.getEnvironment()  .get(TimelineUtils.FLOW_VERSION_TAG_PREFIX);
      String flowRunIdStr = launchContext.getEnvironment() .get(TimelineUtils.FLOW_RUN_ID_TAG_PREFIX);

      long flowRunId = 0L;
      if (flowRunIdStr != null && !flowRunIdStr.isEmpty()) {
        flowRunId = Long.parseLong(flowRunIdStr);
      }
      flowContext = new FlowContext(flowName, flowVersion, flowRunId);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Flow context: " + flowContext + " created for an application "
                + applicationID);
      }
    }
    return flowContext;
  }

  protected ContainerTokenIdentifier verifyAndGetContainerTokenIdentifier(
      org.apache.hadoop.yarn.api.records.Token token,
      ContainerTokenIdentifier containerTokenIdentifier) throws YarnException,
      InvalidToken {
    byte[] password =
        context.getContainerTokenSecretManager().retrievePassword(
            containerTokenIdentifier);
    byte[] tokenPass = token.getPassword().array();
    if (password == null || tokenPass == null
        || !Arrays.equals(password, tokenPass)) {
      throw new InvalidToken(
        "Invalid container token used for starting container on : "
            + context.getNodeId().toString());
    }
    return containerTokenIdentifier;
  }

  /**
   * Increase resource of a list of containers on this NodeManager.
   */
  @Override
  @Deprecated
  public IncreaseContainersResourceResponse increaseContainersResource(
      IncreaseContainersResourceRequest requests)
          throws YarnException, IOException {
    ContainerUpdateResponse resp = updateContainer(
        ContainerUpdateRequest.newInstance(requests.getContainersToIncrease()));
    return IncreaseContainersResourceResponse.newInstance(
        resp.getSuccessfullyUpdatedContainers(), resp.getFailedRequests());
  }

  /**
   * Update resource of a list of containers on this NodeManager.
   */
  @Override
  public ContainerUpdateResponse updateContainer(ContainerUpdateRequest
      request) throws YarnException, IOException {
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi, nmTokenIdentifier);
    List<ContainerId> successfullyUpdatedContainers
        = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedContainers =
        new HashMap<ContainerId, SerializedException>();
    // Synchronize with NodeStatusUpdaterImpl#registerWithRM
    // to avoid race condition during NM-RM resync (due to RM restart) while a
    // container resource is being increased in NM, in particular when the
    // increased container has not yet been added to the increasedContainers
    // map in NMContext.
    synchronized (this.context) {
      // Process container resource increase requests
      for (org.apache.hadoop.yarn.api.records.Token token :
          request.getContainersToUpdate()) {
        ContainerId containerId = null;
        try {
          if (token.getIdentifier() == null) {
            throw new IOException(INVALID_CONTAINERTOKEN_MSG);
          }
          ContainerTokenIdentifier containerTokenIdentifier =
              BuilderUtils.newContainerTokenIdentifier(token);
          verifyAndGetContainerTokenIdentifier(token,
              containerTokenIdentifier);
          authorizeStartAndResourceIncreaseRequest(
              nmTokenIdentifier, containerTokenIdentifier, false);
          containerId = containerTokenIdentifier.getContainerID();
          // Reuse the startContainer logic to update NMToken,
          // as container resource increase request will have come with
          // an updated NMToken.
          updateNMTokenIdentifier(nmTokenIdentifier);
          updateContainerInternal(containerId, containerTokenIdentifier);
          successfullyUpdatedContainers.add(containerId);
        } catch (YarnException | InvalidToken e) {
          failedContainers.put(containerId, SerializedException.newInstance(e));
        } catch (IOException e) {
          throw RPCUtil.getRemoteException(e);
        }
      }
    }
    return ContainerUpdateResponse.newInstance(
        successfullyUpdatedContainers, failedContainers);
  }

  @SuppressWarnings("unchecked")
  private void updateContainerInternal(ContainerId containerId,
      ContainerTokenIdentifier containerTokenIdentifier)
      throws YarnException, IOException {

    // 获取容器
    Container container = context.getContainers().get(containerId);
    // Check container existence
    // 检查 container 是否存在
    if (container == null) {
      if (nodeStatusUpdater.isContainerRecentlyStopped(containerId)) {
        throw RPCUtil.getRemoteException("Container " + containerId.toString()
            + " was recently stopped on node manager.");
      } else {
        throw RPCUtil.getRemoteException("Container " + containerId.toString()
            + " is not handled by this NodeManager");
      }
    }
    // 检查版本
    // Check container version.
    int currentVersion = container.getContainerTokenIdentifier().getVersion();
    if (containerTokenIdentifier.getVersion() <= currentVersion) {
      throw RPCUtil.getRemoteException("Container " + containerId.toString()
          + " has update version [" + currentVersion + "] >= requested version"
          + " [" + containerTokenIdentifier.getVersion() + "]");
    }
    // 检查目标资源的有效性。
    // Check validity of the target resource.
    Resource currentResource = container.getResource();
    ExecutionType currentExecType = container.getContainerTokenIdentifier().getExecutionType();
    boolean isResourceChange = false;
    boolean isExecTypeUpdate = false;
    Resource targetResource = containerTokenIdentifier.getResource();
    ExecutionType targetExecType = containerTokenIdentifier.getExecutionType();

    //
    // Is true if either the resources has increased or execution type updated from opportunistic to guaranteed
    boolean isIncrease = false;
    if (!currentResource.equals(targetResource)) {
      isResourceChange = true;
      isIncrease = Resources.fitsIn(currentResource, targetResource)
          && !Resources.fitsIn(targetResource, currentResource);
    } else if (!currentExecType.equals(targetExecType)) {
      isExecTypeUpdate = true;
      isIncrease = currentExecType == ExecutionType.OPPORTUNISTIC &&
          targetExecType == ExecutionType.GUARANTEED;
    }
    if (isIncrease) {
      org.apache.hadoop.yarn.api.records.Container increasedContainer = null;
      if (isResourceChange) {
        increasedContainer =
            org.apache.hadoop.yarn.api.records.Container.newInstance(
                containerId, null, null, targetResource, null, null,
                currentExecType);
        if (context.getIncreasedContainers().putIfAbsent(containerId,
            increasedContainer) != null){
          throw RPCUtil.getRemoteException("Container " + containerId.toString()
              + " resource is being increased -or- " +
              "is undergoing ExecutionType promoted.");
        }
      }
    }
    this.readLock.lock();
    try {
      if (!serviceStopped) {
        // Dispatch message to Container to actually make the change.
        // 处理UpdateContainerTokenEvent 事件
        dispatcher.getEventHandler().handle(new UpdateContainerTokenEvent(
            container.getContainerId(), containerTokenIdentifier,
            isResourceChange, isExecTypeUpdate, isIncrease));
      } else {
        throw new YarnException(
            "Unable to change container resource as the NodeManager is "
                + "in the process of shutting down");
      }
    } finally {
      this.readLock.unlock();
    }
  }

  @Private
  @VisibleForTesting
  protected void updateNMTokenIdentifier(NMTokenIdentifier nmTokenIdentifier)
      throws InvalidToken {
    context.getNMTokenSecretManager().appAttemptStartContainer(
      nmTokenIdentifier);
  }

  /**
   * Stop a list of containers running on this NodeManager.
   */
  @Override
  public StopContainersResponse stopContainers(StopContainersRequest requests)
      throws YarnException, IOException {

    List<ContainerId> succeededRequests = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    for (ContainerId id : requests.getContainerIds()) {
      try {
        Container container = this.context.getContainers().get(id);
        authorizeGetAndStopContainerRequest(id, container, true, identifier);
        stopContainerInternal(id);
        succeededRequests.add(id);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return StopContainersResponse
      .newInstance(succeededRequests, failedRequests);
  }

  @SuppressWarnings("unchecked")
  protected void stopContainerInternal(ContainerId containerID)
      throws YarnException, IOException {
    String containerIDStr = containerID.toString();
    Container container = this.context.getContainers().get(containerID);
    LOG.info("Stopping container with container Id: " + containerIDStr);

    if (container == null) {
      if (!nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " is not handled by this NodeManager");
      }
    } else {
      if (container.isRecovering()) {
        throw new NMNotYetReadyException("Container " + containerIDStr
            + " is recovering, try later");
      }
      context.getNMStateStore().storeContainerKilled(containerID);
      container.sendKillEvent(ContainerExitStatus.KILLED_BY_APPMASTER,
          "Container killed by the ApplicationMaster.");

      NMAuditLogger.logSuccess(container.getUser(),    
        AuditConstants.STOP_CONTAINER, "ContainerManageImpl", containerID
          .getApplicationAttemptId().getApplicationId(), containerID);
    }
  }

  /**
   * Get a list of container statuses running on this NodeManager
   */
  @Override
  public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException, IOException {

    List<ContainerStatus> succeededRequests = new ArrayList<ContainerStatus>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    for (ContainerId id : request.getContainerIds()) {
      try {
        ContainerStatus status = getContainerStatusInternal(id, identifier);
        succeededRequests.add(status);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return GetContainerStatusesResponse.newInstance(succeededRequests,
      failedRequests);
  }

  protected ContainerStatus getContainerStatusInternal(ContainerId containerID,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    String containerIDStr = containerID.toString();
    Container container = this.context.getContainers().get(containerID);

    LOG.info("Getting container-status for " + containerIDStr);
    authorizeGetAndStopContainerRequest(containerID, container, false,
      nmTokenIdentifier);

    if (container == null) {
      if (nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " was recently stopped on node manager.");
      } else {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " is not handled by this NodeManager");
      }
    }
    ContainerStatus containerStatus = container.cloneAndGetContainerStatus();
    logContainerStatus("Returning ", containerStatus);
    return containerStatus;
  }

  private void logContainerStatus(String prefix, ContainerStatus status) {
    StringBuilder sb = new StringBuilder();
    sb.append(prefix);
    sb.append("ContainerStatus: [");
    sb.append("ContainerId: ");
    sb.append(status.getContainerId()).append(", ");
    sb.append("ExecutionType: ");
    sb.append(status.getExecutionType()).append(", ");
    sb.append("State: ");
    sb.append(status.getState()).append(", ");
    sb.append("Capability: ");
    sb.append(status.getCapability()).append(", ");
    sb.append("Diagnostics: ");
    sb.append(LOG.isDebugEnabled() ? status.getDiagnostics() : "...");
    sb.append(", ");
    sb.append("ExitStatus: ");
    sb.append(status.getExitStatus()).append(", ");
    sb.append("IP: ");
    sb.append(status.getIPs()).append(", ");
    sb.append("Host: ");
    sb.append(status.getHost()).append(", ");
    sb.append("ContainerSubState: ");
    sb.append(status.getContainerSubState());
    sb.append("]");
    LOG.info(sb.toString());
  }

  @Private
  @VisibleForTesting
  protected void authorizeGetAndStopContainerRequest(ContainerId containerId,
      Container container, boolean stopRequest, NMTokenIdentifier identifier)
      throws YarnException {
    if (identifier == null) {
      throw RPCUtil.getRemoteException(INVALID_NMTOKEN_MSG);
    }
    /*
     * For get/stop container status; we need to verify that 1) User (NMToken)
     * application attempt only has started container. 2) Requested containerId
     * belongs to the same application attempt (NMToken) which was used. (Note:-
     * This will prevent user in knowing another application's containers).
     */
    ApplicationId nmTokenAppId =
        identifier.getApplicationAttemptId().getApplicationId();
    
    if ((!nmTokenAppId.equals(containerId.getApplicationAttemptId().getApplicationId()))
        || (container != null && !nmTokenAppId.equals(container
            .getContainerId().getApplicationAttemptId().getApplicationId()))) {
      String msg;
      if (stopRequest) {
        msg = identifier.getApplicationAttemptId()
            + " attempted to stop non-application container : "
            + containerId;
        NMAuditLogger.logFailure("UnknownUser", AuditConstants.STOP_CONTAINER,
            "ContainerManagerImpl", "Trying to stop unknown container!",
            nmTokenAppId, containerId);
      } else {
        msg = identifier.getApplicationAttemptId()
            + " attempted to get status for non-application container : "
            + containerId;
      }
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }
  }

  class ContainerEventDispatcher implements EventHandler<ContainerEvent> {
    @Override
    public void handle(ContainerEvent event) {
      Map<ContainerId,Container> containers =
        ContainerManagerImpl.this.context.getContainers();
      Container c = containers.get(event.getContainerID());
      if (c != null) {
        c.handle(event);
        if (nmMetricsPublisher != null) {
          nmMetricsPublisher.publishContainerEvent(event);
        }
      } else {
        LOG.warn("Event " + event + " sent to absent container " +
            event.getContainerID());
      }
    }
  }

  class ApplicationEventDispatcher implements EventHandler<ApplicationEvent> {
    @Override
    public void handle(ApplicationEvent event) {
      Application app =
          ContainerManagerImpl.this.context.getApplications().get(
              event.getApplicationID());
      if (app != null) {
        app.handle(event);
        if (nmMetricsPublisher != null) {
          nmMetricsPublisher.publishApplicationEvent(event);
        }
      } else {
        LOG.warn("Event " + event + " sent to absent application "
            + event.getApplicationID());
      }
    }
  }

  private static final class LocalizationEventHandlerWrapper implements
      EventHandler<LocalizationEvent> {

    private EventHandler<LocalizationEvent> origLocalizationEventHandler;
    private NMTimelinePublisher timelinePublisher;

    LocalizationEventHandlerWrapper(EventHandler<LocalizationEvent> handler,
        NMTimelinePublisher publisher) {
      this.origLocalizationEventHandler = handler;
      this.timelinePublisher = publisher;
    }

    @Override
    public void handle(LocalizationEvent event) {
      origLocalizationEventHandler.handle(event);
      if (timelinePublisher != null) {
        timelinePublisher.publishLocalizationEvent(event);
      }
    }
  }

  /**
   * Implements AuxiliaryLocalPathHandler.
   * It links NodeManager's LocalDirsHandlerService to the Auxiliary Services
   */
  static class AuxiliaryLocalPathHandlerImpl
      implements AuxiliaryLocalPathHandler {
    private LocalDirsHandlerService dirhandlerService;
    AuxiliaryLocalPathHandlerImpl(
        LocalDirsHandlerService dirhandlerService) {
      this.dirhandlerService = dirhandlerService;
    }

    @Override
    public Path getLocalPathForRead(String path) throws IOException {
      return dirhandlerService.getLocalPathForRead(path);
    }

    @Override
    public Path getLocalPathForWrite(String path) throws IOException {
      return dirhandlerService.getLocalPathForWrite(path);
    }

    @Override
    public Path getLocalPathForWrite(String path, long size)
        throws IOException {
      return dirhandlerService.getLocalPathForWrite(path, size, false);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(ContainerManagerEvent event) {
    switch (event.getType()) {
      // 完成 application
      case FINISH_APPS:
        CMgrCompletedAppsEvent appsFinishedEvent = (CMgrCompletedAppsEvent) event;
        for (ApplicationId appID : appsFinishedEvent.getAppsToCleanup()) {
          Application app = this.context.getApplications().get(appID);
          if (app == null) {
            LOG.info("couldn't find application " + appID + " while processing"
                + " FINISH_APPS event. The ResourceManager allocated resources"
                + " for this application to the NodeManager but no active"
                + " containers were found to process.");
            continue;
          }

          boolean shouldDropEvent = false;
          // 获取application引用的Container
          for (Container container : app.getContainers().values()) {
            if (container.isRecovering()) {
              LOG.info("drop FINISH_APPS event to " + appID + " because "
                  + "container " + container.getContainerId()
                  + " is recovering");
              shouldDropEvent = true;
              break;
            }
          }
          if (shouldDropEvent) {
            continue;
          }

          String diagnostic = "";
          if (appsFinishedEvent.getReason() == CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN) {
            // kill
            diagnostic = "Application killed on shutdown";
          } else if (appsFinishedEvent.getReason() == CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER) {
            diagnostic = "Application killed by ResourceManager";
          }
          // 处理完成事件
          this.dispatcher.getEventHandler().handle(  new ApplicationFinishEvent(appID,   diagnostic));
        }
        break;
      case FINISH_CONTAINERS:
        CMgrCompletedContainersEvent containersFinishedEvent = (CMgrCompletedContainersEvent) event;

        // 获取需要清理的 ContainerId
        for (ContainerId containerId : containersFinishedEvent .getContainersToCleanup()) {
          // 获取 Application 并验证Application 是不是坏了
          ApplicationId appId =containerId.getApplicationAttemptId().getApplicationId();
          Application app = this.context.getApplications().get(appId);

          if (app == null) {
            LOG.warn("couldn't find app " + appId + " while processing"
                + " FINISH_CONTAINERS event");
            continue;
          }
          // 获取 Container
          Container container = app.getContainers().get(containerId);
          if (container == null) {
            LOG.warn("couldn't find container " + containerId
                + " while processing FINISH_CONTAINERS event");
            continue;
          }

          if (container.isRecovering()) {
            LOG.info("drop FINISH_CONTAINERS event to " + containerId
                + " because container is recovering");
            continue;
          }
          // 处理 Containe Kill 事件
          this.dispatcher.getEventHandler().handle(
                new ContainerKillEvent(containerId,
                    ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
                    "Container Killed by ResourceManager"));
        }
        break;
      case UPDATE_CONTAINERS:
        CMgrUpdateContainersEvent containersDecreasedEvent = (CMgrUpdateContainersEvent) event;
        for (org.apache.hadoop.yarn.api.records.Container container  : containersDecreasedEvent.getContainersToUpdate()) {
          try {
            // 构建鉴权体系
            ContainerTokenIdentifier containerTokenIdentifier =  BuilderUtils.newContainerTokenIdentifier( container.getContainerToken());

            // 更新 Container
            updateContainerInternal(container.getId(),  containerTokenIdentifier);
          } catch (YarnException e) {
            LOG.error("Unable to decrease container resource", e);
          } catch (IOException e) {
            LOG.error("Unable to update container resource in store", e);
          }
        }
        break;
      case SIGNAL_CONTAINERS:
        // 向 Container 发送信息
        CMgrSignalContainersEvent containersSignalEvent =  (CMgrSignalContainersEvent) event;
        for (SignalContainerRequest request : containersSignalEvent.getContainersToSignal()) {
          internalSignalToContainer(request, "ResourceManager");
        }
        break;
      default:
          throw new YarnRuntimeException(
              "Got an unknown ContainerManagerEvent type: " + event.getType());
    }
  }

  @Override
  public void stateChanged(Service service) {
    // TODO Auto-generated method stub
  }
  
  public Context getContext() {
    return this.context;
  }

  public Map<String, ByteBuffer> getAuxServiceMetaData() {
    return this.auxiliaryServices.getMetaData();
  }

  @Private
  public AMRMProxyService getAMRMProxyService() {
    return this.amrmProxyService;
  }

  @Private
  protected void setAMRMProxyService(AMRMProxyService amrmProxyService) {
    this.amrmProxyService = amrmProxyService;
  }

  protected boolean isServiceStopped() {
    return serviceStopped;
  }

  @Override
  public OpportunisticContainersStatus getOpportunisticContainersStatus() {
    return this.containerScheduler.getOpportunisticContainersStatus();
  }

  @Override
  public void updateQueuingLimit(ContainerQueuingLimit queuingLimit) {
    this.containerScheduler.updateQueuingLimit(queuingLimit);
  }

  @SuppressWarnings("unchecked")
  @Override
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    internalSignalToContainer(request, "Application Master");
    return new SignalContainerResponsePBImpl();
  }

  @Override
  @SuppressWarnings("unchecked")
  public ResourceLocalizationResponse localize(
      ResourceLocalizationRequest request) throws YarnException, IOException {

    ContainerId containerId = request.getContainerId();
    Container container = preReInitializeOrLocalizeCheck(containerId,
        ReInitOp.LOCALIZE);
    try {
      Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
          container.getResourceSet().addResources(request.getLocalResources());
      if (req != null && !req.isEmpty()) {
        dispatcher.getEventHandler()
            .handle(new ContainerLocalizationRequestEvent(container, req));
      }
    } catch (URISyntaxException e) {
      LOG.info("Error when parsing local resource URI for " + containerId, e);
      throw new YarnException(e);
    }

    return ResourceLocalizationResponse.newInstance();
  }

  @Override
  public ReInitializeContainerResponse reInitializeContainer(
      ReInitializeContainerRequest request) throws YarnException, IOException {
    reInitializeContainer(request.getContainerId(),
        request.getContainerLaunchContext(), request.getAutoCommit());
    return ReInitializeContainerResponse.newInstance();
  }

  @Override
  public RestartContainerResponse restartContainer(ContainerId containerId)
      throws YarnException, IOException {
    reInitializeContainer(containerId, null, true);
    return RestartContainerResponse.newInstance();
  }

  /**
   * ReInitialize a container using a new Launch Context. If the
   * retryFailureContext is not provided, The container is
   * terminated on Failure.
   *
   * NOTE: Auto-Commit is true by default. This also means that the rollback
   *       context is purged as soon as the command to start the new process
   *       is sent. (The Container moves to RUNNING state)
   *
   * @param containerId Container Id.
   * @param autoCommit Auto Commit flag.
   * @param reInitLaunchContext Target Launch Context.
   * @throws YarnException YARN Exception.
   */
  public void reInitializeContainer(ContainerId containerId,
      ContainerLaunchContext reInitLaunchContext, boolean autoCommit)
      throws YarnException {
    LOG.debug("{} requested reinit", containerId);
    Container container = preReInitializeOrLocalizeCheck(containerId,
        ReInitOp.RE_INIT);
    ResourceSet resourceSet = new ResourceSet();
    try {
      if (reInitLaunchContext != null) {
        resourceSet.addResources(reInitLaunchContext.getLocalResources());
      }
      dispatcher.getEventHandler().handle(
          new ContainerReInitEvent(containerId, reInitLaunchContext,
              resourceSet, autoCommit));
      container.setIsReInitializing(true);
    } catch (URISyntaxException e) {
      LOG.info("Error when parsing local resource URI for upgrade of" +
          "Container [" + containerId + "]", e);
      throw new YarnException(e);
    }
  }

  /**
   * Rollback the last reInitialization, if possible.
   * @param containerId Container ID.
   * @return Rollback Response.
   * @throws YarnException YARN Exception.
   */
  @Override
  public RollbackResponse rollbackLastReInitialization(ContainerId containerId)
      throws YarnException {
    Container container = preReInitializeOrLocalizeCheck(containerId,
        ReInitOp.ROLLBACK);
    if (container.canRollback()) {
      dispatcher.getEventHandler().handle(
          new ContainerEvent(containerId, ContainerEventType.ROLLBACK_REINIT));
      container.setIsReInitializing(true);
    } else {
      throw new YarnException("Nothing to rollback to !!");
    }
    return RollbackResponse.newInstance();
  }

  /**
   * Commit last reInitialization after which no rollback will be possible.
   * @param containerId Container ID.
   * @return Commit Response.
   * @throws YarnException YARN Exception.
   */
  @Override
  public CommitResponse commitLastReInitialization(ContainerId containerId)
      throws YarnException {
    Container container = preReInitializeOrLocalizeCheck(containerId,
        ReInitOp.COMMIT);
    if (container.canRollback()) {
      container.commitUpgrade();
    } else {
      throw new YarnException("Nothing to Commit !!");
    }
    return CommitResponse.newInstance();
  }

  private Container preReInitializeOrLocalizeCheck(ContainerId containerId,
      ReInitOp op) throws YarnException {
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi, nmTokenIdentifier);
    if (!nmTokenIdentifier.getApplicationAttemptId().getApplicationId()
        .equals(containerId.getApplicationAttemptId().getApplicationId())) {
      throw new YarnException("ApplicationMaster not authorized to perform " +
          "["+ op + "] on Container [" + containerId + "]!!");
    }
    Container container = context.getContainers().get(containerId);
    if (container == null) {
      throw new YarnException("Specified " + containerId + " does not exist!");
    }
    if (!container.isRunning() || container.isReInitializing()
        || container.getContainerTokenIdentifier().getExecutionType()
        == ExecutionType.OPPORTUNISTIC) {
      throw new YarnException("Cannot perform " + op + " on [" + containerId
          + "]. Current state is [" + container.getContainerState() + ", " +
          "isReInitializing=" + container.isReInitializing() + "]. Container"
          + " Execution Type is [" + container.getContainerTokenIdentifier()
          .getExecutionType() + "].");
    }
    return container;
  }

  @SuppressWarnings("unchecked")
  private void internalSignalToContainer(SignalContainerRequest request,
      String sentBy) {
    // 获取容器
    ContainerId containerId = request.getContainerId();
    Container container = this.context.getContainers().get(containerId);
    if (container != null) {
      LOG.info(containerId + " signal request " + request.getCommand()
            + " by " + sentBy);

      // 发送通知事件
      this.dispatcher.getEventHandler().handle(
          new SignalContainersLauncherEvent(container,
              request.getCommand()));
    } else {
      LOG.info("Container " + containerId + " no longer exists");
    }
  }

  @Override
  public ContainerScheduler getContainerScheduler() {
    return this.containerScheduler;
  }

  @Override
  public void handleCredentialUpdate() {
    Set<ApplicationId> invalidApps = logHandler.getInvalidTokenApps();
    if (!invalidApps.isEmpty()) {
      dispatcher.getEventHandler().handle(new LogHandlerTokenUpdatedEvent());
    }
  }
}
