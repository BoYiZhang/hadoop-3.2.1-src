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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import org.apache.hadoop.yarn.server.nodemanager.recovery.RecoveryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.DiskValidator;
import org.apache.hadoop.util.DiskValidatorFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.impl.pb.LocalResourcePBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.LocalResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.DirectoryCollection.DirsChangeListener;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.api.LocalizationProtocol;
import org.apache.hadoop.yarn.server.nodemanager.api.ResourceLocalizationSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerAction;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerHeartbeatResponse;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalizerStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceFailedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalCacheCleaner.LocalCacheCleanerStats;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ApplicationLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizerResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceFailedLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceLocalizedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRecoveredEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceReleaseEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.security.LocalizerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.LocalResourceTrackerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredLocalizationState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredUserResources;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerBuilderUtils;
import org.apache.hadoop.yarn.util.FSDownload;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ResourceLocalizationService extends CompositeService
    implements EventHandler<LocalizationEvent>, LocalizationProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceLocalizationService.class);

  // 私有目录
  public static final String NM_PRIVATE_DIR = "nmPrivate";

  // 私有目录权限 700
  public static final FsPermission NM_PRIVATE_PERM = new FsPermission((short) 0700);

  // 共有目录权限 755
  private static final FsPermission PUBLIC_FILECACHE_FOLDER_PERMS = new FsPermission((short) 0755);

  //  0.0.0.0 : 8040
  private Server server;
  // BoYi-Pro.local/192.168.8.188:8040
  private InetSocketAddress localizationServerAddress;

  @VisibleForTesting
  // 缓存大小 :  10G
  long cacheTargetSize;
  // 缓存清理周期 10min
  private long cacheCleanupPeriod;
  // DefaultContainerExecutor  or  LinuxContainerExecutor
  private final ContainerExecutor exec;
  // AsyncDispatcher
  protected final Dispatcher dispatcher;
  // org.apache.hadoop.yarn.server.nodemanager.DeletionService
  private final DeletionService delService;


  // Service org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService$LocalizerTracker
  private LocalizerTracker localizerTracker;

  private RecordFactory recordFactory;

  // org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor@24c4ddae[Running, pool size = 1, active threads = 0, queued tasks = 1, completed tasks = 0]
  private final ScheduledExecutorService cacheCleanup;
  // 权限相关
  private LocalizerTokenSecretManager secretManager;
  // 持久化存储
  private NMStateStoreService stateStore;


  @VisibleForTesting
  // 度量数据
  final NodeManagerMetrics metrics;

  @VisibleForTesting
  // LocalResourcesTrackerImpl
  LocalResourcesTracker publicRsrc;

  private LocalDirsHandlerService dirsHandler;

  private DirsChangeListener localDirsChangeListener;

  private DirsChangeListener logDirsChangeListener;


  //  NodeManager#NMContext 信息
  //
  //  nodeId = {NodeIdPBImpl@3904} "192.168.8.188:57344"
  //  conf = {YarnConfiguration@3079} "Configuration: core-default.xml, core-site.xml, yarn-default.xml, yarn-site.xml, resource-types.xml"
  //  metrics = {NodeManagerMetrics@2120}
  //  applications = {ConcurrentHashMap@3905}  size = 0
  //  systemCredentials = {HashMap@3906}  size = 0
  //  containers = {ConcurrentSkipListMap@3907}  size = 0
  //  registeringCollectors = null
  //  knownCollectors = null
  //  increasedContainers = {ConcurrentHashMap@3908}  size = 0
  //  containerTokenSecretManager = {NMContainerTokenSecretManager@3909}
  //  nmTokenSecretManager = {NMTokenSecretManagerInNM@3910}
  //  containerManager = {ContainerManagerImpl@2592} "Service org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl in state org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl: STARTED"
  //  nodeResourceMonitor = {NodeResourceMonitorImpl@3911} "Service org.apache.hadoop.yarn.server.nodemanager.NodeResourceMonitorImpl in state org.apache.hadoop.yarn.server.nodemanager.NodeResourceMonitorImpl: STARTED"
  //  dirsHandler = {LocalDirsHandlerService@2118} "Service org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService in state org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService: STARTED"
  //  aclsManager = {ApplicationACLsManager@3912}
  //  webServer = {WebServer@3913} "Service org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer in state org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer: INITED"
  //  nodeHealthStatus = {NodeHealthStatusPBImpl@3914} "is_node_healthy: true health_report: "Healthy" last_health_report_time: 1612342130099"
  //  stateStore = {NMNullStateStoreService@2122} "Service org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService in state org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService: STARTED"
  //  isDecommissioned = false
  //  logAggregationReportForApps = {ConcurrentLinkedQueue@3915}  size = 0
  //  nodeStatusUpdater = {NodeStatusUpdaterImpl@3832} "Service org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl in state org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl: INITED"
  //  isDistSchedulingEnabled = false
  //  deletionService = {DeletionService@2117} "Service org.apache.hadoop.yarn.server.nodemanager.DeletionService in state org.apache.hadoop.yarn.server.nodemanager.DeletionService: STARTED"
  //  containerAllocator = {OpportunisticContainerAllocator@3916}
  //  executor = {DefaultContainerExecutor@2115}
  //  nmTimelinePublisher = null
  //  containerStateTransitionListener = {NodeManager$DefaultContainerStateListener@3917}
  //  resourcePluginManager = {ResourcePluginManager@3918}
  //  nmLogAggregationStatusTracker = {NMLogAggregationStatusTracker@3919} "Service org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker in state org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker: INITED"
  private Context nmContext;

  // BasicDiskValidator
  private DiskValidator diskValidator;

  /**
   * 每个用户一个 LocalResourcesTracker ??????
   * 用户名 -> LocalResourcesTracker
   * Map of LocalResourceTrackers keyed by username, for private resources.
   */
  @VisibleForTesting
  final ConcurrentMap<String, LocalResourcesTracker> privateRsrc =  new ConcurrentHashMap<String,LocalResourcesTracker>();

  /**
   * appid -> LocalResourcesTracker ??????
   * Map of LocalResourceTrackers keyed by appid, for application resources.
   */
  private final ConcurrentMap<String,LocalResourcesTracker> appRsrc = new ConcurrentHashMap<String,LocalResourcesTracker>();

  //  FileContext 中的信息
  //
  //  defaultFS = {LocalFs@3928}
  //  workingDir = {Path@3929} "file:/opt/workspace/apache/hadoop-3.2.1-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager"
  //  umask = {FsPermission@3930} "----w--w-"
  //  conf = {YarnConfiguration@3079} "Configuration: core-default.xml, core-site.xml, yarn-default.xml, yarn-site.xml, resource-types.xml"
  //  ugi = {UserGroupInformation@3931} "henghe (auth:SIMPLE)"
  //  resolveSymlinks = true
  //  tracer = {Tracer@3932} "Tracer(FSClient/192.168.8.188)"
  //  util = {FileContext$Util@3933}
  FileContext lfs;

  public ResourceLocalizationService(Dispatcher dispatcher,
      ContainerExecutor exec, DeletionService delService,
      LocalDirsHandlerService dirsHandler, Context context,
      NodeManagerMetrics metrics) {

    super(ResourceLocalizationService.class.getName());
    // DefaultContainerExecutor  or  LinuxContainerExecutor
    this.exec = exec;
    // AsyncDispatcher
    this.dispatcher = dispatcher;
    // org.apache.hadoop.yarn.server.nodemanager.DeletionService
    this.delService = delService;
    this.dirsHandler = dirsHandler;

    // org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor@24c4ddae[Running, pool size = 1, active threads = 0, queued tasks = 1, completed tasks = 0]
    this.cacheCleanup = new HadoopScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder()
          .setNameFormat("ResourceLocalizationService Cache Cleanup")
          .build());

    this.stateStore = context.getNMStateStore();


    //  NodeManager#NMContext 信息
    //
    //  nodeId = {NodeIdPBImpl@3904} "192.168.8.188:57344"
    //  conf = {YarnConfiguration@3079} "Configuration: core-default.xml, core-site.xml, yarn-default.xml, yarn-site.xml, resource-types.xml"
    //  metrics = {NodeManagerMetrics@2120}
    //  applications = {ConcurrentHashMap@3905}  size = 0
    //  systemCredentials = {HashMap@3906}  size = 0
    //  containers = {ConcurrentSkipListMap@3907}  size = 0
    //  registeringCollectors = null
    //  knownCollectors = null
    //  increasedContainers = {ConcurrentHashMap@3908}  size = 0
    //  containerTokenSecretManager = {NMContainerTokenSecretManager@3909}
    //  nmTokenSecretManager = {NMTokenSecretManagerInNM@3910}
    //  containerManager = {ContainerManagerImpl@2592} "Service org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl in state org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl: STARTED"
    //  nodeResourceMonitor = {NodeResourceMonitorImpl@3911} "Service org.apache.hadoop.yarn.server.nodemanager.NodeResourceMonitorImpl in state org.apache.hadoop.yarn.server.nodemanager.NodeResourceMonitorImpl: STARTED"
    //  dirsHandler = {LocalDirsHandlerService@2118} "Service org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService in state org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService: STARTED"
    //  aclsManager = {ApplicationACLsManager@3912}
    //  webServer = {WebServer@3913} "Service org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer in state org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer: INITED"
    //  nodeHealthStatus = {NodeHealthStatusPBImpl@3914} "is_node_healthy: true health_report: "Healthy" last_health_report_time: 1612342130099"
    //  stateStore = {NMNullStateStoreService@2122} "Service org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService in state org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService: STARTED"
    //  isDecommissioned = false
    //  logAggregationReportForApps = {ConcurrentLinkedQueue@3915}  size = 0
    //  nodeStatusUpdater = {NodeStatusUpdaterImpl@3832} "Service org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl in state org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl: INITED"
    //  isDistSchedulingEnabled = false
    //  deletionService = {DeletionService@2117} "Service org.apache.hadoop.yarn.server.nodemanager.DeletionService in state org.apache.hadoop.yarn.server.nodemanager.DeletionService: STARTED"
    //  containerAllocator = {OpportunisticContainerAllocator@3916}
    //  executor = {DefaultContainerExecutor@2115}
    //  nmTimelinePublisher = null
    //  containerStateTransitionListener = {NodeManager$DefaultContainerStateListener@3917}
    //  resourcePluginManager = {ResourcePluginManager@3918}
    //  nmLogAggregationStatusTracker = {NMLogAggregationStatusTracker@3919} "Service org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker in state org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker: INITED"
    this.nmContext = context;

    // 度量数据
    this.metrics = metrics;
  }

  FileContext getLocalFileContext(Configuration conf) {
    try {
      return FileContext.getLocalFSFileContext(conf);
    } catch (IOException e) {
      throw new YarnRuntimeException("Failed to access local fs");
    }
  }

  private void validateConf(Configuration conf) {
    int perDirFileLimit =
        conf.getInt(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY,
          YarnConfiguration.DEFAULT_NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY);
    if (perDirFileLimit <= 36) {
      LOG.error(YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
          + " parameter is configured with very low value.");
      throw new YarnRuntimeException(
        YarnConfiguration.NM_LOCAL_CACHE_MAX_FILES_PER_DIRECTORY
            + " parameter is configured with a value less than 37.");
    } else {
      LOG.info("per directory file limit = " + perDirFileLimit);
    }
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.validateConf(conf);

    this.publicRsrc = new LocalResourcesTrackerImpl(null, null, dispatcher, true, conf, stateStore, dirsHandler);

    this.recordFactory = RecordFactoryProvider.getRecordFactory(conf);

    try {
      // 构建本地文件系统
      lfs = getLocalFileContext(conf);
      // 设置权限  : 755
      // Umask 为022表示默bai认创建新文du件权限为755
      lfs.setUMask(new FsPermission((short) FsPermission.DEFAULT_UMASK));

      if (!stateStore.canRecover()|| stateStore.isNewlyCreated()) {
        cleanUpLocalDirs(lfs, delService);
        cleanupLogDirs(lfs, delService);

        // ${yarn.nodemanager.local-dirs} : /opt/tools/hadoop-3.2.1/local-dirs
        initializeLocalDirs(lfs);
        //  ${yarn.nodemanager.log-dirs} : /opt/tools/hadoop-3.2.1/logs/userlogs
        initializeLogDirs(lfs);
      }
    } catch (Exception e) {
      throw new YarnRuntimeException(
        "Failed to initialize LocalizationService", e);
    }

    diskValidator = DiskValidatorFactory.getInstance(YarnConfiguration.DEFAULT_DISK_VALIDATOR);

    // 缓存大小 :  10G
    cacheTargetSize = conf.getLong(YarnConfiguration.NM_LOCALIZER_CACHE_TARGET_SIZE_MB, YarnConfiguration.DEFAULT_NM_LOCALIZER_CACHE_TARGET_SIZE_MB) << 20;

    // 清理周期 10min
    cacheCleanupPeriod =  conf.getLong(YarnConfiguration.NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS, YarnConfiguration.DEFAULT_NM_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS);

    // yarn.nodemanager.bind-host
    // yarn.nodemanager.localizer.address  :  0.0.0.0:8040
    localizationServerAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_PORT);

    // ResourceLocalizationService$LocalizerTracker
    localizerTracker = createLocalizerTracker(conf);


    addService(localizerTracker);


    dispatcher.register(LocalizerEventType.class, localizerTracker);


    localDirsChangeListener = new DirsChangeListener() {
      @Override
      public void onDirsChanged() {
        checkAndInitializeLocalDirs();
      }
    };


    logDirsChangeListener = new DirsChangeListener() {
      @Override
      public void onDirsChanged() {
        initializeLogDirs(lfs);
      }
    };
    super.serviceInit(conf);
  }

  //Recover localized resources after an NM restart
  public void recoverLocalizedResources(RecoveredLocalizationState state)
      throws URISyntaxException, IOException {
    LocalResourceTrackerState trackerState = state.getPublicTrackerState();
    recoverTrackerResources(publicRsrc, trackerState);

    try (RecoveryIterator<Map.Entry<String, RecoveredUserResources>> it
             = state.getIterator()) {
      while (it.hasNext()) {
        Map.Entry<String, RecoveredUserResources> userEntry = it.next();
        String user = userEntry.getKey();
        RecoveredUserResources userResources = userEntry.getValue();
        trackerState = userResources.getPrivateTrackerState();
        LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user,
            null, dispatcher, true, super.getConfig(), stateStore,
            dirsHandler);
        LocalResourcesTracker oldTracker = privateRsrc.putIfAbsent(user,
            tracker);
        if (oldTracker != null) {
          tracker = oldTracker;
        }
        recoverTrackerResources(tracker, trackerState);

        for (Map.Entry<ApplicationId, LocalResourceTrackerState> appEntry :
            userResources.getAppTrackerStates().entrySet()) {
          trackerState = appEntry.getValue();
          ApplicationId appId = appEntry.getKey();
          String appIdStr = appId.toString();
          LocalResourcesTracker tracker1 = new LocalResourcesTrackerImpl(user,
              appId, dispatcher, false, super.getConfig(), stateStore,
              dirsHandler);
          LocalResourcesTracker oldTracker1 = appRsrc.putIfAbsent(appIdStr,
              tracker1);
          if (oldTracker1 != null) {
            tracker1 = oldTracker1;
          }
          recoverTrackerResources(tracker1, trackerState);
        }
      }
    }
  }

  private void recoverTrackerResources(LocalResourcesTracker tracker,
      LocalResourceTrackerState state) throws URISyntaxException, IOException {
    try (RecoveryIterator<LocalizedResourceProto> it =
             state.getCompletedResourcesIterator()) {
      while (it != null && it.hasNext()) {
        LocalizedResourceProto proto = it.next();
        LocalResource rsrc = new LocalResourcePBImpl(proto.getResource());
        LocalResourceRequest req = new LocalResourceRequest(rsrc);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Recovering localized resource " + req + " at "
              + proto.getLocalPath());
        }
        tracker.handle(new ResourceRecoveredEvent(req,
            new Path(proto.getLocalPath()), proto.getSize()));
      }
    }

    try (RecoveryIterator<Map.Entry<LocalResourceProto, Path>> it =
             state.getStartedResourcesIterator()) {
      while (it != null && it.hasNext()) {
        Map.Entry<LocalResourceProto, Path> entry = it.next();
        LocalResource rsrc = new LocalResourcePBImpl(entry.getKey());
        LocalResourceRequest req = new LocalResourceRequest(rsrc);
        Path localPath = entry.getValue();
        tracker.handle(new ResourceRecoveredEvent(req, localPath, 0));

        // delete any in-progress localizations, containers will request again
        LOG.info("Deleting in-progress localization for " + req + " at "
            + localPath);
        tracker.remove(tracker.getLocalizedResource(req), delService);
      }
    }

    // TODO: remove untracked directories in local filesystem
  }

  @Override
  public LocalizerHeartbeatResponse heartbeat(LocalizerStatus status) {
    return localizerTracker.processHeartbeat(status);
  }

  @Override
  public void serviceStart() throws Exception {

    // 开启定时清理 : 每10min执行一次...
    cacheCleanup.scheduleWithFixedDelay(new CacheCleanup(dispatcher), cacheCleanupPeriod, cacheCleanupPeriod, TimeUnit.MILLISECONDS);

    // 构建RPC 服务
    server = createServer();
    // 启动RPC服务.
    server.start();

    // 更新localizationServer地址...
    // BoYi-Pro.local/192.168.8.188:8040
    localizationServerAddress =
        getConfig().updateConnectAddr(YarnConfiguration.NM_BIND_HOST,
                                      YarnConfiguration.NM_LOCALIZER_ADDRESS,
                                      YarnConfiguration.DEFAULT_NM_LOCALIZER_ADDRESS,
                                      server.getListenerAddress());
    LOG.info("Localizer started on port " + server.getPort());


    super.serviceStart();

    // 注册监听程序
    dirsHandler.registerLocalDirsChangeListener(localDirsChangeListener);

    // 注册监听程序
    dirsHandler.registerLogDirsChangeListener(logDirsChangeListener);
  }

  LocalizerTracker createLocalizerTracker(Configuration conf) {
    return new LocalizerTracker(conf);
  }

  Server createServer() {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);
    if (UserGroupInformation.isSecurityEnabled()) {
      secretManager = new LocalizerTokenSecretManager();      
    }

    // yarn.nodemanager.localizer.client.thread-count : 5
    Server server = rpc.getServer(LocalizationProtocol.class, this,
        localizationServerAddress, conf, secretManager, 
        conf.getInt(YarnConfiguration.NM_LOCALIZER_CLIENT_THREAD_COUNT, 
            YarnConfiguration.DEFAULT_NM_LOCALIZER_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      server.refreshServiceAcl(conf, NMPolicyProvider.getInstance());
    }
    
    return server;
  }

  @Override
  public void serviceStop() throws Exception {
    dirsHandler.deregisterLocalDirsChangeListener(localDirsChangeListener);
    dirsHandler.deregisterLogDirsChangeListener(logDirsChangeListener);
    if (server != null) {
      server.stop();
    }
    cacheCleanup.shutdown();
    super.serviceStop();
  }

  @Override
  public void handle(LocalizationEvent event) {
    // TODO: create log dir as $logdir/$user/$appId
    switch (event.getType()) {
    case INIT_APPLICATION_RESOURCES:
      handleInitApplicationResources(
          ((ApplicationLocalizationEvent)event).getApplication());
      break;
    case LOCALIZE_CONTAINER_RESOURCES:
      handleInitContainerResources((ContainerLocalizationRequestEvent) event);
      break;
    case CONTAINER_RESOURCES_LOCALIZED:
      handleContainerResourcesLocalized((ContainerLocalizationEvent) event);
      break;
    case CACHE_CLEANUP:
      // 执行清理操作...
      handleCacheCleanup();
      break;
    case CLEANUP_CONTAINER_RESOURCES:
      handleCleanupContainerResources((ContainerLocalizationCleanupEvent)event);
      break;
    case DESTROY_APPLICATION_RESOURCES:
      handleDestroyApplicationResources(
          ((ApplicationLocalizationEvent)event).getApplication());
      break;
    default:
      throw new YarnRuntimeException("Unknown localization event: " + event);
    }
  }
  
  /**
   * Handle event received the first time any container is scheduled
   * by a given application.
   */
  @SuppressWarnings("unchecked")
  private void handleInitApplicationResources(Application app) {
    // 0) Create application tracking structs
    String userName = app.getUser();
    privateRsrc.putIfAbsent(userName, new LocalResourcesTrackerImpl(userName,
        null, dispatcher, true, super.getConfig(), stateStore, dirsHandler));
    String appIdStr = app.getAppId().toString();
    appRsrc.putIfAbsent(appIdStr, new LocalResourcesTrackerImpl(app.getUser(),
        app.getAppId(), dispatcher, false, super.getConfig(), stateStore,
        dirsHandler));
    // 1) Signal container init
    //
    // This is handled by the ApplicationImpl state machine and allows
    // containers to proceed with launching.
    dispatcher.getEventHandler().handle(new ApplicationInitedEvent(
          app.getAppId()));
  }
  
  /**
   * For each of the requested resources for a container, determines the
   * appropriate {@link LocalResourcesTracker} and forwards a 
   * {@link LocalResourceRequest} to that tracker.
   */
  private void handleInitContainerResources(
      ContainerLocalizationRequestEvent rsrcReqs) {
    Container c = rsrcReqs.getContainer();
    EnumSet<ContainerState> set =
        EnumSet.of(ContainerState.LOCALIZING,
            ContainerState.RUNNING, ContainerState.REINITIALIZING);
    if (!set.contains(c.getContainerState())) {
      LOG.warn(c.getContainerId() + " is at " + c.getContainerState()
          + " state, do not localize resources.");
      return;
    }
    // create a loading cache for the file statuses
    LoadingCache<Path,Future<FileStatus>> statCache =
        CacheBuilder.newBuilder().build(FSDownload.createStatusCacheLoader(getConfig()));
    LocalizerContext ctxt = new LocalizerContext(
        c.getUser(), c.getContainerId(), c.getCredentials(), statCache);
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs =
      rsrcReqs.getRequestedResources();
    for (Map.Entry<LocalResourceVisibility, Collection<LocalResourceRequest>> e :
         rsrcs.entrySet()) {
      LocalResourcesTracker tracker =
          getLocalResourcesTracker(e.getKey(), c.getUser(),
              c.getContainerId().getApplicationAttemptId()
                  .getApplicationId());
      for (LocalResourceRequest req : e.getValue()) {
        tracker.handle(new ResourceRequestEvent(req, e.getKey(), ctxt));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Localizing " + req.getPath() +
              " for container " + c.getContainerId());
        }
      }
    }
  }

  /**
   * Once a container's resources are localized, kill the corresponding
   * {@link ContainerLocalizer}
   */
  private void handleContainerResourcesLocalized(
      ContainerLocalizationEvent event) {
    Container c = event.getContainer();
    String locId = c.getContainerId().toString();
    localizerTracker.endContainerLocalization(locId);
  }

  @VisibleForTesting
  LocalCacheCleanerStats handleCacheCleanup() {

    //构建LocalCacheCleaner
    LocalCacheCleaner cleaner = new LocalCacheCleaner(delService, cacheTargetSize);

    cleaner.addResources(publicRsrc);


    for (LocalResourcesTracker t : privateRsrc.values()) {
      cleaner.addResources(t);
    }


    LocalCacheCleaner.LocalCacheCleanerStats stats = cleaner.cleanCache();


    if (LOG.isDebugEnabled()) {
      LOG.debug(stats.toStringDetailed());
    } else if (LOG.isInfoEnabled()) {
      LOG.info(stats.toString());
    }

    // Update metrics
    metrics.setCacheSizeBeforeClean(stats.getCacheSizeBeforeClean());
    metrics.setTotalBytesDeleted(stats.getTotalDelSize());
    metrics.setPrivateBytesDeleted(stats.getPrivateDelSize());
    metrics.setPublicBytesDeleted(stats.getPublicDelSize());
    return stats;
  }


  @SuppressWarnings("unchecked")
  private void handleCleanupContainerResources(
      ContainerLocalizationCleanupEvent rsrcCleanup) {
    Container c = rsrcCleanup.getContainer();
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs =
      rsrcCleanup.getResources();
    for (Map.Entry<LocalResourceVisibility, Collection<LocalResourceRequest>> e :
         rsrcs.entrySet()) {
      LocalResourcesTracker tracker = getLocalResourcesTracker(e.getKey(), c.getUser(),
          c.getContainerId().getApplicationAttemptId()
          .getApplicationId());
      for (LocalResourceRequest req : e.getValue()) {
        tracker.handle(new ResourceReleaseEvent(req,
            c.getContainerId()));
      }
    }
    String locId = c.getContainerId().toString();
    localizerTracker.cleanupPrivLocalizers(locId);

    // Delete the container directories
    String userName = c.getUser();
    String containerIDStr = c.toString();
    String appIDStr =
        c.getContainerId().getApplicationAttemptId().getApplicationId()
            .toString();
    
    // Try deleting from good local dirs and full local dirs because a dir might
    // have gone bad while the app was running(disk full). In addition
    // a dir might have become good while the app was running.
    // Check if the container dir exists and if it does, try to delete it

    for (String localDir : dirsHandler.getLocalDirsForCleanup()) {
      // Delete the user-owned container-dir
      Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, userName);
      Path allAppsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(allAppsdir, appIDStr);
      Path containerDir = new Path(appDir, containerIDStr);
      submitDirForDeletion(userName, containerDir);

      // Delete the nmPrivate container-dir

      Path sysDir = new Path(localDir, NM_PRIVATE_DIR);
      Path appSysDir = new Path(sysDir, appIDStr);
      Path containerSysDir = new Path(appSysDir, containerIDStr);
      submitDirForDeletion(null, containerSysDir);
    }

    dispatcher.getEventHandler().handle(
        new ContainerEvent(c.getContainerId(),
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP));
  }
  
  private void submitDirForDeletion(String userName, Path dir) {
    try {
      lfs.getFileStatus(dir);
      FileDeletionTask deletionTask = new FileDeletionTask(delService, userName,
          dir, null);
      delService.delete(deletionTask);
    } catch (UnsupportedFileSystemException ue) {
      LOG.warn("Local dir " + dir + " is an unsupported filesystem", ue);
    } catch (IOException ie) {
      // ignore
      return;
    }
  }


  @SuppressWarnings({"unchecked"})
  private void handleDestroyApplicationResources(Application application) {
    String userName = application.getUser();
    ApplicationId appId = application.getAppId();
    String appIDStr = application.toString();
    LocalResourcesTracker appLocalRsrcsTracker =
      appRsrc.remove(appId.toString());
    if (appLocalRsrcsTracker != null) {
      for (LocalizedResource rsrc : appLocalRsrcsTracker ) {
        Path localPath = rsrc.getLocalPath();
        if (localPath != null) {
          try {
            stateStore.removeLocalizedResource(userName, appId, localPath);
          } catch (IOException e) {
            LOG.error("Unable to remove resource " + rsrc + " for " + appIDStr
                + " from state store", e);
          }
        }
      }
    } else {
      LOG.warn("Removing uninitialized application " + application);
    }

    // Delete the application directories
    userName = application.getUser();
    appIDStr = application.toString();

    for (String localDir : dirsHandler.getLocalDirsForCleanup()) {

      // Delete the user-owned app-dir
      Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);
      Path userdir = new Path(usersdir, userName);
      Path allAppsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
      Path appDir = new Path(allAppsdir, appIDStr);
      submitDirForDeletion(userName, appDir);

      // Delete the nmPrivate app-dir
      Path sysDir = new Path(localDir, NM_PRIVATE_DIR);
      Path appSysDir = new Path(sysDir, appIDStr);
      submitDirForDeletion(null, appSysDir);
    }

    // TODO: decrement reference counts of all resources associated with this
    // app

    dispatcher.getEventHandler().handle(new ApplicationEvent(
          application.getAppId(),
          ApplicationEventType.APPLICATION_RESOURCES_CLEANEDUP));
  }


  LocalResourcesTracker getLocalResourcesTracker(
      LocalResourceVisibility visibility, String user, ApplicationId appId) {
    switch (visibility) {
      default:
      case PUBLIC:
        return publicRsrc;
      case PRIVATE:
        return privateRsrc.get(user);
      case APPLICATION:
        return appRsrc.get(appId.toString());
    }
  }

  private String getUserFileCachePath(String user) {
    return StringUtils.join(Path.SEPARATOR, Arrays.asList(".",
      ContainerLocalizer.USERCACHE, user, ContainerLocalizer.FILECACHE));

  }

  private String getAppFileCachePath(String user, String appId) {
    return StringUtils.join(Path.SEPARATOR, Arrays.asList(".",
        ContainerLocalizer.USERCACHE, user, ContainerLocalizer.APPCACHE, appId,
        ContainerLocalizer.FILECACHE));
  }
  
  @VisibleForTesting
  @Private
  public PublicLocalizer getPublicLocalizer() {
    return localizerTracker.publicLocalizer;
  }

  @VisibleForTesting
  @Private
  public LocalizerRunner getLocalizerRunner(String locId) {
    return localizerTracker.privLocalizers.get(locId);
  }
  
  @VisibleForTesting
  @Private
  public Map<String, LocalizerRunner> getPrivateLocalizers() {
    return localizerTracker.privLocalizers;
  }
  
  /**
   * Sub-component handling the spawning of {@link ContainerLocalizer}s
   */
  class LocalizerTracker extends AbstractService implements EventHandler<LocalizerEvent>  {


    // 共有
    private final PublicLocalizer publicLocalizer;


    // 私有
    private final Map<String,LocalizerRunner> privLocalizers;

    LocalizerTracker(Configuration conf) {
      this(conf, new HashMap<String,LocalizerRunner>());
    }

    LocalizerTracker(Configuration conf,
        Map<String,LocalizerRunner> privLocalizers) {
      super(LocalizerTracker.class.getName());
      this.publicLocalizer = new PublicLocalizer(conf);
      this.privLocalizers = privLocalizers;
    }
    
    @Override
    public synchronized void serviceStart() throws Exception {
      publicLocalizer.start();
      super.serviceStart();
    }

    public LocalizerHeartbeatResponse processHeartbeat(LocalizerStatus status) {
      String locId = status.getLocalizerId();
      synchronized (privLocalizers) {
        // 获取LocalizerRunner
        LocalizerRunner localizer = privLocalizers.get(locId);
        if (null == localizer) {
          // TODO process resources anyway
          LOG.info("Unknown localizer with localizerId " + locId
              + " is sending heartbeat. Ordering it to DIE");
          LocalizerHeartbeatResponse response =
            recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
          response.setLocalizerAction(LocalizerAction.DIE);
          return response;
        }
        // 处理心跳数据
        return localizer.processHeartbeat(status.getResources());
      }
    }
    
    @Override
    public void serviceStop() throws Exception {
      for (LocalizerRunner localizer : privLocalizers.values()) {
        localizer.interrupt();
      }
      publicLocalizer.interrupt();
      super.serviceStop();
    }

    @Override
    public void handle(LocalizerEvent event) {
      String locId = event.getLocalizerId();
      switch (event.getType()) {
      case REQUEST_RESOURCE_LOCALIZATION:
        // 0) find running localizer or start new thread
        LocalizerResourceRequestEvent req = (LocalizerResourceRequestEvent)event;


        // 根据可见性进行处理
        switch (req.getVisibility()) {
        case PUBLIC:
          publicLocalizer.addResource(req);
          break;
        case PRIVATE:
        case APPLICATION:
          synchronized (privLocalizers) {
            // 获取 LocalizerRunner
            LocalizerRunner localizer = privLocalizers.get(locId);

            if (localizer != null && localizer.killContainerLocalizer.get()) {
              // Old localizer thread has been stopped, remove it and creates a new localizer thread.
              LOG.info("New " + event.getType() + " localize request for "
                  + locId + ", remove old private localizer.");
              cleanupPrivLocalizers(locId);
              localizer = null;
            }
            if (null == localizer) {
              LOG.info("Created localizer for " + locId);
              localizer = new LocalizerRunner(req.getContext(), locId);
              privLocalizers.put(locId, localizer);
              localizer.start();
            }
            // 1) propagate event
            localizer.addResource(req);
          }
          break;
        }
        break;
      }
    }

    public void cleanupPrivLocalizers(String locId) {
      synchronized (privLocalizers) {
        LocalizerRunner localizer = privLocalizers.get(locId);
        if (null == localizer) {
          return; // ignore; already gone
        }
        privLocalizers.remove(locId);
        localizer.interrupt();
      }
    }

    public void endContainerLocalization(String locId) {
      LocalizerRunner localizer;
      synchronized (privLocalizers) {
        localizer = privLocalizers.get(locId);
        if (null == localizer) {
          return; // ignore
        }
      }
      localizer.endContainerLocalization();
    }
  }
  

  private static ExecutorService createLocalizerExecutor(Configuration conf) {

    // yarn.nodemanager.localizer.fetch.thread-count : 4
    int nThreads = conf.getInt(
        YarnConfiguration.NM_LOCALIZER_FETCH_THREAD_COUNT,
        YarnConfiguration.DEFAULT_NM_LOCALIZER_FETCH_THREAD_COUNT);
    ThreadFactory tf = new ThreadFactoryBuilder()
      .setNameFormat("PublicLocalizer #%d")
      .build();
    return HadoopExecutors.newFixedThreadPool(nThreads, tf);
  }


  class PublicLocalizer extends Thread {

    final FileContext lfs;

    final Configuration conf;

    // 线程池
    // yarn.nodemanager.localizer.fetch.thread-count : 4
    final ExecutorService threadPool;

    // 队列
    final CompletionService<Path> queue;

    // Its shared between public localizer and dispatcher thread.
    final Map<Future<Path>,LocalizerResourceRequestEvent> pending;

    PublicLocalizer(Configuration conf) {
      super("Public Localizer");
      this.lfs = getLocalFileContext(conf);
      this.conf = conf;
      this.pending = Collections.synchronizedMap(
          new HashMap<Future<Path>, LocalizerResourceRequestEvent>());

      // yarn.nodemanager.localizer.fetch.thread-count : 4
      this.threadPool = createLocalizerExecutor(conf);

      // 创建队列
      this.queue = new ExecutorCompletionService<Path>(threadPool);
    }

    public void addResource(LocalizerResourceRequestEvent request) {
      // TODO handle failures, cancellation, requests by other containers
      LocalizedResource rsrc = request.getResource();
      LocalResourceRequest key = rsrc.getRequest();
      LOG.info("Downloading public resource: " + key);
      /*
       多个containers或许会请求相同的资源.
       所以我们在以下情况下启动下载:
       1.  ResourceState 为DOWNLOADING
       2.  我们能够获得非阻塞信号量锁。
       否则，我们将跳过此资源，因为它正在下载或已失败/本地化。

       * Here multiple containers may request the same resource. So we need
       * to start downloading only when
       * 1) ResourceState == DOWNLOADING
       * 2) We are able to acquire non blocking semaphore lock.
       * If not we will skip this resource as either it is getting downloaded
       * or it FAILED / LOCALIZED.
       */

      // 尝试加锁
      if (rsrc.tryAcquire()) {
        // 状态是否为 DOWNLOADING
        if (rsrc.getState() == ResourceState.DOWNLOADING) {
          // 获取LocalResource
          LocalResource resource = request.getResource().getRequest();
          try {

            // 获取public 的根目录
            Path publicRootPath =
                dirsHandler.getLocalPathForWrite("." + Path.SEPARATOR
                    + ContainerLocalizer.FILECACHE,
                  ContainerLocalizer.getEstimatedSize(resource), true);

            // 获取目标 目录
            Path publicDirDestPath =
                publicRsrc.getPathForLocalization(key, publicRootPath,
                    delService);


            if (publicDirDestPath == null) {
              return;
            }

            // 验证 & 构架目录
            if (!publicDirDestPath.getParent().equals(publicRootPath)) {
              createParentDirs(publicDirDestPath, publicRootPath);
              if (diskValidator != null) {
                diskValidator.checkStatus(
                    new File(publicDirDestPath.toUri().getPath()));
              } else {
                throw new DiskChecker.DiskErrorException(
                    "Disk Validator is null!");
              }
            }

            // 在此处显式同步挂起，以避免将来的任务在挂起更新之前完成并退出队列
            // explicitly synchronize pending here to avoid future task
            // completing and being dequeued before pending updated
            synchronized (pending) {
              // 构建FSDownload ?
              pending.put(queue.submit(new FSDownload(lfs, null, conf,
                  publicDirDestPath, resource, request.getContext().getStatCache())),
                  request);
            }
          } catch (IOException e) {
            rsrc.unlock();
            publicRsrc.handle(new ResourceFailedLocalizationEvent(request
              .getResource().getRequest(), e.getMessage()));
            LOG.error("Local path for public localization is not found. "
                + " May be disks failed.", e);
          } catch (IllegalArgumentException ie) {
            rsrc.unlock();
            publicRsrc.handle(new ResourceFailedLocalizationEvent(request
                .getResource().getRequest(), ie.getMessage()));
            LOG.error("Local path for public localization is not found. "
                + " Incorrect path. " + request.getResource().getRequest()
                .getPath(), ie);
          } catch (RejectedExecutionException re) {
            rsrc.unlock();
            publicRsrc.handle(new ResourceFailedLocalizationEvent(request
              .getResource().getRequest(), re.getMessage()));
            LOG.error("Failed to submit rsrc " + rsrc + " for download."
                + " Either queue is full or threadpool is shutdown.", re);
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Skip downloading resource: " + key + " since it's in"
                + " state: " + rsrc.getState());
          }
          rsrc.unlock();
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip downloading resource: " + key + " since it is locked"
              + " by other threads");
        }
      }
    }

    private void createParentDirs(Path destDirPath, Path destDirRoot)
        throws IOException {
      if (destDirPath == null || destDirPath.equals(destDirRoot)) {
        return;
      }
      createParentDirs(destDirPath.getParent(), destDirRoot);
      createDir(destDirPath, PUBLIC_FILECACHE_FOLDER_PERMS);
    }

    private void createDir(Path dirPath, FsPermission perms)
        throws IOException {
      lfs.mkdir(dirPath, perms, false);
      if (!perms.equals(perms.applyUMask(lfs.getUMask()))) {
        lfs.setPermission(dirPath, perms);
      }
    }

    @Override
    public void run() {
      try {
        // TODO shutdown, better error handling esp. DU
        while (!Thread.currentThread().isInterrupted()) {
          try {
            // 获取任务路径
            Future<Path> completed = queue.take();
            // 从缓存中移除
            LocalizerResourceRequestEvent assoc = pending.remove(completed);
            try {
              if (null == assoc) {
                LOG.error("Localized unknown resource to " + completed);
                // TODO delete
                return;
              }
              // 获取路径
              Path local = completed.get();

              // 获取请求..
              LocalResourceRequest key = assoc.getResource().getRequest();

              // 处理缓存任务...
              publicRsrc.handle(new ResourceLocalizedEvent(key, local, FileUtil.getDU(new File(local.toUri()))));

              assoc.getResource().unlock();

            } catch (ExecutionException e) {
              String user = assoc.getContext().getUser();
              ApplicationId applicationId = assoc.getContext().getContainerId().getApplicationAttemptId().getApplicationId();
              LocalResourcesTracker tracker =
                getLocalResourcesTracker(LocalResourceVisibility.APPLICATION, user, applicationId);
              final String diagnostics = "Failed to download resource " +
                  assoc.getResource() + " " + e.getCause();
              tracker.handle(new ResourceFailedLocalizationEvent(
                  assoc.getResource().getRequest(), diagnostics));
              publicRsrc.handle(new ResourceFailedLocalizationEvent(
                  assoc.getResource().getRequest(), diagnostics));
              LOG.error(diagnostics);
              assoc.getResource().unlock();
            } catch (CancellationException e) {
              // ignore; shutting down
            }
          } catch (InterruptedException e) {
            return;
          }
        }
      } catch(Throwable t) {
        LOG.error("Error: Shutting down", t);
      } finally {
        LOG.info("Public cache exiting");
        threadPool.shutdownNow();
      }
    }

  }

  /**
   * Runs the {@link ContainerLocalizer} itself in a separate process with
   * access to user's credentials. One {@link LocalizerRunner} per localizerId.
   * 
   */
  class LocalizerRunner extends Thread {

    // 上下文信息
    final LocalizerContext context;

    //
    final String localizerId;

    // 等待处理的任务.
    final Map<LocalResourceRequest,LocalizerResourceRequestEvent> scheduled;

    // 它是私有本地化程序和调度程序线程之间的共享列表。
    // Its a shared list between Private Localizer and dispatcher thread.
    final List<LocalizerResourceRequestEvent> pending;


    private AtomicBoolean killContainerLocalizer = new AtomicBoolean(false);

    // TODO: threadsafe, use outer?
    private final RecordFactory recordFactory =    RecordFactoryProvider.getRecordFactory(getConfig());

    LocalizerRunner(LocalizerContext context, String localizerId) {
      super("LocalizerRunner for " + localizerId);
      this.context = context;
      this.localizerId = localizerId;

      // 同步方法...
      this.pending =
          Collections
            .synchronizedList(new ArrayList<LocalizerResourceRequestEvent>());

      // 待调度程序..
      this.scheduled =
          new HashMap<LocalResourceRequest, LocalizerResourceRequestEvent>();
    }

    // 将请求加入缓存.
    public void addResource(LocalizerResourceRequestEvent request) {
      pending.add(request);
    }

    public void endContainerLocalization() {
      killContainerLocalizer.set(true);
    }

    /**
     * 查找 下一个 要提供给派生定位器的资源。
     *
     * Find next resource to be given to a spawned localizer.
     * 
     * @return the next resource to be localized
     */
    private LocalResource findNextResource() {
      synchronized (pending) {
        // 迭代缓存中的数据
        for (Iterator<LocalizerResourceRequestEvent> i = pending.iterator(); i.hasNext();) {



         // 获取请求的事件
         LocalizerResourceRequestEvent evt = i.next();



         // 获取请求的资源
         LocalizedResource nRsrc = evt.getResource();



         // 只有在资源处于下载状态时才应进行资源下载
         // Resource download should take place ONLY if resource is in Downloading state
         if (nRsrc.getState() != ResourceState.DOWNLOADING) {
           i.remove();
           continue;
         }
         /*
          *
          * 多个容器将尝试下载同一资源。所以资源下载应该只在
          *  1） 我们可以在资源上获得一个非阻塞信号量锁
          *  2） 资源仍处于下载状态
          *
          * Multiple containers will try to download the same resource. So the resource download should start only if
          * 1) We can acquire a non blocking semaphore lock on resource
          * 2) Resource is still in DOWNLOADING state
          */
         if (nRsrc.tryAcquire()) {

           // 如果状态处于下载中...
           if (nRsrc.getState() == ResourceState.DOWNLOADING) {

             // 获取请求信息
             LocalResourceRequest nextRsrc = nRsrc.getRequest();

             //构建本地资源对象 LocalResource
             LocalResource next = recordFactory.newRecordInstance(LocalResource.class);
             next.setResource(URL.fromPath(nextRsrc.getPath()));
             next.setTimestamp(nextRsrc.getTimestamp());
             next.setType(nextRsrc.getType());
             next.setVisibility(evt.getVisibility());
             next.setPattern(evt.getPattern());
             // 加入调度缓存中...
             scheduled.put(nextRsrc, evt);

             // 返回
             return next;
           } else {
             // Need to release acquired lock
             nRsrc.unlock();
           }
         }
       }
       return null;
      }
    }

    // 处理心跳数据
    LocalizerHeartbeatResponse processHeartbeat(
        List<LocalResourceStatus> remoteResourceStatuses) {
      LocalizerHeartbeatResponse response =
        recordFactory.newRecordInstance(LocalizerHeartbeatResponse.class);
      String user = context.getUser();
      ApplicationId applicationId =
          context.getContainerId().getApplicationAttemptId().getApplicationId();

      boolean fetchFailed = false;
      // Update resource statuses.
      for (LocalResourceStatus stat : remoteResourceStatuses) {
        LocalResource rsrc = stat.getResource();
        LocalResourceRequest req = null;
        try {
          req = new LocalResourceRequest(rsrc);
        } catch (URISyntaxException e) {
          LOG.error(
              "Got exception in parsing URL of LocalResource:"
                  + rsrc.getResource(), e);
          continue;
        }
        LocalizerResourceRequestEvent assoc = scheduled.get(req);
        if (assoc == null) {
          // internal error
          LOG.error("Unknown resource reported: " + req);
          continue;
        }
        LocalResourcesTracker tracker =
            getLocalResourcesTracker(req.getVisibility(), user, applicationId);
        if (tracker == null) {
          // This is likely due to a race between heartbeat and
          // app cleaning up.
          continue;
        }
        switch (stat.getStatus()) {
          case FETCH_SUCCESS:
            // notify resource
            try {
              tracker.handle(new ResourceLocalizedEvent(req,
                  stat.getLocalPath().toPath(), stat.getLocalSize()));
            } catch (URISyntaxException e) { }

            // unlocking the resource and removing it from scheduled resource
            // list
            assoc.getResource().unlock();
            scheduled.remove(req);
            break;
          case FETCH_PENDING:
            break;
          case FETCH_FAILURE:
            final String diagnostics = stat.getException().toString();
            LOG.warn(req + " failed: " + diagnostics);
            fetchFailed = true;
            tracker.handle(new ResourceFailedLocalizationEvent(req,
                diagnostics));

            // unlocking the resource and removing it from scheduled resource
            // list
            assoc.getResource().unlock();
            scheduled.remove(req);
            break;
          default:
            LOG.info("Unknown status: " + stat.getStatus());
            fetchFailed = true;
            tracker.handle(new ResourceFailedLocalizationEvent(req,
                stat.getException().getMessage()));
            break;
        }
      }
      if (fetchFailed || killContainerLocalizer.get()) {
        response.setLocalizerAction(LocalizerAction.DIE);
        return response;
      }

      // Give the localizer resources for remote-fetching.
      List<ResourceLocalizationSpec> rsrcs =
          new ArrayList<ResourceLocalizationSpec>();

      /*
       * TODO : It doesn't support multiple downloads per ContainerLocalizer
       * at the same time. We need to think whether we should support this.
       */
      LocalResource next = findNextResource();
      if (next != null) {
        try {
          LocalResourcesTracker tracker = getLocalResourcesTracker(
              next.getVisibility(), user, applicationId);
          if (tracker != null) {
            Path localPath = getPathForLocalization(next, tracker);
            if (localPath != null) {
              rsrcs.add(NodeManagerBuilderUtils.newResourceLocalizationSpec(
                  next, localPath));
            }
          }
        } catch (IOException e) {
          LOG.error("local path for PRIVATE localization could not be " +
            "found. Disks might have failed.", e);
        } catch (IllegalArgumentException e) {
          LOG.error("Incorrect path for PRIVATE localization."
              + next.getResource().getFile(), e);
        } catch (URISyntaxException e) {
          LOG.error(
              "Got exception in parsing URL of LocalResource:"
                  + next.getResource(), e);
        }
      }

      response.setLocalizerAction(LocalizerAction.LIVE);
      response.setResourceSpecs(rsrcs);
      return response;
    }

    private Path getPathForLocalization(LocalResource rsrc,
        LocalResourcesTracker tracker) throws IOException, URISyntaxException {
      String user = context.getUser();


      ApplicationId appId = context.getContainerId().getApplicationAttemptId().getApplicationId();
      LocalResourceVisibility vis = rsrc.getVisibility();
      String cacheDirectory = null;

      if (vis == LocalResourceVisibility.PRIVATE) {// PRIVATE Only
        cacheDirectory = getUserFileCachePath(user);
      } else {// APPLICATION ONLY
        cacheDirectory = getAppFileCachePath(user, appId.toString());
      }
      Path dirPath =
          dirsHandler.getLocalPathForWrite(cacheDirectory,
            ContainerLocalizer.getEstimatedSize(rsrc), false);

      return tracker.getPathForLocalization(new LocalResourceRequest(rsrc),
          dirPath, delService);
    }

    @Override
    @SuppressWarnings("unchecked") // dispatcher not typed
    public void run() {
      Path nmPrivateCTokensPath = null;
      Throwable exception = null;
      try {
        // Get nmPrivateDir
        nmPrivateCTokensPath =
          dirsHandler.getLocalPathForWrite(
                NM_PRIVATE_DIR + Path.SEPARATOR
                    + String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT,
                        localizerId));

        // 0) init queue, etc.
        // 1) write credentials to private dir
        writeCredentials(nmPrivateCTokensPath);
        // 2) exec initApplication and wait
        if (dirsHandler.areDisksHealthy()) {

          exec.startLocalizer(new LocalizerStartContext.Builder()
                  .setNmPrivateContainerTokens(nmPrivateCTokensPath)
                  .setNmAddr(localizationServerAddress)
                  .setUser(context.getUser())
                  .setAppId(context.getContainerId()
                          .getApplicationAttemptId().getApplicationId().toString())
                  .setLocId(localizerId)
                  .setDirsHandler(dirsHandler)
                  .build());


        } else {
          throw new IOException("All disks failed. "
              + dirsHandler.getDisksHealthReport(false));
        }
      // TODO handle ExitCodeException separately?
      } catch (FSError fe) {
        exception = fe;
      } catch (Exception e) {
        exception = e;
      } finally {
        if (exception != null) {
          LOG.info("Localizer failed for "+localizerId, exception);
          // On error, report failure to Container and signal ABORT
          // Notify resource of failed localization
          ContainerId cId = context.getContainerId();
          dispatcher.getEventHandler().handle(new ContainerResourceFailedEvent(
              cId, null, exception.getMessage()));
        }
        List<Path> paths = new ArrayList<Path>();
        for (LocalizerResourceRequestEvent event : scheduled.values()) {
          // This means some resources were in downloading state. Schedule
          // deletion task for localization dir and tmp dir used for downloading
          Path locRsrcPath = event.getResource().getLocalPath();
          if (locRsrcPath != null) {
            Path locRsrcDirPath = locRsrcPath.getParent();
            paths.add(locRsrcDirPath);
            paths.add(new Path(locRsrcDirPath + "_tmp"));
          }
          event.getResource().unlock();
        }
        if (!paths.isEmpty()) {
          FileDeletionTask deletionTask = new FileDeletionTask(delService,
              context.getUser(), null, paths);
          delService.delete(deletionTask);
        }
        FileDeletionTask deletionTask = new FileDeletionTask(delService, null,
            nmPrivateCTokensPath, null);
        delService.delete(deletionTask);
      }
    }

    private Credentials getSystemCredentialsSentFromRM(
        LocalizerContext localizerContext) throws IOException {
      ApplicationId appId =
          localizerContext.getContainerId().getApplicationAttemptId()
            .getApplicationId();
      Credentials systemCredentials =
          nmContext.getSystemCredentialsForApps().get(appId);
      if (systemCredentials == null) {
        return null;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding new framework-token for " + appId
            + " for localization: " + systemCredentials.getAllTokens());
      }
      return systemCredentials;
    }
    
    private void writeCredentials(Path nmPrivateCTokensPath)
        throws IOException {
      DataOutputStream tokenOut = null;
      try {
        Credentials credentials = context.getCredentials();
        if (UserGroupInformation.isSecurityEnabled()) {
          Credentials systemCredentials =
              getSystemCredentialsSentFromRM(context);
          if (systemCredentials != null) {
            credentials = systemCredentials;
          }
        }

        FileContext lfs = getLocalFileContext(getConfig());
        tokenOut =
            lfs.create(nmPrivateCTokensPath, EnumSet.of(CREATE, OVERWRITE));
        LOG.info("Writing credentials to the nmPrivate file "
            + nmPrivateCTokensPath.toString());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Credentials list in " + nmPrivateCTokensPath.toString()
              + ": ");
          for (Token<? extends TokenIdentifier> tk : credentials
              .getAllTokens()) {
            LOG.debug(tk + " : " + buildTokenFingerprint(tk));
          }
        }
        if (UserGroupInformation.isSecurityEnabled()) {
          credentials = new Credentials(credentials);
          LocalizerTokenIdentifier id = secretManager.createIdentifier();
          Token<LocalizerTokenIdentifier> localizerToken =
              new Token<LocalizerTokenIdentifier>(id, secretManager);
          credentials.addToken(id.getKind(), localizerToken);
        }
        credentials.writeTokenStorageToStream(tokenOut);
      } finally {
        if (tokenOut != null) {
          tokenOut.close();
        }
      }
    }

  }

  /**
   * Returns a fingerprint of a token.  The fingerprint is suitable for use in
   * logging, because it cannot be used to determine the secret.  The
   * fingerprint is built using the first 10 bytes of a SHA-256 hash of the
   * string encoding of the token.  The returned string contains the hex
   * representation of each byte, delimited by a space.
   *
   * @param tk token
   * @return token fingerprint
   * @throws IOException if there is an I/O error
   */
  @VisibleForTesting
  static String buildTokenFingerprint(Token<? extends TokenIdentifier> tk)
      throws IOException {
    char[] digest = DigestUtils.sha256Hex(tk.encodeToUrlString()).toCharArray();
    StringBuilder fingerprint = new StringBuilder();
    for (int i = 0; i < 10; ++i) {
      if (i > 0) {
        fingerprint.append(' ');
      }
      fingerprint.append(digest[2 * i]);
      fingerprint.append(digest[2 * i + 1]);
    }
    return fingerprint.toString();
  }

  static class CacheCleanup extends Thread {

    private final Dispatcher dispatcher;

    public CacheCleanup(Dispatcher dispatcher) {
      super("CacheCleanup");
      this.dispatcher = dispatcher;
    }

    @Override
    @SuppressWarnings("unchecked") // dispatcher not typed
    public void run() {
      dispatcher.getEventHandler().handle(
          new LocalizationEvent(LocalizationEventType.CACHE_CLEANUP));
    }

  }

  private void initializeLocalDirs(FileContext lfs) {
    List<String> localDirs = dirsHandler.getLocalDirs();
    for (String localDir : localDirs) {
      // 循环初始化
      LOG.info("ResourceLocalizationService#initializeLogDirs : "+localDir);
      initializeLocalDir(lfs, localDir);
    }
  }

  private void initializeLocalDir(FileContext lfs, String localDir) {

    Map<Path, FsPermission> pathPermissionMap = getLocalDirsPathPermissionsMap(localDir);
    for (Map.Entry<Path, FsPermission> entry : pathPermissionMap.entrySet()) {
      FileStatus status;
      try {
        status = lfs.getFileStatus(entry.getKey());
      }
      catch(FileNotFoundException fs) {
        status = null;
      }
      catch(IOException ie) {
        String msg = "Could not get file status for local dir " + entry.getKey();
        LOG.warn(msg, ie);
        throw new YarnRuntimeException(msg, ie);
      }
      if(status == null) {
        try {
          lfs.mkdir(entry.getKey(), entry.getValue(), true);
          status = lfs.getFileStatus(entry.getKey());
        } catch (IOException e) {
          String msg = "Could not initialize local dir " + entry.getKey();
          LOG.warn(msg, e);
          throw new YarnRuntimeException(msg, e);
        }
      }
      FsPermission perms = status.getPermission();
      if(!perms.equals(entry.getValue())) {
        try {
          lfs.setPermission(entry.getKey(), entry.getValue());
        }
        catch(IOException ie) {
          String msg = "Could not set permissions for local dir " + entry.getKey();
          LOG.warn(msg, ie);
          throw new YarnRuntimeException(msg, ie);
        }
      }
    }
  }

  private void initializeLogDirs(FileContext lfs) {
    // 获取log 目录
    List<String> logDirs = dirsHandler.getLogDirs();
    for (String logDir : logDirs) {
      // 循环初始化
      LOG.info("ResourceLocalizationService#initializeLogDirs : "+logDir);
      initializeLogDir(lfs, logDir);
    }
  }

  private void initializeLogDir(FileContext fs, String logDir) {
    try {
      fs.mkdir(new Path(logDir), null, true);
    } catch (FileAlreadyExistsException fe) {
      // do nothing
    } catch (IOException e) {
      String msg = "Could not initialize log dir " + logDir;
      LOG.warn(msg, e);
      throw new YarnRuntimeException(msg, e);
    }
  }

  private void cleanupLogDirs(FileContext fs, DeletionService del) {
    for (String logDir : dirsHandler.getLogDirsForCleanup()) {
      try {
        cleanupLogDir(fs, del, logDir);
      } catch (IOException e) {
        LOG.warn("failed to cleanup app log dir " + logDir, e);
      }
    }
  }

  private void cleanupLogDir(FileContext fs, DeletionService del,
      String logDir) throws IOException {
    if (!fs.util().exists(new Path(logDir))){
      return;
    }
    renameAppLogDir(logDir);
    deleteAppLogDir(fs, del, logDir);
  }

  private void renameAppLogDir(String logDir) throws IOException {
    long currentTimeStamp = System.currentTimeMillis();
    RemoteIterator<FileStatus> fileStatuses =
        lfs.listStatus(new Path(logDir));
    if (fileStatuses != null) {
      while (fileStatuses.hasNext()) {
        FileStatus fileStatus = fileStatuses.next();
        String appName = fileStatus.getPath().getName();
        if (appName.matches("^application_\\d+_\\d+$")) {
          lfs.rename(new Path(logDir, appName),
              new Path(logDir, appName + "_DEL_" + currentTimeStamp));
        }
      }
    }
  }

  private void deleteAppLogDir(FileContext fs, DeletionService del,
      String logDir) throws IOException {
    RemoteIterator<FileStatus> fileStatuses =
        fs.listStatus(new Path(logDir));
    if (fileStatuses != null) {
      while (fileStatuses.hasNext()) {
        FileStatus fileStatus = fileStatuses.next();
        String appName = fileStatus.getPath().getName();
        if (appName.matches("^application_\\d+_\\d+_DEL_\\d+$")) {
          LOG.info("delete app log dir," + appName);
          FileDeletionTask deletionTask = new FileDeletionTask(del, null,
              fileStatus.getPath(), null);
          del.delete(deletionTask);
        }
      }
    }
  }

  private void cleanUpLocalDirs(FileContext lfs, DeletionService del) {
    for (String localDir : dirsHandler.getLocalDirsForCleanup()) {
      cleanUpLocalDir(lfs, del, localDir);
    }
  }

  private void cleanUpLocalDir(FileContext lfs, DeletionService del,
      String localDir) {
    long currentTimeStamp = System.currentTimeMillis();
    renameLocalDir(lfs, localDir, ContainerLocalizer.USERCACHE,
      currentTimeStamp);
    renameLocalDir(lfs, localDir, ContainerLocalizer.FILECACHE,
      currentTimeStamp);
    renameLocalDir(lfs, localDir, ResourceLocalizationService.NM_PRIVATE_DIR,
      currentTimeStamp);
    try {
      deleteLocalDir(lfs, del, localDir);
    } catch (IOException e) {
      // Do nothing, just give the warning
      LOG.warn("Failed to delete localDir: " + localDir);
    }
  }

  private void renameLocalDir(FileContext lfs, String localDir,
      String localSubDir, long currentTimeStamp) {
    try {
      lfs.rename(new Path(localDir, localSubDir), new Path(
          localDir, localSubDir + "_DEL_" + currentTimeStamp));
    } catch (FileNotFoundException ex) {
      // No need to handle this exception
      // localSubDir may not be exist
    } catch (Exception ex) {
      // Do nothing, just give the warning
      LOG.warn("Failed to rename the local file under " +
          localDir + "/" + localSubDir);
    }
  }

  private void deleteLocalDir(FileContext lfs, DeletionService del,
      String localDir) throws IOException {
    RemoteIterator<FileStatus> fileStatus = lfs.listStatus(new Path(localDir));
    if (fileStatus != null) {
      while (fileStatus.hasNext()) {
        FileStatus status = fileStatus.next();
        try {
          if (status.getPath().getName().matches(".*" +
              ContainerLocalizer.USERCACHE + "_DEL_.*")) {
            LOG.info("usercache path : " + status.getPath().toString());
            cleanUpFilesPerUserDir(lfs, del, status.getPath());
          } else if (status.getPath().getName()
              .matches(".*" + NM_PRIVATE_DIR + "_DEL_.*")
              ||
              status.getPath().getName()
                  .matches(".*" + ContainerLocalizer.FILECACHE + "_DEL_.*")) {
            FileDeletionTask deletionTask = new FileDeletionTask(del, null,
                status.getPath(), null);
            del.delete(deletionTask);
          }
        } catch (IOException ex) {
          // Do nothing, just give the warning
          LOG.warn("Failed to delete this local Directory: " +
              status.getPath().getName());
        }
      }
    }
  }

  private void cleanUpFilesPerUserDir(FileContext lfs, DeletionService del,
      Path userDirPath) throws IOException {
    RemoteIterator<FileStatus> userDirStatus = lfs.listStatus(userDirPath);
    FileDeletionTask dependentDeletionTask = new FileDeletionTask(del, null,
        userDirPath, new ArrayList<Path>());
    if (userDirStatus != null && userDirStatus.hasNext()) {
      List<FileDeletionTask> deletionTasks = new ArrayList<FileDeletionTask>();
      while (userDirStatus.hasNext()) {
        FileStatus status = userDirStatus.next();
        String owner = status.getOwner();
        List<Path> pathList = new ArrayList<>();
        pathList.add(status.getPath());
        FileDeletionTask deletionTask = new FileDeletionTask(del, owner, null,
            pathList);
        deletionTask.addDeletionTaskDependency(dependentDeletionTask);
        deletionTasks.add(deletionTask);
      }
      for (FileDeletionTask task : deletionTasks) {
        del.delete(task);
      }
    } else {
      del.delete(dependentDeletionTask);
    }
  }
  
  /**
   * Check each local dir to ensure it has been setup correctly and will
   * attempt to fix any issues it finds.
   * @return void
   */
  @VisibleForTesting
  void checkAndInitializeLocalDirs() {
    // 获取目录
    List<String> dirs = dirsHandler.getLocalDirs();


    List<String> checkFailedDirs = new ArrayList<String>();
    for (String dir : dirs) {
      try {
        checkLocalDir(dir);
      } catch (YarnRuntimeException e) {
        checkFailedDirs.add(dir);
      }
    }

    // 处理异常 目录
    for (String dir : checkFailedDirs) {
      LOG.info("Attempting to initialize " + dir);
      initializeLocalDir(lfs, dir);
      try {
        checkLocalDir(dir);
      } catch (YarnRuntimeException e) {
        String msg =
            "Failed to setup local dir " + dir + ", which was marked as good.";
        LOG.warn(msg, e);
        throw new YarnRuntimeException(msg, e);
      }
    }
  }

  private boolean checkLocalDir(String localDir) {

    Map<Path, FsPermission> pathPermissionMap = getLocalDirsPathPermissionsMap(localDir);

    for (Map.Entry<Path, FsPermission> entry : pathPermissionMap.entrySet()) {
      FileStatus status;
      try {
        status = lfs.getFileStatus(entry.getKey());
      } catch (Exception e) {
        String msg =
            "Could not carry out resource dir checks for " + localDir
                + ", which was marked as good";
        LOG.warn(msg, e);
        throw new YarnRuntimeException(msg, e);
      }

      if (!status.getPermission().equals(entry.getValue())) {
        String msg =
            "Permissions incorrectly set for dir " + entry.getKey()
                + ", should be " + entry.getValue() + ", actual value = "
                + status.getPermission();
        LOG.warn(msg);
        throw new YarnRuntimeException(msg);
      }
    }
    return true;
  }

  private Map<Path, FsPermission> getLocalDirsPathPermissionsMap(String localDir) {
    Map<Path, FsPermission> localDirPathFsPermissionsMap = new HashMap<Path, FsPermission>();

    FsPermission defaultPermission =
        FsPermission.getDirDefault().applyUMask(lfs.getUMask());
    FsPermission nmPrivatePermission =
        NM_PRIVATE_PERM.applyUMask(lfs.getUMask());

    Path userDir = new Path(localDir, ContainerLocalizer.USERCACHE);
    Path fileDir = new Path(localDir, ContainerLocalizer.FILECACHE);
    Path sysDir = new Path(localDir, NM_PRIVATE_DIR);

    localDirPathFsPermissionsMap.put(userDir, defaultPermission);
    localDirPathFsPermissionsMap.put(fileDir, defaultPermission);
    localDirPathFsPermissionsMap.put(sysDir, nmPrivatePermission);
    return localDirPathFsPermissionsMap;
  }
}
