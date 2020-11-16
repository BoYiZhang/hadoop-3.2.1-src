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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.TimeUnit;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.hdfs.server.datanode.FinalizedReplica;
import org.apache.hadoop.hdfs.server.datanode.LocalReplica;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetricHelper;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.DatanodeUtil;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaAlreadyExistsException;
import org.apache.hadoop.hdfs.server.datanode.ReplicaBuilder;
import org.apache.hadoop.hdfs.server.datanode.ReplicaHandler;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.ReplicaNotFoundException;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.UnexpectedReplicaStateException;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.RamDiskReplicaTracker.RamDiskReplica;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.hdfs.server.protocol.BlockRecoveryCommand.RecoveringBlock;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.apache.hadoop.util.InstrumentedLock;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 ***************************************************/
@InterfaceAudience.Private
class FsDatasetImpl implements FsDatasetSpi<FsVolumeImpl> {
  static final Logger LOG = LoggerFactory.getLogger(FsDatasetImpl.class);
  private final static boolean isNativeIOAvailable;
  private Timer timer;
  static {
    isNativeIOAvailable = NativeIO.isAvailable();
    if (Path.WINDOWS && !isNativeIOAvailable) {
      LOG.warn("Data node cannot fully support concurrent reading"
          + " and writing without native code extensions on Windows.");
    }
  }

  @Override // FsDatasetSpi
  public FsVolumeReferences getFsVolumeReferences() {
    return new FsVolumeReferences(volumes.getVolumes());
  }

  @Override
  public DatanodeStorage getStorage(final String storageUuid) {
    return storageMap.get(storageUuid);
  }

  @Override // FsDatasetSpi
  public StorageReport[] getStorageReports(String bpid)
      throws IOException {
    List<StorageReport> reports;
    // Volumes are the references from a copy-on-write snapshot, so the
    // access on the volume metrics doesn't require an additional lock.
    List<FsVolumeImpl> curVolumes = volumes.getVolumes();
    reports = new ArrayList<>(curVolumes.size());
    for (FsVolumeImpl volume : curVolumes) {
      try (FsVolumeReference ref = volume.obtainReference()) {
        StorageReport sr = new StorageReport(volume.toDatanodeStorage(),
            false,
            volume.getCapacity(),
            volume.getDfsUsed(),
            volume.getAvailable(),
            volume.getBlockPoolUsed(bpid),
            volume.getNonDfsUsed());
        reports.add(sr);
      } catch (ClosedChannelException e) {
        continue;
      }
    }

    return reports.toArray(new StorageReport[reports.size()]);
  }

  @Override
  public FsVolumeImpl getVolume(final ExtendedBlock b) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final ReplicaInfo r =
          volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
      return r != null ? (FsVolumeImpl) r.getVolume() : null;
    }
  }

  @Override // FsDatasetSpi
  public Block getStoredBlock(String bpid, long blkid)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      //调用getFile()获取副本的数据块文件引用
      ReplicaInfo r = volumeMap.get(bpid, blkid);
      if (r == null) {
        return null;
      }
      //构造Block对象返回
      return new Block(blkid, r.getBytesOnDisk(), r.getGenerationStamp());
    }
  }


  /**
   * This should be primarily used for testing.
   * @return clone of replica store in datanode memory
   */
  ReplicaInfo fetchReplicaInfo(String bpid, long blockId) {
    ReplicaInfo r = volumeMap.get(bpid, blockId);
    if (r == null) {
      return null;
    }
    switch(r.getState()) {
    case FINALIZED:
    case RBW:
    case RWR:
    case RUR:
    case TEMPORARY:
      return new ReplicaBuilder(r.getState()).from(r).build();
    }
    return null;
  }
  
  @Override // FsDatasetSpi
  public LengthInputStream getMetaDataInputStream(ExtendedBlock b)
      throws IOException {
    ReplicaInfo info = getBlockReplica(b);
    if (info == null || !info.metadataExists()) {
      return null;
    }
    return info.getMetadataInputStream(0);
  }

  // 当前数据节点的Datanode对象的引用。
  final DataNode datanode;

  // DataStorage对象的引用， 在FsDatasetImpl中调用这个对象开启和关闭存储空间回收站功能。
  final DataStorage dataStorage;

  // FsVolumeList对象的引用， FsDatasetImpl使用FsVolumeList对象提供的功能管理Datanode上的所有数据块，
  // 并且通过对volumes字段的修改添加和删除新的存储目录。
  private final FsVolumeList volumes;

  // 在FsDatasetImpl中， 对于每一个存储目录都会构造一个DataStorage对象， storageMap字段就是用来保存这些存储的， 是storageUuid->DataStorage的映射。
  // FsDatasetImpl可以通过storageUuid获取对应的DataStorage对象。
  final Map<String, DatanodeStorage> storageMap;
  final FsDatasetAsyncDiskService asyncDiskService;
  final Daemon lazyWriter;

  // FsDatasetCache类型， 用于将数据块缓存到内存中的工具类。
  final FsDatasetCache cacheManager;
  private final Configuration conf;
  private final int volFailuresTolerated;
  private final int volsConfigured;
  private volatile boolean fsRunning;

  // ReplicaMap类型， 用于记录Datanode上所有数据块副本的信息。
  final ReplicaMap volumeMap;
  final Map<String, Set<Long>> deletingBlock;
  final RamDiskReplicaTracker ramDiskReplicaTracker;

  // FsDatasetAsyncDiskService类型， 用于在Datanode磁盘存储上进行一些异步的IO操作。
  // 例如在进行删除数据块副本操作时， 会调用FsDatasetAsyncDiskService.deleteAsync()异步地将数据块文件从磁盘上删除。
  final RamDiskAsyncLazyPersistService asyncLazyPersistService;





  private static final int MAX_BLOCK_EVICTIONS_PER_ITERATION = 3;

  private final int smallBufferSize;

  final LocalFileSystem localFS;

  private boolean blockPinningEnabled;
  private final int maxDataLength;

  @VisibleForTesting
  final AutoCloseableLock datasetLock;
  private final Condition datasetLockCondition;
  
  /**
   * An FSDataset has a directory where it loads its data files.
   */
  FsDatasetImpl(DataNode datanode, DataStorage storage, Configuration conf
      ) throws IOException {

    this.fsRunning = true;
    // DataNode{data=null, localName='192.168.8.188:9866', datanodeUuid='9efa402a-df6b-48cf-9273-5468f68cc42f', xmitsInProgress=0}
    this.datanode = datanode;
    //lv=-57;cid=CID-67c28180-c700-49d1-a3f4-07db5dc12a17;nsid=0;c=0
    this.dataStorage = storage;
    // Configuration: core-default.xml, core-site.xml, hdfs-default.xml, hdfs-site.xml
    this.conf = conf;
    // 512
    this.smallBufferSize = DFSUtilClient.getSmallBufferSize(conf);


    this.datasetLock = new AutoCloseableLock(
        new InstrumentedLock(getClass().getName(), LOG,
          new ReentrantLock(true),
          conf.getTimeDuration(
            DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY,
            DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS),
          300));


    this.datasetLockCondition = datasetLock.newCondition();


    // 操作所需的卷数是卷总数减去我们可以容忍的失败卷数。 : 0
    // The number of volumes required for operation is the total number
    // of volumes minus the number of failed volumes we can tolerate.
    volFailuresTolerated = datanode.getDnConf().getVolFailuresTolerated();
    // 0   [DISK]file:/tools/hadoop-3.2.1/data/hdfs/data
    Collection<StorageLocation> dataLocations = DataNode.getStorageLocations(conf);


    List<VolumeFailureInfo> volumeFailureInfos = getInitialVolumeFailureInfos(
        dataLocations, storage);

    volsConfigured = datanode.getDnConf().getVolsConfigured();


    int volsFailed = volumeFailureInfos.size();

    if (volFailuresTolerated < DataNode.MAX_VOLUME_FAILURE_TOLERATED_LIMIT
        || volFailuresTolerated >= volsConfigured) {
      throw new DiskErrorException("Invalid value configured for "
          + "dfs.datanode.failed.volumes.tolerated - " + volFailuresTolerated
          + ". Value configured is either less than maxVolumeFailureLimit or greater than "
          + "to the number of configured volumes (" + volsConfigured + ").");
    }

    //  MAX_VOLUME_FAILURE_TOLERATED_LIMIT :  - 1 ==>  无限制
    if (volFailuresTolerated == DataNode.MAX_VOLUME_FAILURE_TOLERATED_LIMIT) {
      if (volsConfigured == volsFailed) {
        throw new DiskErrorException(
            "Too many failed volumes - " + "current valid volumes: "
                + storage.getNumStorageDirs() + ", volumes configured: "
                + volsConfigured + ", volumes failed: " + volsFailed
                + ", volume failures tolerated: " + volFailuresTolerated);
      }
    } else {
      if (volsFailed > volFailuresTolerated) {
        throw new DiskErrorException(
            "Too many failed volumes - " + "current valid volumes: "
                + storage.getNumStorageDirs() + ", volumes configured: "
                + volsConfigured + ", volumes failed: " + volsFailed
                + ", volume failures tolerated: " + volFailuresTolerated);
      }
    }

    storageMap = new ConcurrentHashMap<String, DatanodeStorage>();

    //加载lock
    volumeMap = new ReplicaMap(datasetLock);


    ramDiskReplicaTracker = RamDiskReplicaTracker.getInstance(conf, this);

    @SuppressWarnings("unchecked")
    final VolumeChoosingPolicy<FsVolumeImpl> blockChooserImpl =
        ReflectionUtils.newInstance(conf.getClass(
            DFSConfigKeys.DFS_DATANODE_FSDATASET_VOLUME_CHOOSING_POLICY_KEY,
            RoundRobinVolumeChoosingPolicy.class,
            VolumeChoosingPolicy.class), conf);

    // FsVolumelmpl：
    // 管理Datanode一个存储目录下的所有数据块。
    // 由于一个存储目录可以存储多个块池的数据块，
    // 所以FsVolumelmpl会持有这个存储目录中保存的所有块池的BlockPoolSlice对象
    volumes = new FsVolumeList(volumeFailureInfos, datanode.getBlockScanner(),  blockChooserImpl);

    asyncDiskService = new FsDatasetAsyncDiskService(datanode, this);


    asyncLazyPersistService = new RamDiskAsyncLazyPersistService(datanode, conf);


    deletingBlock = new HashMap<String, Set<Long>>();

    for (int idx = 0; idx < storage.getNumStorageDirs(); idx++) {


      addVolume(storage.getStorageDir(idx));
    }


    setupAsyncLazyPersistThreads();

    cacheManager = new FsDatasetCache(this);





    // Start the lazy writer once we have built the replica maps.
    // We need to start the lazy writer even if MaxLockedMemory is set to
    // zero because we may have un-persisted replicas in memory from before
    // the process restart. To minimize the chances of data loss we'll
    // ensure they get written to disk now.
    if (ramDiskReplicaTracker.numReplicasNotPersisted() > 0 ||
        datanode.getDnConf().getMaxLockedMemory() > 0) {
      lazyWriter = new Daemon(new LazyWriter(conf));
      lazyWriter.start();
    } else {
      lazyWriter = null;
    }

    registerMBean(datanode.getDatanodeUuid());



    // Add a Metrics2 Source Interface. This is same
    // data as MXBean. We can remove the registerMbean call
    // in a release where we can break backward compatibility
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register("FSDatasetState", "FSDatasetState", this);

    localFS = FileSystem.getLocal(conf);


    blockPinningEnabled = conf.getBoolean(
      DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED,
      DFSConfigKeys.DFS_DATANODE_BLOCK_PINNING_ENABLED_DEFAULT);



    maxDataLength = conf.getInt(
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);

  }

  @Override
  public AutoCloseableLock acquireDatasetLock() {
    return datasetLock.acquire();
  }

  /**
   * Gets initial volume failure information for all volumes that failed
   * immediately at startup.  The method works by determining the set difference
   * between all configured storage locations and the actual storage locations in
   * use after attempting to put all of them into service.
   *
   * @return each storage location that has failed
   */
  private static List<VolumeFailureInfo> getInitialVolumeFailureInfos(
      Collection<StorageLocation> dataLocations, DataStorage storage) {
    Set<StorageLocation> failedLocationSet = Sets.newHashSetWithExpectedSize(
        dataLocations.size());
    for (StorageLocation sl: dataLocations) {
      failedLocationSet.add(sl);
    }
    for (Iterator<Storage.StorageDirectory> it = storage.dirIterator();
         it.hasNext(); ) {
      Storage.StorageDirectory sd = it.next();
      failedLocationSet.remove(sd.getStorageLocation());
    }
    List<VolumeFailureInfo> volumeFailureInfos = Lists.newArrayListWithCapacity(
        failedLocationSet.size());
    long failureDate = Time.now();
    for (StorageLocation failedStorageLocation: failedLocationSet) {
      volumeFailureInfos.add(new VolumeFailureInfo(failedStorageLocation,
          failureDate));
    }
    return volumeFailureInfos;
  }

  /**
   * Activate a volume to serve requests.
   * @throws IOException if the storage UUID already exists.
   */
  private void activateVolume(
      ReplicaMap replicaMap,
      Storage.StorageDirectory sd, StorageType storageType,
      FsVolumeReference ref) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      DatanodeStorage dnStorage = storageMap.get(sd.getStorageUuid());
      if (dnStorage != null) {
        final String errorMsg = String.format(
            "Found duplicated storage UUID: %s in %s.",
            sd.getStorageUuid(), sd.getVersionFile());
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }
      volumeMap.mergeAll(replicaMap);
      storageMap.put(sd.getStorageUuid(),
          new DatanodeStorage(sd.getStorageUuid(),
              DatanodeStorage.State.NORMAL,
              storageType));
      asyncDiskService.addVolume((FsVolumeImpl) ref.getVolume());
      volumes.addVolume(ref);
    }
  }

  private void addVolume(Storage.StorageDirectory sd) throws IOException {
    final StorageLocation storageLocation = sd.getStorageLocation();

    // If IOException raises from FsVolumeImpl() or getVolumeMap(), there is
    // nothing needed to be rolled back to make various data structures, e.g.,
    // storageMap and asyncDiskService, consistent.
    FsVolumeImpl fsVolume = new FsVolumeImplBuilder()
                              .setDataset(this)
                              .setStorageID(sd.getStorageUuid())
                              .setStorageDirectory(sd)
                              .setFileIoProvider(datanode.getFileIoProvider())
                              .setConf(this.conf)
                              .build();
    FsVolumeReference ref = fsVolume.obtainReference();
    ReplicaMap tempVolumeMap = new ReplicaMap(datasetLock);
    fsVolume.getVolumeMap(tempVolumeMap, ramDiskReplicaTracker);

    activateVolume(tempVolumeMap, sd, storageLocation.getStorageType(), ref);
    LOG.info("Added volume - " + storageLocation + ", StorageType: " +
        storageLocation.getStorageType());
  }

  @VisibleForTesting
  public FsVolumeImpl createFsVolume(String storageUuid,
      Storage.StorageDirectory sd,
      final StorageLocation location) throws IOException {
    return new FsVolumeImplBuilder()
        .setDataset(this)
        .setStorageID(storageUuid)
        .setStorageDirectory(sd)
        .setFileIoProvider(datanode.getFileIoProvider())
        .setConf(conf)
        .build();
  }

  @Override
  public void addVolume(final StorageLocation location,
      final List<NamespaceInfo> nsInfos)
      throws IOException {
    // Prepare volume in DataStorage
    final DataStorage.VolumeBuilder builder;
    try {
      builder = dataStorage.prepareVolume(datanode, location, nsInfos);
    } catch (IOException e) {
      volumes.addVolumeFailureInfo(new VolumeFailureInfo(location, Time.now()));
      throw e;
    }

    final Storage.StorageDirectory sd = builder.getStorageDirectory();

    StorageType storageType = location.getStorageType();
    final FsVolumeImpl fsVolume =
        createFsVolume(sd.getStorageUuid(), sd, location);
    final ReplicaMap tempVolumeMap = new ReplicaMap(new AutoCloseableLock());
    ArrayList<IOException> exceptions = Lists.newArrayList();

    for (final NamespaceInfo nsInfo : nsInfos) {
      String bpid = nsInfo.getBlockPoolID();
      try {
        fsVolume.addBlockPool(bpid, this.conf, this.timer);
        fsVolume.getVolumeMap(bpid, tempVolumeMap, ramDiskReplicaTracker);
      } catch (IOException e) {
        LOG.warn("Caught exception when adding " + fsVolume +
            ". Will throw later.", e);
        exceptions.add(e);
      }
    }
    if (!exceptions.isEmpty()) {
      try {
        sd.unlock();
      } catch (IOException e) {
        exceptions.add(e);
      }
      throw MultipleIOException.createIOException(exceptions);
    }

    final FsVolumeReference ref = fsVolume.obtainReference();
    setupAsyncLazyPersistThread(fsVolume);

    builder.build();
    activateVolume(tempVolumeMap, sd, storageType, ref);
    LOG.info("Added volume - " + location + ", StorageType: " + storageType);
  }

  /**
   * Removes a set of volumes from FsDataset.
   * @param storageLocsToRemove a set of
   * {@link StorageLocation}s for each volume.
   * @param clearFailure set true to clear failure information.
   */
  @Override
  public void removeVolumes(
      final Collection<StorageLocation> storageLocsToRemove,
      boolean clearFailure) {
    Collection<StorageLocation> storageLocationsToRemove =
        new ArrayList<>(storageLocsToRemove);
    Map<String, List<ReplicaInfo>> blkToInvalidate = new HashMap<>();
    List<String> storageToRemove = new ArrayList<>();
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      for (int idx = 0; idx < dataStorage.getNumStorageDirs(); idx++) {
        Storage.StorageDirectory sd = dataStorage.getStorageDir(idx);
        final StorageLocation sdLocation = sd.getStorageLocation();
        LOG.info("Checking removing StorageLocation " +
            sdLocation + " with id " + sd.getStorageUuid());
        if (storageLocationsToRemove.contains(sdLocation)) {
          LOG.info("Removing StorageLocation " + sdLocation + " with id " +
              sd.getStorageUuid() + " from FsDataset.");
          // Disable the volume from the service.
          asyncDiskService.removeVolume(sd.getStorageUuid());
          volumes.removeVolume(sdLocation, clearFailure);
          volumes.waitVolumeRemoved(5000, datasetLockCondition);

          // Removed all replica information for the blocks on the volume.
          // Unlike updating the volumeMap in addVolume(), this operation does
          // not scan disks.
          for (String bpid : volumeMap.getBlockPoolList()) {
            List<ReplicaInfo> blocks = new ArrayList<>();
            for (Iterator<ReplicaInfo> it =
                  volumeMap.replicas(bpid).iterator(); it.hasNext();) {
              ReplicaInfo block = it.next();
              final StorageLocation blockStorageLocation =
                  block.getVolume().getStorageLocation();
              LOG.trace("checking for block " + block.getBlockId() +
                  " with storageLocation " + blockStorageLocation);
              if (blockStorageLocation.equals(sdLocation)) {
                blocks.add(block);
                it.remove();
              }
            }
            blkToInvalidate.put(bpid, blocks);
          }

          storageToRemove.add(sd.getStorageUuid());
          storageLocationsToRemove.remove(sdLocation);
        }
      }

      // A reconfigure can remove the storage location which is already
      // removed when the failure was detected by DataNode#checkDiskErrorAsync.
      // Now, lets remove this from the failed volume list.
      if (clearFailure) {
        for (StorageLocation storageLocToRemove : storageLocationsToRemove) {
          volumes.removeVolumeFailureInfo(storageLocToRemove);
        }
      }
      setupAsyncLazyPersistThreads();
    }

    // Call this outside the lock.
    for (Map.Entry<String, List<ReplicaInfo>> entry :
        blkToInvalidate.entrySet()) {
      String bpid = entry.getKey();
      List<ReplicaInfo> blocks = entry.getValue();
      for (ReplicaInfo block : blocks) {
        invalidate(bpid, block);
      }
    }

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      for(String storageUuid : storageToRemove) {
        storageMap.remove(storageUuid);
      }
    }
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getDfsUsed() throws IOException {
    return volumes.getDfsUsed();
  }

  /**
   * Return the total space used by dfs datanode
   */
  @Override // FSDatasetMBean
  public long getBlockPoolUsed(String bpid) throws IOException {
    return volumes.getBlockPoolUsed(bpid);
  }
  
  /**
   * Return true - if there are still valid volumes on the DataNode. 
   */
  @Override // FsDatasetSpi
  public boolean hasEnoughResource() {
    if (volFailuresTolerated == DataNode.MAX_VOLUME_FAILURE_TOLERATED_LIMIT) {
      // If volFailuresTolerated configured maxVolumeFailureLimit then minimum
      // one volume is required.
      return volumes.getVolumes().size() >= 1;
    } else {
      return getNumFailedVolumes() <= volFailuresTolerated;
    }
  }

  /**
   * Return total capacity, used and unused
   */
  @Override // FSDatasetMBean
  public long getCapacity() throws IOException {
    return volumes.getCapacity();
  }

  /**
   * Return how many bytes can still be stored in the FSDataset
   */
  @Override // FSDatasetMBean
  public long getRemaining() throws IOException {
    return volumes.getRemaining();
  }

  /**
   * Return the number of failed volumes in the FSDataset.
   */
  @Override // FSDatasetMBean
  public int getNumFailedVolumes() {
    return volumes.getVolumeFailureInfos().length;
  }

  @Override // FSDatasetMBean
  public String[] getFailedStorageLocations() {
    VolumeFailureInfo[] infos = volumes.getVolumeFailureInfos();
    List<String> failedStorageLocations = Lists.newArrayListWithCapacity(
        infos.length);
    for (VolumeFailureInfo info: infos) {
      failedStorageLocations.add(
          info.getFailedStorageLocation().getNormalizedUri().toString());
    }
    return failedStorageLocations.toArray(
        new String[failedStorageLocations.size()]);
  }

  @Override // FSDatasetMBean
  public long getLastVolumeFailureDate() {
    long lastVolumeFailureDate = 0;
    for (VolumeFailureInfo info: volumes.getVolumeFailureInfos()) {
      long failureDate = info.getFailureDate();
      if (failureDate > lastVolumeFailureDate) {
        lastVolumeFailureDate = failureDate;
      }
    }
    return lastVolumeFailureDate;
  }

  @Override // FSDatasetMBean
  public long getEstimatedCapacityLostTotal() {
    long estimatedCapacityLostTotal = 0;
    for (VolumeFailureInfo info: volumes.getVolumeFailureInfos()) {
      estimatedCapacityLostTotal += info.getEstimatedCapacityLost();
    }
    return estimatedCapacityLostTotal;
  }

  @Override // FsDatasetSpi
  public VolumeFailureSummary getVolumeFailureSummary() {
    VolumeFailureInfo[] infos = volumes.getVolumeFailureInfos();
    if (infos.length == 0) {
      return null;
    }
    List<String> failedStorageLocations = Lists.newArrayListWithCapacity(
        infos.length);
    long lastVolumeFailureDate = 0;
    long estimatedCapacityLostTotal = 0;
    for (VolumeFailureInfo info: infos) {
      failedStorageLocations.add(
          info.getFailedStorageLocation().getNormalizedUri().toString());
      long failureDate = info.getFailureDate();
      if (failureDate > lastVolumeFailureDate) {
        lastVolumeFailureDate = failureDate;
      }
      estimatedCapacityLostTotal += info.getEstimatedCapacityLost();
    }
    return new VolumeFailureSummary(
        failedStorageLocations.toArray(new String[failedStorageLocations.size()]),
        lastVolumeFailureDate, estimatedCapacityLostTotal);
  }

  @Override // FSDatasetMBean
  public long getCacheUsed() {
    return cacheManager.getCacheUsed();
  }

  @Override // FSDatasetMBean
  public long getCacheCapacity() {
    return cacheManager.getCacheCapacity();
  }

  @Override // FSDatasetMBean
  public long getNumBlocksFailedToCache() {
    return cacheManager.getNumBlocksFailedToCache();
  }

  @Override // FSDatasetMBean
  public long getNumBlocksFailedToUncache() {
    return cacheManager.getNumBlocksFailedToUncache();
  }

  /**
   * Get metrics from the metrics source
   *
   * @param collector to contain the resulting metrics snapshot
   * @param all if true, return all metrics even if unchanged.
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    try {
      DataNodeMetricHelper.getMetrics(collector, this, "FSDatasetState");
    } catch (Exception e) {
        LOG.warn("Exception thrown while metric collection. Exception : "
          + e.getMessage());
    }
  }

  @Override // FSDatasetMBean
  public long getNumBlocksCached() {
    return cacheManager.getNumBlocksCached();
  }

  /**
   * Find the block's on-disk length
   */
  @Override // FsDatasetSpi
  public long getLength(ExtendedBlock b) throws IOException {
    return getBlockReplica(b).getBlockDataLength();
  }

  /**
   * Get File name for a given block.
   */
  private ReplicaInfo getBlockReplica(ExtendedBlock b) throws IOException {
    return getBlockReplica(b.getBlockPoolId(), b.getBlockId());
  }
  
  /**
   * Get File name for a given block.
   */
  ReplicaInfo getBlockReplica(String bpid, long blockId) throws IOException {
    ReplicaInfo r = validateBlockFile(bpid, blockId);
    if (r == null) {
      throw new FileNotFoundException("BlockId " + blockId + " is not valid.");
    }
    return r;
  }

  @Override // FsDatasetSpi
  public InputStream getBlockInputStream(ExtendedBlock b,
      long seekOffset) throws IOException {

    ReplicaInfo info;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    }

    if (info != null && info.getVolume().isTransientStorage()) {
      ramDiskReplicaTracker.touch(b.getBlockPoolId(), b.getBlockId());
      datanode.getMetrics().incrRamDiskBlocksReadHits();
    }

    if (info != null) {
      return info.getDataInputStream(seekOffset);
    } else {
      throw new IOException("No data exists for block " + b);
    }
  }

  /**
   * Get the meta info of a block stored in volumeMap. To find a block,
   * block pool Id, block Id and generation stamp must match.
   * @param b extended block
   * @return the meta replica information
   * @throws ReplicaNotFoundException if no entry is in the map or 
   *                        there is a generation stamp mismatch
   */
  ReplicaInfo getReplicaInfo(ExtendedBlock b)
      throws ReplicaNotFoundException {
    ReplicaInfo info = volumeMap.get(b.getBlockPoolId(), b.getLocalBlock());
    if (info == null) {
      if (volumeMap.get(b.getBlockPoolId(), b.getLocalBlock().getBlockId())
          == null) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
      } else {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b);
      }
    }
    return info;
  }

  /**
   * Get the meta info of a block stored in volumeMap. Block is looked up
   * without matching the generation stamp.
   * @param bpid block pool Id
   * @param blkid block Id
   * @return the meta replica information; null if block was not found
   * @throws ReplicaNotFoundException if no entry is in the map or 
   *                        there is a generation stamp mismatch
   */
  @VisibleForTesting
  ReplicaInfo getReplicaInfo(String bpid, long blkid)
      throws ReplicaNotFoundException {
    ReplicaInfo info = volumeMap.get(bpid, blkid);
    if (info == null) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.NON_EXISTENT_REPLICA + bpid + ":" + blkid);
    }
    return info;
  }

  /**
   * Returns handles to the block file and its metadata file
   */
  @Override // FsDatasetSpi
  public ReplicaInputStreams getTmpInputStreams(ExtendedBlock b,
      long blkOffset, long metaOffset) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo info = getReplicaInfo(b);
      FsVolumeReference ref = info.getVolume().obtainReference();
      try {
        InputStream blockInStream = info.getDataInputStream(blkOffset);
        try {
          InputStream metaInStream = info.getMetadataInputStream(metaOffset);
          return new ReplicaInputStreams(
              blockInStream, metaInStream, ref, datanode.getFileIoProvider());
        } catch (IOException e) {
          IOUtils.cleanup(null, blockInStream);
          throw e;
        }
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
    }
  }

  static File moveBlockFiles(Block b, ReplicaInfo replicaInfo, File destdir)
      throws IOException {
    final File dstfile = new File(destdir, b.getBlockName());
    final File dstmeta = FsDatasetUtil.getMetaFile(dstfile, b.getGenerationStamp());
    try {
      replicaInfo.renameMeta(dstmeta.toURI());
    } catch (IOException e) {
      throw new IOException("Failed to move meta file for " + b
          + " from " + replicaInfo.getMetadataURI() + " to " + dstmeta, e);
    }
    try {
      replicaInfo.renameData(dstfile.toURI());
    } catch (IOException e) {
      throw new IOException("Failed to move block file for " + b
          + " from " + replicaInfo.getBlockURI() + " to "
          + dstfile.getAbsolutePath(), e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("addFinalizedBlock: Moved " + replicaInfo.getMetadataURI()
          + " to " + dstmeta + " and " + replicaInfo.getBlockURI()
          + " to " + dstfile);
    }
    return dstfile;
  }

  /**
   * Copy the block and meta files for the given block to the given destination.
   * @return the new meta and block files.
   * @throws IOException
   */
  static File[] copyBlockFiles(long blockId, long genStamp,
      ReplicaInfo srcReplica, File destRoot, boolean calculateChecksum,
      int smallBufferSize, final Configuration conf) throws IOException {
    final File destDir = DatanodeUtil.idToBlockDir(destRoot, blockId);
    // blockName is same as the filename for the block
    final File dstFile = new File(destDir, srcReplica.getBlockName());
    final File dstMeta = FsDatasetUtil.getMetaFile(dstFile, genStamp);
    return copyBlockFiles(srcReplica, dstMeta, dstFile, calculateChecksum,
        smallBufferSize, conf);
  }

  static File[] copyBlockFiles(ReplicaInfo srcReplica, File dstMeta,
                               File dstFile, boolean calculateChecksum,
                               int smallBufferSize, final Configuration conf)
      throws IOException {

    if (calculateChecksum) {
      computeChecksum(srcReplica, dstMeta, smallBufferSize, conf);
    } else {
      try {
        srcReplica.copyMetadata(dstMeta.toURI());
      } catch (IOException e) {
        throw new IOException("Failed to copy " + srcReplica + " metadata to "
            + dstMeta, e);
      }
    }
    try {
      srcReplica.copyBlockdata(dstFile.toURI());
    } catch (IOException e) {
      throw new IOException("Failed to copy " + srcReplica + " block file to "
          + dstFile, e);
    }
    if (LOG.isDebugEnabled()) {
      if (calculateChecksum) {
        LOG.debug("Copied " + srcReplica.getMetadataURI() + " meta to "
            + dstMeta + " and calculated checksum");
      } else {
        LOG.debug("Copied " + srcReplica.getBlockURI() + " to " + dstFile);
      }
    }
    return new File[] {dstMeta, dstFile};
  }

  /**
   * Move block files from one storage to another storage.
   * @return Returns the Old replicaInfo
   * @throws IOException
   */
  @Override
  public ReplicaInfo moveBlockAcrossStorage(ExtendedBlock block,
      StorageType targetStorageType, String targetStorageId)
      throws IOException {
    ReplicaInfo replicaInfo = getReplicaInfo(block);
    if (replicaInfo.getState() != ReplicaState.FINALIZED) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + block);
    }
    if (replicaInfo.getNumBytes() != block.getNumBytes()) {
      throw new IOException("Corrupted replica " + replicaInfo
          + " with a length of " + replicaInfo.getNumBytes()
          + " expected length is " + block.getNumBytes());
    }
    if (replicaInfo.getVolume().getStorageType() == targetStorageType) {
      throw new ReplicaAlreadyExistsException("Replica " + replicaInfo
          + " already exists on storage " + targetStorageType);
    }

    if (replicaInfo.isOnTransientStorage()) {
      // Block movement from RAM_DISK will be done by LazyPersist mechanism
      throw new IOException("Replica " + replicaInfo
          + " cannot be moved from storageType : "
          + replicaInfo.getVolume().getStorageType());
    }

    FsVolumeReference volumeRef = null;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      volumeRef = volumes.getNextVolume(targetStorageType, targetStorageId,
          block.getNumBytes());
    }
    try {
      moveBlock(block, replicaInfo, volumeRef);
    } finally {
      if (volumeRef != null) {
        volumeRef.close();
      }
    }

    // Replace the old block if any to reschedule the scanning.
    return replicaInfo;
  }

  /**
   * Moves a block from a given volume to another.
   *
   * @param block       - Extended Block
   * @param replicaInfo - ReplicaInfo
   * @param volumeRef   - Volume Ref - Closed by caller.
   * @return newReplicaInfo
   * @throws IOException
   */
  @VisibleForTesting
  ReplicaInfo moveBlock(ExtendedBlock block, ReplicaInfo replicaInfo,
      FsVolumeReference volumeRef) throws IOException {
    ReplicaInfo newReplicaInfo = copyReplicaToVolume(block, replicaInfo,
        volumeRef);
    finalizeNewReplica(newReplicaInfo, block);
    removeOldReplica(replicaInfo, newReplicaInfo, block.getBlockPoolId());
    return newReplicaInfo;
  }

  /**
   * Cleanup the replicaInfo object passed.
   *
   * @param bpid           - block pool id
   * @param replicaInfo    - ReplicaInfo
   */
  private void cleanupReplica(String bpid, ReplicaInfo replicaInfo) {
    if (replicaInfo.deleteBlockData() || !replicaInfo.blockDataExists()) {
      FsVolumeImpl volume = (FsVolumeImpl) replicaInfo.getVolume();
      volume.onBlockFileDeletion(bpid, replicaInfo.getBytesOnDisk());
      if (replicaInfo.deleteMetadata() || !replicaInfo.metadataExists()) {
        volume.onMetaFileDeletion(bpid, replicaInfo.getMetadataLength());
      }
    }
  }

  /**
   * Create a new temporary replica of replicaInfo object in specified volume.
   *
   * @param block       - Extended Block
   * @param replicaInfo - ReplicaInfo
   * @param volumeRef   - Volume Ref - Closed by caller.
   * @return newReplicaInfo new replica object created in specified volume.
   * @throws IOException
   */
  @VisibleForTesting
  ReplicaInfo copyReplicaToVolume(ExtendedBlock block, ReplicaInfo replicaInfo,
      FsVolumeReference volumeRef) throws IOException {
    FsVolumeImpl targetVolume = (FsVolumeImpl) volumeRef.getVolume();
    // Copy files to temp dir first
    ReplicaInfo newReplicaInfo = targetVolume.moveBlockToTmpLocation(block,
        replicaInfo, smallBufferSize, conf);
    return newReplicaInfo;
  }

  /**
   * Finalizes newReplica by calling finalizeReplica internally.
   *
   * @param newReplicaInfo - ReplicaInfo
   * @param block          - Extended Block
   * @throws IOException
   */
  @VisibleForTesting
  void finalizeNewReplica(ReplicaInfo newReplicaInfo,
      ExtendedBlock block) throws IOException {
    // Finalize the copied files
    try {
      String bpid = block.getBlockPoolId();
      finalizeReplica(bpid, newReplicaInfo);
      FsVolumeImpl volume = (FsVolumeImpl) newReplicaInfo.getVolume();
      volume.incrNumBlocks(bpid);
    } catch (IOException ioe) {
      // Cleanup block data and metadata
      // Decrement of dfsUsed and noOfBlocks for volume not required
      newReplicaInfo.deleteBlockData();
      newReplicaInfo.deleteMetadata();
      throw ioe;
    }
  }

  /**
   * Moves a given block from one volume to another volume. This is used by disk
   * balancer.
   *
   * @param block       - ExtendedBlock
   * @param destination - Destination volume
   * @return Old replica info
   */
  @Override
  public ReplicaInfo moveBlockAcrossVolumes(ExtendedBlock block, FsVolumeSpi
      destination) throws IOException {
    ReplicaInfo replicaInfo = getReplicaInfo(block);
    if (replicaInfo.getState() != ReplicaState.FINALIZED) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_REPLICA + block);
    }

    FsVolumeReference volumeRef = null;

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      volumeRef = destination.obtainReference();
    }

    try {
      moveBlock(block, replicaInfo, volumeRef);
    } finally {
      if (volumeRef != null) {
        volumeRef.close();
      }
    }
    return replicaInfo;
  }

  /**
   * Compute and store the checksum for a block file that does not already have
   * its checksum computed.
   *
   * @param srcReplica source {@link ReplicaInfo}, containing only the checksum
   *     header, not a calculated checksum
   * @param dstMeta destination meta file, into which this method will write a
   *     full computed checksum
   * @param smallBufferSize buffer size to use
   * @param conf the {@link Configuration}
   * @throws IOException
   */
  static void computeChecksum(ReplicaInfo srcReplica, File dstMeta,
      int smallBufferSize, final Configuration conf)
      throws IOException {
    final File srcMeta = new File(srcReplica.getMetadataURI());

    DataChecksum checksum;
    try (FileInputStream fis =
             srcReplica.getFileIoProvider().getFileInputStream(
                 srcReplica.getVolume(), srcMeta)) {
      checksum = BlockMetadataHeader.readDataChecksum(
          fis, DFSUtilClient.getIoFileBufferSize(conf), srcMeta);
    }

    final byte[] data = new byte[1 << 16];
    final byte[] crcs = new byte[checksum.getChecksumSize(data.length)];

    DataOutputStream metaOut = null;
    try {
      File parentFile = dstMeta.getParentFile();
      if (parentFile != null) {
        if (!parentFile.mkdirs() && !parentFile.isDirectory()) {
          throw new IOException("Destination '" + parentFile
              + "' directory cannot be created");
        }
      }
      metaOut = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(dstMeta), smallBufferSize));
      BlockMetadataHeader.writeHeader(metaOut, checksum);

      int offset = 0;
      try (InputStream dataIn = srcReplica.getDataInputStream(0)) {

        for (int n; (n = dataIn.read(data, offset, data.length - offset)) != -1; ) {
          if (n > 0) {
            n += offset;
            offset = n % checksum.getBytesPerChecksum();
            final int length = n - offset;

            if (length > 0) {
              checksum.calculateChunkedSums(data, 0, length, crcs, 0);
              metaOut.write(crcs, 0, checksum.getChecksumSize(length));

              System.arraycopy(data, length, data, 0, offset);
            }
          }
        }
      }

      // calculate and write the last crc
      checksum.calculateChunkedSums(data, 0, offset, crcs, 0);
      metaOut.write(crcs, 0, 4);
      metaOut.close();
      metaOut = null;
    } finally {
      IOUtils.closeStream(metaOut);
    }
  }


  @Override  // FsDatasetSpi
  public ReplicaHandler append(ExtendedBlock b,
      long newGS, long expectedBlockLen) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // If the block was successfully finalized because all packets
      // were successfully processed at the Datanode but the ack for
      // some of the packets were not received by the client. The client
      // re-opens the connection and retries sending those packets.
      // The other reason is that an "append" is occurring to this block.

      // check the validity of the parameter
      if (newGS < b.getGenerationStamp()) {
        throw new IOException("The new generation stamp " + newGS +
            " should be greater than the replica " + b + "'s generation stamp");
      }
      ReplicaInfo replicaInfo = getReplicaInfo(b);
      LOG.info("Appending to " + replicaInfo);
      if (replicaInfo.getState() != ReplicaState.FINALIZED) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.UNFINALIZED_REPLICA + b);
      }
      if (replicaInfo.getNumBytes() != expectedBlockLen) {
        throw new IOException("Corrupted replica " + replicaInfo +
            " with a length of " + replicaInfo.getNumBytes() +
            " expected length is " + expectedBlockLen);
      }

      FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
      ReplicaInPipeline replica = null;
      try {
        replica = append(b.getBlockPoolId(), replicaInfo, newGS,
            b.getNumBytes());
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      return new ReplicaHandler(replica, ref);
    }
  }
  
  /** Append to a finalized replica
   * Change a finalized replica to be a RBW replica and 
   * bump its generation stamp to be the newGS
   * 
   * @param bpid block pool Id
   * @param replicaInfo a finalized replica
   * @param newGS new generation stamp
   * @param estimateBlockLen estimate block length
   * @return a RBW replica
   * @throws IOException if moving the replica from finalized directory 
   *         to rbw directory fails
   */
  private ReplicaInPipeline append(String bpid,
      ReplicaInfo replicaInfo, long newGS, long estimateBlockLen)
      throws IOException {

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // If the block is cached, start uncaching it.
      if (replicaInfo.getState() != ReplicaState.FINALIZED) {
        throw new IOException("Only a Finalized replica can be appended to; "
            + "Replica with blk id " + replicaInfo.getBlockId() + " has state "
            + replicaInfo.getState());
      }

      // 将当前副本从缓存中移出。
      // If the block is cached, start uncaching it.
      cacheManager.uncacheBlock(bpid, replicaInfo.getBlockId());

      // If there are any hardlinks to the block, break them.  This ensures
      // we are not appending to a file that is part of a previous/ directory.
      replicaInfo.breakHardLinksIfNeeded();

      FsVolumeImpl v = (FsVolumeImpl)replicaInfo.getVolume();

      //构造新的ReplicaInfo对象
      ReplicaInPipeline rip = v.append(bpid, replicaInfo,  newGS, estimateBlockLen);


      if (rip.getReplicaInfo().getState() != ReplicaState.RBW) {
        throw new IOException("Append on block " + replicaInfo.getBlockId() +
            " returned a replica of state " + rip.getReplicaInfo().getState()
            + "; expected RBW");
      }
      // Replace finalized replica by a RBW replica in replicas map
      volumeMap.add(bpid, rip.getReplicaInfo());
      return rip;
    }

  }

  @SuppressWarnings("serial")
  private static class MustStopExistingWriter extends Exception {
    private final ReplicaInPipeline rip;

    MustStopExistingWriter(ReplicaInPipeline rip) {
      this.rip = rip;
    }

    ReplicaInPipeline getReplicaInPipeline() {
      return rip;
    }
  }

  private ReplicaInfo recoverCheck(ExtendedBlock b, long newGS, 
      long expectedBlockLen) throws IOException, MustStopExistingWriter {
    ReplicaInfo replicaInfo = getReplicaInfo(b.getBlockPoolId(), b.getBlockId());
    
    // check state
    // 检查副本状态， 只有FINALIZED以及RBW状态的副本才能进行追加写操作
    if (replicaInfo.getState() != ReplicaState.FINALIZED &&
        replicaInfo.getState() != ReplicaState.RBW) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNFINALIZED_AND_NONRBW_REPLICA + replicaInfo);
    }

    // check generation stamp
    // 检查时间戳， 时间戳的范围应当是在原记录的时间戳与新时间戳之间
    long replicaGenerationStamp = replicaInfo.getGenerationStamp();
    if (replicaGenerationStamp < b.getGenerationStamp() ||
        replicaGenerationStamp > newGS) {
      throw new ReplicaNotFoundException(
          ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + replicaGenerationStamp
          + ". Expected GS range is [" + b.getGenerationStamp() + ", " + 
          newGS + "].");
    }

    //
    // stop the previous writer before check a replica's length
    long replicaLen = replicaInfo.getNumBytes();
    if (replicaInfo.getState() == ReplicaState.RBW) {
      ReplicaInPipeline rbw = (ReplicaInPipeline) replicaInfo;

      if (!rbw.attemptToSetWriter(null, Thread.currentThread())) {
        throw new MustStopExistingWriter(rbw);
      }
      // check length: bytesRcvd, bytesOnDisk, and bytesAcked should be the same
      // 确认bytesRcvd、 bytesOnDisk和bytesAcked相同
      if (replicaLen != rbw.getBytesOnDisk() 
          || replicaLen != rbw.getBytesAcked()) {
        throw new ReplicaAlreadyExistsException("RBW replica " + replicaInfo + 
            "bytesRcvd(" + rbw.getNumBytes() + "), bytesOnDisk(" + 
            rbw.getBytesOnDisk() + "), and bytesAcked(" + rbw.getBytesAcked() +
            ") are not the same.");
      }
    }
    
    // check block length
    // 确认副本长度和期待长度相同， 否则抛出异常
    if (replicaLen != expectedBlockLen) {
      throw new IOException("Corrupted replica " + replicaInfo + 
          " with a length of " + replicaLen + 
          " expected length is " + expectedBlockLen);
    }
    
    return replicaInfo;
  }

  @Override  // FsDatasetSpi
  public ReplicaHandler recoverAppend(
      ExtendedBlock b, long newGS, long expectedBlockLen) throws IOException {
    LOG.info("Recover failed append to " + b);

    while (true) {
      try {

        try (AutoCloseableLock lock = datasetLock.acquire()) {

          //先调用recoverCheck()方法对ReplicaInfo进行检查
          ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);


          FsVolumeReference ref = replicaInfo.getVolume().obtainReference();
          ReplicaInPipeline replica;
          try {


            // change the replica's state/gs etc.
            // 之后更改副本的状态以及时间戳
            if (replicaInfo.getState() == ReplicaState.FINALIZED) {
              //FINALIZED状态的副本变为RBW状态
              replica = append(b.getBlockPoolId(), replicaInfo,
                               newGS, b.getNumBytes());
            } else { //RBW

              //RBW状态的副本， 则直接更新时间戳即可
              replicaInfo.bumpReplicaGS(newGS);
              replica = (ReplicaInPipeline) replicaInfo;

            }
          } catch (IOException e) {
            IOUtils.cleanup(null, ref);
            throw e;
          }
          return new ReplicaHandler(replica, ref);
        }


      } catch (MustStopExistingWriter e) {
        e.getReplicaInPipeline()
            .stopWriter(datanode.getDnConf().getXceiverStopTimeout());
      }
    }
  }

  @Override // FsDatasetSpi
  public Replica recoverClose(ExtendedBlock b, long newGS,
      long expectedBlockLen) throws IOException {
    LOG.info("Recover failed close " + b);
    while (true) {
      try {
        try (AutoCloseableLock lock = datasetLock.acquire()) {
          // check replica's state
          ReplicaInfo replicaInfo = recoverCheck(b, newGS, expectedBlockLen);
          // bump the replica's GS
          replicaInfo.bumpReplicaGS(newGS);
          // finalize the replica if RBW
          if (replicaInfo.getState() == ReplicaState.RBW) {
            finalizeReplica(b.getBlockPoolId(), replicaInfo);
          }
          return replicaInfo;
        }
      } catch (MustStopExistingWriter e) {
        e.getReplicaInPipeline()
            .stopWriter(datanode.getDnConf().getXceiverStopTimeout());
      }
    }
  }

  @Override // FsDatasetSpi
  public ReplicaHandler createRbw( StorageType storageType, String storageId, ExtendedBlock b, boolean allowLazyPersist) throws IOException {

    try (AutoCloseableLock lock = datasetLock.acquire()) {

      // 获取副本信息
      ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(),  b.getBlockId());

      if (replicaInfo != null) {
        throw new ReplicaAlreadyExistsException("Block " + b +
            " already exists in state " + replicaInfo.getState() +
            " and thus cannot be created.");
      }
      // create a new block
      FsVolumeReference ref = null;

      // 只有块大小是操作系统 page 大小的倍数时才使用ramdisk。
      // 这大大简化了对部分使用的副本的保留。
      //
      //
      // Use ramdisk only if block size is a multiple of OS page size.
      // This simplifies reservation for partially used replicas
      // significantly.
      if (allowLazyPersist &&
          lazyWriter != null &&
          b.getNumBytes() % cacheManager.getOsPageSize() == 0 &&
          reserveLockedMemory(b.getNumBytes())) {
        try {
          // First try to place the block on a transient volume.
          ref = volumes.getNextTransientVolume(b.getNumBytes());
          datanode.getMetrics().incrRamDiskBlocksWrite();
        } catch (DiskOutOfSpaceException de) {
          // Ignore the exception since we just fall back to persistent storage.
          LOG.warn("Insufficient space for placing the block on a transient "
              + "volume, fall back to persistent storage: "
              + de.getMessage());
        } finally {
          if (ref == null) {
            cacheManager.release(b.getNumBytes());
          }
        }
      }

      if (ref == null) {
        ref = volumes.getNextVolume(storageType, storageId, b.getNumBytes());
      }

      FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
      // create an rbw file to hold block in the designated volume
      // 创建rbw文件以在指定卷中保存块
      if (allowLazyPersist && !v.isTransientStorage()) {
        datanode.getMetrics().incrRamDiskBlocksWriteFallback();
      }

      ReplicaInPipeline newReplicaInfo;
      try {

        // 构建newReplicaInfo
        newReplicaInfo = v.createRbw(b);
        if (newReplicaInfo.getReplicaInfo().getState() != ReplicaState.RBW) {
          throw new IOException("CreateRBW returned a replica of state "
              + newReplicaInfo.getReplicaInfo().getState()
              + " for block " + b.getBlockId());
        }
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      //持久化内存信息
      volumeMap.add(b.getBlockPoolId(), newReplicaInfo.getReplicaInfo());
      return new ReplicaHandler(newReplicaInfo, ref);
    }
  }

  @Override // FsDatasetSpi
  public ReplicaHandler recoverRbw(
      ExtendedBlock b, long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    LOG.info("Recover RBW replica " + b);

    while (true) {
      try {
        try (AutoCloseableLock lock = datasetLock.acquire()) {

          /// 获取副本信息
          ReplicaInfo replicaInfo = getReplicaInfo(b.getBlockPoolId(), b.getBlockId());

          // 检查副本的状态, 如果副本的状态不是 RBW , 抛出异常
          // check the replica's state
          if (replicaInfo.getState() != ReplicaState.RBW) {
            throw new ReplicaNotFoundException(
                ReplicaNotFoundException.NON_RBW_REPLICA + replicaInfo);
          }
          ReplicaInPipeline rbw = (ReplicaInPipeline)replicaInfo;
          //停止副本原先的写入线程， 并将当前线程更新为写入线程
          if (!rbw.attemptToSetWriter(null, Thread.currentThread())) {
            throw new MustStopExistingWriter(rbw);
          }
          LOG.info("At " + datanode.getDisplayName() + ", Recovering " + rbw);

          // 回复RBW 实现
          return recoverRbwImpl(rbw, b, newGS, minBytesRcvd, maxBytesRcvd);
        }
      } catch (MustStopExistingWriter e) {
        e.getReplicaInPipeline().stopWriter(
            datanode.getDnConf().getXceiverStopTimeout());
      }
    }
  }

  private ReplicaHandler recoverRbwImpl(ReplicaInPipeline rbw,
      ExtendedBlock b, long newGS, long minBytesRcvd, long maxBytesRcvd)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {

      // 检查时间戳标记
      // check generation stamp
      long replicaGenerationStamp = rbw.getGenerationStamp();
      // 检查时间戳范围， 如果不在b.getGenerationStamp()至newGS之间则抛出异常
      // 副本时间戳不匹配,直接抛出异常
      if (replicaGenerationStamp < b.getGenerationStamp() ||
          replicaGenerationStamp > newGS) {
        throw new ReplicaNotFoundException(
            ReplicaNotFoundException.UNEXPECTED_GS_REPLICA + b +
                ". Expected GS range is [" + b.getGenerationStamp() + ", " +
                newGS + "].");
      }

      // 检查副本长度
      //检查副本的长度， 如果不在minBytesRcvd与maxBytesRcvd之间则抛出异常
      // check replica length
      long bytesAcked = rbw.getBytesAcked();
      long numBytes = rbw.getNumBytes();
      if (bytesAcked < minBytesRcvd || numBytes > maxBytesRcvd) {
        throw new ReplicaNotFoundException("Unmatched length replica " +
            rbw + ": BytesAcked = " + bytesAcked +
            " BytesRcvd = " + numBytes + " are not in the range of [" +
            minBytesRcvd + ", " + maxBytesRcvd + "].");
      }

      long bytesOnDisk = rbw.getBytesOnDisk();
      long blockDataLength = rbw.getReplicaInfo().getBlockDataLength();
      if (bytesOnDisk != blockDataLength) {
        LOG.info("Resetting bytesOnDisk to match blockDataLength (={}) for " +
            "replica {}", blockDataLength, rbw);
        bytesOnDisk = blockDataLength;
        rbw.setLastChecksumAndDataLen(bytesOnDisk, null);
      }


      if (bytesOnDisk < bytesAcked) {
        throw new ReplicaNotFoundException("Found fewer bytesOnDisk than " +
            "bytesAcked for replica " + rbw);
      }

      FsVolumeReference ref = rbw.getReplicaInfo()
          .getVolume().obtainReference();
      try {
        // Truncate the potentially corrupt portion.
        // If the source was client and the last node in the pipeline was lost,
        // any corrupt data written after the acked length can go unnoticed.

        // 清空可能损坏的部分。
        //如果source 是client，并且管道中的最后一个节点丢失，
        // 则在确认长度之后写入的任何损坏数据都会忽略。

        //将副本中所有没有确认的数据包删除
        if (bytesOnDisk > bytesAcked) {
          rbw.getReplicaInfo().truncateBlock(bytesAcked);
          rbw.setNumBytes(bytesAcked);
          rbw.setLastChecksumAndDataLen(bytesAcked, null);
        }
        // 设置新的时间戳
        // bump the replica's generation stamp to newGS
        rbw.getReplicaInfo().bumpReplicaGS(newGS);
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      return new ReplicaHandler(rbw, ref);
    }
  }
  
  @Override // FsDatasetSpi
  public ReplicaInPipeline convertTemporaryToRbw(
      final ExtendedBlock b) throws IOException {

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final long blockId = b.getBlockId();
      final long expectedGs = b.getGenerationStamp();
      final long visible = b.getNumBytes();
      LOG.info("Convert " + b + " from Temporary to RBW, visible length="
          + visible);

      final ReplicaInfo temp;
      {
        // get replica
        final ReplicaInfo r = volumeMap.get(b.getBlockPoolId(), blockId);
        //检查副本信息是否存在
        if (r == null) {
          throw new ReplicaNotFoundException(
              ReplicaNotFoundException.NON_EXISTENT_REPLICA + b);
        }
        // 检查副本状态是否为TEMPORARY,否则拋出异常
        // check the replica's state
        if (r.getState() != ReplicaState.TEMPORARY) {
          throw new ReplicaAlreadyExistsException(
              "r.getState() != ReplicaState.TEMPORARY, r=" + r);
        }
        temp = r;
      }
      //检查时间戳是否正常
      // check generation stamp
      if (temp.getGenerationStamp() != expectedGs) {
        throw new ReplicaAlreadyExistsException(
            "temp.getGenerationStamp() != expectedGs = " + expectedGs
                + ", temp=" + temp);
      }

      // TODO: check writer?
      // set writer to the current thread
      // temp.setWriter(Thread.currentThread());

      // check length
      // 检查副本长度， 如果副本长度小于传入的长度， 则抛出异常
      final long numBytes = temp.getNumBytes();
      if (numBytes < visible) {
        throw new IOException(numBytes + " = numBytes < visible = "
            + visible + ", temp=" + temp);
      }
      // check volume
      // 检查副本所在存储目录的FsVolumeImpl是否存在， 不存在则抛出异常
      final FsVolumeImpl v = (FsVolumeImpl) temp.getVolume();
      if (v == null) {
        throw new IOException("r.getVolume() = null, temp=" + temp);
      }

      //将副本从tmp目录移动到rbw目录
      final ReplicaInPipeline rbw = v.convertTemporaryToRbw(b, temp);

      if(rbw.getState() != ReplicaState.RBW) {
        throw new IOException("Expected replica state: " + ReplicaState.RBW
            + " obtained " + rbw.getState() + " for converting block "
            + b.getBlockId());
      }
      //构造新的ReplicaInfo对象， 并在volumeMap中更新
      // overwrite the RBW in the volume map
      volumeMap.add(b.getBlockPoolId(), rbw.getReplicaInfo());
      return rbw;
    }
  }

  private boolean isReplicaProvided(ReplicaInfo replicaInfo) {
    if (replicaInfo == null) {
      return false;
    }
    return replicaInfo.getVolume().getStorageType() == StorageType.PROVIDED;
  }

  @Override // FsDatasetSpi
  public ReplicaHandler createTemporary(StorageType storageType,
      String storageId, ExtendedBlock b, boolean isTransfer)
      throws IOException {
    long startTimeMs = Time.monotonicNow();
    long writerStopTimeoutMs = datanode.getDnConf().getXceiverStopTimeout();
    ReplicaInfo lastFoundReplicaInfo = null;
    boolean isInPipeline = false;

    // 这是一个do {} while(); 循环代码...
    do {

      try (AutoCloseableLock lock = datasetLock.acquire()) {


        ReplicaInfo currentReplicaInfo =  volumeMap.get(b.getBlockPoolId(), b.getBlockId());
        if (currentReplicaInfo == lastFoundReplicaInfo) {
          break;
        } else {
          //是否在构建中
          isInPipeline = currentReplicaInfo.getState() == ReplicaState.TEMPORARY
              || currentReplicaInfo.getState() == ReplicaState.RBW;
          /* 如果当前的block不是外部存储并且是旧的block 直接拒绝
           * 否则，如果是请求转移，则接受它。
           * 否则，如果状态不是RBW/Temporary，则拒绝
           * 如果是外部存储，则忽略这个副本。
           *
           * If the current block is not PROVIDED and old, reject.
           * else If transfer request, then accept it.
           * else if state is not RBW/Temporary, then reject
           * If current block is PROVIDED, ignore the replica.
           */
          if (((currentReplicaInfo.getGenerationStamp() >= b
              .getGenerationStamp()) || (!isTransfer && !isInPipeline))
              && !isReplicaProvided(currentReplicaInfo)) {
            throw new ReplicaAlreadyExistsException("Block " + b
                + " already exists in state " + currentReplicaInfo.getState()
                + " and thus cannot be created.");
          }
          lastFoundReplicaInfo = currentReplicaInfo;
        }

      }
      if (!isInPipeline) {
        // 构建中, 忽略...
        continue;
      }
      // Hang too long, just bail out. This is not supposed to happen.
      // 如果挂起太久了, 抛出异常, 正常不应该发生这种情况
      long writerStopMs = Time.monotonicNow() - startTimeMs;
      if (writerStopMs > writerStopTimeoutMs) {
        LOG.warn("Unable to stop existing writer for block " + b + " after " 
            + writerStopMs + " miniseconds.");
        throw new IOException("Unable to stop existing writer for block " + b
            + " after " + writerStopMs + " miniseconds.");
      }

      // if lastFoundReplicaInfo is PROVIDED and FINALIZED,
      // stopWriter isn't required.

      // 如果lastFoundReplicaInfo 是外部存储 并且是FINALIZED . 不需要stopWriter
      if (isReplicaProvided(lastFoundReplicaInfo) &&
          lastFoundReplicaInfo.getState() == ReplicaState.FINALIZED) {
        continue;
      }
      // 停止外部写入
      // Stop the previous writer
      ((ReplicaInPipeline)lastFoundReplicaInfo).stopWriter(writerStopTimeoutMs);
    } while (true);

    //
    if (lastFoundReplicaInfo != null
        && !isReplicaProvided(lastFoundReplicaInfo)) {
      // 旧的块文件应该同步删除，因为如果在同一个卷中分配，它可能会和新块发生冲突。
      //
      // 在锁外作为其磁盘IO进行删除。

      // Old blockfile should be deleted synchronously as it might collide
      // with the new block if allocated in same volume.
      // Do the deletion outside of lock as its DISK IO.
      invalidate(b.getBlockPoolId(), new Block[] { lastFoundReplicaInfo },
          false);
    }


    try (AutoCloseableLock lock = datasetLock.acquire()) {

      // 获取FsVolumeReference
      FsVolumeReference ref = volumes.getNextVolume(storageType, storageId, b .getNumBytes());

      //获取一个FsVolumeImpl对象来存储该副本
      FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
      ReplicaInPipeline newReplicaInfo;
      try {
        // 构建Temporary 存储
        newReplicaInfo = v.createTemporary(b);
      } catch (IOException e) {
        IOUtils.cleanup(null, ref);
        throw e;
      }
      //将新构造的ReplicaInPipeline对象放入FsDatasetImpl.volumeMap中
      volumeMap.add(b.getBlockPoolId(), newReplicaInfo.getReplicaInfo());
      return new ReplicaHandler(newReplicaInfo, ref);
    }
  }

  /**
   * Sets the offset in the meta file so that the
   * last checksum will be overwritten.
   */
  @Override // FsDatasetSpi
  public void adjustCrcChannelPosition(ExtendedBlock b, ReplicaOutputStreams streams, 
      int checksumSize) throws IOException {
    FileOutputStream file = (FileOutputStream)streams.getChecksumOut();
    FileChannel channel = file.getChannel();
    long oldPos = channel.position();
    long newPos = oldPos - checksumSize;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Changing meta file offset of block " + b + " from " +
          oldPos + " to " + newPos);
    }
    channel.position(newPos);
  }

  //
  // REMIND - mjc - eventually we should have a timeout system
  // in place to clean up block files left by abandoned clients.
  // We should have some timer in place, so that if a blockfile
  // is created but non-valid, and has been idle for >48 hours,
  // we can GC it safely.
  //

  /**
   * Complete the block write!
   */
  @Override // FsDatasetSpi
  public void finalizeBlock(ExtendedBlock b, boolean fsyncDir)
      throws IOException {
    ReplicaInfo replicaInfo = null;
    ReplicaInfo finalizedReplicaInfo = null;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      if (Thread.interrupted()) {
        // Don't allow data modifications from interrupted threads
        throw new IOException("Cannot finalize block from Interrupted Thread");
      }
      replicaInfo = getReplicaInfo(b);
      if (replicaInfo.getState() == ReplicaState.FINALIZED) {
        // this is legal, when recovery happens on a file that has
        // been opened for append but never modified
        return;
      }
      finalizedReplicaInfo = finalizeReplica(b.getBlockPoolId(), replicaInfo);
    }
    /*
     * Sync the directory after rename from tmp/rbw to Finalized if
     * configured. Though rename should be atomic operation, sync on both
     * dest and src directories are done because IOUtils.fsync() calls
     * directory's channel sync, not the journal itself.
     */
    if (fsyncDir && finalizedReplicaInfo instanceof FinalizedReplica
        && replicaInfo instanceof LocalReplica) {
      FinalizedReplica finalizedReplica =
          (FinalizedReplica) finalizedReplicaInfo;
      finalizedReplica.fsyncDirectory();
      LocalReplica localReplica = (LocalReplica) replicaInfo;
      localReplica.fsyncDirectory();
    }
  }

  private ReplicaInfo finalizeReplica(String bpid, ReplicaInfo replicaInfo)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // Compare generation stamp of old and new replica before finalizing
      if (volumeMap.get(bpid, replicaInfo.getBlockId()).getGenerationStamp()
          > replicaInfo.getGenerationStamp()) {
        throw new IOException("Generation Stamp should be monotonically "
            + "increased.");
      }

      ReplicaInfo newReplicaInfo = null;
      // 如果副本处于恢复状态， 并且恢复前的状态为FINALIZED
      // 则直接将副本状态转换为FINALIZED即可
      if (replicaInfo.getState() == ReplicaState.RUR &&
          replicaInfo.getOriginalReplica().getState()
          == ReplicaState.FINALIZED) {
        newReplicaInfo = replicaInfo.getOriginalReplica();
        ((FinalizedReplica)newReplicaInfo).loadLastPartialChunkChecksum();
      } else {
        //对于其他情况， 则首先获取FsVolumeImpl对象
        FsVolumeImpl v = (FsVolumeImpl)replicaInfo.getVolume();
        if (v == null) {
          throw new IOException("No volume for block " + replicaInfo);
        }
        // 然后调用addFinalizedBlock()方法将数据块移至finalized目录下
        newReplicaInfo = v.addFinalizedBlock(
            bpid, replicaInfo, replicaInfo, replicaInfo.getBytesReserved());
        if (v.isTransientStorage()) {
          releaseLockedMemory(
              replicaInfo.getOriginalBytesReserved()
                  - replicaInfo.getNumBytes(),
              false);
          ramDiskReplicaTracker.addReplica(
              bpid, replicaInfo.getBlockId(), v, replicaInfo.getNumBytes());
          datanode.getMetrics().addRamDiskBytesWrite(replicaInfo.getNumBytes());
        }
      }
      assert newReplicaInfo.getState() == ReplicaState.FINALIZED
          : "Replica should be finalized";

      //更新volumeMap中的副本信息
      volumeMap.add(bpid, newReplicaInfo);

      //返回新构造的ReplicaInfo对象
      return newReplicaInfo;
    }
  }

  /**
   * Remove the temporary block file (if any)
   */
  @Override // FsDatasetSpi
  public void unfinalizeBlock(ExtendedBlock b) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(),
          b.getLocalBlock());
      if (replicaInfo != null &&
          replicaInfo.getState() == ReplicaState.TEMPORARY) {
        // remove from volumeMap
        // 从内存volumeMap 移除
        volumeMap.remove(b.getBlockPoolId(), b.getLocalBlock());

        // delete the on-disk temp file
        // 删除磁盘文件
        if (delBlockFromDisk(replicaInfo)) {
          LOG.warn("Block " + b + " unfinalized and removed. ");
        }
        if (replicaInfo.getVolume().isTransientStorage()) {
          ramDiskReplicaTracker.discardReplica(b.getBlockPoolId(),
              b.getBlockId(), true);
        }
      }
    }
  }

  /**
   * Remove a block from disk
   * @param info the replica that needs to be deleted
   * @return true if data for the replica are deleted; false otherwise
   */
  private boolean delBlockFromDisk(ReplicaInfo info) {
    
    if (!info.deleteBlockData()) {
      LOG.warn("Not able to delete the block data for replica " + info);
      return false;
    } else { // remove the meta file
      if (!info.deleteMetadata()) {
        LOG.warn("Not able to delete the meta data for replica " + info);
        return false;
      }
    }
    return true;
  }

  @Override // FsDatasetSpi
  public List<Long> getCacheReport(String bpid) {
    return cacheManager.getCachedBlocks(bpid);
  }

  @Override
  public Map<DatanodeStorage, BlockListAsLongs> getBlockReports(String bpid) {

    //构造返回值对象
    Map<DatanodeStorage, BlockListAsLongs> blockReportsMap =  new HashMap<DatanodeStorage, BlockListAsLongs>();

    Map<String, BlockListAsLongs.Builder> builders =  new HashMap<String, BlockListAsLongs.Builder>();

    List<FsVolumeImpl> curVolumes = null;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // 0 : /tools/hadoop-3.2.1/data/hdfs/data
      curVolumes = volumes.getVolumes();
      for (FsVolumeSpi v : curVolumes) {
        // v:  FsVolumeImple : /tools/hadoop-3.2.1/data/hdfs/data
        //  builders : DS-23b73402-fa18-451d-930f-831de2e8686e -> {BlockListAsLongs$Builder@4462}
        builders.put(v.getStorageID(), BlockListAsLongs.builder(maxDataLength));
      }
      // volumeMap :  BP-451827885-192.168.8.156-1584099133244 -> {FoldedTreeSet@4644}  size = 5077
      Set<String> missingVolumesReported = new HashSet<>();
      for (ReplicaInfo b : volumeMap.replicas(bpid)) {
        // skip PROVIDED replicas.
        // 跳过外部存储
        if (b.getVolume().getStorageType() == StorageType.PROVIDED) {
          continue;
        }
        // 获取存储id  : DS-23b73402-fa18-451d-930f-831de2e8686e
        String volStorageID = b.getVolume().getStorageID();

        if (!builders.containsKey(volStorageID)) {
          if (!missingVolumesReported.contains(volStorageID)) {
            LOG.warn("Storage volume: " + volStorageID + " missing for the"
                + " replica block: " + b + ". Probably being removed!");
            // 添加丢失卷信息
            missingVolumesReported.add(volStorageID);
          }
          continue;
        }
        LOG.info(" volStorageID: {} ,blockID : {} , bState  : {} ",volStorageID, b.getBlockId() , b.getState());
        switch(b.getState()) {
        case FINALIZED:
        case RBW:
        case RWR:
          // 写入FINALIZED,RBW , RWR 到 builders
          builders.get(volStorageID).add(b);
          break;
        case RUR:
          // 构建中的副本 写入builders
          ReplicaInfo orig = b.getOriginalReplica();
          builders.get(volStorageID).add(orig);
          break;
        case TEMPORARY:
          break;
        default:
          assert false : "Illegal ReplicaInfo state.";
        }
      }
    }

    for (FsVolumeImpl v : curVolumes) {
      // 写入所有的副本
      blockReportsMap.put(v.toDatanodeStorage(),
                          builders.get(v.getStorageID()).build());
    }

    return blockReportsMap;
  }

  /**
   * Gets a list of references to the finalized blocks for the given block pool.
   * <p>
   * Callers of this function should call
   * {@link FsDatasetSpi#acquireDatasetLock} to avoid blocks' status being
   * changed during list iteration.
   * </p>
   * @return a list of references to the finalized blocks for the given block
   *         pool.
   */
  @Override
  public List<ReplicaInfo> getFinalizedBlocks(String bpid) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final List<ReplicaInfo> finalized = new ArrayList<ReplicaInfo>(
          volumeMap.size(bpid));
      for (ReplicaInfo b : volumeMap.replicas(bpid)) {
        if (b.getState() == ReplicaState.FINALIZED) {
          finalized.add(b);
        }
      }
      return finalized;
    }
  }

  /**
   * Check if a block is valid.
   *
   * @param b           The block to check.
   * @param minLength   The minimum length that the block must have.  May be 0.
   * @param state       If this is null, it is ignored.  If it is non-null, we
   *                        will check that the replica has this state.
   *
   * @throws ReplicaNotFoundException          If the replica is not found 
   *
   * @throws UnexpectedReplicaStateException   If the replica is not in the 
   *                                             expected state.
   * @throws FileNotFoundException             If the block file is not found or there
   *                                              was an error locating it.
   * @throws EOFException                      If the replica length is too short.
   * 
   * @throws IOException                       May be thrown from the methods called. 
   */
  @Override // FsDatasetSpi
  public void checkBlock(ExtendedBlock b, long minLength, ReplicaState state)
      throws ReplicaNotFoundException, UnexpectedReplicaStateException,
      FileNotFoundException, EOFException, IOException {
    final ReplicaInfo replicaInfo = volumeMap.get(b.getBlockPoolId(), 
        b.getLocalBlock());
    if (replicaInfo == null) {
      throw new ReplicaNotFoundException(b);
    }
    if (replicaInfo.getState() != state) {
      throw new UnexpectedReplicaStateException(b,state);
    }
    if (!replicaInfo.blockDataExists()) {
      throw new FileNotFoundException(replicaInfo.getBlockURI().toString());
    }
    long onDiskLength = getLength(b);
    if (onDiskLength < minLength) {
      throw new EOFException(b + "'s on-disk length " + onDiskLength
          + " is shorter than minLength " + minLength);
    }
  }

  /**
   * Check whether the given block is a valid one.
   * valid means finalized
   */
  @Override // FsDatasetSpi
  public boolean isValidBlock(ExtendedBlock b) {
    // If block passed is null, we should return false.
    if (b == null) {
      return false;
    }
    return isValid(b, ReplicaState.FINALIZED);
  }
  
  /**
   * Check whether the given block is a valid RBW.
   */
  @Override // {@link FsDatasetSpi}
  public boolean isValidRbw(final ExtendedBlock b) {
    // If block passed is null, we should return false.
    if (b == null) {
      return false;
    }
    return isValid(b, ReplicaState.RBW);
  }

  /** Does the block exist and have the given state? */
  private boolean isValid(final ExtendedBlock b, final ReplicaState state) {
    try {
      checkBlock(b, 0, state);
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  /**
   * Find the file corresponding to the block and return it if it exists.
   */
  ReplicaInfo validateBlockFile(String bpid, long blockId) {
    //Should we check for metadata file too?
    final ReplicaInfo r;
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      r = volumeMap.get(bpid, blockId);
    }
    if (r != null) {
      if (r.blockDataExists()) {
        return r;
      }
      // if file is not null, but doesn't exist - possibly disk failed
      datanode.checkDiskErrorAsync(r.getVolume());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("blockId=" + blockId + ", replica=" + r);
    }
    return null;
  }

  /** Check the files of a replica. */
  static void checkReplicaFiles(final ReplicaInfo r) throws IOException {
    //check replica's data exists
    if (!r.blockDataExists()) {
      throw new FileNotFoundException("Block data not found, r=" + r);
    }
    if (r.getBytesOnDisk() != r.getBlockDataLength()) {
      throw new IOException("Block length mismatch, len="
          + r.getBlockDataLength() + " but r=" + r);
    }

    //check replica's meta file
    if (!r.metadataExists()) {
      throw new IOException(r.getMetadataURI() + " does not exist, r=" + r);
    }
    if (r.getMetadataLength() == 0) {
      throw new IOException("Metafile is empty, r=" + r);
    }
  }

  /**
   * We're informed that a block is no longer valid. Delete it.
   */
  @Override // FsDatasetSpi
  public void invalidate(String bpid, Block invalidBlks[]) throws IOException {
    invalidate(bpid, invalidBlks, true);
  }

  private void invalidate(String bpid, Block[] invalidBlks, boolean async)
      throws IOException {
    final List<String> errors = new ArrayList<String>();
    for (int i = 0; i < invalidBlks.length; i++) {
      final ReplicaInfo removing;
      final FsVolumeImpl v;
      try (AutoCloseableLock lock = datasetLock.acquire()) {
        // 获取内存中的副本信息
        final ReplicaInfo info = volumeMap.get(bpid, invalidBlks[i]);


        if (info == null) {

          ReplicaInfo infoByBlockId =  volumeMap.get(bpid, invalidBlks[i].getBlockId());

          if (infoByBlockId == null) {
            // It is okay if the block is not found -- it
            // may be deleted earlier.
            LOG.info("Failed to delete replica " + invalidBlks[i]
                + ": ReplicaInfo not found.");
          } else {
            errors.add("Failed to delete replica " + invalidBlks[i]
                + ": GenerationStamp not matched, existing replica is "
                + Block.toString(infoByBlockId));
          }
          continue;
        }

        v = (FsVolumeImpl)info.getVolume();
        if (v == null) {
          errors.add("Failed to delete replica " + invalidBlks[i]
              +  ". No volume for replica " + info);
          continue;
        }
        try {
          File blockFile = new File(info.getBlockURI());
          if (blockFile != null && blockFile.getParentFile() == null) {
            errors.add("Failed to delete replica " + invalidBlks[i]
                +  ". Parent not found for block file: " + blockFile);
            continue;
          }
        } catch(IllegalArgumentException e) {
          LOG.warn("Parent directory check failed; replica " + info
              + " is not backed by a local file");
        }

        //从volumeMap中移除副本信息
        removing = volumeMap.remove(bpid, invalidBlks[i]);
        addDeletingBlock(bpid, removing.getBlockId());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Block file " + removing.getBlockURI()
              + " is to be deleted");
        }
        if (removing instanceof ReplicaInPipeline) {
          ((ReplicaInPipeline) removing).releaseAllBytesReserved();
        }
      }

      if (v.isTransientStorage()) {

        RamDiskReplica replicaInfo =
          ramDiskReplicaTracker.getReplica(bpid, invalidBlks[i].getBlockId());
        if (replicaInfo != null) {
          if (!replicaInfo.getIsPersisted()) {
            datanode.getMetrics().incrRamDiskBlocksDeletedBeforeLazyPersisted();
          }
          ramDiskReplicaTracker.discardReplica(replicaInfo.getBlockPoolId(),
            replicaInfo.getBlockId(), true);
        }
      }

      // 如果副本正在被短路读取， 并且副本信息存储在短路读取的共享内存中， 则删除
      // If a DFSClient has the replica in its cache of short-circuit file
      // descriptors (and the client is using ShortCircuitShm), invalidate it.
      datanode.getShortCircuitRegistry().processBlockInvalidation(
                new ExtendedBlockId(invalidBlks[i].getBlockId(), bpid));

      // 如果副本在Datanode中被缓存， 则从缓存中删除当前副本
      // If the block is cached, start uncaching it.
      cacheManager.uncacheBlock(bpid, invalidBlks[i].getBlockId());

      try {
        if (async) {
          // Delete the block asynchronously to make sure we can do it fast
          // enough.
          // It's ok to unlink the block file before the uncache operation
          // finishes.

          // 调用异步的磁盘文件删除功能， 删除数据块文件以及校验文件
          asyncDiskService.deleteAsync(v.obtainReference(), removing,
              new ExtendedBlock(bpid, invalidBlks[i]),
              dataStorage.getTrashDirectoryForReplica(bpid, removing));
        } else {
          asyncDiskService.deleteSync(v.obtainReference(), removing,
              new ExtendedBlock(bpid, invalidBlks[i]),
              dataStorage.getTrashDirectoryForReplica(bpid, removing));
        }
      } catch (ClosedChannelException e) {
        LOG.warn("Volume " + v + " is closed, ignore the deletion task for " +
            "block " + invalidBlks[i]);
      }
    }
    if (!errors.isEmpty()) {
      StringBuilder b = new StringBuilder("Failed to delete ")
        .append(errors.size()).append(" (out of ").append(invalidBlks.length)
        .append(") replica(s):");
      for(int i = 0; i < errors.size(); i++) {
        b.append("\n").append(i).append(") ").append(errors.get(i));
      }
      throw new IOException(b.toString());
    }
  }

  /**
   * Invalidate a block but does not delete the actual on-disk block file.
   *
   * It should only be used when deactivating disks.
   *
   * @param bpid the block pool ID.
   * @param block The block to be invalidated.
   */
  public void invalidate(String bpid, ReplicaInfo block) {
    // If a DFSClient has the replica in its cache of short-circuit file
    // descriptors (and the client is using ShortCircuitShm), invalidate it.
    datanode.getShortCircuitRegistry().processBlockInvalidation(
        new ExtendedBlockId(block.getBlockId(), bpid));

    // If the block is cached, start uncaching it.
    cacheManager.uncacheBlock(bpid, block.getBlockId());

    datanode.notifyNamenodeDeletedBlock(new ExtendedBlock(bpid, block),
        block.getStorageUuid());
  }

  /**
   * Asynchronously attempts to cache a single block via {@link FsDatasetCache}.
   */
  private void cacheBlock(String bpid, long blockId) {
    FsVolumeImpl volume;
    String blockFileName;
    long length, genstamp;
    Executor volumeExecutor;

    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ReplicaInfo info = volumeMap.get(bpid, blockId);
      boolean success = false;
      try {
        if (info == null) {
          LOG.warn("Failed to cache block with id " + blockId + ", pool " +
              bpid + ": ReplicaInfo not found.");
          return;
        }
        if (info.getState() != ReplicaState.FINALIZED) {
          LOG.warn("Failed to cache block with id " + blockId + ", pool " +
              bpid + ": replica is not finalized; it is in state " +
              info.getState());
          return;
        }
        try {
          volume = (FsVolumeImpl)info.getVolume();
          if (volume == null) {
            LOG.warn("Failed to cache block with id " + blockId + ", pool " +
                bpid + ": volume not found.");
            return;
          }
        } catch (ClassCastException e) {
          LOG.warn("Failed to cache block with id " + blockId +
              ": volume was not an instance of FsVolumeImpl.");
          return;
        }
        if (volume.isTransientStorage()) {
          LOG.warn("Caching not supported on block with id " + blockId +
              " since the volume is backed by RAM.");
          return;
        }
        success = true;
      } finally {
        if (!success) {
          cacheManager.numBlocksFailedToCache.incrementAndGet();
        }
      }
      blockFileName = info.getBlockURI().toString();
      length = info.getVisibleLength();
      genstamp = info.getGenerationStamp();
      volumeExecutor = volume.getCacheExecutor();
    }
    cacheManager.cacheBlock(blockId, bpid, 
        blockFileName, length, genstamp, volumeExecutor);
  }

  @Override // FsDatasetSpi
  public void cache(String bpid, long[] blockIds) {
    for (int i=0; i < blockIds.length; i++) {
      cacheBlock(bpid, blockIds[i]);
    }
  }

  @Override // FsDatasetSpi
  public void uncache(String bpid, long[] blockIds) {
    for (int i=0; i < blockIds.length; i++) {
      cacheManager.uncacheBlock(bpid, blockIds[i]);
    }
  }

  @Override
  public boolean isCached(String bpid, long blockId) {
    return cacheManager.isCached(bpid, blockId);
  }

  @Override // FsDatasetSpi
  public boolean contains(final ExtendedBlock block) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final long blockId = block.getLocalBlock().getBlockId();
      final String bpid = block.getBlockPoolId();
      final ReplicaInfo r = volumeMap.get(bpid, blockId);
      return (r != null && r.blockDataExists());
    }
  }

  /**
   * check if a data directory is healthy
   *
   * if some volumes failed - the caller must emove all the blocks that belong
   * to these failed volumes.
   * @return the failed volumes. Returns null if no volume failed.
   * @param failedVolumes
   */
  @Override // FsDatasetSpi
  public void handleVolumeFailures(Set<FsVolumeSpi> failedVolumes) {
    volumes.handleVolumeFailures(failedVolumes);
  }
    

  @Override // FsDatasetSpi
  public String toString() {
    return "FSDataset{dirpath='"+volumes+"'}";
  }

  private ObjectName mbeanName;
  
  /**
   * Register the FSDataset MBean using the name
   *        "hadoop:service=DataNode,name=FSDatasetState-<datanodeUuid>"
   */
  void registerMBean(final String datanodeUuid) {
    // We wrap to bypass standard mbean naming convetion.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    try {
      StandardMBean bean = new StandardMBean(this,FSDatasetMBean.class);
      mbeanName = MBeans.register("DataNode", "FSDatasetState-" + datanodeUuid, bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Error registering FSDatasetState MBean", e);
    }
    LOG.info("Registered FSDatasetState MBean");
  }

  @Override // FsDatasetSpi
  public void shutdown() {
    fsRunning = false;

    if (lazyWriter != null) {
      ((LazyWriter) lazyWriter.getRunnable()).stop();
      lazyWriter.interrupt();
    }

    if (mbeanName != null) {
      MBeans.unregister(mbeanName);
    }
    
    if (asyncDiskService != null) {
      asyncDiskService.shutdown();
    }

    if (asyncLazyPersistService != null) {
      asyncLazyPersistService.shutdown();
    }
    
    if(volumes != null) {
      volumes.shutdown();
    }

    if (lazyWriter != null) {
      try {
        lazyWriter.join();
      } catch (InterruptedException ie) {
        LOG.warn("FsDatasetImpl.shutdown ignoring InterruptedException " +
                     "from LazyWriter.join");
      }
    }
  }

  @Override // FSDatasetMBean
  public String getStorageInfo() {
    return toString();
  }

  /**
   *
   * 处理磁盘上的块和卷映射中的块之间的差异
   * 检查给定block的差异性.
   * 检查内存和磁盘中block的差异性:
   * 1. 如果block文件丢失, 删除内存中的信息.
   * 2. 如果block文件存在,内存中不存在,更新内存中的信息
   * 3. 如果时间戳不匹配,使用正确的时间错
   * 4. 如果如果内存中的block大小跟实际的blokck大小不一致,标记这个block为损坏,更新内存中的block
   * 5. 如果内存中的block和磁盘中的block不匹配,更新内存中的block文件
   *
   * Reconcile the difference between blocks on the disk and blocks in volumeMap
   *
   * Check the given block for inconsistencies.
   * Look at the current state of the block and reconcile the differences as follows:
   * <ul>
   * <li>If the block file is missing, delete the block from volumeMap</li>
   * <li>If the block file exists and the block is missing in volumeMap,
   * add the block to volumeMap <li>
   * <li>If generation stamp does not match, then update the block with right
   * generation stamp</li>
   * <li>If the block length in memory does not match the actual block file length
   * then mark the block as corrupt and update the block length in memory</li>
   * <li>If the file in {@link ReplicaInfo} does not match the file on
   * the disk, update {@link ReplicaInfo} with the correct file</li>
   * </ul>
   *
   * @param bpid block pool ID
   * @param scanInfo {@link ScanInfo} for a given block
   */
  @Override
  public void checkAndUpdate(String bpid, ScanInfo scanInfo)
      throws IOException {

    // 获取blockId
    long blockId = scanInfo.getBlockId();

    // 获取块文件
    File diskFile = scanInfo.getBlockFile();

    // 获取元数据文件
    File diskMetaFile = scanInfo.getMetaFile();

    // 获取volume信息
    FsVolumeSpi vol = scanInfo.getVolume();

    Block corruptBlock = null;
    ReplicaInfo memBlockInfo;
    // 获取lock
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      //获取内存block信息
      memBlockInfo = volumeMap.get(bpid, blockId);

      // block正在持久化中, 忽略不同信息
      if (memBlockInfo != null &&
          memBlockInfo.getState() != ReplicaState.FINALIZED) {
        // Block is not finalized - ignore the difference
        return;
      }

      // 获取fileIoProvider
      final FileIoProvider fileIoProvider = datanode.getFileIoProvider();

      // 验证磁盘元数据是否存在
      final boolean diskMetaFileExists = diskMetaFile != null &&
          fileIoProvider.exists(vol, diskMetaFile);

      // 验证block文件是否存在
      final boolean diskFileExists = diskFile != null &&
          fileIoProvider.exists(vol, diskFile);

      // 生成戳标记 ?????
      final long diskGS = diskMetaFileExists ?
          Block.getGenerationStamp(diskMetaFile.getName()) :
          HdfsConstants.GRANDFATHER_GENERATION_STAMP;


      //外部存储?? 略过...
      if (vol.getStorageType() == StorageType.PROVIDED) {
        if (memBlockInfo == null) {
          // replica exists on provided store but not in memory
          ReplicaInfo diskBlockInfo =
              new ReplicaBuilder(ReplicaState.FINALIZED)
              .setFileRegion(scanInfo.getFileRegion())
              .setFsVolume(vol)
              .setConf(conf)
              .build();

          volumeMap.add(bpid, diskBlockInfo);
          LOG.warn("Added missing block to memory " + diskBlockInfo);
        } else {
          // replica exists in memory but not in the provided store
          volumeMap.remove(bpid, blockId);
          LOG.warn("Deleting missing provided block " + memBlockInfo);
        }
        return;
      }

      // 如果磁盘文件不存在
      if (!diskFileExists) {
        //如果元数据为空
        if (memBlockInfo == null) {
          // Block file does not exist and block does not exist in memory
          // If metadata file exists then delete it
          // block文件不存在, 内存中不存在, 但是有原信息文件存在,则删除.
          if (diskMetaFileExists && fileIoProvider.delete(vol, diskMetaFile)) {
            LOG.warn("Deleted a metadata file without a block "
                + diskMetaFile.getAbsolutePath());
          }
          return;
        }
        //如果元信息文件不存在
        if (!memBlockInfo.blockDataExists()) {
          // Block is in memory and not on the disk
          // Remove the block from volumeMap
          // block在内存中,但是不在磁盘中, 从内存中清理到元信息.
          volumeMap.remove(bpid, blockId);

          if (vol.isTransientStorage()) {
            ramDiskReplicaTracker.discardReplica(bpid, blockId, true);
          }
          LOG.warn("Removed block " + blockId
              + " from memory with missing block file on the disk");
          // Finally remove the metadata file
          // 最后移除元信息文件
          if (diskMetaFileExists && fileIoProvider.delete(vol, diskMetaFile)) {
            LOG.warn("Deleted a metadata file for the deleted block "
                + diskMetaFile.getAbsolutePath());
          }
        }
        return;
      }
      /* block文件存在于disk
       * Block file exists on the disk
       */
      if (memBlockInfo == null) {
        // Block is missing in memory - add the block to volumeMap
        // 内存中的block信息丢失, 向内存中添加block信息.

        //构造内存对象
        ReplicaInfo diskBlockInfo = new ReplicaBuilder(ReplicaState.FINALIZED)
            .setBlockId(blockId)
            .setLength(diskFile.length())
            .setGenerationStamp(diskGS)
            .setFsVolume(vol)
            .setDirectoryToUse(diskFile.getParentFile())
            .build();

        //更新内存信息
        volumeMap.add(bpid, diskBlockInfo);
        if (vol.isTransientStorage()) {
          long lockedBytesReserved =
              cacheManager.reserve(diskBlockInfo.getNumBytes()) > 0 ?
                  diskBlockInfo.getNumBytes() : 0;
          ramDiskReplicaTracker.addReplica(
              bpid, blockId, (FsVolumeImpl) vol, lockedBytesReserved);
        }
        LOG.warn("Added missing block to memory " + diskBlockInfo);
        return;
      }
      /*
       * Block exists in volumeMap and the block file exists on the disk
       * block 在内存和磁盘中都存在
       */
      // 对比block文件
      // Compare block files
      if (memBlockInfo.blockDataExists()) {
        if (memBlockInfo.getBlockURI().compareTo(diskFile.toURI()) != 0) {
          if (diskMetaFileExists) {
            if (memBlockInfo.metadataExists()) {
              // We have two sets of block+meta files. Decide which one to
              // keep.
              ReplicaInfo diskBlockInfo =
                  new ReplicaBuilder(ReplicaState.FINALIZED)
                    .setBlockId(blockId)
                    .setLength(diskFile.length())
                    .setGenerationStamp(diskGS)
                    .setFsVolume(vol)
                    .setDirectoryToUse(diskFile.getParentFile())
                    .build();
              ((FsVolumeImpl) vol).resolveDuplicateReplicas(bpid,
                  memBlockInfo, diskBlockInfo, volumeMap);
            }
          } else {
            if (!fileIoProvider.delete(vol, diskFile)) {
              LOG.warn("Failed to delete " + diskFile);
            }
          }
        }
      } else {
        // Block refers to a block file that does not exist.
        // Update the block with the file found on the disk. Since the block
        // file and metadata file are found as a pair on the disk, update
        // the block based on the metadata file found on the disk
        LOG.warn("Block file in replica "
            + memBlockInfo.getBlockURI()
            + " does not exist. Updating it to the file found during scan "
            + diskFile.getAbsolutePath());

        // 更新内存信息
        memBlockInfo.updateWithReplica(
            StorageLocation.parse(diskFile.toString()));

        LOG.warn("Updating generation stamp for block " + blockId
            + " from " + memBlockInfo.getGenerationStamp() + " to " + diskGS);
        memBlockInfo.setGenerationStamp(diskGS);
      }

      // Compare generation stamp
      // 对比时间戳
      if (memBlockInfo.getGenerationStamp() != diskGS) {
        File memMetaFile = FsDatasetUtil.getMetaFile(diskFile, 
            memBlockInfo.getGenerationStamp());
        if (fileIoProvider.exists(vol, memMetaFile)) {
          String warningPrefix = "Metadata file in memory "
              + memMetaFile.getAbsolutePath()
              + " does not match file found by scan ";
          if (!diskMetaFileExists) {
            LOG.warn(warningPrefix + "null");
          } else if (memMetaFile.compareTo(diskMetaFile) != 0) {
            LOG.warn(warningPrefix + diskMetaFile.getAbsolutePath());
          }
        } else {
          // Metadata file corresponding to block in memory is missing


          // If metadata file found during the scan is on the same directory
          // as the block file, then use the generation stamp from it
          try {
            File memFile = new File(memBlockInfo.getBlockURI());
            long gs = diskMetaFileExists &&
                diskMetaFile.getParent().equals(memFile.getParent()) ? diskGS
                : HdfsConstants.GRANDFATHER_GENERATION_STAMP;

            LOG.warn("Updating generation stamp for block " + blockId
                + " from " + memBlockInfo.getGenerationStamp() + " to " + gs);

            memBlockInfo.setGenerationStamp(gs);
          } catch (IllegalArgumentException e) {
            //exception arises because the URI cannot be converted to a file
            LOG.warn("Block URI could not be resolved to a file", e);
          }
        }
      }

      // Compare block size
      // 对比大小
      if (memBlockInfo.getNumBytes() != memBlockInfo.getBlockDataLength()) {
        // Update the length based on the block file
        corruptBlock = new Block(memBlockInfo);
        LOG.warn("Updating size of block " + blockId + " from "
            + memBlockInfo.getNumBytes() + " to "
            + memBlockInfo.getBlockDataLength());
        memBlockInfo.setNumBytes(memBlockInfo.getBlockDataLength());
      }
    }

    // Send corrupt block report outside the lock
    // 发送损坏block信息
    if (corruptBlock != null) {
      LOG.warn("Reporting the block " + corruptBlock
          + " as corrupt due to length mismatch");
      try {
        datanode.reportBadBlocks(new ExtendedBlock(bpid, corruptBlock),
            memBlockInfo.getVolume());
      } catch (IOException e) {
        LOG.warn("Failed to report bad block " + corruptBlock, e);
      }
    }
  }

  /**
   * @deprecated use {@link #fetchReplicaInfo(String, long)} instead.
   */
  @Override // FsDatasetSpi
  @Deprecated
  public ReplicaInfo getReplica(String bpid, long blockId) {
    return volumeMap.get(bpid, blockId);
  }

  @Override 
  public String getReplicaString(String bpid, long blockId) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final Replica r = volumeMap.get(bpid, blockId);
      return r == null ? "null" : r.toString();
    }
  }

  @Override // FsDatasetSpi
  public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock)
      throws IOException {
    return initReplicaRecovery(rBlock.getBlock().getBlockPoolId(), volumeMap,
        rBlock.getBlock().getLocalBlock(), rBlock.getNewGenerationStamp(),
        datanode.getDnConf().getXceiverStopTimeout());
  }

  /** static version of {@link #initReplicaRecovery(RecoveringBlock)}. */
  static ReplicaRecoveryInfo initReplicaRecovery(String bpid, ReplicaMap map,
      Block block, long recoveryId, long xceiverStopTimeout) throws IOException {
    while (true) {
      try {
        try (AutoCloseableLock lock = map.getLock().acquire()) {
          return initReplicaRecoveryImpl(bpid, map, block, recoveryId);
        }
      } catch (MustStopExistingWriter e) {
        e.getReplicaInPipeline().stopWriter(xceiverStopTimeout);
      }
    }
  }

  static ReplicaRecoveryInfo initReplicaRecoveryImpl(String bpid, ReplicaMap map,
      Block block, long recoveryId)
          throws IOException, MustStopExistingWriter {

    // 从volumeMap中获取副本信息
    final ReplicaInfo replica = map.get(bpid, block.getBlockId());
    LOG.info("initReplicaRecovery: " + block + ", recoveryId=" + recoveryId + ", replica=" + replica);

    // //确认副本存在
    // check replica
    if (replica == null) {
      return null;
    }

    // 如果有写线程则停止写线程
    // stop writer if there is any
    if (replica.getState() == ReplicaState.TEMPORARY ||
        replica.getState() == ReplicaState.RBW) {
      final ReplicaInPipeline rip = (ReplicaInPipeline)replica;

      //确认接收的数据大于可见的数据
      if (!rip.attemptToSetWriter(null, Thread.currentThread())) {
        throw new MustStopExistingWriter(rip);
      }

      // check replica bytes on disk.
      // 检查副本文件
      if (replica.getBytesOnDisk() < replica.getVisibleLength()) {
        throw new IOException("getBytesOnDisk() < getVisibleLength(), rip="
            + replica);
      }

      //check the replica's files
      checkReplicaFiles(replica);
    }
    //
    // 检查副本时间戳
    // check generation stamp
    if (replica.getGenerationStamp() < block.getGenerationStamp()) {
      throw new IOException(
          "replica.getGenerationStamp() < block.getGenerationStamp(), block="
          + block + ", replica=" + replica);
    }

    // 检查recoveryId， 确认不是一个过期的恢复操作
    // check recovery id
    if (replica.getGenerationStamp() >= recoveryId) {
      throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
          + " replica.getGenerationStamp() >= recoveryId = " + recoveryId
          + ", block=" + block + ", replica=" + replica);
    }

    //构造ReplicaUnderRecovery对象， 如果已经存在RUR对象， 则更新recoveryId
    //check RUR
    final ReplicaInfo rur;
    if (replica.getState() == ReplicaState.RUR) {
      rur = replica;
      //注意， 这里要判断这个恢复操作是否是一个过期或者重复的操作
      if (rur.getRecoveryID() >= recoveryId) {
        throw new RecoveryInProgressException(
            "rur.getRecoveryID() >= recoveryId = " + recoveryId
            + ", block=" + block + ", rur=" + rur);
      }
      final long oldRecoveryID = rur.getRecoveryID();
      rur.setRecoveryID(recoveryId);
      LOG.info("initReplicaRecovery: update recovery id for " + block
          + " from " + oldRecoveryID + " to " + recoveryId);
    }
    else {
      //  ReplicaUnderRecovery对象不存在， 则构造RUR对象， 并更新volumeMap
      rur = new ReplicaBuilder(ReplicaState.RUR)
          .from(replica).setRecoveryId(recoveryId).build();
      map.add(bpid, rur);
      LOG.info("initReplicaRecovery: changing replica state for "
          + block + " from " + replica.getState()
          + " to " + rur.getState());
      if (replica.getState() == ReplicaState.TEMPORARY || replica
          .getState() == ReplicaState.RBW) {
        ((ReplicaInPipeline) replica).releaseAllBytesReserved();
      }
    }
    // 返回副本恢复前的信息
    return rur.createInfo();
  }

  @Override // FsDatasetSpi
  public Replica updateReplicaUnderRecovery(
                                    final ExtendedBlock oldBlock,
                                    final long recoveryId,
                                    final long newBlockId,
                                    final long newlength) throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      // get replica
      // 获取副本信息
      final String bpid = oldBlock.getBlockPoolId();
      final ReplicaInfo replica = volumeMap.get(bpid, oldBlock.getBlockId());
      LOG.info("updateReplica: " + oldBlock
          + ", recoveryId=" + recoveryId
          + ", length=" + newlength
          + ", replica=" + replica);

      // check replica
      // 检查副本信息是否存在
      if (replica == null) {
        throw new ReplicaNotFoundException(oldBlock);
      }

      //check replica state
      // 判断副本状态， 如果不是RUR则抛出异常
      if (replica.getState() != ReplicaState.RUR) {
        throw new IOException("replica.getState() != " + ReplicaState.RUR
            + ", replica=" + replica);
      }

      // 检查副本长度是否正常
      // check replica's byte on disk
      if (replica.getBytesOnDisk() != oldBlock.getNumBytes()) {
        throw new IOException("THIS IS NOT SUPPOSED TO HAPPEN:"
            + " replica.getBytesOnDisk() != block.getNumBytes(), block="
            + oldBlock + ", replica=" + replica);
      }

      //  检查副本的数据块文件和校验文件是否匹配
      //check replica files before update
      checkReplicaFiles(replica);

      // update replica
      // 将副本状态更新为目标状态
      final ReplicaInfo finalized = updateReplicaUnderRecovery(oldBlock
          .getBlockPoolId(), replica, recoveryId,
          newBlockId, newlength);


      boolean copyTruncate = newBlockId != oldBlock.getBlockId();
      if (!copyTruncate) {
        assert finalized.getBlockId() == oldBlock.getBlockId()
            && finalized.getGenerationStamp() == recoveryId
            && finalized.getNumBytes() == newlength
            : "Replica information mismatched: oldBlock=" + oldBlock
            + ", recoveryId=" + recoveryId + ", newlength=" + newlength
            + ", newBlockId=" + newBlockId + ", finalized=" + finalized;
      } else {
        assert finalized.getBlockId() == oldBlock.getBlockId()
            && finalized.getGenerationStamp() == oldBlock.getGenerationStamp()
            && finalized.getNumBytes() == oldBlock.getNumBytes()
            : "Finalized and old information mismatched: oldBlock=" + oldBlock
            + ", genStamp=" + oldBlock.getGenerationStamp()
            + ", len=" + oldBlock.getNumBytes()
            + ", finalized=" + finalized;
      }
      // check replica files after update
      // 检查副本的数据块文件和校验文件是否匹配
      checkReplicaFiles(finalized);

      //返回存储ID
      return finalized;
    }
  }

  // 私有的updateReplicaUnderRecovery()方法
  private ReplicaInfo updateReplicaUnderRecovery(
                                          String bpid,
                                          ReplicaInfo rur,
                                          long recoveryId,
                                          long newBlockId,
                                          long newlength) throws IOException {
    //check recovery id
    // 检查恢复标识， 如果是过期的恢复操作， 则抛出异常
    if (rur.getRecoveryID() != recoveryId) {
      throw new IOException("rur.getRecoveryID() != recoveryId = " + recoveryId
          + ", rur=" + rur);
    }

    boolean copyOnTruncate = newBlockId > 0L && rur.getBlockId() != newBlockId;
    // bump rur's GS to be recovery id
    if(!copyOnTruncate) {
      //更新副本时间戳到recoveryId
      rur.bumpReplicaGS(recoveryId);
    }

    //update length
    if (rur.getNumBytes() < newlength) {
      throw new IOException("rur.getNumBytes() < newlength = " + newlength
          + ", rur=" + rur);
    }

    if (rur.getNumBytes() > newlength) {
      if(!copyOnTruncate) {
        rur.breakHardLinksIfNeeded();
        //则调用truncate()方法截短数据块文件， 并重新计算校验和
        rur.truncateBlock(newlength);
        // update RUR with the new length
        rur.setNumBytes(newlength);
      } else {
        // Copying block to a new block with new blockId.
        // Not truncating original block.
        FsVolumeImpl volume = (FsVolumeImpl) rur.getVolume();
        ReplicaInPipeline newReplicaInfo = volume.updateRURCopyOnTruncate(
            rur, bpid, newBlockId, recoveryId, newlength);
        if (newReplicaInfo.getState() != ReplicaState.RBW) {
          throw new IOException("Append on block " + rur.getBlockId()
              + " returned a replica of state " + newReplicaInfo.getState()
              + "; expected RBW");
        }

        newReplicaInfo.setNumBytes(newlength);
        volumeMap.add(bpid, newReplicaInfo.getReplicaInfo());
        //将副本状态由RUR改为FINALIZED， 并更新volumeMap
        finalizeReplica(bpid, newReplicaInfo.getReplicaInfo());
      }
    }
    // finalize the block
    return finalizeReplica(bpid, rur);
  }

  @Override // FsDatasetSpi
  public long getReplicaVisibleLength(final ExtendedBlock block)
  throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final Replica replica = getReplicaInfo(block.getBlockPoolId(),
          block.getBlockId());
      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException(
            "replica.getGenerationStamp() < block.getGenerationStamp(), block="
                + block + ", replica=" + replica);
      }
      return replica.getVisibleLength();
    }
  }

  @Override
  public void addBlockPool(String bpid, Configuration conf)
      throws IOException {
    LOG.info("Adding block pool " + bpid);
    AddBlockPoolException volumeExceptions = new AddBlockPoolException();
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      try {
        volumes.addBlockPool(bpid, conf);
      } catch (AddBlockPoolException e) {
        volumeExceptions.mergeException(e);
      }
      volumeMap.initBlockPool(bpid);
    }
    try {
      volumes.getAllVolumesMap(bpid, volumeMap, ramDiskReplicaTracker);
    } catch (AddBlockPoolException e) {
      volumeExceptions.mergeException(e);
    }
    if (volumeExceptions.hasExceptions()) {
      throw volumeExceptions;
    }
  }

  @Override
  public void shutdownBlockPool(String bpid) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      LOG.info("Removing block pool " + bpid);
      Map<DatanodeStorage, BlockListAsLongs> blocksPerVolume
          = getBlockReports(bpid);
      volumeMap.cleanUpBlockPool(bpid);
      volumes.removeBlockPool(bpid, blocksPerVolume);
    }
  }
  
  /**
   * Class for representing the Datanode volume information
   */
  private static class VolumeInfo {
    final String directory;
    final long usedSpace; // size of space used by HDFS
    final long freeSpace; // size of free space excluding reserved space
    final long reservedSpace; // size of space reserved for non-HDFS
    final long reservedSpaceForReplicas; // size of space reserved RBW or
                                    // re-replication
    final long numBlocks;
    final StorageType storageType;

    VolumeInfo(FsVolumeImpl v, long usedSpace, long freeSpace) {
      this.directory = v.toString();
      this.usedSpace = usedSpace;
      this.freeSpace = freeSpace;
      this.reservedSpace = v.getReserved();
      this.reservedSpaceForReplicas = v.getReservedForReplicas();
      this.numBlocks = v.getNumBlocks();
      this.storageType = v.getStorageType();
    }
  }  

  private Collection<VolumeInfo> getVolumeInfo() {
    Collection<VolumeInfo> info = new ArrayList<VolumeInfo>();
    for (FsVolumeImpl volume : volumes.getVolumes()) {
      long used = 0;
      long free = 0;
      try (FsVolumeReference ref = volume.obtainReference()) {
        used = volume.getDfsUsed();
        free = volume.getAvailable();
      } catch (ClosedChannelException e) {
        continue;
      } catch (IOException e) {
        LOG.warn(e.getMessage());
        used = 0;
        free = 0;
      }
      
      info.add(new VolumeInfo(volume, used, free));
    }
    return info;
  }

  @Override
  public Map<String, Object> getVolumeInfoMap() {
    final Map<String, Object> info = new HashMap<String, Object>();
    Collection<VolumeInfo> volumes = getVolumeInfo();
    for (VolumeInfo v : volumes) {

      final Map<String, Object> innerInfo = new HashMap<String, Object>();

      //存储目录中已经使用的空间
      innerInfo.put("usedSpace", v.usedSpace);

      //存储目录中可用的空间
      innerInfo.put("freeSpace", v.freeSpace);

      //存储目录中预留的空间
      innerInfo.put("reservedSpace", v.reservedSpace);
      //存储目录中副本预留的空间
      innerInfo.put("reservedSpaceForReplicas", v.reservedSpaceForReplicas);
      //存储 block 数量
      innerInfo.put("numBlocks", v.numBlocks);
      //存储目录类型
      innerInfo.put("storageType", v.storageType);

      //存储目录的路径
      info.put(v.directory, innerInfo);

    }
    return info;
  }

  @Override //FsDatasetSpi
  public void deleteBlockPool(String bpid, boolean force)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      List<FsVolumeImpl> curVolumes = volumes.getVolumes();
      if (!force) {
        for (FsVolumeImpl volume : curVolumes) {
          try (FsVolumeReference ref = volume.obtainReference()) {
            if (!volume.isBPDirEmpty(bpid)) {
              LOG.warn(bpid
                  + " has some block files, cannot delete unless forced");
              throw new IOException("Cannot delete block pool, "
                  + "it contains some block files");
            }
          } catch (ClosedChannelException e) {
            // ignore.
          }
        }
      }
      for (FsVolumeImpl volume : curVolumes) {
        try (FsVolumeReference ref = volume.obtainReference()) {
          volume.deleteBPDirectories(bpid, force);
        } catch (ClosedChannelException e) {
          // ignore.
        }
      }
    }
  }
  
  @Override // FsDatasetSpi
  public BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block)
      throws IOException {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      final Replica replica = volumeMap.get(block.getBlockPoolId(),
          block.getBlockId());
      if (replica == null) {
        throw new ReplicaNotFoundException(block);
      }
      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException(
            "Replica generation stamp < block generation stamp, block="
            + block + ", replica=" + replica);
      } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
        block.setGenerationStamp(replica.getGenerationStamp());
      }
    }

    ReplicaInfo r = getBlockReplica(block);
    File blockFile = new File(r.getBlockURI());
    File metaFile = new File(r.getMetadataURI());
    BlockLocalPathInfo info = new BlockLocalPathInfo(block,
        blockFile.getAbsolutePath(), metaFile.toString());
    return info;
  }

  @Override
  public void enableTrash(String bpid) {
    dataStorage.enableTrash(bpid);
  }

  @Override
  public void clearTrash(String bpid) {
    dataStorage.clearTrash(bpid);
  }

  @Override
  public boolean trashEnabled(String bpid) {
    return dataStorage.trashEnabled(bpid);
  }

  @Override
  public void setRollingUpgradeMarker(String bpid) throws IOException {
    dataStorage.setRollingUpgradeMarker(bpid);
  }

  @Override
  public void clearRollingUpgradeMarker(String bpid) throws IOException {
    dataStorage.clearRollingUpgradeMarker(bpid);
  }


  @Override
  public void onCompleteLazyPersist(String bpId, long blockId,
      long creationTime, File[] savedFiles, FsVolumeImpl targetVolume) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      ramDiskReplicaTracker.recordEndLazyPersist(bpId, blockId, savedFiles);

      targetVolume.incDfsUsedAndNumBlocks(bpId, savedFiles[0].length()
          + savedFiles[1].length());

      // Update metrics (ignore the metadata file size)
      datanode.getMetrics().incrRamDiskBlocksLazyPersisted();
      datanode.getMetrics().incrRamDiskBytesLazyPersisted(savedFiles[1].length());
      datanode.getMetrics().addRamDiskBlocksLazyPersistWindowMs(
          Time.monotonicNow() - creationTime);

      if (LOG.isDebugEnabled()) {
        LOG.debug("LazyWriter: Finish persisting RamDisk block: "
            + " block pool Id: " + bpId + " block id: " + blockId
            + " to block file " + savedFiles[1] + " and meta file " + savedFiles[0]
            + " on target volume " + targetVolume);
      }
    }
  }

  @Override
  public void onFailLazyPersist(String bpId, long blockId) {
    RamDiskReplica block = null;
    block = ramDiskReplicaTracker.getReplica(bpId, blockId);
    if (block != null) {
      LOG.warn("Failed to save replica " + block + ". re-enqueueing it.");
      ramDiskReplicaTracker.reenqueueReplicaNotPersisted(block);
    }
  }

  @Override
  public void submitBackgroundSyncFileRangeRequest(ExtendedBlock block,
      ReplicaOutputStreams outs, long offset, long nbytes, int flags) {
    FsVolumeImpl fsVolumeImpl = this.getVolume(block);
    asyncDiskService.submitSyncFileRangeRequest(fsVolumeImpl, outs, offset,
        nbytes, flags);
  }

  private boolean ramDiskConfigured() {
    for (FsVolumeImpl v: volumes.getVolumes()){
      if (v.isTransientStorage()) {
        return true;
      }
    }
    return false;
  }

  // Add/Remove per DISK volume async lazy persist thread when RamDisk volume is
  // added or removed.
  // This should only be called when the FsDataSetImpl#volumes list is finalized.
  private void setupAsyncLazyPersistThreads() {
    for (FsVolumeImpl v: volumes.getVolumes()){
      setupAsyncLazyPersistThread(v);
    }
  }

  private void setupAsyncLazyPersistThread(final FsVolumeImpl v) {
    // Skip transient volumes
    if (v.isTransientStorage()) {
      return;
    }
    boolean ramDiskConfigured = ramDiskConfigured();
    // Add thread for DISK volume if RamDisk is configured
    if (ramDiskConfigured &&
        asyncLazyPersistService != null &&
        !asyncLazyPersistService.queryVolume(v)) {
      asyncLazyPersistService.addVolume(v);
    }

    // Remove thread for DISK volume if RamDisk is not configured
    if (!ramDiskConfigured &&
        asyncLazyPersistService != null &&
        asyncLazyPersistService.queryVolume(v)) {
      asyncLazyPersistService.removeVolume(v);
    }
  }

  /**
   * Cleanup the old replica and notifies the NN about new replica.
   *
   * @param replicaInfo    - Old replica to be deleted
   * @param newReplicaInfo - New replica object
   * @param bpid           - block pool id
   */
  private void removeOldReplica(ReplicaInfo replicaInfo,
      ReplicaInfo newReplicaInfo, final String bpid) {
    // Before deleting the files from old storage we must notify the
    // NN that the files are on the new storage. Else a blockReport from
    // the transient storage might cause the NN to think the blocks are lost.
    // Replicas must be evicted from client short-circuit caches, because the
    // storage will no longer be same, and thus will require validating
    // checksum.  This also stops a client from holding file descriptors,
    // which would prevent the OS from reclaiming the memory.
    ExtendedBlock extendedBlock =
        new ExtendedBlock(bpid, newReplicaInfo);
    datanode.getShortCircuitRegistry().processBlockInvalidation(
        ExtendedBlockId.fromExtendedBlock(extendedBlock));
    datanode.notifyNamenodeReceivedBlock(
        extendedBlock, null, newReplicaInfo.getStorageUuid(),
        newReplicaInfo.isOnTransientStorage());

    // Remove the old replicas
    cleanupReplica(bpid, replicaInfo);

    // If deletion failed then the directory scanner will cleanup the blocks
    // eventually.
  }

  class LazyWriter implements Runnable {
    private volatile boolean shouldRun = true;
    final int checkpointerInterval;

    public LazyWriter(Configuration conf) {
      this.checkpointerInterval = conf.getInt(
          DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_SEC,
          DFSConfigKeys.DFS_DATANODE_LAZY_WRITER_INTERVAL_DEFAULT_SEC);
    }

    /**
     * Checkpoint a pending replica to persistent storage now.
     * If we fail then move the replica to the end of the queue.
     * @return true if there is more work to be done, false otherwise.
     */
    private boolean saveNextReplica() {
      RamDiskReplica block = null;
      FsVolumeReference targetReference;
      FsVolumeImpl targetVolume;
      ReplicaInfo replicaInfo;
      boolean succeeded = false;

      try {
        block = ramDiskReplicaTracker.dequeueNextReplicaToPersist();
        if (block != null) {
          try (AutoCloseableLock lock = datasetLock.acquire()) {
            replicaInfo = volumeMap.get(block.getBlockPoolId(), block.getBlockId());

            // If replicaInfo is null, the block was either deleted before
            // it could be checkpointed or it is already on persistent storage.
            // This can occur if a second replica on persistent storage was found
            // after the lazy write was scheduled.
            if (replicaInfo != null &&
                replicaInfo.getVolume().isTransientStorage()) {
              // Pick a target volume to persist the block.
              targetReference = volumes.getNextVolume(
                  StorageType.DEFAULT, null, replicaInfo.getNumBytes());
              targetVolume = (FsVolumeImpl) targetReference.getVolume();

              ramDiskReplicaTracker.recordStartLazyPersist(
                  block.getBlockPoolId(), block.getBlockId(), targetVolume);

              if (LOG.isDebugEnabled()) {
                LOG.debug("LazyWriter: Start persisting RamDisk block:"
                    + " block pool Id: " + block.getBlockPoolId()
                    + " block id: " + block.getBlockId()
                    + " on target volume " + targetVolume);
              }

              asyncLazyPersistService.submitLazyPersistTask(
                  block.getBlockPoolId(), block.getBlockId(),
                  replicaInfo.getGenerationStamp(), block.getCreationTime(),
                  replicaInfo, targetReference);
            }
          }
        }
        succeeded = true;
      } catch(IOException ioe) {
        LOG.warn("Exception saving replica " + block, ioe);
      } finally {
        if (!succeeded && block != null) {
          LOG.warn("Failed to save replica " + block + ". re-enqueueing it.");
          onFailLazyPersist(block.getBlockPoolId(), block.getBlockId());
        }
      }
      return succeeded;
    }

    /**
     * Attempt to evict one or more transient block replicas until we
     * have at least bytesNeeded bytes free.
     */
    public void evictBlocks(long bytesNeeded) throws IOException {
      int iterations = 0;

      final long cacheCapacity = cacheManager.getCacheCapacity();

      while (iterations++ < MAX_BLOCK_EVICTIONS_PER_ITERATION &&
             (cacheCapacity - cacheManager.getCacheUsed()) < bytesNeeded) {
        RamDiskReplica replicaState = ramDiskReplicaTracker.getNextCandidateForEviction();

        if (replicaState == null) {
          break;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Evicting block " + replicaState);
        }

        ReplicaInfo replicaInfo, newReplicaInfo;
        final String bpid = replicaState.getBlockPoolId();

        try (AutoCloseableLock lock = datasetLock.acquire()) {
          replicaInfo = getReplicaInfo(replicaState.getBlockPoolId(),
                                       replicaState.getBlockId());
          Preconditions.checkState(replicaInfo.getVolume().isTransientStorage());
          ramDiskReplicaTracker.discardReplica(replicaState.getBlockPoolId(),
              replicaState.getBlockId(), false);

          // Move the replica from lazyPersist/ to finalized/ on
          // the target volume
          newReplicaInfo =
              replicaState.getLazyPersistVolume().activateSavedReplica(bpid,
                  replicaInfo, replicaState);
          // Update the volumeMap entry.
          volumeMap.add(bpid, newReplicaInfo);

          // Update metrics
          datanode.getMetrics().incrRamDiskBlocksEvicted();
          datanode.getMetrics().addRamDiskBlocksEvictionWindowMs(
              Time.monotonicNow() - replicaState.getCreationTime());
          if (replicaState.getNumReads() == 0) {
            datanode.getMetrics().incrRamDiskBlocksEvictedWithoutRead();
          }

          // Delete the block+meta files from RAM disk and release locked
          // memory.
          removeOldReplica(replicaInfo, newReplicaInfo, bpid);
        }
      }
    }

    @Override
    public void run() {
      int numSuccessiveFailures = 0;

      while (fsRunning && shouldRun) {
        try {
          numSuccessiveFailures = saveNextReplica() ? 0 : (numSuccessiveFailures + 1);

          // Sleep if we have no more work to do or if it looks like we are not
          // making any forward progress. This is to ensure that if all persist
          // operations are failing we don't keep retrying them in a tight loop.
          if (numSuccessiveFailures >= ramDiskReplicaTracker.numReplicasNotPersisted()) {
            Thread.sleep(checkpointerInterval * 1000);
            numSuccessiveFailures = 0;
          }
        } catch (InterruptedException e) {
          LOG.info("LazyWriter was interrupted, exiting");
          break;
        } catch (Exception e) {
          LOG.warn("Ignoring exception in LazyWriter:", e);
        }
      }
    }

    public void stop() {
      shouldRun = false;
    }
  }
  
  @Override
  public void setPinning(ExtendedBlock block) throws IOException {
    if (!blockPinningEnabled) {
      return;
    }
    
    ReplicaInfo r = getBlockReplica(block);
    r.setPinning(localFS);
  }
  
  @Override
  public boolean getPinning(ExtendedBlock block) throws IOException {
    if (!blockPinningEnabled) {
      return  false;
    }
    ReplicaInfo r = getBlockReplica(block);
    return r.getPinning(localFS);
  }
  
  @Override
  public boolean isDeletingBlock(String bpid, long blockId) {
    synchronized(deletingBlock) {
      Set<Long> s = deletingBlock.get(bpid);
      return s != null ? s.contains(blockId) : false;
    }
  }
  
  public void removeDeletedBlocks(String bpid, Set<Long> blockIds) {
    synchronized (deletingBlock) {
      Set<Long> s = deletingBlock.get(bpid);
      if (s != null) {
        for (Long id : blockIds) {
          s.remove(id);
        }
      }
    }
  }
  
  private void addDeletingBlock(String bpid, Long blockId) {
    synchronized(deletingBlock) {
      Set<Long> s = deletingBlock.get(bpid);
      if (s == null) {
        s = new HashSet<Long>();
        deletingBlock.put(bpid, s);
      }
      s.add(blockId);
    }
  }

  void releaseLockedMemory(long count, boolean roundup) {
    if (roundup) {
      cacheManager.release(count);
    } else {
      cacheManager.releaseRoundDown(count);
    }
  }

  /**
   * Attempt to evict blocks from cache Manager to free the requested
   * bytes.
   *
   * @param bytesNeeded
   */
  @VisibleForTesting
  public void evictLazyPersistBlocks(long bytesNeeded) {
    try {
      ((LazyWriter) lazyWriter.getRunnable()).evictBlocks(bytesNeeded);
    } catch(IOException ioe) {
      LOG.info("Ignoring exception ", ioe);
    }
  }

  /**
   * Attempt to reserve the given amount of memory with the cache Manager.
   * @param bytesNeeded
   * @return
   */
  boolean reserveLockedMemory(long bytesNeeded) {
    if (cacheManager.reserve(bytesNeeded) > 0) {
      return true;
    }

    // Round up bytes needed to osPageSize and attempt to evict
    // one more more blocks to free up the reservation.
    bytesNeeded = cacheManager.roundUpPageSize(bytesNeeded);
    evictLazyPersistBlocks(bytesNeeded);
    return cacheManager.reserve(bytesNeeded) > 0;
  }

  @VisibleForTesting
  public void setTimer(Timer newTimer) {
    this.timer = newTimer;
  }

  /**
   * Return the number of BP service count.
   */
  public int getBPServiceCount() {
    return datanode.getBpOsCount();
  }

  /**
   * Return the number of volume.
   */
  public int getVolumeCount() {
    return volumes.getVolumes().size();
  }

  void stopAllDataxceiverThreads(FsVolumeImpl volume) {
    try (AutoCloseableLock lock = datasetLock.acquire()) {
      for (String blockPoolId : volumeMap.getBlockPoolList()) {
        Collection<ReplicaInfo> replicas = volumeMap.replicas(blockPoolId);
        for (ReplicaInfo replicaInfo : replicas) {
          if ((replicaInfo.getState() == ReplicaState.TEMPORARY
              || replicaInfo.getState() == ReplicaState.RBW)
              && replicaInfo.getVolume().equals(volume)) {
            ReplicaInPipeline replicaInPipeline =
                (ReplicaInPipeline) replicaInfo;
            replicaInPipeline.interruptThread();
          }
        }
      }
    }
  }
}

