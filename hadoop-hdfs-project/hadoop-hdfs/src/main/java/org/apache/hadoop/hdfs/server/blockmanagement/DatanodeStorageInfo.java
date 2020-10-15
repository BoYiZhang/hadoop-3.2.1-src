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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.FoldedTreeSet;

import com.google.common.annotations.VisibleForTesting;

/**
 * 数据节点有一个或多个存储。Datanode中的存储由此类表示。
 * A Datanode has one or more storages. A storage in the Datanode is represented
 * by this class.
 */
public class DatanodeStorageInfo {
  public static final DatanodeStorageInfo[] EMPTY_ARRAY = {};

  public static DatanodeInfo[] toDatanodeInfos(
      DatanodeStorageInfo[] storages) {
    return storages == null ? null: toDatanodeInfos(Arrays.asList(storages));
  }
  static DatanodeInfo[] toDatanodeInfos(List<DatanodeStorageInfo> storages) {
    final DatanodeInfo[] datanodes = new DatanodeInfo[storages.size()];
    for(int i = 0; i < storages.size(); i++) {
      datanodes[i] = storages.get(i).getDatanodeDescriptor();
    }
    return datanodes;
  }

  static DatanodeDescriptor[] toDatanodeDescriptors(
      DatanodeStorageInfo[] storages) {
    DatanodeDescriptor[] datanodes = new DatanodeDescriptor[storages.length];
    for (int i = 0; i < storages.length; ++i) {
      datanodes[i] = storages[i].getDatanodeDescriptor();
    }
    return datanodes;
  }

  public static String[] toStorageIDs(DatanodeStorageInfo[] storages) {
    if (storages == null) {
      return null;
    }
    String[] storageIDs = new String[storages.length];
    for(int i = 0; i < storageIDs.length; i++) {
      storageIDs[i] = storages[i].getStorageID();
    }
    return storageIDs;
  }

  public static StorageType[] toStorageTypes(DatanodeStorageInfo[] storages) {
    if (storages == null) {
      return null;
    }
    StorageType[] storageTypes = new StorageType[storages.length];
    for(int i = 0; i < storageTypes.length; i++) {
      storageTypes[i] = storages[i].getStorageType();
    }
    return storageTypes;
  }

  public void updateFromStorage(DatanodeStorage storage) {
    state = storage.getState();
    storageType = storage.getStorageType();
  }

  //  Datanode 的信息
  private final DatanodeDescriptor dn;

  // 存储策略id
  private final String storageID;

  //数据存储类型  disk，ssd等
  private StorageType storageType;

  //状态[三种] : NORMAL  READ_ONLY_SHARED[用于调试]  FAILED
  private State state;

  //容量
  private long capacity;

  private long dfsUsed;


  private long nonDfsUsed;

  //剩余容量
  private volatile long remaining;

  private long blockPoolUsed;

  //核心存储
  private final FoldedTreeSet<BlockInfo> blocks = new FoldedTreeSet<>();

  /** The number of block reports received */
  private int blockReportCount = 0;

  /**
   * Set to false on any NN failover, and reset to true
   * whenever a block report is received.
   *
   * 当Namenode出现失败时， 会将这个字段设置为false
   * 当Namenode正常接收到这个存储的心跳后， 会将这个字段设置为 true
   *
   */
  private boolean heartbeatedSinceFailover = false;

  /**
   * At startup or at failover, the storages in the cluster may have pending
   * block deletions from a previous incarnation of the NameNode. The block
   * contents are considered as stale until a block report is received. When a
   * storage is considered as stale, the replicas on it are also considered as
   * stale. If any block has at least one stale replica, then no invalidations
   * will be processed for this block. See HDFS-1972.
   *
   * 当Namenode出现失败或者正在启动时， Datanode会挂起上一次Namenode发起的删除操作。
   * 这个时候我们就认为当前存储为stale状态， 直到Namenode收到了这个存储的块汇报。
   * 当一个存储处于stale状态时， 这个存储上的所有副本都是stale状态的， 如果一个数据块至少有一个stale副本，
   * 那么这个数据块也为stale状态， stale状态数据块的所有副本是不可以执行删除操作的（ HDFS-1972） 。
   *
   */
  private boolean blockContentsStale = true;

  DatanodeStorageInfo(DatanodeDescriptor dn, DatanodeStorage s) {
    this(dn, s.getStorageID(), s.getStorageType(), s.getState());
  }

  DatanodeStorageInfo(DatanodeDescriptor dn, String storageID,
      StorageType storageType, State state) {
    this.dn = dn;
    this.storageID = storageID;
    this.storageType = storageType;
    this.state = state;
  }

  public int getBlockReportCount() {
    return blockReportCount;
  }

  void setBlockReportCount(int blockReportCount) {
    this.blockReportCount = blockReportCount;
  }

  public boolean areBlockContentsStale() {
    return blockContentsStale;
  }

  void markStaleAfterFailover() {
    heartbeatedSinceFailover = false;
    blockContentsStale = true;
  }

  void receivedHeartbeat(StorageReport report) {
    updateState(report);
    heartbeatedSinceFailover = true;
  }

  void receivedBlockReport() {
    if (heartbeatedSinceFailover) {
      blockContentsStale = false;
    }
    blockReportCount++;
  }

  @VisibleForTesting
  public void setUtilizationForTesting(long capacity, long dfsUsed,
                      long remaining, long blockPoolUsed) {
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = blockPoolUsed;
  }

  State getState() {
    return this.state;
  }

  void setState(State state) {
    this.state = state;
  }

  void setHeartbeatedSinceFailover(boolean value) {
    heartbeatedSinceFailover = value;
  }

  boolean areBlocksOnFailedStorage() {
    return getState() == State.FAILED && !blocks.isEmpty();
  }

  @VisibleForTesting
  public String getStorageID() {
    return storageID;
  }

  public StorageType getStorageType() {
    return storageType;
  }

  long getCapacity() {
    return capacity;
  }

  long getDfsUsed() {
    return dfsUsed;
  }

  long getNonDfsUsed() {
    return nonDfsUsed;
  }

  long getRemaining() {
    return remaining;
  }

  long getBlockPoolUsed() {
    return blockPoolUsed;
  }
  /**
   * For use during startup. Expects block to be added in sorted order
   * to enable fast insert in to the DatanodeStorageInfo
   *
   * @param b Block to add to DatanodeStorageInfo
   * @param reportedBlock The reported replica
   * @return Enum describing if block was added, replaced or already existed
   */
  public AddBlockResult addBlockInitial(BlockInfo b, Block reportedBlock) {
    // First check whether the block belongs to a different storage
    // on the same DN.
    AddBlockResult result = AddBlockResult.ADDED;
    DatanodeStorageInfo otherStorage =
        b.findStorageInfo(getDatanodeDescriptor());

    if (otherStorage != null) {
      if (otherStorage != this) {
        // The block belongs to a different storage. Remove it first.
        otherStorage.removeBlock(b);
        result = AddBlockResult.REPLACED;
      } else {
        // The block is already associated with this storage.
        return AddBlockResult.ALREADY_EXIST;
      }
    }

    b.addStorage(this, reportedBlock);
    blocks.addSortedLast(b);
    return result;
  }

  public AddBlockResult addBlock(BlockInfo b, Block reportedBlock) {
    // 首先检查块是否属于同一DN上的不同存储器。
    // First check whether the block belongs to a different storage on the same DN.
    AddBlockResult result = AddBlockResult.ADDED;
    DatanodeStorageInfo otherStorage =
        b.findStorageInfo(getDatanodeDescriptor());

    if (otherStorage != null) {
      if (otherStorage != this) {
        //如果当前数据块属于另一个存储， 则先从该存储上删除这个数据块
        // The block belongs to a different storage. Remove it first.
        otherStorage.removeBlock(b);
        result = AddBlockResult.REPLACED;
      } else {
        //数据块已经添加到了当前存储上， 不需要再次添加
        // The block is already associated with this storage.
        return AddBlockResult.ALREADY_EXIST;
      }
    }

    //首先将当前存储添加到数据块所属的存储列表中
    b.addStorage(this, reportedBlock);

    //之后将当前数据块添加到存储管理的数据块blocks中
    blocks.add(b);
    return result;
  }

  AddBlockResult addBlock(BlockInfo b) {
    return addBlock(b, b);
  }

  boolean removeBlock(BlockInfo b) {
    blocks.remove(b);
    return b.removeStorage(this);
  }

  int numBlocks() {
    return blocks.size();
  }
  
  /**
   * @return iterator to an unmodifiable set of blocks
   * related to this {@link DatanodeStorageInfo}
   */
  Iterator<BlockInfo> getBlockIterator() {
    return Collections.unmodifiableSet(blocks).iterator();
  }

  void updateState(StorageReport r) {
    capacity = r.getCapacity();
    dfsUsed = r.getDfsUsed();
    nonDfsUsed = r.getNonDfsUsed();
    remaining = r.getRemaining();
    blockPoolUsed = r.getBlockPoolUsed();
  }

  public DatanodeDescriptor getDatanodeDescriptor() {
    return dn;
  }

  /** Increment the number of blocks scheduled for each given storage */ 
  public static void incrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().incrementBlocksScheduled(s.getStorageType());
    }
  }

  /**
   * Decrement the number of blocks scheduled for each given storage. This will
   * be called during abandon block or delete of UC block.
   */
  public static void decrementBlocksScheduled(DatanodeStorageInfo... storages) {
    for (DatanodeStorageInfo s : storages) {
      s.getDatanodeDescriptor().decrementBlocksScheduled(s.getStorageType());
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || !(obj instanceof DatanodeStorageInfo)) {
      return false;
    }
    final DatanodeStorageInfo that = (DatanodeStorageInfo)obj;
    return this.storageID.equals(that.storageID);
  }

  @Override
  public int hashCode() {
    return storageID.hashCode();
  }

  @Override
  public String toString() {
    return "[" + storageType + "]" + storageID + ":" + state + ":" + dn;
  }
  
  StorageReport toStorageReport() {
    return new StorageReport(
        new DatanodeStorage(storageID, state, storageType),
        false, capacity, dfsUsed, remaining, blockPoolUsed, nonDfsUsed);
  }

  /**
   * The fill ratio of the underlying TreeSet holding blocks.
   *
   * @return the fill ratio of the tree
   */
  public double treeSetFillRatio() {
    return blocks.fillRatio();
  }

  /**
   * Compact the underlying TreeSet holding blocks.
   *
   * @param timeout Maximum time to spend compacting the tree set in
   *                milliseconds.
   *
   * @return true if compaction completed, false if aborted
   */
  public boolean treeSetCompact(long timeout) {
    return blocks.compact(timeout);
  }

  static Iterable<StorageType> toStorageTypes(
      final Iterable<DatanodeStorageInfo> infos) {
    return new Iterable<StorageType>() {
        @Override
        public Iterator<StorageType> iterator() {
          return new Iterator<StorageType>() {
            final Iterator<DatanodeStorageInfo> i = infos.iterator();
            @Override
            public boolean hasNext() {return i.hasNext();}
            @Override
            public StorageType next() {return i.next().getStorageType();}
            @Override
            public void remove() {
              throw new UnsupportedOperationException();
            }
          };
        }
      };
  }

  /** @return the first {@link DatanodeStorageInfo} corresponding to
   *          the given datanode
   */
  static DatanodeStorageInfo getDatanodeStorageInfo(
      final Iterable<DatanodeStorageInfo> infos,
      final DatanodeDescriptor datanode) {
    if (datanode == null) {
      return null;
    }
    for(DatanodeStorageInfo storage : infos) {
      if (storage.getDatanodeDescriptor() == datanode) {
        return storage;
      }
    }
    return null;
  }

  @VisibleForTesting
  void setRemainingForTests(int remaining) {
    this.remaining = remaining;
  }

  enum AddBlockResult {
    ADDED, REPLACED, ALREADY_EXIST
  }
}
