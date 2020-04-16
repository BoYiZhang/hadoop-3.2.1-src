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
package org.apache.hadoop.hdfs.shortcircuit;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.Slot;
import org.apache.hadoop.hdfs.util.IOUtilsClient;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * ShortCircuitReplica类封装了一个短路读数据块副本的所有信息， 只有获取了
 * ShortCircuitReplica对象， 才能构造BlockReaderLocal对象完成短路读操作.
 *
 *
 * A ShortCircuitReplica object contains file descriptors for a block that
 * we are reading via short-circuit local reads.
 *
 * The file descriptors can be shared between multiple threads because
 * all the operations we perform are stateless-- i.e., we use pread
 * instead of read, to avoid using the shared position state.
 */
@InterfaceAudience.Private
public class ShortCircuitReplica {
  public static final Logger LOG = LoggerFactory.getLogger(
      ShortCircuitCache.class);

  /**
   * 对应BlockId， 唯一标识这个ShortCircuitReplica对象
   * Identifies this ShortCircuitReplica object.
   */
  final ExtendedBlockId key;

  /**
   * 副本的数据块文件输入流。
   * The block data input stream.
   */
  private final FileInputStream dataStream;

  /**
   * 副本的校验文件输入流。
   * The block metadata input stream.
   *
   * TODO: make this nullable if the file has no checksums on disk.
   */
  private final FileInputStream metaStream;

  /**
   * 副本校验文件头。
   * Block metadata header.
   */
  private final BlockMetadataHeader metaHeader;

  /**
   * 管理这个ShortCircuitReplica对象的ShortCircuitCache对象
   * The cache we belong to.
   */
  private final ShortCircuitCache cache;

  /**
   * Monotonic time at which the replica was created.
   */
  private final long creationTimeMs;

  /**
   * 这个ShortCircuitReplica对象对应的共享内存中的槽位。
   * If non-null, the shared memory slot associated with this replica.
   */
  private final Slot slot;

  /**
   *
   * 当前ShortCircuitReplica对象在内存中的映射数据
   *
   * Current mmap state.
   *
   * Protected by the cache lock.
   */
  Object mmapData;

  /**
   * 当前ShortCircuitReplica对象是否在cahce中被删除
   *
   * True if this replica has been purged from the cache; false otherwise.
   *
   * Protected by the cache lock.
   */
  boolean purged = false;

  /**
   *
   * 标识当前ShortCircuitReplica对象被引用的次
   * 数。 ShortCircuitReplica只可能被ShortCircuitCache、 BlockReaderLocal以及
   * ClientMmap对象引用。 当我们构建一个ShortCircuitReplica对象时， 这个值为2，
   * 因为在创建时， ShortCircuitReplica对象就已经被ShortCircuitCache以及请求类引
   * 用了。 只有refCount==0时， ShortCircuitReplica对象才可以被关闭。
   *
   * Number of external references to this replica.  Replicas are referenced
   * by the cache, BlockReaderLocal instances, and by ClientMmap instances.
   * The number starts at 2 because when we create a replica, it is referenced
   * by both the cache and the requester.
   *
   * Protected by the cache lock.
   */
  int refCount = 2;

  /**
   *
   * ShortCircuitReplica对象被加入ShortCircuitCache的evictable队列
   * 的时间。 如果不在evictable队列中， 则为null。
   *
   * The monotonic time in nanoseconds at which the replica became evictable, or
   * null if it is not evictable.
   *
   * Protected by the cache lock.
   */
  private Long evictableTimeNs = null;

  public ShortCircuitReplica(ExtendedBlockId key,
      FileInputStream dataStream, FileInputStream metaStream,
      ShortCircuitCache cache, long creationTimeMs, Slot slot) throws IOException {
    this.key = key;
    this.dataStream = dataStream;
    this.metaStream = metaStream;
    this.metaHeader =
          BlockMetadataHeader.preadHeader(metaStream.getChannel());
    if (metaHeader.getVersion() != 1) {
      throw new IOException("invalid metadata header version " +
          metaHeader.getVersion() + ".  Can only handle version 1.");
    }
    this.cache = cache;
    this.creationTimeMs = creationTimeMs;
    this.slot = slot;
  }

  /**
   *
   *  unref()用于在不需要对 ShortCircuitReplica对象的引用时，
   *  解除对ShortCircuitReplica对象的引用。
   *
   * Decrement the reference count.
   */
  public void unref() {
    cache.unref(this);
  }

  /**
   * Check if the replica is stale.
   *
   * Must be called with the cache lock held.
   */
  boolean isStale() {
    if (slot != null) {
      // Check staleness by looking at the shared memory area we use to
      // communicate with the DataNode.
      boolean stale = !slot.isValid();
      LOG.trace("{}: checked shared memory segment.  isStale={}", this, stale);
      return stale;
    } else {
      // Fall back to old, time-based staleness method.
      long deltaMs = Time.monotonicNow() - creationTimeMs;
      long staleThresholdMs = cache.getStaleThresholdMs();
      if (deltaMs > staleThresholdMs) {
        LOG.trace("{} is stale because it's {} ms old and staleThreadholdMS={}",
            this, deltaMs, staleThresholdMs);
        return true;
      } else {
        LOG.trace("{} is not stale because it's only {} ms old "
            + "and staleThresholdMs={}",  this, deltaMs, staleThresholdMs);
        return false;
      }
    }
  }

  /**
   *
   * 共享内存的槽位中添加一个免校验（nochecksum） 的锚（anchor） 。 只有当Datanode通过mlock操作将数据块副本缓存到内存
   * 后， 也就是副本对应的Slot是可锚状态时， 才可以添加这个锚。
   *
   *
   * Try to add a no-checksum anchor to our shared memory slot.
   *
   * It is only possible to add this anchor when the block is mlocked on the Datanode.
   * The DataNode will not munlock the block until the number of no-checksum anchors
   * for the block reaches zero.
   *
   * This method does not require any synchronization.
   *
   * @return     True if we successfully added a no-checksum anchor.
   */
  public boolean addNoChecksumAnchor() {
    if (slot == null) {
      return false;
    }
    boolean result = slot.addAnchor();
    LOG.trace("{}: {} no-checksum anchor to slot {}",
        this, result ? "added" : "could not add", slot);
    return result;
  }

  /**
   * Remove a no-checksum anchor for our shared memory slot.
   *
   * This method does not require any synchronization.
   */
  public void removeNoChecksumAnchor() {
    if (slot != null) {
      slot.removeAnchor();
    }
  }

  /**
   * Check if the replica has an associated mmap that has been fully loaded.
   *
   * Must be called with the cache lock held.
   */
  @VisibleForTesting
  public boolean hasMmap() {
    return ((mmapData != null) && (mmapData instanceof MappedByteBuffer));
  }

  /**
   * Free the mmap associated with this replica.
   *
   * Must be called with the cache lock held.
   */
  void munmap() {
    MappedByteBuffer mmap = (MappedByteBuffer)mmapData;

    //调用munmap()系统调用移除映射
    NativeIO.POSIX.munmap(mmap);
    mmapData = null;
  }

  /**
   * 关闭ShortCircuitReplica对象
   * Close the replica.
   *
   * Must be called after there are no more references to the replica in the
   * cache or elsewhere.
   */
  void close() {
    String suffix = "";

    Preconditions.checkState(refCount == 0,
        "tried to close replica with refCount %d: %s", refCount, this);
    refCount = -1;
    Preconditions.checkState(purged,
        "tried to close unpurged replica %s", this);
    if (hasMmap()) {
      munmap();
      if (LOG.isTraceEnabled()) {
        suffix += "  munmapped.";
      }
    }
    IOUtilsClient.cleanupWithLogger(LOG, dataStream, metaStream);
    if (slot != null) {
      cache.scheduleSlotReleaser(slot);
      if (LOG.isTraceEnabled()) {
        suffix += "  scheduling " + slot + " for later release.";
      }
    }
    LOG.trace("closed {}{}", this, suffix);
  }

  public FileInputStream getDataStream() {
    return dataStream;
  }

  public FileInputStream getMetaStream() {
    return metaStream;
  }

  public BlockMetadataHeader getMetaHeader() {
    return metaHeader;
  }

  public ExtendedBlockId getKey() {
    return key;
  }

  public ClientMmap getOrCreateClientMmap(boolean anchor) {
    return cache.getOrCreateClientMmap(this, anchor);
  }

  //loadMmapInternal()方法用于将ShortCircuitReplica对应的数据块文件映射到内存中，
  //这个方法是在客户端对副本进行零拷贝读取时调用的， 这个方法的逆方法是munmap()。
  MappedByteBuffer loadMmapInternal() {
    try {
      FileChannel channel = dataStream.getChannel();

      // 膜拜大神!!!!!!
      // 调用java.nio.Channel.map()方法创建文件的内存映射
      MappedByteBuffer mmap = channel.map(MapMode.READ_ONLY, 0,
          Math.min(Integer.MAX_VALUE, channel.size()));

      LOG.warn("{}: created mmap of size {}", this, channel.size());
      return mmap;
    } catch (IOException e) {
      LOG.warn(this + ": mmap error", e);
      return null;
    } catch (RuntimeException e) {
      LOG.warn(this + ": mmap error", e);
      return null;
    }
  }

  /**
   * Get the evictable time in nanoseconds.
   *
   * Note: you must hold the cache lock to call this function.
   *
   * @return the evictable time in nanoseconds.
   */
  public Long getEvictableTimeNs() {
    return evictableTimeNs;
  }

  /**
   * Set the evictable time in nanoseconds.
   *
   * Note: you must hold the cache lock to call this function.
   *
   * @param evictableTimeNs   The evictable time in nanoseconds, or null
   *                          to set no evictable time.
   */
  void setEvictableTimeNs(Long evictableTimeNs) {
    this.evictableTimeNs = evictableTimeNs;
  }

  @VisibleForTesting
  public Slot getSlot() {
    return slot;
  }

  /**
   * Convert the replica to a string for debugging purposes.
   * Note that we can't take the lock here.
   */
  @Override
  public String toString() {
    return "ShortCircuitReplica{" + "key=" + key
        + ", metaHeader.version=" + metaHeader.getVersion()
        + ", metaHeader.checksum=" + metaHeader.getChecksum()
        + ", ident=" + "0x" + Integer.toHexString(System.identityHashCode(this))
        + ", creationTimeMs=" + creationTimeMs + "}";
  }
}
