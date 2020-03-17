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

package org.apache.hadoop.hdfs.protocol;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.StorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * A block storage policy describes how to select the storage types
 * for the replicas of a block.
 */
// 块存储类型选择策略对象
@InterfaceAudience.Private
public class BlockStoragePolicy implements BlockStoragePolicySpi {
  public static final Logger LOG = LoggerFactory.getLogger(BlockStoragePolicy
      .class);


  /** A 4-bit policy ID */
  // 策略唯一标识Id
  private final byte id;
  /** Policy name */
  // 策略名称
  private final String name;

  // 对于一个新块，存储副本块的可选存储类型信息组
  /** The storage types to store the replicas of a new block. */
  private final StorageType[] storageTypes;

  // 对于第一个创建块，fallback情况时的可选存储类型
  // fallback 即当前 存储类型不可用的时候，退一级选择使用的存储类型。
  /** The fallback storage type for block creation. */
  private final StorageType[] creationFallbacks;

  // 对于块的其余副本，fallback情况时的可选存储类型
  /** The fallback storage type for replication. */
  private final StorageType[] replicationFallbacks;

  // 当创建文件的时候，是否继承祖先目录信息的策略，主要用于主动设置策略的时候
  /**
   * Whether the policy is inherited during file creation.
   * If set then the policy cannot be changed after file creation.
   */
  private boolean copyOnCreateFile;

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks) {
    this(id, name, storageTypes, creationFallbacks, replicationFallbacks,
         false);
  }

  @VisibleForTesting
  public BlockStoragePolicy(byte id, String name, StorageType[] storageTypes,
      StorageType[] creationFallbacks, StorageType[] replicationFallbacks,
      boolean copyOnCreateFile) {
    this.id = id;
    this.name = name;
    this.storageTypes = storageTypes;
    this.creationFallbacks = creationFallbacks;
    this.replicationFallbacks = replicationFallbacks;
    this.copyOnCreateFile = copyOnCreateFile;
  }

  /**
   * 选取存储类型
   * ONE_SSD就必然只有第一个块的副本块是此类型 的，其余副本则是DISK类型存储，
   * 而ALL_SSD则将会全部是SSD的 存储。
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  public List<StorageType> chooseStorageTypes(final short replication) {
    final List<StorageType> types = new LinkedList<>();
    int i = 0, j = 0;

    // 从前往后依次匹配存储类型与对应的副本下标相匹配，同时要过滤掉
    // transient属性的存储类型

    // Do not return transient storage types. We will not have accurate
    // usage information for transient types.
    for (;i < replication && j < storageTypes.length; ++j) {
      if (!storageTypes[j].isTransient()) {
        types.add(storageTypes[j]);
        ++i;
      }
    }
    // 获取最后一个存储类型，统一作为多余副本的存储类型
    final StorageType last = storageTypes[storageTypes.length - 1];
    if (!last.isTransient()) {
      for (; i < replication; i++) {
        types.add(last);
      }
    }
    return types;
  }

  /**
   * Choose the storage types for storing the remaining replicas, given the
   * replication number and the storage types of the chosen replicas.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas.
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  public List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen) {
    return chooseStorageTypes(replication, chosen, null);
  }

  private List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen, final List<StorageType> excess) {
    final List<StorageType> types = chooseStorageTypes(replication);
    diff(types, chosen, excess);
    return types;
  }

  /**
   * Choose the storage types for storing the remaining replicas, given the
   * replication number, the storage types of the chosen replicas and
   * the unavailable storage types. It uses fallback storage in case that
   * the desired storage type is unavailable.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas.
   * @param unavailables the unavailable storage types.
   * @param isNewBlock Is it for new block creation?
   * @return a list of {@link StorageType}s for storing the replicas of a block.
   */
  public List<StorageType> chooseStorageTypes(final short replication,
      final Iterable<StorageType> chosen,
      final EnumSet<StorageType> unavailables,
      final boolean isNewBlock) {
    final List<StorageType> excess = new LinkedList<>();
    final List<StorageType> storageTypes = chooseStorageTypes(
        replication, chosen, excess);
    final int expectedSize = storageTypes.size() - excess.size();
    final List<StorageType> removed = new LinkedList<>();
    for(int i = storageTypes.size() - 1; i >= 0; i--) {
      // replace/remove unavailable storage types.
      // 获取当前需要的存储类型
      final StorageType t = storageTypes.get(i);

      // 如果当前的存储类型是在不可用的存储类型列表中，选择fallback的情况
      if (unavailables.contains(t)) {

        // 根据是否为新块还是普通的副本块，选择相应的fallback的StorageType
        final StorageType fallback = isNewBlock?
            getCreationFallback(unavailables)
            : getReplicationFallback(unavailables);


        if (fallback == null) {
          removed.add(storageTypes.remove(i));
        } else {
          storageTypes.set(i, fallback);
        }
      }
    }
    // remove excess storage types after fallback replacement.
    diff(storageTypes, excess, null);
    if (storageTypes.size() < expectedSize) {
      LOG.warn("Failed to place enough replicas: expected size is {}"
          + " but only {} storage types can be selected (replication={},"
          + " selected={}, unavailable={}" + ", removed={}" + ", policy={}"
          + ")", expectedSize, storageTypes.size(), replication, storageTypes,
          unavailables, removed, this);
    }
    return storageTypes;
  }

  /**
   * Compute the difference between two lists t and c so that after the diff
   * computation we have: t = t - c;
   * Further, if e is not null, set e = e + c - t;
   */
  private static void diff(List<StorageType> t, Iterable<StorageType> c,
      List<StorageType> e) {
    for(StorageType storagetype : c) {
      final int i = t.indexOf(storagetype);
      if (i >= 0) {
        t.remove(i);
      } else if (e != null) {
        e.add(storagetype);
      }
    }
  }

  /**
   * Choose excess storage types for deletion, given the
   * replication number and the storage types of the chosen replicas.
   *
   * @param replication the replication number.
   * @param chosen the storage types of the chosen replicas.
   * @return a list of {@link StorageType}s for deletion.
   */
  public List<StorageType> chooseExcess(final short replication,
      final Iterable<StorageType> chosen) {
    final List<StorageType> types = chooseStorageTypes(replication);
    final List<StorageType> excess = new LinkedList<>();
    diff(types, chosen, excess);
    return excess;
  }

  /** @return the fallback {@link StorageType} for creation. */
  public StorageType getCreationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, creationFallbacks);
  }

  /** @return the fallback {@link StorageType} for replication. */
  public StorageType getReplicationFallback(EnumSet<StorageType> unavailables) {
    return getFallback(unavailables, replicationFallbacks);
  }

  @Override
  public int hashCode() {
    return Byte.valueOf(id).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof BlockStoragePolicy)) {
      return false;
    }
    final BlockStoragePolicy that = (BlockStoragePolicy)obj;
    return this.id == that.id;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + name + ":" + id
        + ", storageTypes=" + Arrays.asList(storageTypes)
        + ", creationFallbacks=" + Arrays.asList(creationFallbacks)
        + ", replicationFallbacks=" + Arrays.asList(replicationFallbacks) + "}";
  }

  public byte getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public StorageType[] getStorageTypes() {
    return this.storageTypes;
  }

  @Override
  public StorageType[] getCreationFallbacks() {
    return this.creationFallbacks;
  }

  @Override
  public StorageType[] getReplicationFallbacks() {
    return this.replicationFallbacks;
  }


  // 在getFallback方法中会选取第一个满足条件的fallback的
  private static StorageType getFallback(EnumSet<StorageType> unavailables,
      StorageType[] fallbacks) {
    for(StorageType fb : fallbacks) {
      // 如果找到满足条件的StorageType，立即返回
      if (!unavailables.contains(fb)) {
        return fb;
      }
    }
    return null;
  }

  @Override
  public boolean isCopyOnCreateFile() {
    return copyOnCreateFile;
  }
}
