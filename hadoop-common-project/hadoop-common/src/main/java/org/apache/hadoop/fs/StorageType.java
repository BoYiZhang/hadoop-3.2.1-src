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

package org.apache.hadoop.fs;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;

/**
 * Defines the types of supported storage media. The default storage
 * medium is assumed to be DISK.
 *
 *
 * HDFS并没有自动检测识别的 功能。  默认是DISK类型
 * 配置属性dfs.datanode.data.dir可以对本地对应存储目录进行设置，
 * 同时带上一个存储类型标签，声明此目录用的是哪种类型的存储 介质，例子如下:
 *
 *   [SSD]file:///grid/dn/ssd0
 *
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public enum StorageType {
  // sorted by the speed of the storage types, from fast to slow
  // 根据存储的速度，从快到慢

  //内存
  RAM_DISK(true),

  //SSD 存储
  SSD(false),

  // 磁盘存储
  DISK(false),


  //并没有特指哪种存储介质,主要指的是高密度存储介质， 用于解决数据扩容的问题。
  ARCHIVE(false),

  // 数据来自于外部存储   通过HDFS能够定位到数据的外部存储。
  PROVIDED(false);

  private final boolean isTransient;

  public static final StorageType DEFAULT = DISK;

  public static final StorageType[] EMPTY_ARRAY = {};

  private static final StorageType[] VALUES = values();

  StorageType(boolean isTransient) {
    this.isTransient = isTransient;
  }

  public boolean isTransient() {
    return isTransient;
  }

  public boolean supportTypeQuota() {
    return !isTransient;
  }

  public boolean isMovable() {
    return !isTransient;
  }

  public static List<StorageType> asList() {
    return Arrays.asList(VALUES);
  }

  public static List<StorageType> getMovableTypes() {
    return getNonTransientTypes();
  }

  public static List<StorageType> getTypesSupportingQuota() {
    return getNonTransientTypes();
  }

  public static StorageType parseStorageType(int i) {
    return VALUES[i];
  }

  public static StorageType parseStorageType(String s) {
    return StorageType.valueOf(StringUtils.toUpperCase(s));
  }

  private static List<StorageType> getNonTransientTypes() {
    List<StorageType> nonTransientTypes = new ArrayList<>();
    for (StorageType t : VALUES) {
      if ( t.isTransient == false ) {
        nonTransientTypes.add(t);
      }
    }
    return nonTransientTypes;
  }
}