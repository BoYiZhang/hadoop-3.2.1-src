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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableList;

import java.util.concurrent.TimeUnit;

@InterfaceAudience.Private
public class CheckpointConf {
  private static final Logger LOG =
      LoggerFactory.getLogger(CheckpointConf.class);
  
  /** How often to checkpoint regardless of number of txns */
  private final long checkpointPeriod;    // in seconds
  
  /** How often to poll the NN to check checkpointTxnCount */
  private final long checkpointCheckPeriod; // in seconds
  
  /** checkpoint once every this many transactions, regardless of time */
  private final long checkpointTxnCount;

  /** maxium number of retries when merge errors occur */
  private final int maxRetriesOnMergeError;

  /** The output dir for legacy OIV image */
  private final String legacyOivImageDir;

  /**
  * multiplier on the checkpoint period to allow other nodes to do the checkpointing, when not the
  * 'primary' checkpoint node
  */
  private double quietMultiplier;

  public CheckpointConf(Configuration conf) {

    // dfs.namenode.checkpoint.check.period : 60 s
    // SecondaryNameNode和CheckpointNode将每隔'dfs.namenode.checkpoint.period'秒以查询未选中的事务数。
    checkpointCheckPeriod = conf.getTimeDuration(
        DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_KEY,
        DFS_NAMENODE_CHECKPOINT_CHECK_PERIOD_DEFAULT, TimeUnit.SECONDS);


    // dfs.namenode.checkpoint.period : 3600  : 两个连续checkpoint的最大延时
    checkpointPeriod = conf.getTimeDuration(DFS_NAMENODE_CHECKPOINT_PERIOD_KEY,
        DFS_NAMENODE_CHECKPOINT_PERIOD_DEFAULT, TimeUnit.SECONDS);

    // dfs.namenode.checkpoint.txns  100万
    checkpointTxnCount = conf.getLong(DFS_NAMENODE_CHECKPOINT_TXNS_KEY, 
                                  DFS_NAMENODE_CHECKPOINT_TXNS_DEFAULT);

    // dfs.namenode.checkpoint.max-retries : 3次
    maxRetriesOnMergeError = conf.getInt(DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_KEY,
                                  DFS_NAMENODE_CHECKPOINT_MAX_RETRIES_DEFAULT);

    // dfs.namenode.legacy-oiv-image.dir
    // 在standby namenode和secondary namenode的检查点期间，保存fsimage的命名空间的位置。
    legacyOivImageDir = conf.get(DFS_NAMENODE_LEGACY_OIV_IMAGE_DIR_KEY);

    // dfs.namenode.checkpoint.check.quiet-multiplier :  1.5
    quietMultiplier = conf.getDouble(DFS_NAMENODE_CHECKPOINT_QUIET_MULTIPLIER_KEY,
      DFS_NAMENODE_CHECKPOINT_QUIET_MULTIPLIER_DEFAULT);


    warnForDeprecatedConfigs(conf);
  }
  
  private static void warnForDeprecatedConfigs(Configuration conf) {
    for (String key : ImmutableList.of(
          "fs.checkpoint.size",
          "dfs.namenode.checkpoint.size")) {
      if (conf.get(key) != null) {
        LOG.warn("Configuration key " + key + " is deprecated! Ignoring..." +
            " Instead please specify a value for " +
            DFS_NAMENODE_CHECKPOINT_TXNS_KEY);
      }
    }
  }

  public long getPeriod() {
    return checkpointPeriod;
  }

  public long getCheckPeriod() {
    return Math.min(checkpointCheckPeriod, checkpointPeriod);
  }

  public long getTxnCount() {
    return checkpointTxnCount;
  }

  public int getMaxRetriesOnMergeError() {
    return maxRetriesOnMergeError;
  }

  public String getLegacyOivImageDir() {
    return legacyOivImageDir;
  }

  public double getQuietPeriod() {
    return this.checkpointPeriod * this.quietMultiplier;
  }
}
