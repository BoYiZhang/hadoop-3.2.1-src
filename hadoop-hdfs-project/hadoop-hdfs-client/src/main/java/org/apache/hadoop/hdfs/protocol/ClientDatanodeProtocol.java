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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSelector;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.hdfs.server.datanode.DiskBalancerWorkStatus;

/** An client-datanode protocol for block recovery
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@KerberosInfo(
    serverPrincipal = HdfsClientConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@TokenInfo(BlockTokenSelector.class)
public interface ClientDatanodeProtocol {
  /**
   * Until version 9, this class ClientDatanodeProtocol served as both
   * the client interface to the DN AND the RPC protocol used to
   * communicate with the NN.
   *
   * This class is used by both the DFSClient and the
   * DN server side to insulate from the protocol serialization.
   *
   * If you are adding/changing DN's interface then you need to
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in ClientDatanodeProtocol.proto.
   *
   * For more details on protocol buffer wire protocol, please see
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   *
   * The log of historical changes can be retrieved from the svn).
   * 9: Added deleteBlockPool method
   *
   * 9 is the last version id when this class was used for protocols
   *  serialization. DO not update this version any further.
   */
  long versionID = 9L;

  /** Return the visible length of a replica. */
  /**
   * 客户端会调用getReplicaVisibleLength()方法从数据节点获取某个数据块副本真实的数 据长度。
   * 当客户端读取一个HDFS文件时，需要获取这个文件对应的所有数据块的长度，
   * 用于建立数据块的输入流，然后读取数据。
   * 但是Namenode元数据中文件的最后一个数据 块长度与Datanode实际存储的可能不一致，
   * 所以客户端在创建输入流时就需要调用
   * getReplicaVisibleLength()方法从Datanode获取这个数据块的真实长度。
   *
   * @param b
   * @return
   * @throws IOException
   */
  long getReplicaVisibleLength(ExtendedBlock b) throws IOException;

  /**
   * 在用户管理员命令中有一个'hdfs dfsadmindatanodehost:port'命令，
   * 用于触发指定的 Datanode重新加载配置文件，
   * 停止服务那些已经从配置文件中删除的块池(blockPool)， 开始服务新添加的块池。
   *
   *
   * Refresh the list of federated namenodes from updated configuration
   * Adds new namenodes and stops the deleted namenodes.
   *
   * @throws IOException on error
   **/
  void refreshNamenodes() throws IOException;

  /**
   *
   * 用于从指定Datanode删除blockpoolId对应的块 池，如果force参数被设置了，
   * 那么无论这个块池目录中有没有数据都会被强制删除;
   * 否 则，只有这个块池目录为空的情况下才会被删除。
   *
   * 如果Datanode还在服务 这个块池，这个命令的执行将会失败。
   * 要停止一个数据节点服务指定的块池，需要调用上 面提到的refreshNamenodes()方法。
   *
   * deleteBlockPool()方法有两个参数，其中blockpoolId用于设置要被删除的块池ID;
   * force 用于设置是否强制删除。
   *
   * Delete the block pool directory. If force is false it is deleted only if
   * it is empty, otherwise it is deleted along with its contents.
   *
   * @param bpid Blockpool id to be deleted.
   * @param force If false blockpool directory is deleted only if it is empty
   *          i.e. if it doesn't contain any block files, otherwise it is
   *          deleted along with its contents.
   * @throws IOException
   */
  void deleteBlockPool(String bpid, boolean force) throws IOException;

  /**
   * Retrieves the path names of the block file and metadata file stored on the
   * local file system.
   *
   * In order for this method to work, one of the following should be satisfied:
   * <ul>
   * <li>
   * The client user must be configured at the datanode to be able to use this
   * method.</li>
   * <li>
   * When security is enabled, kerberos authentication must be used to connect
   * to the datanode.</li>
   * </ul>
   *
   * @param block
   *          the specified block on the local datanode
   * @param token
   *          the block access token.
   * @return the BlockLocalPathInfo of a block
   * @throws IOException
   *           on error
   *
   * HDFS对于本地读取，也就是Client和保存该数据块的Datanode在同一台物理机器上时，是有很多优化的。
   * Client会调用ClientProtocol.getBlockLocaIPathInfo()方法
   * 获取指定数据块文件以及数据块校验文件在当前节点上的本地路径，
   * 然后利用这个本地路径执行本地读取操作，而不是通过流式接口执行远程读取，这样也就大大优化了读取的性能。
   *
   * 在HDFS 2.6版本中，
   * 客户端会通过调用DataTransferProtocol接口从数据节点获取数据块文件的文件描述符，
   * 然后打开并读取文件以实现短路读操作，而不是通过 ClientDatanodeProtoco接口。
   *
   */
  BlockLocalPathInfo getBlockLocalPathInfo(ExtendedBlock block,
      Token<BlockTokenIdentifier> token) throws IOException;

  /**
   * shutdownDatanode()方法用于关闭一个数据节点，这个方法主要是为了支持管理命
   * 令'hdfs dfsadmin-shutdownDatanode <datanode_host:ipc_port> [upgrade]'。
   * Shuts down a datanode.
   *
   * @param forUpgrade If true, data node does extra prep work before shutting
   *          down. The work includes advising clients to wait and saving
   *          certain states for quick restart. This should only be used when
   *          the stored data will remain the same during upgrade/restart.
   * @throws IOException
   */
  void shutdownDatanode(boolean forUpgrade) throws IOException;

  /**
   * Evict clients that are writing to a datanode.
   *
   * @throws IOException
   */
  void evictWriters() throws IOException;

  /**
   * Obtains datanode info
   *
   * getDatanodeInfo()方法用于获取指定Datanode的信息，
   * 这里的信息包括Datanode运行 的HDFS版本、Datanode配置的HDFS版本，
   * 以及Datanode的启动时间。对应于管理命令'hdfs dfsadmin-getDatanodeInfo'。
   *
   * @return software/config version and uptime of the datanode
   */
  DatanodeLocalInfo getDatanodeInfo() throws IOException;

  /**
   * startReconfiguration()方法用于触发Datanode异步地从磁盘重新加载配置，并且应用该 配置。
   * 这个方法用于支持管理命令'hdfs dfsadmin-getDatanodeInfo-reconfigstart'。
   *
   * Asynchronously reload configuration on disk and apply changes.
   */
  void startReconfiguration() throws IOException;

  /**
   * 查询上一次触发的重新加载配置操作的运行情况
   *
   * Get the status of the previously issued reconfig task.
   * @see org.apache.hadoop.conf.ReconfigurationTaskStatus
   */
  ReconfigurationTaskStatus getReconfigurationStatus() throws IOException;

  /**
   * Get a list of allowed properties for reconfiguration.
   */
  List<String> listReconfigurableProperties() throws IOException;

  /**
   * Trigger a new block report.
   */
  void triggerBlockReport(BlockReportOptions options)
    throws IOException;

  /**
   * Get current value of the balancer bandwidth in bytes per second.
   *
   * @return balancer bandwidth
   */
  long getBalancerBandwidth() throws IOException;

  /**
   * 用于获取数据块是存储在指定Datanode的哪个卷 (volume)上的
   * Get volume report of datanode.
   */
  List<DatanodeVolumeInfo> getVolumeReport() throws IOException;

  /**
   * Submit a disk balancer plan for execution.
   */
  void submitDiskBalancerPlan(String planID, long planVersion, String planFile,
                              String planData, boolean skipDateCheck)
       throws IOException;

  /**
   * Cancel an executing plan.
   *
   * @param planID - A SHA-1 hash of the plan string.
   */
  void cancelDiskBalancePlan(String planID) throws IOException;


  /**
   * Gets the status of an executing diskbalancer Plan.
   */
  DiskBalancerWorkStatus queryDiskBalancerPlan() throws IOException;

  /**
   * Gets a run-time configuration value from running diskbalancer instance.
   * For example : Disk Balancer bandwidth of a running instance.
   *
   * @param key runtime configuration key
   * @return value of the key as a string.
   * @throws IOException - Throws if there is no such key
   */
  String getDiskBalancerSetting(String key) throws IOException;
}
