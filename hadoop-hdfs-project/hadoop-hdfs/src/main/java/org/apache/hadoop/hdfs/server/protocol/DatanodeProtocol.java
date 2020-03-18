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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.*;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.security.KerberosInfo;

import javax.annotation.Nonnull;

/**********************************************************************
 * DatanodeProtocol是Datanode与Namenode间的接口，
 * Datanode会使用这个接 口与Namenode握手、注册、发送心跳、进行全量以及增量的数据块汇报。
 *
 * Namenode向Datanode下发名字节点指令是没有任何其他接口的，
 * 只会通过DatanodeProtocol的返回值来下发命令。
 *
 * Protocol that a DFS datanode uses to communicate with the NameNode.
 * It's used to upload current load information and block reports.
 *
 * The only way a NameNode can communicate with a DataNode is by
 * returning values from these functions.
 *
 **********************************************************************/
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, 
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface DatanodeProtocol {
  /**
   * This class is used by both the Namenode (client) and BackupNode (server) 
   * to insulate from the protocol serialization.
   * 
   * If you are adding/changing DN's interface then you need to 
   * change both this class and ALSO related protocol buffer
   * wire protocol definition in DatanodeProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see 
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   *
   *
   * 一个完整的Datanode启动操作会与Namenode进行4次交互，也就是调用4次 DatanodeProtocol定义的方法。
   * 首先调用versionRequest()与Namenode进行握手操作，
   * 然后 调用registerDatanode()向Namenode注册当前的Datanode，
   * 接着调用blockReport()汇报 Datanode上存储的所有数据块，
   * 最后调用cacheReport()汇报Datanode缓存的所有数据块。
   *
   *
   */
  public static final long versionID = 28L;
  
  // error code
  final static int NOTIFY = 0;
  final static int DISK_ERROR = 1; // there are still valid volumes on DN
  final static int INVALID_BLOCK = 2;
  final static int FATAL_DISK_ERROR = 3; // no valid volumes left on DN

  /**
   * Determines actions that data node should perform 
   * when receiving a datanode command.
   *
   *
   *
   */

  //未定义
  final static int DNA_UNKNOWN = 0;    // unknown action

  //数据块复制
  // DNA_TRANSFER指令用于触发数据节点的数据块复制操作，
  // 当HDFS系统中某个数据块 的副本数小于配置的副本系数时，
  // Namenode会通过DNA_TRANSFER指令通知某个拥有这个数据块副本的Datanode将该数据块复制到其他数据节点上。
  final static int DNA_TRANSFER = 1;   // transfer blocks to another datanode

  //数据库删除
  //DNA_INVALIDATE用于 通知Datanode删除数据节点上的指定数据块，
  // 这是因为Namenode发现了某个数据块的副 本数已经超过了配置的副本系数，
  // 这时Namenode会通知某个数据节点删除这个数据节点 上多余的数据块副本。
  final static int DNA_INVALIDATE = 2; // invalidate blocks

  // 关闭数据节点
  // DNA_SHUTDOWN已经废弃不用了，
  // Datanode接收到DNASHUTDOWN指令后会直接抛出UnsupportedOperationException异常。
  // 关闭Datanode是通过 调用ClientDatanodeProtocol.shutdownDatanode()方法来触发的。
  final static int DNA_SHUTDOWN = 3;   // shutdown node

  //重新注册数据节点
  final static int DNA_REGISTER = 4;   // re-register

  //提交上一次升级
  final static int DNA_FINALIZE = 5;   // finalize previous upgrade

  //数据块恢复
  //当客户端在写文件时发生异常退出，会造成数据流管道中不同数据 节点上数据块状态的不一致，
  // 这时Namenode会从数据流管道中选出一个数据节点作为主 恢复节点，
  // 协调数据流管道中的其他数据节点进行租约恢复操作，以同步这个数据块的状 态。

  final static int DNA_RECOVERBLOCK = 6;  // request a block recovery

  //安全相关
  final static int DNA_ACCESSKEYUPDATE = 7;  // update access key

  // 更新平衡器宽度
  final static int DNA_BALANCERBANDWIDTHUPDATE = 8; // update balancer bandwidth

  //缓存数据换
  final static int DNA_CACHE = 9;      // cache blocks

  //取消缓存数据块
  final static int DNA_UNCACHE = 10;   // uncache blocks

  //擦除编码重建命令
  final static int DNA_ERASURE_CODING_RECONSTRUCTION = 11; // erasure coding reconstruction command

  // 块存储移动命令
  int DNA_BLOCK_STORAGE_MOVEMENT = 12; // block storage movement command

  //删除sps工作命令
  int DNA_DROP_SPS_WORK_COMMAND = 13; // drop sps work command


  /** 
   * Register Datanode.
   *
   * 成功进行握手操作后，
   * Datanode会调用ClientProtocol.registerDatanode()方法向 Namenode注册当前的Datanode，
   * 这个方法的参数是一个DatanodeRegistration对象，
   * 它封装 了DatanodeID、Datanode的存储系统的布局版本号(layoutversion)、
   * 当前命名空间的 ID(namespaceId)、集群ID(clusterId)、
   * 文件系统的创建时间(ctime)以及Datanode当 前的软件版本号(softwareVersion)。
   *
   * namenode节点会判断Datanode的软件版本号与Namenode 的软件版本号是否兼容，
   * 如果兼容则进行注册操作，并返回一个DatanodeRegistration对象 供Datanode后续处理逻辑使用。
   *
   * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem#registerDatanode(DatanodeRegistration)
   * @param registration datanode registration information
   * @return the given {@link org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration} with
   *  updated registration information
   */
  @Idempotent
  public DatanodeRegistration registerDatanode(DatanodeRegistration registration
      ) throws IOException;
  
  /**
   * sendHeartbeat() tells the NameNode that the DataNode is still
   * alive and well.  Includes some status info, too. 
   * It also gives the NameNode a chance to return 
   * an array of "DatanodeCommand" objects in HeartbeatResponse.
   * A DatanodeCommand tells the DataNode to invalidate local block(s), 
   * or to copy them to other DataNodes, etc.
   * @param registration datanode registration information
   * @param reports utilization report per storage
   * @param xmitsInProgress number of transfers from this datanode to others
   * @param xceiverCount number of active transceiver threads
   * @param failedVolumes number of failed volumes
   * @param volumeFailureSummary info about volume failures
   * @param requestFullBlockReportLease whether to request a full block
   *                                    report lease.
   * @param slowPeers Details of peer DataNodes that were detected as being
   *                  slow to respond to packet writes. Empty report if no
   *                  slow peers were detected by the DataNode.
   * @throws IOException on error
   *
   * Datanode会定期向Namenode发送 心跳
   * dfs.heartbeat.interval配置项配置，默认是3秒
   *
   * 用于心跳汇报的接口，除了携带标识Datanode 身份的DatanodeRegistration对象外，
   * 还包括数据节点上所有存储的状态、缓存的状态、正 在写文件数据的连接数、读写数据使用的线程数等。
   *
   * sendHeartbeat()会返回一个HeartbeatResponse对象，
   * 这个对象包含了Namenode向Datanode发送的名字节点指令，以及当前Namenode的HA状态。
   *
   * 需要特别注意的是，在开 启了HA的HDFS集群中，
   * Datanode是需要同时向Active Namenode以及Standby Namenode发 送心跳的，
   * 不过只有ActiveNamenode才能向Datanode下发名字节点指令。
   *
   */
  @Idempotent
  public HeartbeatResponse sendHeartbeat(DatanodeRegistration registration,
                                       StorageReport[] reports,
                                       long dnCacheCapacity,
                                       long dnCacheUsed,
                                       int xmitsInProgress,
                                       int xceiverCount,
                                       int failedVolumes,
                                       VolumeFailureSummary volumeFailureSummary,
                                       boolean requestFullBlockReportLease,
                                       @Nonnull SlowPeerReports slowPeers,
                                       @Nonnull SlowDiskReports slowDisks)
      throws IOException;

  /**
   * blockReport() tells the NameNode about all the locally-stored blocks.
   * The NameNode returns an array of Blocks that have become obsolete [淘汰的]
   * and should be deleted.  This function is meant to upload *all*
   * the locally-stored blocks.  It's invoked upon startup and then
   * infrequently afterwards.
   * @param registration datanode registration
   * @param poolId the block pool ID for the blocks
   * @param reports report of blocks per storage
   *     Each finalized block is represented as 3 longs. Each under-
   *     construction replica is represented as 4 longs.
   *     This is done instead of Block[] to reduce memory used by block reports.
   * @param context Context information for this block report.
   *
   * @return - the next command for DN to process.
   * @throws IOException
   *
   * Datanode成功向Namenode注册之后，
   * Datanode会通过调用 DatanodeProtocol.blockReport()方法向Namenode上报它管理的所有数据块的信息。
   * 这个方 法需要三个参数:
   *   Datanode Registration用于标识当前的Datanode;
   *   poolId用于标识数据块所 在的块池ID;
   *   reports是一个StorageBlockReport对象的数组，每个StorageBlockReport对象都用于记录Datanode上一个存储空间存储的数据块。
   *
   *   这里需要特别注意的是，上报的数据块 是以长整型数组保存的，
   *   每个已经提交的数据块(finalized)以3个长整型来表示，
   *   每个构 建中的数据块(under-construction)以4个长整型来表示。
   *   之所以不使用ExtendedBlock对 象保存上报的数据块，
   *   是因为这样可以减少blockReport()操作所使用的内存，
   *
   *   Namenode接 收到消息时，不需要创建大量的ExtendedBlock对象，
   *   只需要不断地从长整型数组中提取 数据块即可。
   *
   */
  @Idempotent
  public DatanodeCommand blockReport(DatanodeRegistration registration,
            String poolId, StorageBlockReport[] reports,
            BlockReportContext context) throws IOException;
    

  /**
   * Namenode接收到blockReport()请求之后，
   * 会根据Datanode上报的数据块存储情况建立 数据块与数据节点之间的对应关系。
   * 同时，Namenode会在blockReport()的响应中携带名字 节点指令，
   * 通知数据节点进行重新注册、发送心跳、备份或者删除Datanode本地磁盘上数 据块副本的操作。
   * 这些名字节点指令都是以DatanodeCommand对象封装的
   *
   * blockReport()方法只在Datanode启动时以及指定间隔时执行一次。
   * 间隔是由 dfs.blockreport.intervalMsec参数配置的，默认是6小时执行一次。
   *
   *
   * Communicates the complete list of locally cached blocks to the NameNode.
   * 
   * This method is similar to
   * {@link #blockReport(DatanodeRegistration, String, StorageBlockReport[], BlockReportContext)},
   * which is used to communicated blocks stored on disk.
   *
   * @param            The datanode registration.
   * @param poolId     The block pool ID for the blocks.
   * @param blockIds   A list of block IDs.
   * @return           The DatanodeCommand.
   * @throws IOException
   */
  @Idempotent
  public DatanodeCommand cacheReport(DatanodeRegistration registration,
      String poolId, List<Long> blockIds) throws IOException;

  /**
   * blockReceivedAndDeleted() allows the DataNode to tell the NameNode about
   * recently-received and -deleted block data. 
   * 
   * For the case of received blocks, a hint for preferred replica to be 
   * deleted when there is any excessive blocks is provided.
   * For example, whenever client code
   * writes a new Block here, or another DataNode copies a Block to
   * this DataNode, it will call blockReceived().
   *
   *
   * Datanode会定期(默认是5分钟，不可以配置)调用blockReceivedAndDeleted()方法
   * 向 Namenode汇报Datanode新接受的数据块或者删除的数据块。
   *
   * Datanode接受一个数据块，可 能是因为Client写入了新的数据块，
   * 或者从别的Datanode上复制一个数据块到当前 Datanode。
   *
   * Datanode删除一个数据块，则有可能是因为该数据块的副本数量过多，
   * Namenode向当前Datanode下发了删除数据块副本的指令。
   *
   * 我们可以把blockReceivedAndDeleted()方法理解为blockReport()的增量汇报，
   * 这个方法的参数包括
   * DatanodeRegistration对象、
   * 增量汇报数据块所在的块池ID，
   * 以及 StorageReceivedDeletedBlocks对象的数组，
   *
   * 这里的StorageReceived DeletedBlocks对象封装了Datanode的一个数据存储上新添加以及删除的数据块集合。
   *
   * Namenode接受了这个请求 之后，会更新它内存中数据块与数据节点的对应关系。
   *
   *
   */
  @Idempotent
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
                            String poolId,
                            StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks)
                            throws IOException;

  /**
   * errorReport() tells the NameNode about something that has gone
   * awry.  Useful for debugging.
   *
   * 该方法用于向名字节点上报运行过程中 发生的一些状况，如磁盘不可用等
   */
  @Idempotent
  public void errorReport(DatanodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException;

  /**
   * 这个方法的返回值是一个NamespaceInfo对象，NamespaceInfo对 象会封装当前HDFS集群的命名空间信息，
   *
   * 包括存储系统的布局版本号(layoutversion)、
   * 当前的命名空间的ID(namespaceId)、集群ID(clusterId)、
   * 文件系统的创建时间 (ctime)、构建时的HDFS版本号(buildVersion)、
   * 块池ID(blockpoolId)、当前的软件 版本号(softwareVersion)等。
   *
   * Datanode获取到NamespaceInfo对象后，
   * 就会比较Datanode 当前的HDFS版本号和Namenode的HDFS版本号，
   * 如果Datanode版本与Namenode版本不能 协同工作，则抛出异常，
   * Datanode也就无法注册到该Namenode上。
   * 如果当前Datanode上已 经有了文件存储的目录，
   * 那么Datanode还会检查Datanode存储上的块池ID、文件系统ID以 及集群ID与Namenode返回的是否一致。
   *
   * @return
   * @throws IOException
   */
  @Idempotent
  public NamespaceInfo versionRequest() throws IOException;

  /**
   * same as {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#reportBadBlocks(LocatedBlock[])}
   * }
   *
   * reportBadBlocks()与ClientProtocol.reportBad.Blocks()方法很类似，
   * Datanode会调用这 个方法向Namenode汇报损坏的数据块。
   *
   * Datanode会在三种情况下调用这个方法:
   *
   * DataBlockScanner线程定期扫描数据节点上存储的数据块， 发现数据块的校验出现错误时;
   *
   * 数据流管道写数据时，  Datanode接受了一个新的数据块， 进行数据块校验操作出现错 误时;
   *
   * 进行数据块复制操作(DataTransfer)，Datanode读取本地存储的数据块时，发现 本地数据块副本的长度小于Namenode记录的长度，
   * 则认为该数据块已经无效，会调用 reportBadBlocks()方法。
   *
   * reportBadBlocks()方法的参数是LocatedBlock对象，
   * 这个对象描述 了出现错误数据块的位置，
   * Namenode收到reportBadBlocks()请求后，
   * 会下发数据块副本删 除指令删除错误的数据块。
   *
   */
  @Idempotent
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;
  
  /**
   * Commit block synchronization in lease recovery
   *
   * 用于在租约恢复操作时同步数据块的状态。
   * 在租约恢复操作时，主数据节点完成所有租约恢复协调操作后调用 commitBlockSynchronization()方法
   * 同步Datanode和Namenode上数据块的状态，
   * 所以 commitBlockSynchronization()方法包含了大量的参数。
   *
   *
   */
  @Idempotent
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets,
      String[] newtargetstorages) throws IOException;
}
