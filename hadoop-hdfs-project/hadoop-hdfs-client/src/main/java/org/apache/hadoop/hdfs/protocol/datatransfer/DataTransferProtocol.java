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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockChecksumOptions;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.StripedBlockInfo;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 用来描述写入或者读出Datanode上数据的基于TCP的流式接口，
 *     HDFS客户端与数据节点以及数据节点
 *     与
 *     数据节点之间的数据块传输
 * 就是基于DataTransferProtocol接口实现的。
 *
 * DataTransferProtocol接口调用并没有使用Hadoop RPC框架提供的功能，
 * 而是定义了用于发送DataTransferProtocol请求的Sender类，
 * 以及用于响应DataTransferProtocol请求的 Receiver类，
 *
 *      Sender类和Receiver类都实现了DataTransferProtocol接口。
 *      我们假设DFSClient发起了一个 DataTransferProtocol.readBlock()操作，
 *      那么DFSClient会调用Sender将这个请求序列化，并 传输给远端的Receiver。
 *      远端的Receiver接收到这个请求后，会反序列化请求，然后调用 代码执行读取操作。
 *
 *
 * Transfer data to/from datanode using a streaming protocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface DataTransferProtocol {
  Logger LOG = LoggerFactory.getLogger(DataTransferProtocol.class);

  /** Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious.
   */
  /*
   * Version 28:
   *    Declare methods in DataTransferProtocol interface.
   */
  int DATA_TRANSFER_VERSION = 28;

  /**
   * Read a block.
   *
   * @param blk the block being read.
   * @param blockToken security token for accessing the block.
   * @param clientName client's name.
   * @param blockOffset offset of the block.
   * @param length maximum number of bytes for this read.
   * @param sendChecksum if false, the DN should skip reading and sending
   *        checksums
   * @param cachingStrategy  The caching strategy to use.
   *
   * 从当前Datanode读取指定的数据块。
   */
  void readBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final long blockOffset,
      final long length,
      final boolean sendChecksum,
      final CachingStrategy cachingStrategy) throws IOException;

  /**
   * 将指定数据块写入数据流管道(pipeLine)中。
   * Write a block to a datanode pipeline.
   * The receiver datanode of this call is the next datanode in the pipeline.
   * The other downstream datanodes are specified by the targets parameter.
   * Note that the receiver {@link DatanodeInfo} is not required in the
   * parameter list since the receiver datanode knows its info.  However, the
   * {@link StorageType} for storing the replica in the receiver datanode is a
   * parameter since the receiver datanode may support multiple storage types.
   *
   * @param blk the block being written.
   * @param storageType for storing the replica in the receiver datanode.
   * @param blockToken security token for accessing the block.
   * @param clientName client's name.
   * @param targets other downstream datanodes in the pipeline.
   * @param targetStorageTypes target {@link StorageType}s corresponding
   *                           to the target datanodes.
   * @param source source datanode.
   * @param stage pipeline stage.
   * @param pipelineSize the size of the pipeline.
   * @param minBytesRcvd minimum number of bytes received.
   * @param maxBytesRcvd maximum number of bytes received.
   * @param latestGenerationStamp the latest generation stamp of the block.
   * @param requestedChecksum the requested checksum mechanism
   * @param cachingStrategy the caching strategy
   * @param allowLazyPersist hint to the DataNode that the block can be
   *                         allocated on transient storage i.e. memory and
   *                         written to disk lazily
   * @param pinning whether to pin the block, so Balancer won't move it.
   * @param targetPinnings whether to pin the block on target datanode
   * @param storageID optional StorageIDs designating where to write the
   *                  block. An empty String or null indicates that this
   *                  has not been provided.
   * @param targetStorageIDs target StorageIDs corresponding to the target
   *                         datanodes.
   */
  void writeBlock(final ExtendedBlock blk,
      final StorageType storageType,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes,
      final DatanodeInfo source,
      final BlockConstructionStage stage,
      final int pipelineSize,
      final long minBytesRcvd,
      final long maxBytesRcvd,
      final long latestGenerationStamp,
      final DataChecksum requestedChecksum,
      final CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean pinning,
      final boolean[] targetPinnings,
      final String storageID,
      final String[] targetStorageIDs) throws IOException;
  /**
   * 将指定数据块复制(transfer)到另一个Datanode上。
   *
   * 数据块复制 操作是指数据流管道中的数据节点出现故障，
   * 需要用新的数据节点替换异常的数 据节点时，
   * DFSClient会调用这个方法将数据流管道中异常数据节点上已经写入的数据块复制到新添加的数据节点上。
   *
   * Transfer a block to another datanode.
   * The block stage must be
   * either {@link BlockConstructionStage#TRANSFER_RBW}
   * or {@link BlockConstructionStage#TRANSFER_FINALIZED}.
   *
   * @param blk the block being transferred.
   * @param blockToken security token for accessing the block.
   * @param clientName client's name.
   * @param targets target datanodes.
   * @param targetStorageIDs StorageID designating where to write the
   *                     block.
   */
  void transferBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes,
      final String[] targetStorageIDs) throws IOException;

  /**
   *
   * 获取一个短路(short circuit)读取数据块的文件描述符
   * Request short circuit access file descriptors from a DataNode.
   *
   * @param blk             The block to get file descriptors for.
   * @param blockToken      Security token for accessing the block.
   * @param slotId          The shared memory slot id to use, or null
   *                          to use no slot id.
   * @param maxVersion      Maximum version of the block data the client
   *                          can understand.
   * @param supportsReceiptVerification  True if the client supports
   *                          receipt verification.
   */
  void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      SlotId slotId, int maxVersion, boolean supportsReceiptVerification)
        throws IOException;

  /**
   * 释放一个短路读取数据块的文件描述符。
   * Release a pair of short-circuit FDs requested earlier.
   *
   * @param slotId          SlotID used by the earlier file descriptors.
   */
  void releaseShortCircuitFds(final SlotId slotId) throws IOException;

  /**
   * 获取保存短路读取数据块的共享内存
   * Request a short circuit shared memory area from a DataNode.
   *
   * @param clientName       The name of the client.
   */
  void requestShortCircuitShm(String clientName) throws IOException;

  /**
   * 将从源Datanode复制来的数据块写入本地Datanode。
   * 写成功后通 知NameNode，并且删除源Datanode上的数据块。
   *
   * 这个方法主要用在数据块平衡 操作(balancing)的场景下。
   * source datanode 和  original datanode  必须不同.
   *
   * Receive a block from a source datanode
   * and then notifies the namenode
   * to remove the copy from the original datanode.
   * Note that the source datanode and the original datanode can be different.
   * It is used for balancing purpose.
   *
   * @param blk the block being replaced.
   * @param storageType the {@link StorageType} for storing the block.
   * @param blockToken security token for accessing the block.
   * @param delHint the hint for deleting the block in the original datanode.
   * @param source the source datanode for receiving the block.
   * @param storageId an optional storage ID to designate where the block is
   *                  replaced to.
   */
  void replaceBlock(final ExtendedBlock blk,
      final StorageType storageType,
      final Token<BlockTokenIdentifier> blockToken,
      final String delHint,
      final DatanodeInfo source,
      final String storageId) throws IOException;

  /**
   * Copy a block.
   * It is used for balancing purpose.
   *
   * 复制当前Datanode上的数据块。这个方法主要用在数据块平衡操作 的场景下。
   *
   * @param blk the block being copied.
   * @param blockToken security token for accessing the block.
   */
  void copyBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException;

  /**
   * 获取指定数据块的校验值。
   * Get block checksum (MD5 of CRC32).
   *
   * @param blk a block.
   * @param blockToken security token for accessing the block.
   * @param blockChecksumOptions determines how the block-level checksum is
   *     computed from underlying block metadata.
   * @throws IOException
   */
  void blockChecksum(ExtendedBlock blk,
      Token<BlockTokenIdentifier> blockToken,
      BlockChecksumOptions blockChecksumOptions) throws IOException;

  /**
   * Get striped block group checksum (MD5 of CRC32).
   *
   * @param stripedBlockInfo a striped block info.
   * @param blockToken security token for accessing the block.
   * @param requestedNumBytes requested number of bytes in the block group
   *                          to compute the checksum.
   * @param blockChecksumOptions determines how the block-level checksum is
   *     computed from underlying block metadata.
   * @throws IOException
   */
  void blockGroupChecksum(StripedBlockInfo stripedBlockInfo,
          Token<BlockTokenIdentifier> blockToken,
          long requestedNumBytes,
          BlockChecksumOptions blockChecksumOptions) throws IOException;
}
