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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaInputStreams;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ReadaheadPool.ReadaheadRequest;
import org.apache.hadoop.net.SocketOutputStream;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.DataChecksum;
import org.apache.htrace.core.TraceScope;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;
import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_SEQUENTIAL;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;

/**
 * Reads a block from the disk and sends it to a recipient.
 * 
 * Data sent from the BlockeSender in the following format:
 * <br><b>Data format:</b> <pre>
 *    +--------------------------------------------------+
 *    | ChecksumHeader | Sequence of data PACKETS...     |
 *    +--------------------------------------------------+ 
 * </pre>   
 * <b>ChecksumHeader format:</b> <pre>
 *    +--------------------------------------------------+
 *    | 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
 *    +--------------------------------------------------+ 
 * </pre>   
 * An empty packet is sent to mark the end of block and read completion.
 * 
 * PACKET Contains a packet header, checksum and data. Amount of data
 * carried is set by BUFFER_SIZE.
 * <pre>
 *   +-----------------------------------------------------+
 *   | Variable length header. See {@link PacketHeader}    |
 *   +-----------------------------------------------------+
 *   | x byte checksum data. x is defined below            |
 *   +-----------------------------------------------------+
 *   | actual data ......                                  |
 *   +-----------------------------------------------------+
 * 
 *   Data is made of Chunks. Each chunk is of length <= BYTES_PER_CHECKSUM.
 *   A checksum is calculated for each chunk.
 *  
 *   x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
 *       CHECKSUM_SIZE
 *  
 *   CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32) 
 *  </pre>
 *  
 *  The client reads data until it receives a packet with 
 *  "LastPacketInBlock" set to true or with a zero length. If there is 
 *  no checksum error, it replies to DataNode with OP_STATUS_CHECKSUM_OK.
 */
class BlockSender implements java.io.Closeable {
  static final Logger LOG = DataNode.LOG;
  static final Log ClientTraceLog = DataNode.ClientTraceLog;
  private static final boolean is32Bit = 
      System.getProperty("sun.arch.data.model").equals("32");
  /**
   * Minimum buffer used while sending data to clients. Used only if
   * transferTo() is enabled. 64KB is not that large. It could be larger, but
   * not sure if there will be much more improvement.
   */
  private static final int MIN_BUFFER_WITH_TRANSFERTO = 64*1024;
  private static final int IO_FILE_BUFFER_SIZE;
  static {
    HdfsConfiguration conf = new HdfsConfiguration();
    IO_FILE_BUFFER_SIZE = DFSUtilClient.getIoFileBufferSize(conf);
  }
  private static final int TRANSFERTO_BUFFER_SIZE = Math.max(IO_FILE_BUFFER_SIZE, MIN_BUFFER_WITH_TRANSFERTO);
  
  /** the block to read from */
  private final ExtendedBlock block;

  /** InputStreams and file descriptors to read block/checksum. */
  private ReplicaInputStreams ris;

  /** updated while using transferTo() */
  private long blockInPosition = -1;
  /** Checksum utility */
  private final DataChecksum checksum;
  /** Initial position to read */
  private long initialOffset;
  /** Current position of read */
  private long offset;
  /** Position of last byte to read from block file */
  private final long endOffset;
  /** Number of bytes in chunk used for computing checksum */
  private final int chunkSize;
  /** Number bytes of checksum computed for a chunk */
  private final int checksumSize;
  /** If true, failure to read checksum is ignored */
  private final boolean corruptChecksumOk;
  /** Sequence number of packet being sent */
  private long seqno;
  /** Set to true if transferTo is allowed for sending data to the client */
  private final boolean transferToAllowed;
  /** Set to true once entire requested byte range has been sent to the client */
  private boolean sentEntireByteRange;
  /** When true, verify checksum while reading from checksum file */
  private final boolean verifyChecksum;
  /** Format used to print client trace log messages */
  private final String clientTraceFmt;
  private volatile ChunkChecksum lastChunkChecksum = null;
  private DataNode datanode;

  /** The replica of the block that is being read. */
  private final Replica replica;

  // Cache-management related fields
  private final long readaheadLength;

  private ReadaheadRequest curReadahead;

  private final boolean alwaysReadahead;
  
  private final boolean dropCacheBehindLargeReads;
  
  private final boolean dropCacheBehindAllReads;
  
  private long lastCacheDropOffset;
  private final FileIoProvider fileIoProvider;
  
  @VisibleForTesting
  static long CACHE_DROP_INTERVAL_BYTES = 1024 * 1024; // 1MB
  
  /**
   * See {{@link BlockSender#isLongRead()}
   */
  private static final long LONG_READ_THRESHOLD_BYTES = 256 * 1024;

  // The number of bytes per checksum here determines the alignment
  // of reads: we always start reading at a checksum chunk boundary,
  // even if the checksum type is NULL. So, choosing too big of a value
  // would risk sending too much unnecessary data. 512 (1 disk sector)
  // is likely to result in minimal extra IO.
  private static final long CHUNK_SIZE = 512;

  private static final String EIO_ERROR = "Input/output error";
  /**
   * Constructor
   * 
   * @param block Block that is being read
   * @param startOffset starting offset to read from
   * @param length length of data to read
   * @param corruptChecksumOk if true, corrupt checksum is okay
   * @param verifyChecksum verify checksum while reading the data
   * @param sendChecksum send checksum to client.
   * @param datanode datanode from which the block is being read
   * @param clientTraceFmt format string used to print client trace logs
   * @throws IOException
   */
  BlockSender(ExtendedBlock block, long startOffset, long length,
              boolean corruptChecksumOk, boolean verifyChecksum,
              boolean sendChecksum, DataNode datanode, String clientTraceFmt,
              CachingStrategy cachingStrategy)
      throws IOException {

    InputStream blockIn = null;

    DataInputStream checksumIn = null;

    FsVolumeReference volumeRef = null;
    // FileIoprovider@5795  DataNode{data=FSDataset{dirpath='[/opt/tools/hadoop-3.2.1/data/hdfs/data, /opt/tools/hadoop-3.2.1/data/hdfs/data01]'}, localName='192.168.8.188:9866', datanodeUuid='9efa402a-df6b-48cf-9273-5468f68cc42f', xmitsInProgress=0}
    this.fileIoProvider = datanode.getFileIoProvider();
    try {

      this.block = block;

      this.corruptChecksumOk = corruptChecksumOk;

      this.verifyChecksum = verifyChecksum;

      this.clientTraceFmt = clientTraceFmt;

      /*
       * If the client asked for the cache to be dropped behind all reads,
       * we honor that.  Otherwise, we use the DataNode defaults.
       * When using DataNode defaults, we use a heuristic where we only
       * drop the cache for large reads.
       */
      if (cachingStrategy.getDropBehind() == null) {

        this.dropCacheBehindAllReads = false;

        this.dropCacheBehindLargeReads =
            datanode.getDnConf().dropCacheBehindReads;

      } else {
        this.dropCacheBehindAllReads =
            this.dropCacheBehindLargeReads =
                 cachingStrategy.getDropBehind().booleanValue();
      }
      /* 默认开启预读取, 除非 请求头 指定 "不开启预读取
       * Similarly, if readahead was explicitly requested, we always do it.
       * Otherwise, we read ahead based on the DataNode settings, and only
       * when the reads are large.
       */
      if (cachingStrategy.getReadahead() == null) {
        this.alwaysReadahead = false;
        ///  readaheadLength : 4194304 = 4M    默认 预读取大小: 4M
        this.readaheadLength = datanode.getDnConf().readaheadLength;

      } else {

        this.alwaysReadahead = true;

        this.readaheadLength = cachingStrategy.getReadahead().longValue();
      }
      this.datanode = datanode;
      
      if (verifyChecksum) {

        // To simplify implementation, callers may not specify verification without sending.
        Preconditions.checkArgument(sendChecksum,
            "If verifying checksum, currently must also send it.");
      }
      // 如果在构造BlockSender之后有一个追加写操作，那么最后一个部分校验和可能被append覆盖，
      // BlockSender需要在append write之前使用部分校验和。
      // if there is a append write happening right after the BlockSender
      // is constructed, the last partial checksum maybe overwritten by the
      // append, the BlockSender need to use the partial checksum before
      // the append write.
      ChunkChecksum chunkChecksum = null;

      final long replicaVisibleLength;
      try(AutoCloseableLock lock = datanode.data.acquireDatasetLock()) {
        // 获取datanode上的副本信息
        // FinalizedReplica, blk_1073746921_6097, FINALIZED
        //  getNumBytes()     = 42648690
        //  getBytesOnDisk()  = 42648690
        //  getVisibleLength()= 42648690
        //  getVolume()       = /opt/tools/hadoop-3.2.1/data/hdfs/data
        //  getBlockURI()     = file:/opt/tools/hadoop-3.2.1/data/hdfs/data/current/BP-451827885-192.168.8.156-1584099133244/current/finalized/subdir0/subdir19/blk_1073746921
        replica = getReplica(block, datanode);
        // 42648690
        replicaVisibleLength = replica.getVisibleLength();
      }
      if (replica.getState() == ReplicaState.RBW) {
        // 副本正在被写入 , 等待写入足够的内容
        final ReplicaInPipeline rbw = (ReplicaInPipeline) replica;

        waitForMinLength(rbw, startOffset + length);

        chunkChecksum = rbw.getLastChecksumAndDataLen();
      }
      if (replica instanceof FinalizedReplica) {
        // 副本已经被写入完成 , 获取该副本的 ChunkChecksum :dataLength = 42648690 checksum = {byte[4]@5845} 四位 : [-11,-56,-5,28]
        chunkChecksum = getPartialChunkChecksumForFinalized(  (FinalizedReplica)replica);
      }

      if (replica.getGenerationStamp() < block.getGenerationStamp()) {
        throw new IOException("Replica gen stamp < block genstamp, block="
            + block + ", replica=" + replica);
      } else if (replica.getGenerationStamp() > block.getGenerationStamp()) {
        if (DataNode.LOG.isDebugEnabled()) {
          DataNode.LOG.debug("Bumping up the client provided"
              + " block's genstamp to latest " + replica.getGenerationStamp()
              + " for block " + block);
        }

        block.setGenerationStamp(replica.getGenerationStamp());
      }
      /// 副本可见长度小于0 , 不可见
      if (replicaVisibleLength < 0) {
        throw new IOException("Replica is not readable, block="+ block + ", replica=" + replica);
      }
      if (DataNode.LOG.isDebugEnabled()) {
        DataNode.LOG.debug("block=" + block + ", replica=" + replica);
      }
      // 是否开启零拷贝   dfs.datanode.transferTo.allowed : true
      // transferToFully() fails on 32 bit platforms for block sizes >= 2GB,
      // use normal transfer in those cases
      this.transferToAllowed = datanode.getDnConf().transferToAllowed &&
        (!is32Bit || length <= Integer.MAX_VALUE);
      // 在读取数据之前获取引用  FsVolumeImple$FsVolumeReferenceImpl@5928
      // Obtain a reference before reading data
      volumeRef = datanode.data.getVolume(block).obtainReference();

      /* 判断是否要验证 DataChecksum
       * (corruptChecksumOK, meta_file_exist): operation
       * True,   True: will verify checksum  
       * True,  False: No verify, e.g., need to read data from a corrupted file 
       * False,  True: will verify checksum
       * False, False: throws IOException file not found
       */
      DataChecksum csum = null;
      if (verifyChecksum || sendChecksum) {
        LengthInputStream metaIn = null;
        boolean keepMetaInOpen = false;
        try {


          DataNodeFaultInjector.get().throwTooManyOpenFiles();


          metaIn = datanode.data.getMetaDataInputStream(block);
          if (!corruptChecksumOk || metaIn != null) {
            if (metaIn == null) {
              //need checksum but meta-data not found
              throw new FileNotFoundException("Meta-data not found for " +
                  block);
            }

            // The meta file will contain only the header if the NULL checksum
            // type was used, or if the replica was written to transient storage.
            // Also, when only header portion of a data packet was transferred
            // and then pipeline breaks, the meta file can contain only the
            // header and 0 byte in the block data file.
            // Checksum verification is not performed for replicas on transient
            // storage.  The header is important for determining the checksum
            // type later when lazy persistence copies the block to non-transient
            // storage and computes the checksum.
            int expectedHeaderSize = BlockMetadataHeader.getHeaderSize();  // 7
            if (!replica.isOnTransientStorage() &&
                metaIn.getLength() >= expectedHeaderSize) {
              checksumIn = new DataInputStream(new BufferedInputStream(
                  metaIn, IO_FILE_BUFFER_SIZE));
              // DataChecksum(type=CRC32C, chunkSize=512)
              csum = BlockMetadataHeader.readDataChecksum(checksumIn, block);
              keepMetaInOpen = true;
            } else if (!replica.isOnTransientStorage() &&
                metaIn.getLength() < expectedHeaderSize) {
              LOG.warn("The meta file length {} is less than the expected " +
                  "header length {}, indicating the meta file is corrupt",
                  metaIn.getLength(), expectedHeaderSize);
              throw new CorruptMetaHeaderException("The meta file length "+
                  metaIn.getLength()+" is less than the expected length "+
                  expectedHeaderSize);
            }
          } else {
            LOG.warn("Could not find metadata file for " + block);
          }
        } catch (FileNotFoundException e) {
          if ((e.getMessage() != null) && !(e.getMessage()
              .contains("Too many open files"))) {
            // The replica is on its volume map but not on disk
            datanode
                .notifyNamenodeDeletedBlock(block, replica.getStorageUuid());
            datanode.data.invalidate(block.getBlockPoolId(),
                new Block[] {block.getLocalBlock()});
          }
          throw e;
        } finally {
          if (!keepMetaInOpen) {  // keepMetaInOpen : true
            IOUtils.closeStream(metaIn);
          }
        }
      }
      if (csum == null) {
        csum = DataChecksum.newDataChecksum(DataChecksum.Type.NULL,
            (int)CHUNK_SIZE);
      }

      /*
       * If chunkSize is very large, then the metadata file is mostly
       * corrupted. For now just truncate bytesPerchecksum to blockLength.
       */       
      int size = csum.getBytesPerChecksum();
      // 如果chunkSize非常大，则元数据文件大部分已损坏。现在只需将bytesPerchecksum截断为blockLength。
      if (size > 10*1024*1024 && size > replicaVisibleLength) {
        //元数据文件损坏了, 重新构建 DataChecksum
        csum = DataChecksum.newDataChecksum(csum.getChecksumType(),
            Math.max((int)replicaVisibleLength, 10*1024*1024));
        size = csum.getBytesPerChecksum();

      }

      //校验块大小 512
      chunkSize = size;
      //校验算法 DataChecksum(type=CRC32C, chunkSize=512)
      checksum = csum;
      //校验和长度  4
      checksumSize = checksum.getChecksumSize();

      // 文件大小 42648690
      length = length < 0 ? replicaVisibleLength : length;

      // end is either last byte on disk or the length for which we have a  checksum
      // end要么是磁盘上的最后一个字节，要么是校验和的长度 :   这里是文件长度.
      long end = chunkChecksum != null ? chunkChecksum.getDataLength()  : replica.getBytesOnDisk();

      if (startOffset < 0 || startOffset > end
          || (length + startOffset) > end) {
        String msg = " Offset " + startOffset + " and length " + length
        + " don't match block " + block + " ( blockLen " + end + " )";
        LOG.warn(datanode.getDNRegistrationForBP(block.getBlockPoolId()) +
            ":sendBlock() : " + msg);
        throw new IOException(msg);
      }

      // 将offset位置设置在校验块的边界上， 也就是校验块的起始位置
      // Ensure read offset is position at the beginning of chunk
      offset = startOffset - (startOffset % chunkSize);
      if (length >= 0) {

        //计算endOffset的位置， 确保endOffset在校验块的结束位置
        // Ensure endOffset points to end of chunk.
        long tmpLen = startOffset + length;
        if (tmpLen % chunkSize != 0) {
          tmpLen += (chunkSize - tmpLen % chunkSize); //补齐数据, 使数据正好是512的整倍数
        }
        if (tmpLen < end) {
          //结束位置还在数据块内， 则可以使用磁盘上的校验值 , 理论上应该不走这里.
          // will use on-disk checksum here since the end is a stable chunk
          end = tmpLen;
        } else if (chunkChecksum != null) {
          //目前有写线程[当前线程]正在处理这个校验块， 则使用内存中的校验值
          // last chunk is changing. flag that we need to use in-memory checksum 
          this.lastChunkChecksum = chunkChecksum;
        }

      }
      endOffset = end; // 设置最后的偏移量为文件的偏移量
      // 将校验文件的坐标移动到offset对应的位置
      // seek to the right offsets
      if (offset > 0 && checksumIn != null) {

        long checksumSkip = (offset / chunkSize) * checksumSize;
        // note blockInStream is seeked when created below

        if (checksumSkip > 0) {
          // Should we use seek() for checksum file as well?
          IOUtils.skipFully(checksumIn, checksumSkip);
        }

      }

      //packet序列号设置为0
      seqno = 0;

      if (DataNode.LOG.isDebugEnabled()) {

        DataNode.LOG.debug("replica=" + replica);

      }
      //将数据块文件的坐标移动到offset位置 准备开始读写
      blockIn = datanode.data.getBlockInputStream(block, offset); // seek to offset
      // 构建block数据的读取流 ReplicaInputStreams
      ris = new ReplicaInputStreams(  blockIn, checksumIn, volumeRef, fileIoProvider);

    } catch (IOException ioe) {
      IOUtils.closeStream(this);
      org.apache.commons.io.IOUtils.closeQuietly(blockIn);
      org.apache.commons.io.IOUtils.closeQuietly(checksumIn);
      throw ioe;
    }
  }

  private ChunkChecksum getPartialChunkChecksumForFinalized(
      FinalizedReplica finalized) throws IOException {
    // There are a number of places in the code base where a finalized replica
    // object is created. If last partial checksum is loaded whenever a
    // finalized replica is created, it would increase latency in DataNode
    // initialization. Therefore, the last partial chunk checksum is loaded
    // lazily.

    // Load last checksum in case the replica is being written concurrently
    final long replicaVisibleLength = replica.getVisibleLength();
    if (replicaVisibleLength % CHUNK_SIZE != 0 &&
        finalized.getLastPartialChunkChecksum() == null) {
      // the finalized replica does not have precomputed last partial
      // chunk checksum. Recompute now.
      try {
        finalized.loadLastPartialChunkChecksum();
        return new ChunkChecksum(finalized.getVisibleLength(),
            finalized.getLastPartialChunkChecksum());
      } catch (FileNotFoundException e) {
        // meta file is lost. Continue anyway to preserve existing behavior.
        DataNode.LOG.warn(
            "meta file " + finalized.getMetaFile() + " is missing!");
        return null;
      }
    } else {
      // If the checksum is null, BlockSender will use on-disk checksum.
      return new ChunkChecksum(finalized.getVisibleLength(),
          finalized.getLastPartialChunkChecksum());
    }
  }

  /**
   * close opened files.
   */
  @Override
  public void close() throws IOException {
    if (ris.getDataInFd() != null &&
        ((dropCacheBehindAllReads) ||
         (dropCacheBehindLargeReads && isLongRead()))) {
      try {
        ris.dropCacheBehindReads(block.getBlockName(), lastCacheDropOffset,
            offset - lastCacheDropOffset, POSIX_FADV_DONTNEED);
      } catch (Exception e) {
        LOG.warn("Unable to drop cache on file close", e);
      }
    }
    if (curReadahead != null) {
      curReadahead.cancel();
    }

    try {
      ris.closeStreams();
    } finally {
      IOUtils.closeStream(ris);
      ris = null;
    }
  }
  
  private static Replica getReplica(ExtendedBlock block, DataNode datanode)
      throws ReplicaNotFoundException {
    Replica replica = datanode.data.getReplica(block.getBlockPoolId(),
        block.getBlockId());
    if (replica == null) {
      throw new ReplicaNotFoundException(block);
    }
    return replica;
  }
  
  /**
   * Wait for rbw replica to reach the length
   * @param rbw replica that is being written to
   * @param len minimum length to reach
   * @throws IOException on failing to reach the len in given wait time
   */
  private static void waitForMinLength(ReplicaInPipeline rbw, long len)
      throws IOException {
    // Wait for 3 seconds for rbw replica to reach the minimum length
    for (int i = 0; i < 30 && rbw.getBytesOnDisk() < len; i++) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    long bytesOnDisk = rbw.getBytesOnDisk();
    if (bytesOnDisk < len) {
      throw new IOException(
          String.format("Need %d bytes, but only %d bytes available", len,
              bytesOnDisk));
    }
  }

  /**
   * Converts an IOExcpetion (not subclasses) to SocketException.
   * This is typically done to indicate to upper layers that the error 
   * was a socket error rather than often more serious exceptions like 
   * disk errors.
   */
  private static IOException ioeToSocketException(IOException ioe) {
    if (ioe.getClass().equals(IOException.class)) {
      // "se" could be a new class in stead of SocketException.
      IOException se = new SocketException("Original Exception : " + ioe);
      se.initCause(ioe);
      /* Change the stacktrace so that original trace is not truncated
       * when printed.*/ 
      se.setStackTrace(ioe.getStackTrace());
      return se;
    }
    // otherwise just return the same exception.
    return ioe;
  }

  /**
   * @param datalen Length of data 
   * @return number of chunks for data of given size
   */
  private int numberOfChunks(long datalen) {
    return (int) ((datalen + chunkSize - 1)/chunkSize);
  }
  
  /**
   * Sends a packet with up to maxChunks chunks of data.
   * 
   * @param pkt buffer used for writing packet data
   * @param maxChunks maximum number of chunks to send
   * @param out stream to send data to
   * @param transferTo use transferTo to send data
   * @param throttler used for throttling data transfer bandwidth
   */
  private int sendPacket(ByteBuffer pkt, int maxChunks, OutputStream out,
      boolean transferTo, DataTransferThrottler throttler) throws IOException {


    int dataLen = (int) Math.min(endOffset - offset,
                             (chunkSize * (long) maxChunks));


    //数据包中包含多少个校验块
    int numChunks = numberOfChunks(dataLen); // Number of chunks be sent in the packet

    //校验数据长度
    int checksumDataLen = numChunks * checksumSize;

    //数据包长度
    int packetLen = dataLen + checksumDataLen + 4;

    boolean lastDataPacket = offset + dataLen == endOffset && dataLen > 0;

    // The packet buffer is organized as follows:
    // _______HHHHCCCCD?D?D?D?
    //        ^   ^
    //        |   \ checksumOff
    //        \ headerOff
    // _ padding, since the header is variable-length
    // H = header and length prefixes
    // C = checksums
    // D? = data, if transferTo is false.

    //将数据包头域写入缓存中
    int headerLen = writePacketHeader(pkt, dataLen, packetLen);

    //数据包头域在缓存中的位置
    // Per above, the header doesn't start at the beginning of the buffer
    int headerOff = pkt.position() - headerLen;

    //校验数据在缓存中的位置
    int checksumOff = pkt.position();

    byte[] buf = pkt.array();
    
    if (checksumSize > 0 && ris.getChecksumIn() != null) {

      //将校验数据写入缓存中
      readChecksum(buf, checksumOff, checksumDataLen);

      // write in progress that we need to use to get last checksum
      if (lastDataPacket && lastChunkChecksum != null) {


        int start = checksumOff + checksumDataLen - checksumSize;


        byte[] updatedChecksum = lastChunkChecksum.getChecksum();


        if (updatedChecksum != null) {
          System.arraycopy(updatedChecksum, 0, buf, start, checksumSize);
        }
      }
    }
    
    int dataOff = checksumOff + checksumDataLen;


    if (!transferTo) { // normal transfer
      try {

        //在普通模式下， 将实际数据写入缓存中
        ris.readDataFully(buf, dataOff, dataLen);
      } catch (IOException ioe) {
        if (ioe.getMessage().startsWith(EIO_ERROR)) {
          throw new DiskFileCorruptException("A disk IO error occurred", ioe);
        }
        throw ioe;
      }

      if (verifyChecksum) {

        //确认校验和正确
        verifyChecksum(buf, dataOff, dataLen, numChunks, checksumOff);
      }
    }
    
    try {
      //transferTo模式
      if (transferTo) {

        //将头域和校验和写入输出流中
        SocketOutputStream sockOut = (SocketOutputStream)out;

        //使用transfer方式， 将数据从数据块文件直接零拷贝到IO流中
        // First write header and checksums
        sockOut.write(buf, headerOff, dataOff - headerOff);

        // no need to flush since we know out is not a buffered stream
        FileChannel fileCh = ((FileInputStream)ris.getDataIn()).getChannel();


        LongWritable waitTime = new LongWritable();

        LongWritable transferTime = new LongWritable();


        fileIoProvider.transferToSocketFully(
            ris.getVolumeRef().getVolume(), sockOut, fileCh, blockInPosition,
            dataLen, waitTime, transferTime);


        datanode.metrics.addSendDataPacketBlockedOnNetworkNanos(waitTime.get());
        datanode.metrics.addSendDataPacketTransferNanos(transferTime.get());


        blockInPosition += dataLen;
      } else {

        //在正常模式下
        //将缓存中的所有数据（包括头域、 校验和以及实际数据） 写入输出流中
        // normal transfer
        out.write(buf, headerOff, dataOff + dataLen - headerOff);
      }
    } catch (IOException e) {
      if (e instanceof SocketTimeoutException) {
        /*
         * writing to client timed out.  This happens if the client reads
         * part of a block and then decides not to read the rest (but leaves
         * the socket open).
         * 
         * Reporting of this case is done in DataXceiver#run
         */
      } else {
        /* Exception while writing to the client. Connection closure from
         * the other end is mostly the case and we do not care much about
         * it. But other things can go wrong, especially in transferTo(),
         * which we do not want to ignore.
         *
         * The message parsing below should not be considered as a good
         * coding example. NEVER do it to drive a program logic. NEVER.
         * It was done here because the NIO throws an IOException for EPIPE.
         */
        String ioem = e.getMessage();
        /*
         * If we got an EIO when reading files or transferTo the client socket,
         * it's very likely caused by bad disk track or other file corruptions.
         */
        if (ioem.startsWith(EIO_ERROR)) {
          throw new DiskFileCorruptException("A disk IO error occurred", e);
        }
        if (!ioem.startsWith("Broken pipe") && !ioem.startsWith("Connection reset")) {
          LOG.error("BlockSender.sendChunks() exception: ", e);
          datanode.getBlockScanner().markSuspectBlock(
              ris.getVolumeRef().getVolume().getStorageID(),
              block);
        }
      }
      throw ioeToSocketException(e);
    }

    if (throttler != null) {

      // rebalancing so throttle
      //调整节流器
      throttler.throttle(packetLen);
    }

    return dataLen;
  }
  
  /**
   * Read checksum into given buffer
   * @param buf buffer to read the checksum into
   * @param checksumOffset offset at which to write the checksum into buf
   * @param checksumLen length of checksum to write
   * @throws IOException on error
   */
  private void readChecksum(byte[] buf, final int checksumOffset,
      final int checksumLen) throws IOException {
    if (checksumSize <= 0 && ris.getChecksumIn() == null) {
      return;
    }
    try {
      ris.readChecksumFully(buf, checksumOffset, checksumLen);
    } catch (IOException e) {
      LOG.warn(" Could not read or failed to verify checksum for data"
          + " at offset " + offset + " for block " + block, e);
      ris.closeChecksumStream();
      if (corruptChecksumOk) {
        if (checksumLen > 0) {
          // Just fill the array with zeros.
          Arrays.fill(buf, checksumOffset, checksumOffset + checksumLen,
              (byte) 0);
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Compute checksum for chunks and verify the checksum that is read from
   * the metadata file is correct.
   * 
   * @param buf buffer that has checksum and data
   * @param dataOffset position where data is written in the buf
   * @param datalen length of data
   * @param numChunks number of chunks corresponding to data
   * @param checksumOffset offset where checksum is written in the buf
   * @throws ChecksumException on failed checksum verification
   */
  public void verifyChecksum(final byte[] buf, final int dataOffset,
      final int datalen, final int numChunks, final int checksumOffset)
      throws ChecksumException {
    int dOff = dataOffset;
    int cOff = checksumOffset;
    int dLeft = datalen;

    for (int i = 0; i < numChunks; i++) {
      checksum.reset();
      int dLen = Math.min(dLeft, chunkSize);
      checksum.update(buf, dOff, dLen);
      if (!checksum.compare(buf, cOff)) {
        long failedPos = offset + datalen - dLeft;
        StringBuilder replicaInfoString = new StringBuilder();
        if (replica != null) {
          replicaInfoString.append(" for replica: " + replica.toString());
        }
        throw new ChecksumException("Checksum failed at " + failedPos
            + replicaInfoString, failedPos);
      }
      dLeft -= dLen;
      dOff += dLen;
      cOff += checksumSize;
    }
  }
  
  /**
   * sendBlock() is used to read block and its metadata and stream the data to
   * either a client or to another datanode. 
   * 
   * @param out  stream to which the block is written to
   * @param baseStream optional. if non-null, <code>out</code> is assumed to 
   *        be a wrapper over this stream. This enables optimizations for
   *        sending the data, e.g. 
   *        {@link SocketOutputStream#transferToFully(FileChannel, 
   *        long, int)}.
   * @param throttler for sending data.
   * @return total bytes read, including checksum data.
   */
  long sendBlock(DataOutputStream out, OutputStream baseStream, 
                 DataTransferThrottler throttler) throws IOException {
    final TraceScope scope = datanode.getTracer().
        newScope("sendBlock_" + block.getBlockId());
    try {
      return doSendBlock(out, baseStream, throttler);
    } finally {
      scope.close();
    }
  }

  private long doSendBlock(DataOutputStream out, OutputStream baseStream,
        DataTransferThrottler throttler) throws IOException {
    if (out == null) {
      throw new IOException( "out stream is null" );
    }
    initialOffset = offset;

    long totalRead = 0;

    OutputStream streamForSendChunks = out;
    
    lastCacheDropOffset = initialOffset;

    if (isLongRead() && ris.getDataInFd() != null) {
      // Advise that this file descriptor will be accessed sequentially.
      ris.dropCacheBehindReads(block.getBlockName(), 0, 0,
          POSIX_FADV_SEQUENTIAL);
    }
    //1． 将数据预读取至操作系统的缓存中
    // Trigger readahead of beginning of file if configured.
    manageOsCache();

    final long startTime = ClientTraceLog.isDebugEnabled() ? System.nanoTime() : 0;

    //2． 构造存放数据包（packet） 的缓冲区
    try {
      int maxChunksPerPacket;

      // pktBufSize : 33
      int pktBufSize = PacketHeader.PKT_MAX_HEADER_LEN;
      boolean transferTo = transferToAllowed && !verifyChecksum
          && baseStream instanceof SocketOutputStream
          && ris.getDataIn() instanceof FileInputStream;


      if (transferTo) {
        FileChannel fileChannel =  ((FileInputStream)ris.getDataIn()).getChannel();
        blockInPosition = fileChannel.position();
        streamForSendChunks = baseStream;

        // 这里的TRANSFERTO_BUFFER_SIZE大小默认是64KB
        // maxChunksPerPacket变量表明一个数据包中最多包含多少个校验块 : 128 个
        maxChunksPerPacket = numberOfChunks(TRANSFERTO_BUFFER_SIZE);
        //缓沖区中只存放校验数据 pktBufSize : 545
        // Smaller packet size to only hold checksum when doing transferTo
        pktBufSize += checksumSize * maxChunksPerPacket;
      } else {


        //这里的IO—FILE_BUFFER_SIZE大小默认是4KB
        maxChunksPerPacket = Math.max(1, numberOfChunks(IO_FILE_BUFFER_SIZE));
        // Packet size includes both checksum and data
        //缓冲区存放校验数据以及实际数据
        pktBufSize += (chunkSize + checksumSize) * maxChunksPerPacket;
      }
      //构造缓沖区pktBuf  : 545
      ByteBuffer pktBuf = ByteBuffer.allocate(pktBufSize);

      //循环调用sendPacket()发送packet
      while (endOffset > offset && !Thread.currentThread().isInterrupted()) {

        manageOsCache();
        long len = sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks,
            transferTo, throttler);
        offset += len;
        totalRead += len + (numberOfChunks(len) * checksumSize);
        seqno++;
      }
      //如果当前线程被中断， 则不再发送完整的数据块
      // If this thread was interrupted, then it did not send the full block.
      if (!Thread.currentThread().isInterrupted()) {
        try {
          // 发送一个空的数据包用以标识数据块的结束
          // send an empty packet to mark the end of the block
          sendPacket(pktBuf, maxChunksPerPacket, streamForSendChunks, transferTo,
              throttler);
          out.flush();
        } catch (IOException e) { //socket error
          throw ioeToSocketException(e);
        }

        sentEntireByteRange = true;
      }
    } finally {
      if ((clientTraceFmt != null) && ClientTraceLog.isDebugEnabled()) {
        final long endTime = System.nanoTime();
        ClientTraceLog.debug(String.format(clientTraceFmt, totalRead,
            initialOffset, endTime - startTime));
      }
      // 调用close()文件关闭数据块文件、 校验文件以及回收操作系统缓冲区
      close();
    }
    return totalRead;
  }

  /**
   *
   * Manage the OS buffer cache by performing read-ahead and drop-behind.
   */
  private void manageOsCache() throws IOException {
    // We can't manage the cache for this block if we don't have a file
    // descriptor to work with.
    if (ris.getDataInFd() == null) {
      return;
    }

    //按条件触发预读取操作
    // Perform readahead if necessary
    if ((readaheadLength > 0) && (datanode.readaheadPool != null) &&
          (alwaysReadahead || isLongRead())) {

      //满足预读取条件， 则调用ReadaheadPool.readaheadStream()方法触发预读取
      curReadahead = datanode.readaheadPool.readaheadStream(
          clientTraceFmt, ris.getDataInFd(), offset, readaheadLength,
          Long.MAX_VALUE, curReadahead);
    }

    //丢弃刚才从缓存中读取的数据， 因为不再需要使用这些数据了
    // Drop what we've just read from cache, since we aren't likely to need it again
    if (dropCacheBehindAllReads ||
        (dropCacheBehindLargeReads && isLongRead())) {
      //丢弃数据的位置
      long nextCacheDropOffset = lastCacheDropOffset + CACHE_DROP_INTERVAL_BYTES;
      if (offset >= nextCacheDropOffset) {
        //如果下一次读取数据的位置大于丢弃数据的位置， 则将读取数据位置前的数据全部丢弃
        long dropLength = offset - lastCacheDropOffset;
        ris.dropCacheBehindReads(block.getBlockName(), lastCacheDropOffset,
            dropLength, POSIX_FADV_DONTNEED);
        lastCacheDropOffset = offset;
      }
    }
  }

  /**
   * Returns true if we have done a long enough read for this block to qualify
   * for the DataNode-wide cache management defaults.  We avoid applying the
   * cache management defaults to smaller reads because the overhead would be
   * too high.
   *
   * Note that if the client explicitly asked for dropBehind, we will do it
   * even on short reads.
   * 
   * This is also used to determine when to invoke
   * posix_fadvise(POSIX_FADV_SEQUENTIAL).
   */
  private boolean isLongRead() {
    return (endOffset - initialOffset) > LONG_READ_THRESHOLD_BYTES;
  }

  /**
   * Write packet header into {@code pkt},
   * return the length of the header written.
   */
  private int writePacketHeader(ByteBuffer pkt, int dataLen, int packetLen) {
    pkt.clear();
    // both syncBlock and syncPacket are false
    PacketHeader header = new PacketHeader(packetLen, offset, seqno,
        (dataLen == 0), dataLen, false);
    
    int size = header.getSerializedSize();
    pkt.position(PacketHeader.PKT_MAX_HEADER_LEN - size);
    header.putInBuffer(pkt);
    return size;
  }
  
  boolean didSendEntireByteRange() {
    return sentEntireByteRange;
  }

  /**
   * @return the checksum type that will be used with this block transfer.
   */
  DataChecksum getChecksum() {
    return checksum;
  }

  /**
   * @return the offset into the block file where the sender is currently
   * reading.
   */
  long getOffset() {
    return offset;
  }
}
