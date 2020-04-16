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
package org.apache.hadoop.hdfs.client.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf.ShortCircuitConf;
import org.apache.hadoop.hdfs.client.impl.metrics.BlockReaderIoProvider;
import org.apache.hadoop.hdfs.client.impl.metrics.BlockReaderLocalMetrics;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ClientMmap;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitReplica;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * 进行本地短路读取（ 请参考短路读取相关小节） 的
 * BlockReader。 当客户端与Datanode在同一台物理机器上时， 客户端可以直接从
 * 本地磁盘读取数据， 绕过Datanode进程， 从而提高了读取性能
 *
 *
 * BlockReaderLocal enables local short circuited reads. If the DFS client is on
 * the same machine as the datanode, then the client can read files directly
 * from the local file system rather than going through the datanode for better
 * performance. <br>
 * {@link BlockReaderLocal} works as follows:
 * <ul>
 * <li>The client performing short circuit reads must be configured at the
 * datanode.</li>
 * <li>The client gets the file descriptors for the metadata file and the data
 * file for the block using
 * {@link org.apache.hadoop.hdfs.server.datanode.DataXceiver#requestShortCircuitFds}.
 * </li>
 * <li>The client reads the file descriptors.</li>
 * </ul>
 */
@InterfaceAudience.Private
class BlockReaderLocal implements BlockReader {
  static final Logger LOG = LoggerFactory.getLogger(BlockReaderLocal.class);

  private static final DirectBufferPool bufferPool = new DirectBufferPool();

  private static BlockReaderLocalMetrics metrics;
  private static Lock metricsInitializationLock = new ReentrantLock();
  private final BlockReaderIoProvider blockReaderIoProvider;
  private static final Timer TIMER = new Timer();

  public static class Builder {
    private final int bufferSize;
    private boolean verifyChecksum;
    private int maxReadahead;
    private String filename;
    private ShortCircuitReplica replica;
    private long dataPos;
    private ExtendedBlock block;
    private StorageType storageType;
    private ShortCircuitConf shortCircuitConf;

    public Builder(ShortCircuitConf conf) {
      this.shortCircuitConf = conf;
      this.maxReadahead = Integer.MAX_VALUE;
      this.verifyChecksum = !conf.isSkipShortCircuitChecksums();
      this.bufferSize = conf.getShortCircuitBufferSize();
    }

    public Builder setVerifyChecksum(boolean verifyChecksum) {
      this.verifyChecksum = verifyChecksum;
      return this;
    }

    public Builder setCachingStrategy(CachingStrategy cachingStrategy) {
      long readahead = cachingStrategy.getReadahead() != null ?
          cachingStrategy.getReadahead() :
              HdfsClientConfigKeys.DFS_DATANODE_READAHEAD_BYTES_DEFAULT;
      this.maxReadahead = (int)Math.min(Integer.MAX_VALUE, readahead);
      return this;
    }

    public Builder setFilename(String filename) {
      this.filename = filename;
      return this;
    }

    public Builder setShortCircuitReplica(ShortCircuitReplica replica) {
      this.replica = replica;
      return this;
    }

    public Builder setStartOffset(long startOffset) {
      this.dataPos = Math.max(0, startOffset);
      return this;
    }

    public Builder setBlock(ExtendedBlock block) {
      this.block = block;
      return this;
    }

    public Builder setStorageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
    }

    public BlockReaderLocal build() {
      Preconditions.checkNotNull(replica);
      return new BlockReaderLocal(this);
    }
  }

  private boolean closed = false;

  /**
   * Pair of streams for this block.
   */
  private final ShortCircuitReplica replica;

  /**
   * The data FileChannel.
   */
  private final FileChannel dataIn;

  /**
   * The next place we'll read from in the block data FileChannel.
   *
   * If data is buffered in dataBuf, this offset will be larger than the
   * offset of the next byte which a read() operation will give us.
   */
  private long dataPos;

  /**
   * The Checksum FileChannel.
   */
  private final FileChannel checksumIn;

  /**
   * Checksum type and size.
   */
  private final DataChecksum checksum;

  /**
   * If false, we will always skip the checksum.
   */
  private final boolean verifyChecksum;

  /**
   * Name of the block, for logging purposes.
   */
  private final String filename;

  /**
   * Block ID and Block Pool ID.
   */
  private final ExtendedBlock block;

  /**
   * Cache of Checksum#bytesPerChecksum.
   */
  private final int bytesPerChecksum;

  /**
   * Cache of Checksum#checksumSize.
   */
  private final int checksumSize;

  /**
   * Maximum number of chunks to allocate.
   *
   * This is used to allocate dataBuf and checksumBuf, in the event that
   * we need them.
   */
  private final int maxAllocatedChunks;

  /**
   * True if zero readahead was requested.
   */
  private final boolean zeroReadaheadRequested;

  /**
   * Maximum amount of readahead we'll do.  This will always be at least the,
   * size of a single chunk, even if {@link #zeroReadaheadRequested} is true.
   * The reason is because we need to do a certain amount of buffering in order
   * to do checksumming.
   *
   * This determines how many bytes we'll use out of dataBuf and checksumBuf.
   * Why do we allocate buffers, and then (potentially) only use part of them?
   * The rationale is that allocating a lot of buffers of different sizes would
   * make it very difficult for the DirectBufferPool to re-use buffers.
   */
  private final int maxReadaheadLength;

  /**
   * Buffers data starting at the current dataPos and extending on
   * for dataBuf.limit().
   *
   * This may be null if we don't need it.
   */
  private ByteBuffer dataBuf;

  /**
   * Buffers checksums starting at the current checksumPos and extending on
   * for checksumBuf.limit().
   *
   * This may be null if we don't need it.
   */
  private ByteBuffer checksumBuf;

  /**
   * StorageType of replica on DataNode.
   */
  private StorageType storageType;

  private BlockReaderLocal(Builder builder) {
    this.replica = builder.replica;
    this.dataIn = replica.getDataStream().getChannel();
    this.dataPos = builder.dataPos;
    this.checksumIn = replica.getMetaStream().getChannel();
    BlockMetadataHeader header = builder.replica.getMetaHeader();
    this.checksum = header.getChecksum();
    this.verifyChecksum = builder.verifyChecksum &&
        (this.checksum.getChecksumType().id != DataChecksum.CHECKSUM_NULL);
    this.filename = builder.filename;
    this.block = builder.block;
    this.bytesPerChecksum = checksum.getBytesPerChecksum();
    this.checksumSize = checksum.getChecksumSize();

    this.maxAllocatedChunks = (bytesPerChecksum == 0) ? 0 :
        ((builder.bufferSize + bytesPerChecksum - 1) / bytesPerChecksum);
    // Calculate the effective maximum readahead.
    // We can't do more readahead than there is space in the buffer.
    int maxReadaheadChunks = (bytesPerChecksum == 0) ? 0 :
        ((Math.min(builder.bufferSize, builder.maxReadahead) +
            bytesPerChecksum - 1) / bytesPerChecksum);
    if (maxReadaheadChunks == 0) {
      this.zeroReadaheadRequested = true;
      maxReadaheadChunks = 1;
    } else {
      this.zeroReadaheadRequested = false;
    }
    this.maxReadaheadLength = maxReadaheadChunks * bytesPerChecksum;
    this.storageType = builder.storageType;

    if (builder.shortCircuitConf.isScrMetricsEnabled()) {
      metricsInitializationLock.lock();
      try {
        if (metrics == null) {
          metrics = BlockReaderLocalMetrics.create();
        }
      } finally {
        metricsInitializationLock.unlock();
      }
    }

    this.blockReaderIoProvider = new BlockReaderIoProvider(
        builder.shortCircuitConf, metrics, TIMER);
  }

  private synchronized void createDataBufIfNeeded() {
    if (dataBuf == null) {
      dataBuf = bufferPool.getBuffer(maxAllocatedChunks * bytesPerChecksum);
      dataBuf.position(0);
      dataBuf.limit(0);
    }
  }

  private synchronized void freeDataBufIfExists() {
    if (dataBuf != null) {
      // When disposing of a dataBuf, we have to move our stored file index
      // backwards.
      dataPos -= dataBuf.remaining();
      dataBuf.clear();
      bufferPool.returnBuffer(dataBuf);
      dataBuf = null;
    }
  }

  private synchronized void createChecksumBufIfNeeded() {
    if (checksumBuf == null) {
      checksumBuf = bufferPool.getBuffer(maxAllocatedChunks * checksumSize);
      checksumBuf.position(0);
      checksumBuf.limit(0);
    }
  }

  private synchronized void freeChecksumBufIfExists() {
    if (checksumBuf != null) {
      checksumBuf.clear();
      bufferPool.returnBuffer(checksumBuf);
      checksumBuf = null;
    }
  }

  //将dataBuf缓冲区中的数据拉取到buf中， 然后返回读取的字节数
  private synchronized int drainDataBuf(ByteBuffer buf) {
    if (dataBuf == null) return -1;
    int oldLimit = dataBuf.limit();
    int nRead = Math.min(dataBuf.remaining(), buf.remaining());
    if (nRead == 0) {
      return (dataBuf.remaining() == 0) ? -1 : 0;
    }
    try {
      dataBuf.limit(dataBuf.position() + nRead);
      buf.put(dataBuf);
    } finally {
      dataBuf.limit(oldLimit);
    }
    return nRead;
  }

  /**
   *
   * 将数据从输入流读入指定buf中， 并将校验和读入checksumBuf中进行校验操作
   *
   * Read from the block file into a buffer.
   *
   * This function overwrites checksumBuf.  It will increment dataPos.
   *
   * @param buf   The buffer to read into.  May be dataBuf.
   *              The position and limit of this buffer should be set to
   *              multiples of the checksum size.
   * @param canSkipChecksum  True if we can skip checksumming.
   *
   * @return      Total bytes read.  0 on EOF.
   */
  private synchronized int fillBuffer(ByteBuffer buf, boolean canSkipChecksum)
      throws IOException {
    int total = 0;
    long startDataPos = dataPos;
    int startBufPos = buf.position();
    while (buf.hasRemaining()) {
      int nRead = blockReaderIoProvider.read(dataIn, buf, dataPos);
      if (nRead < 0) {
        break;
      }
      dataPos += nRead;
      total += nRead;
    }
    if (canSkipChecksum) {
      freeChecksumBufIfExists();
      return total;
    }
    if (total > 0) {
      try {
        buf.limit(buf.position());
        buf.position(startBufPos);
        createChecksumBufIfNeeded();
        int checksumsNeeded = (total + bytesPerChecksum - 1) /
            bytesPerChecksum;
        checksumBuf.clear();
        checksumBuf.limit(checksumsNeeded * checksumSize);
        long checksumPos = BlockMetadataHeader.getHeaderSize()
            + ((startDataPos / bytesPerChecksum) * checksumSize);
        while (checksumBuf.hasRemaining()) {
          int nRead = checksumIn.read(checksumBuf, checksumPos);
          if (nRead < 0) {
            throw new IOException("Got unexpected checksum file EOF at " +
                checksumPos + ", block file position " + startDataPos +
                " for block " + block + " of file " + filename);
          }
          checksumPos += nRead;
        }
        checksumBuf.flip();

        checksum.verifyChunkedSums(buf, checksumBuf, filename, startDataPos);
      } finally {
        buf.position(buf.limit());
      }
    }
    return total;
  }


  //createNoChecksumContext()会判断如果verifyChecksum字段为false， 也就是当前配置
  //本来就不需要进行校验， 则直接返回true， 创建免校验上下文成功。 如果当前配置需要进
  //行校验， 那么尝试在Datanode和Client共享内存中副本的Slot上添加一个免校验的锚

  //当且仅当Datanode已经缓存了这个副本
  //时， 才可以添加一个锚。 因为当Datanode尝试缓存一个数据块副本时， 会验证数据块的校
  //验和， 然后通过mmap以及mlock将数据块缓存到内存中。 也就是说， 当前Datanode上缓存
  //的数据块是经过校验的、 是正确的， 不用再次进行校验


  private boolean createNoChecksumContext() {
    return !verifyChecksum ||
        // Checksums are not stored for replicas on transient storage.  We do
        // not anchor, because we do not intend for client activity to block
        // eviction from transient storage on the DataNode side.
        (storageType != null && storageType.isTransient()) ||
        replica.addNoChecksumAnchor();
  }

  private void releaseNoChecksumContext() {
    if (verifyChecksum) {
      if (storageType == null || !storageType.isTransient()) {
        replica.removeNoChecksumAnchor();
      }
    }
  }

  // read(ByteBuffer buf)方法的代码可以切分为三块：
  // ①判断能否通过createNoChecksumContext()方法创建一个免校验上下文；
  // ②如果可以免校验， 并且无预读取请求， 则调用readWithoutBounceBuffer()方法读取数据；
  // ③如果不可以免校验， 并且开启了预读取， 则调用readWithBounceBuffer()方法读取数据。
  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {

    //能否跳过数据校验
    boolean canSkipChecksum = createNoChecksumContext();
    try {
      String traceFormatStr = "read(buf.remaining={}, block={}, filename={}, "
          + "canSkipChecksum={})";
      LOG.trace(traceFormatStr + ": starting",
          buf.remaining(), block, filename, canSkipChecksum);
      int nRead;
      try {
        //可以跳过数据校验， 以及不需要预读取时， 调用readWithoutBounceBuffer()方法
        if (canSkipChecksum && zeroReadaheadRequested) {
          nRead = readWithoutBounceBuffer(buf);
        } else {
          //需要校验， 以及开启了预读取时， 调用readWithBounceBuffer()方法
          nRead = readWithBounceBuffer(buf, canSkipChecksum);
        }
      } catch (IOException e) {
        LOG.trace(traceFormatStr + ": I/O error",
            buf.remaining(), block, filename, canSkipChecksum, e);
        throw e;
      }
      LOG.trace(traceFormatStr + ": returning {}",
          buf.remaining(), block, filename, canSkipChecksum, nRead);
      return nRead;
    } finally {
      if (canSkipChecksum) releaseNoChecksumContext();
    }
  }

  //它不使用额外的数据以及校验和缓冲区预
  //读取数据以及校验和， 而是直接从数据流中将数据读取到缓冲区。
  private synchronized int readWithoutBounceBuffer(ByteBuffer buf)
      throws IOException {

    //释放dataBuffer
    freeDataBufIfExists();
    //释放checksumBuffer
    freeChecksumBufIfExists();

    int total = 0;

    //直接从输入流中将数据读取到buf
    while (buf.hasRemaining()) {
      int nRead = blockReaderIoProvider.read(dataIn, buf, dataPos);
      if (nRead <= 0) break;
      dataPos += nRead;
      total += nRead;
    }
    return (total == 0 && (dataPos == dataIn.size())) ? -1 : total;
  }

  /**
   *
   * 调用fillBuffer()方法将数据读入dataBuf缓冲区中， 将校验和读入
   * checksumBuf缓冲区中。 这里要注意， dataBuf缓冲区中的数据始终是chunk（一
   * 个校验值对应的数据长度） 的整数倍。
   *
   * Fill the data buffer.  If necessary, validate the data against the
   * checksums.
   *
   * We always want the offsets of the data contained in dataBuf to be
   * aligned to the chunk boundary.  If we are validating checksums, we
   * accomplish this by seeking backwards in the file until we're on a
   * chunk boundary.  (This is necessary because we can't checksum a
   * partial chunk.)  If we are not validating checksums, we simply only
   * fill the latter part of dataBuf.
   *
   * @param canSkipChecksum  true if we can skip checksumming.
   * @return                 true if we hit EOF.
   * @throws IOException
   */
  private synchronized boolean fillDataBuf(boolean canSkipChecksum)
      throws IOException {
    createDataBufIfNeeded();
    final int slop = (int)(dataPos % bytesPerChecksum);
    final long oldDataPos = dataPos;
    dataBuf.limit(maxReadaheadLength);
    if (canSkipChecksum) {
      dataBuf.position(slop);
      fillBuffer(dataBuf, true);
    } else {
      dataPos -= slop;
      dataBuf.position(0);
      fillBuffer(dataBuf, false);
    }
    dataBuf.limit(dataBuf.position());
    dataBuf.position(Math.min(dataBuf.position(), slop));
    LOG.trace("loaded {} bytes into bounce buffer from offset {} of {}",
        dataBuf.remaining(), oldDataPos, block);
    return dataBuf.limit() != maxReadaheadLength;
  }

  /**
   * Read using the bounce buffer.
   *
   * A 'direct' read actually has three phases. The first drains any
   * remaining bytes from the slow read buffer. After this the read is
   * guaranteed to be on a checksum chunk boundary. If there are still bytes
   * to read, the fast direct path is used for as many remaining bytes as
   * possible, up to a multiple of the checksum chunk size. Finally, any
   * 'odd' bytes remaining at the end of the read cause another slow read to
   * be issued, which involves an extra copy.
   *
   * Every 'slow' read tries to fill the slow read buffer in one go for
   * efficiency's sake. As described above, all non-checksum-chunk-aligned
   * reads will be served from the slower read path.
   *
   * @param buf              The buffer to read into.
   * @param canSkipChecksum  True if we can skip checksums.
   *
   *
   * readWithBounceBuffer()方法在BlockReaderLocal对象上申请了两个缓冲区：
   *          dataBuf，数据缓冲区；
   *          checksumBuf； 校验和缓冲区。
   *
   * dataBuf缓冲区的大小为maxReadaheadLength，
   * 这个长度始终是校验块（chunk， 一个校验值对应的数据长度） 的
   * 整数倍， 这样设计是为了进行校验操作时比较方便， 能够以校验块为单位读取数据。
   * dataBuf和checksumBuf的构造使用了direct byte buffer， 也就是堆外内存上的缓冲区
   *
   * dataBuf以及checksumBuf都是通过调用java.nio.ByteBuffer.allocateDirect()方
   * 法分配的堆外内存。 这里值得我们积累， 对于比较大的缓冲区， 可以通过调用
   * java.nio提供的方法， 将缓冲区分配在堆外， 节省宝贵的堆内存空间。
   *
   */
  private synchronized int readWithBounceBuffer(ByteBuffer buf,
        boolean canSkipChecksum) throws IOException {
    int total = 0;

    //调用drainDataBuf()， 将dataBuf缓冲区中的数据写入buf
    int bb = drainDataBuf(buf); // drain bounce buffer if possible
    if (bb >= 0) {
      total += bb;
      if (buf.remaining() == 0) return total;
    }
    boolean eof = true, done = false;
    do {

      //如果buf的空间足够大， 并且输入流游标在chunk边界上， 则直接从IO流中将数据写入buf
      if (buf.isDirect() && (buf.remaining() >= maxReadaheadLength)
            && ((dataPos % bytesPerChecksum) == 0)) {
        // Fast lane: try to read directly into user-supplied buffer, bypassing
        // bounce buffer.
        int oldLimit = buf.limit();
        int nRead;
        try {
          buf.limit(buf.position() + maxReadaheadLength);
          nRead = fillBuffer(buf, canSkipChecksum);
        } finally {
          buf.limit(oldLimit);
        }
        if (nRead < maxReadaheadLength) {
          done = true;
        }
        if (nRead > 0) {
          eof = false;
        }
        total += nRead;
      } else {

        //否则， 将数据读入dataBuf缓存
        // Slow lane: refill bounce buffer.
        if (fillDataBuf(canSkipChecksum)) {
          done = true;
        }

        //然后将dataBuf中的数据导入buf
        bb = drainDataBuf(buf); // drain bounce buffer if possible
        if (bb >= 0) {
          eof = false;
          total += bb;
        }
      }
    } while ((!done) && (buf.remaining() > 0));
    return (eof && total == 0) ? -1 : total;
  }

  @Override
  public synchronized int read(byte[] arr, int off, int len)
        throws IOException {
    boolean canSkipChecksum = createNoChecksumContext();
    int nRead;
    try {
      final String traceFormatStr = "read(arr.length={}, off={}, len={}, "
          + "filename={}, block={}, canSkipChecksum={})";
      LOG.trace(traceFormatStr + ": starting",
          arr.length, off, len, filename, block, canSkipChecksum);
      try {
        if (canSkipChecksum && zeroReadaheadRequested) {
          nRead = readWithoutBounceBuffer(arr, off, len);
        } else {
          nRead = readWithBounceBuffer(arr, off, len, canSkipChecksum);
        }
      } catch (IOException e) {
        LOG.trace(traceFormatStr + ": I/O error",
            arr.length, off, len, filename, block, canSkipChecksum, e);
        throw e;
      }
      LOG.trace(traceFormatStr + ": returning {}",
          arr.length, off, len, filename, block, canSkipChecksum, nRead);
    } finally {
      if (canSkipChecksum) releaseNoChecksumContext();
    }
    return nRead;
  }

  private synchronized int readWithoutBounceBuffer(byte arr[], int off,
        int len) throws IOException {
    freeDataBufIfExists();
    freeChecksumBufIfExists();
    int nRead = blockReaderIoProvider.read(
        dataIn, ByteBuffer.wrap(arr, off, len), dataPos);
    if (nRead > 0) {
      dataPos += nRead;
    } else if ((nRead == 0) && (dataPos == dataIn.size())) {
      return -1;
    }
    return nRead;
  }

  private synchronized int readWithBounceBuffer(byte arr[], int off, int len,
        boolean canSkipChecksum) throws IOException {
    createDataBufIfNeeded();
    if (!dataBuf.hasRemaining()) {
      dataBuf.position(0);
      dataBuf.limit(maxReadaheadLength);
      fillDataBuf(canSkipChecksum);
    }
    if (dataBuf.remaining() == 0) return -1;
    int toRead = Math.min(dataBuf.remaining(), len);
    dataBuf.get(arr, off, toRead);
    return toRead;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    int discardedFromBuf = 0;
    long remaining = n;
    if ((dataBuf != null) && dataBuf.hasRemaining()) {
      discardedFromBuf = (int)Math.min(dataBuf.remaining(), n);
      dataBuf.position(dataBuf.position() + discardedFromBuf);
      remaining -= discardedFromBuf;
    }
    LOG.trace("skip(n={}, block={}, filename={}): discarded {} bytes from "
            + "dataBuf and advanced dataPos by {}",
        n, block, filename, discardedFromBuf, remaining);
    dataPos += remaining;
    return n;
  }

  @Override
  public int available() {
    // We never do network I/O in BlockReaderLocal.
    return Integer.MAX_VALUE;
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) return;
    closed = true;
    LOG.trace("close(filename={}, block={})", filename, block);
    replica.unref();
    freeDataBufIfExists();
    freeChecksumBufIfExists();
    if (metrics != null) {
      metrics.collectThreadLocalStates();
    }
  }

  @Override
  public synchronized void readFully(byte[] arr, int off, int len)
      throws IOException {
    BlockReaderUtil.readFully(this, arr, off, len);
  }

  @Override
  public synchronized int readAll(byte[] buf, int off, int len)
      throws IOException {
    return BlockReaderUtil.readAll(this, buf, off, len);
  }

  @Override
  public boolean isShortCircuit() {
    return true;
  }

  /**
   *
   *
   * Get or create a memory map for this replica.
   *
   * There are two kinds of ClientMmap objects we could fetch here: one that
   * will always read pre-checksummed data, and one that may read data that
   * hasn't been checksummed.
   *
   * If we fetch the former, "safe" kind of ClientMmap, we have to increment
   * the anchor count on the shared memory slot.  This will tell the DataNode
   * not to munlock the block until this ClientMmap is closed.
   * If we fetch the latter, we don't bother with anchoring.
   *
   * @param opts     The options to use, such as SKIP_CHECKSUMS.
   *
   * @return         null on failure; the ClientMmap otherwise.
   */
  @Override
  public ClientMmap getClientMmap(EnumSet<ReadOption> opts) {
    boolean anchor = verifyChecksum &&
        !opts.contains(ReadOption.SKIP_CHECKSUMS);
    if (anchor) {
      if (!createNoChecksumContext()) {
        LOG.trace("can't get an mmap for {} of {} since SKIP_CHECKSUMS was not "
            + "given, we aren't skipping checksums, and the block is not "
            + "mlocked.", block, filename);
        return null;
      }
    }
    ClientMmap clientMmap = null;
    try {
      //获取映射对象
      clientMmap = replica.getOrCreateClientMmap(anchor);
    } finally {
      if ((clientMmap == null) && anchor) {
        releaseNoChecksumContext();
      }
    }
    return clientMmap;
  }

  @VisibleForTesting
  boolean getVerifyChecksum() {
    return this.verifyChecksum;
  }

  @VisibleForTesting
  int getMaxReadaheadLength() {
    return this.maxReadaheadLength;
  }

  /**
   * Make the replica anchorable.  Normally this can only be done by the
   * DataNode.  This method is only for testing.
   */
  @VisibleForTesting
  void forceAnchorable() {
    replica.getSlot().makeAnchorable();
  }

  /**
   * Make the replica unanchorable.  Normally this can only be done by the
   * DataNode.  This method is only for testing.
   */
  @VisibleForTesting
  void forceUnanchorable() {
    replica.getSlot().makeUnanchorable();
  }

  @Override
  public DataChecksum getDataChecksum() {
    return checksum;
  }

  @Override
  public int getNetworkDistance() {
    return 0;
  }
}
