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
import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.POSIX;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Unsafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.primitives.Ints;

import javax.annotation.Nonnull;

/**
 *
 * ShortCircuitShm类用来抽象短路读取时用到的一段共享内存，
 * 这段共享内存中会有多个槽位，
 * 每个槽位都保存了一个短路读副本的信息。
 *
 * A shared memory segment used to implement short-circuit reads.
 */
public class ShortCircuitShm {
  private static final Logger LOG = LoggerFactory.getLogger(
      ShortCircuitShm.class);

  protected static final int BYTES_PER_SLOT = 64;

  private static final Unsafe unsafe = safetyDance();

  private static Unsafe safetyDance() {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      return (Unsafe)f.get(null);
    } catch (Throwable e) {
      LOG.error("failed to load misc.Unsafe", e);
    }
    return null;
  }

  /**
   * Calculate the usable size of a shared memory segment.
   * We round down to a multiple of the slot size and do some validation.
   *
   * @param stream The stream we're using.
   * @return       The usable size of the shared memory segment.
   */
  private static int getUsableLength(FileInputStream stream)
      throws IOException {
    int intSize = Ints.checkedCast(stream.getChannel().size());
    int slots = intSize / BYTES_PER_SLOT;
    if (slots == 0) {
      throw new IOException("size of shared memory segment was " +
          intSize + ", but that is not enough to hold even one slot.");
    }
    return slots * BYTES_PER_SLOT;
  }

  /**
   * Identifies a DfsClientShm.
   */
  public static class ShmId implements Comparable<ShmId> {
    private static final Random random = new Random();
    private final long hi;
    private final long lo;

    /**
     * Generate a random ShmId.
     *
     * We generate ShmIds randomly to prevent a malicious client from
     * successfully guessing one and using that to interfere with another
     * client.
     */
    public static ShmId createRandom() {
      return new ShmId(random.nextLong(), random.nextLong());
    }

    public ShmId(long hi, long lo) {
      this.hi = hi;
      this.lo = lo;
    }

    public long getHi() {
      return hi;
    }

    public long getLo() {
      return lo;
    }

    @Override
    public boolean equals(Object o) {
      if ((o == null) || (o.getClass() != this.getClass())) {
        return false;
      }
      ShmId other = (ShmId)o;
      return new EqualsBuilder().
          append(hi, other.hi).
          append(lo, other.lo).
          isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().
          append(this.hi).
          append(this.lo).
          toHashCode();
    }

    @Override
    public String toString() {
      return String.format("%016x%016x", hi, lo);
    }

    @Override
    public int compareTo(@Nonnull ShmId other) {
      return ComparisonChain.start().
          compare(hi, other.hi).
          compare(lo, other.lo).
          result();
    }
  }

  /**
   * 用来唯一标识一个Slot。
   *
   * Uniquely identifies a slot.
   */
  public static class SlotId {
    private final ShmId shmId;
    private final int slotIdx;

    public SlotId(ShmId shmId, int slotIdx) {
      this.shmId = shmId;
      this.slotIdx = slotIdx;
    }

    public ShmId getShmId() {
      return shmId;
    }

    public int getSlotIdx() {
      return slotIdx;
    }

    @Override
    public boolean equals(Object o) {
      if ((o == null) || (o.getClass() != this.getClass())) {
        return false;
      }
      SlotId other = (SlotId)o;
      return new EqualsBuilder().
          append(shmId, other.shmId).
          append(slotIdx, other.slotIdx).
          isEquals();
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().
          append(this.shmId).
          append(this.slotIdx).
          toHashCode();
    }

    @Override
    public String toString() {
      return String.format("SlotId(%s:%d)", shmId.toString(), slotIdx);
    }
  }

  /**
   * 迭代器类型， 用来遍历当前ShortCircuitShm中保存的所有Slot对象。
   */
  public class SlotIterator implements Iterator<Slot> {
    int slotIdx = -1;

    @Override
    public boolean hasNext() {
      synchronized (ShortCircuitShm.this) {
        return allocatedSlots.nextSetBit(slotIdx + 1) != -1;
      }
    }

    @Override
    public Slot next() {
      synchronized (ShortCircuitShm.this) {
        int nextSlotIdx = allocatedSlots.nextSetBit(slotIdx + 1);
        if (nextSlotIdx == -1) {
          throw new NoSuchElementException();
        }
        slotIdx = nextSlotIdx;
        return slots[nextSlotIdx];
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("SlotIterator " +
          "doesn't support removal");
    }
  }

  /**
   * Slot类是ShortCircuitShm中最重要的一个内部类，
   * 它抽象了共享内存中的一个槽位，
   * 并提供了操作内存映射文件对应槽位数据的方法。
   *
   * A slot containing information about a replica.
   *
   * The format is:
   * word 0
   *   bit 0:32   Slot flags (see below).
   *   bit 33:63  Anchor count.
   * word 1:7
   *   Reserved for future use, such as statistics.
   *   Padding is also useful for avoiding false sharing.
   *
   * Little-endian versus big-endian is not relevant here since both the client
   * and the server reside on the same computer and use the same orientation.
   */
  public class Slot {
    /**
     *
     *
     * VALID_FLAG——用来标识当前Slot是否有效。 DFSClient在共享内存中新创建一
     * 个Slot时， 会设置这个标志位。 当这个Slot对应的副本不再有效时， Datanode取
     * 消这个标志位。 同时， 当Client认为Datanode不再使用这个Slot通信时， 也会取消
     * 这个标志位。
     *
     * Flag indicating that the slot is valid.
     *
     * The DFSClient sets this flag when it allocates a new slot within one of
     * its shared memory regions.
     *
     * The DataNode clears this flag when the replica associated with this slot
     * is no longer valid.  The client itself also clears this flag when it
     * believes that the DataNode is no longer using this slot to communicate.
     */
    private static final long VALID_FLAG =          1L<<63;

    /**
     *
     * ANCHORABLE_FLAG——当Datanode将Slot对应的副本通过mlock操作缓存到内
     * 存中时， 会设置这个标志位。 当这个标志位被设置时， DFSClient的
     * BlockReaderLocal读取Slot对应的副本时不再需要校验， 并且客户端可以进行零
     * 拷贝读取。 每当客户端进行这样的读取操作时， 都需要在Slot上添加一个锚计
     * 数， 只有当Slot没有引用的锚， 也就是锚计数为零时， Datanode才可以从缓存中
     * 移出这个数据块。
     *
     *
     * Flag indicating that the slot can be anchored.
     */
    private static final long ANCHORABLE_FLAG =     1L<<62;

    /**
     * 保存当前Slot在共享内存中的地址
     * The slot address in memory.
     */
    private final long slotAddress;

    /**
     * 当前 Slot对应的短路读数据块id。
     *
     * BlockId of the block this slot is used for.
     */
    private final ExtendedBlockId blockId;

    Slot(long slotAddress, ExtendedBlockId blockId) {
      this.slotAddress = slotAddress;
      this.blockId = blockId;
    }

    /**
     * Get the short-circuit memory segment associated with this Slot.
     *
     * @return      The enclosing short-circuit memory segment.
     */
    public ShortCircuitShm getShm() {
      return ShortCircuitShm.this;
    }

    /**
     * Get the ExtendedBlockId associated with this slot.
     *
     * @return      The ExtendedBlockId of this slot.
     */
    public ExtendedBlockId getBlockId() {
      return blockId;
    }

    /**
     * Get the SlotId of this slot, containing both shmId and slotIdx.
     *
     * @return      The SlotId of this slot.
     */
    public SlotId getSlotId() {
      return new SlotId(getShmId(), getSlotIdx());
    }

    /**
     * Get the Slot index.
     *
     * @return      The index of this slot.
     */
    public int getSlotIdx() {
      return Ints.checkedCast(
          (slotAddress - baseAddress) / BYTES_PER_SLOT);
    }

    /**
     * Clear the slot.
     */
    void clear() {
      unsafe.putLongVolatile(null, this.slotAddress, 0);
    }

    private boolean isSet(long flag) {
      long prev = unsafe.getLongVolatile(null, this.slotAddress);
      return (prev & flag) != 0;
    }

    private void setFlag(long flag) {
      long prev;
      do {
        //这里通过调用native方法， 从内存中获取这8字节
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        //进行与操作， 如果结果不为空， 则证明标志位已经设置了
        if ((prev & flag) != 0) {
          return;
        }
        //否则， 就将当前值与flag进行或操作， 也就添加了标志位
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev | flag));
    }

    private void clearFlag(long flag) {
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        if ((prev & flag) == 0) {
          return;
        }
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev & (~flag)));
    }

    public boolean isValid() {
      return isSet(VALID_FLAG);
    }

    /**
     * makeValid()方法是在Slot对象创建时调用的， 也就是Slot对象在创建之
     * 后就是有效的。
     *
     */
    public void makeValid() {
      setFlag(VALID_FLAG);
    }

    public void makeInvalid() {
      clearFlag(VALID_FLAG);
    }

    public boolean isAnchorable() {
      return isSet(ANCHORABLE_FLAG);
    }

    /**
     * makeAnchorable()方法则是在数据块被成功放入缓存时， 也就是
     * CachingTask成功调用mlock()系统调用后执行的， 可锚（Anchorable） 槽位对应的数据块
     * 在读取时是不需要校验的， 也就是在本地读取该数据块时不需要执行校验操作， 同时客户
     * 端可以使用零拷贝模式读取这个数据块。
     *
     */
    public void makeAnchorable() {
      setFlag(ANCHORABLE_FLAG);
    }

    public void makeUnanchorable() {
      clearFlag(ANCHORABLE_FLAG);
    }

    public boolean isAnchored() {
      long prev = unsafe.getLongVolatile(null, this.slotAddress);
      // Slot is no longer valid.
      return (prev & VALID_FLAG) != 0 && ((prev & 0x7fffffff) != 0);
    }

    /**
     * 当客户端通过免校
     * 验模式（缓存中的数据块不需要校验） 或者零拷贝模式读取数据块时， 会调用
     * addAnchor()方法增加这个Slot的锚次数， 表明当前数据块正在被引用。 只有当Slot的锚次
     * 数为零时， Datanode才可以将该Slot对应的数据块从缓存中移出
     *
     * addAnchor()方法首先判断Slot是否有效并且可锚， 然后判断是否有太
     * 多线程锚定了这个Slot， 如果不是则增加Slot对象的锚次数
     *
     *
     * Try to add an anchor for a given slot.
     *
     * When a slot is anchored, we know that the block it refers to is resident
     * in memory.
     *
     * @return          True if the slot is anchored.
     */
    public boolean addAnchor() {
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        if ((prev & VALID_FLAG) == 0) {
          // Slot is no longer valid.
          //Slot不是有效的， 直接返回false
          return false;
        }
        if ((prev & ANCHORABLE_FLAG) == 0) {
          //Slot并不是可锚的， 直接返回false
          // Slot can't be anchored right now.
          return false;
        }

        if ((prev & 0x7fffffff) == 0x7fffffff) {
          //有太多线程锚定了这个Slot
          // Too many other threads have anchored the slot (2 billion?)
          return false;
        }

        //否则， 增加当前Slot的锚次数
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev + 1));
      return true;
    }

    /**
     * Remove an anchor for a given slot.
     */
    public void removeAnchor() {
      long prev;
      do {
        prev = unsafe.getLongVolatile(null, this.slotAddress);
        Preconditions.checkState((prev & 0x7fffffff) != 0,
            "Tried to remove anchor for slot " + slotAddress +", which was " +
            "not anchored.");
      } while (!unsafe.compareAndSwapLong(null, this.slotAddress,
                  prev, prev - 1));
    }

    @Override
    public String toString() {
      return "Slot(slotIdx=" + getSlotIdx() + ", shm=" + getShm() + ")";
    }
  }

  /**
   *
   * 用来唯一标识一个ShortCircuitShm， 有两个long类型的id字段， 是随机产生的
   *
   * ID for this SharedMemorySegment.
   */
  private final ShmId shmId;

  /**
   * 内存映射文件的起始地址
   *
   * The base address of the memory-mapped file.
   */
  private final long baseAddress;

  /**
   * 内存映射文件的长度。
   *
   * The mmapped length of the shared memory segment
   */
  private final int mmappedLength;

  /**
   *
   * 用于描述共享内存中的一个槽位， 每个槽位都用来追踪一个短路数据块的
   * 状态。 由于是在共享内存中， 这个状态可以由Datanode进程修改， 也可以由
   * DFSClient修改
   *
   * 用来保存共享内存中所有的Slot对象
   *
   *
   * The slots associated with this shared memory segment.
   * slot[i] contains the slot at offset i * BYTES_PER_SLOT,
   * or null if that slot is not allocated.
   */
  private final Slot slots[];

  /**
   * 一个bitmap用来标识slots数组中哪些位置被占用。 这里积累
   * BitSet的使用。
   *
   * A bitset where each bit represents a slot which is in use.
   */
  private final BitSet allocatedSlots;

  /**
   * ShortCircuitShm实现共享内存是通过mmap映射文件的方式， 客户端和Datanode会打
   * 开同一个文件， 然后进行内存映射以达到共享内存的目的。
   *
   *
   * 它会首先打开共享文件， 执行mmap内存映射操作， 然后根
   * 据共享文件的大小计算出共享内存中可以有多少个Slot， 最后构造slots字段以及
   * allocatedSlots字段
   *
   *
   * Create the ShortCircuitShm.
   *
   * @param shmId       The ID to use.
   * @param stream      The stream that we're going to use to create this
   *                    shared memory segment.
   *
   *                    Although this is a FileInputStream, we are going to
   *                    assume that the underlying file descriptor is writable
   *                    as well as readable. It would be more appropriate to use
   *                    a RandomAccessFile here, but that class does not have
   *                    any public accessor which returns a FileDescriptor,
   *                    unlike FileInputStream.
   */
  public ShortCircuitShm(ShmId shmId, FileInputStream stream)
        throws IOException {
    if (!NativeIO.isAvailable()) {
      throw new UnsupportedOperationException("NativeIO is not available.");
    }
    if (Shell.WINDOWS) {
      throw new UnsupportedOperationException(
          "DfsClientShm is not yet implemented for Windows.");
    }
    if (unsafe == null) {
      throw new UnsupportedOperationException(
          "can't use DfsClientShm because we failed to " +
          "load misc.Unsafe.");
    }
    this.shmId = shmId;
    this.mmappedLength = getUsableLength(stream);

    //可以看到， 这里实现共享内存是通过mmap方式， 打开同一个文件， 并进行内存映射
    this.baseAddress = POSIX.mmap(stream.getFD(),
        POSIX.MMAP_PROT_READ | POSIX.MMAP_PROT_WRITE, true, mmappedLength);

    //每个Slot是8字节， 这里通过文件的大小计算出当前共享内存中有多少个Slot
    this.slots = new Slot[mmappedLength / BYTES_PER_SLOT];

    this.allocatedSlots = new BitSet(slots.length);
    LOG.trace("creating {}(shmId={}, mmappedLength={}, baseAddress={}, "
        + "slots.length={})", this.getClass().getSimpleName(), shmId,
        mmappedLength, String.format("%x", baseAddress), slots.length);
  }

  public final ShmId getShmId() {
    return shmId;
  }

  /**
   * Determine if this shared memory object is empty.
   *
   * @return    True if the shared memory object is empty.
   */
  synchronized final public boolean isEmpty() {
    return allocatedSlots.nextSetBit(0) == -1;
  }

  /**
   * Determine if this shared memory object is full.
   *
   * @return    True if the shared memory object is full.
   */
  synchronized final public boolean isFull() {
    return allocatedSlots.nextClearBit(0) >= slots.length;
  }

  /**
   * Calculate the base address of a slot.
   *
   * @param slotIdx   Index of the slot.
   * @return          The base address of the slot.
   */
  private long calculateSlotAddress(int slotIdx) {
    long offset = slotIdx;
    offset *= BYTES_PER_SLOT;
    return this.baseAddress + offset;
  }

  /**
   *
   * 当前共享内存中注册一个Slot对象，
   * allocAndRegisterSlot()方法首先从slots数组中获取一个空闲的位置，
   * 然后构造Slot对象， 将这个Slot对象放入slots数组中， 最后返回新构造的Slot对象。
   *
   * Allocate a new slot and register it.
   *
   * This function chooses an empty slot, initializes it, and then returns
   * the relevant Slot object.
   *
   * @return    The new slot.
   */
  synchronized public final Slot allocAndRegisterSlot(
      ExtendedBlockId blockId) {

    //获取第一个空闲的位置， 这里使用了BitSet（注意积累BitSet的使用）
    int idx = allocatedSlots.nextClearBit(0);
    if (idx >= slots.length) {
      throw new RuntimeException(this + ": no more slots are available.");
    }
    allocatedSlots.set(idx, true);

    //构造Slot对象， 然后放入slots字段中
    Slot slot = new Slot(calculateSlotAddress(idx), blockId);
    slot.clear();
    slot.makeValid();
    slots[idx] = slot;
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + ": allocAndRegisterSlot " + idx + ": allocatedSlots=" + allocatedSlots +
                  StringUtils.getStackTrace(Thread.currentThread()));
    }
    return slot;
  }

  synchronized public final Slot getSlot(int slotIdx)
      throws InvalidRequestException {
    if (!allocatedSlots.get(slotIdx)) {
      throw new InvalidRequestException(this + ": slot " + slotIdx +
          " does not exist.");
    }
    return slots[slotIdx];
  }

  /**
   * Register a slot.
   *
   * This function looks at a slot which has already been initialized (by
   * another process), and registers it with us.  Then, it returns the
   * relevant Slot object.
   *
   * @return    The slot.
   *
   * @throws InvalidRequestException
   *            If the slot index we're trying to allocate has not been
   *            initialized, or is already in use.
   */
  synchronized public final Slot registerSlot(int slotIdx,
      ExtendedBlockId blockId) throws InvalidRequestException {
    if (slotIdx < 0) {
      throw new InvalidRequestException(this + ": invalid negative slot " +
          "index " + slotIdx);
    }
    if (slotIdx >= slots.length) {
      throw new InvalidRequestException(this + ": invalid slot " +
          "index " + slotIdx);
    }
    if (allocatedSlots.get(slotIdx)) {
      throw new InvalidRequestException(this + ": slot " + slotIdx +
          " is already in use.");
    }
    Slot slot = new Slot(calculateSlotAddress(slotIdx), blockId);
    if (!slot.isValid()) {
      throw new InvalidRequestException(this + ": slot " + slotIdx +
          " is not marked as valid.");
    }
    slots[slotIdx] = slot;
    allocatedSlots.set(slotIdx, true);
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + ": registerSlot " + slotIdx + ": allocatedSlots=" + allocatedSlots +
                  StringUtils.getStackTrace(Thread.currentThread()));
    }
    return slot;
  }

  /**
   *
   * 从ShortCircuitShm移除一个Slot对象
   * Unregisters a slot.
   *
   * This doesn't alter the contents of the slot.  It just means
   *
   * @param slotIdx  Index of the slot to unregister.
   */
  synchronized public final void unregisterSlot(int slotIdx) {
    Preconditions.checkState(allocatedSlots.get(slotIdx),
        "tried to unregister slot " + slotIdx + ", which was not registered.");
    allocatedSlots.set(slotIdx, false);

    //将slots数组中对应的槽位设置为null
    slots[slotIdx] = null;
    LOG.trace("{}: unregisterSlot {}", this, slotIdx);
  }

  /**
   * Iterate over all allocated slots.
   *
   * Note that this method isn't safe if
   *
   * @return        The slot iterator.
   */
  public SlotIterator slotIterator() {
    return new SlotIterator();
  }

  public void free() {
    try {

      // 将共享内存释放
      POSIX.munmap(baseAddress, mmappedLength);
    } catch (IOException e) {
      LOG.warn(this + ": failed to munmap", e);
    }
    LOG.trace(this + ": freed");
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + shmId + ")";
  }
}
