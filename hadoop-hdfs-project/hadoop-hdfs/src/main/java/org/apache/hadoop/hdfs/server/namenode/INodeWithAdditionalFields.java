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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.LongBitFormat;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

import com.google.common.base.Preconditions;

/**
 * {@link INode} with additional fields including id, name, permission,
 * access time and modification time.
 */
@InterfaceAudience.Private
public abstract class INodeWithAdditionalFields extends INode
    implements LinkedElement {


  // 枚举类PermissionStatusFormat就是用来解析以及处理permission字段的工具类，
  // 提供了获取permission字段中mode、group、user部分
  // 对应的文件模式、用户组名以及用户名的方法。


  // Note: this format is used both in-memory and on-disk.  Changes will be
  // incompatible.

  enum PermissionStatusFormat implements LongBitFormat.Enum {
    MODE(null, 16),
    GROUP(MODE.BITS, 24),
    USER(GROUP.BITS, 24);

    final LongBitFormat BITS;

    private PermissionStatusFormat(LongBitFormat previous, int length) {
      BITS = new LongBitFormat(name(), previous, length, 0);
    }

    //提取最后24个比特的user信息，并通过SerialNumberManager获取用户名
    static String getUser(long permission) {
      //首先获取permission中最后24个比特的用户标识
      final int n = (int)USER.BITS.retrieve(permission);
      //通过SerialNumberManager类获取用户标识对应的用户名
      String s = SerialNumberManager.USER.getString(n);
      assert s != null;
      return s;
    }

    //提取中间24个比特的group信息，并通过SerialNumberManager获取用户组名
    static String getGroup(long permission) {

      //首先获取permission中间24个比特的用户组标识
      final int n = (int)GROUP.BITS.retrieve(permission);
      //使用SerialNumberManager获取用户组标识对应的用户组名
      return SerialNumberManager.GROUP.getString(n);
    }

    //提取前16个比特的mode信息
    static short getMode(long permission) {

      return (short)MODE.BITS.retrieve(permission);
    }

    /**
     * 将一个PermissionStatus类，转换成long类型的permission信息
     *
     * Encode the {@link PermissionStatus} to a long. */
    static long toLong(PermissionStatus ps) {
      long permission = 0L;
      final int user = SerialNumberManager.USER.getSerialNumber(
          ps.getUserName());
      assert user != 0;
      permission = USER.BITS.combine(user, permission);
      // ideally should assert on group but inodes are created with null
      // group and then updated only when added to a directory.
      final int group = SerialNumberManager.GROUP.getSerialNumber(
          ps.getGroupName());
      permission = GROUP.BITS.combine(group, permission);
      final int mode = ps.getPermission().toShort();
      permission = MODE.BITS.combine(mode, permission);
      return permission;
    }

    static PermissionStatus toPermissionStatus(long id,
        SerialNumberManager.StringTable stringTable) {
      int uid = (int)USER.BITS.retrieve(id);
      int gid = (int)GROUP.BITS.retrieve(id);
      return new PermissionStatus(
          SerialNumberManager.USER.getString(uid, stringTable),
          SerialNumberManager.GROUP.getString(gid, stringTable),
          new FsPermission(getMode(id)));
    }

    @Override
    public int getLength() {
      return BITS.getLength();
    }
  }

  /** The inode id. */
  final private long id;
  /**
   *  The inode name is in java UTF8 encoding; 
   *  The name in HdfsFileStatus should keep the same encoding as this.
   *  if this encoding is changed, implicitly getFileInfo and listStatus in
   *  clientProtocol are changed; The decoding at the client
   *  side should change accordingly.
   */
  private byte[] name = null;
  /**
   *
   *
   * Permission encoded using {@link PermissionStatusFormat}.
   * Codes other than {@link #clonePermissionStatus(INodeWithAdditionalFields)}
   * and {@link #updatePermissionStatus(PermissionStatusFormat, long)}
   * should not modify it.
   *
   * permission字段主要包括3个部分的信息:用户信息、用户组信息和权限信息。
   *
   * permission字段是long类型的，
   *
   * 其中前16个比特用来存放文件模式标识(mode，类似 于Linux中的777)
   * 中间25个比特用来存放用户组标识(group)，
   * 最后23个比特用来存 放用户名标识(user)
   *
   *
   */
  private long permission = 0L;
  /** The last modification time*/
  private long modificationTime = 0L;
  /** The last access time*/
  private long accessTime = 0L;

  /** For implementing {@link LinkedElement}. */
  private LinkedElement next = null;
  /** An array {@link Feature}s. */
  private static final Feature[] EMPTY_FEATURE = new Feature[0];


  //保存当前 INode拥有哪些特性的字段，它是一个Feature类型的数组，默认值是空数组
  protected Feature[] features = EMPTY_FEATURE;


  private INodeWithAdditionalFields(INode parent, long id, byte[] name,
      long permission, long modificationTime, long accessTime) {
    super(parent);
    this.id = id;
    this.name = name;
    this.permission = permission;
    this.modificationTime = modificationTime;
    this.accessTime = accessTime;
  }

  INodeWithAdditionalFields(long id, byte[] name, PermissionStatus permissions,
      long modificationTime, long accessTime) {
    this(null, id, name, PermissionStatusFormat.toLong(permissions),
        modificationTime, accessTime);
  }
  
  /** @param other Other node to be copied */
  INodeWithAdditionalFields(INodeWithAdditionalFields other) {
    this(other.getParentReference() != null ? other.getParentReference()
        : other.getParent(), other.getId(), other.getLocalNameBytes(),
        other.permission, other.modificationTime, other.accessTime);
  }

  @Override
  public void setNext(LinkedElement next) {
    this.next = next;
  }
  
  @Override
  public LinkedElement getNext() {
    return next;
  }

  /** Get inode id */
  @Override
  public final long getId() {
    return this.id;
  }

  @Override
  public final byte[] getLocalNameBytes() {
    return name;
  }
  
  @Override
  public final void setLocalName(byte[] name) {
    this.name = name;
  }

  /** Clone the {@link PermissionStatus}. */
  final void clonePermissionStatus(INodeWithAdditionalFields that) {
    this.permission = that.permission;
  }

  @Override
  final PermissionStatus getPermissionStatus(int snapshotId) {
    return new PermissionStatus(getUserName(snapshotId), getGroupName(snapshotId),
        getFsPermission(snapshotId));
  }

  private final void updatePermissionStatus(PermissionStatusFormat f, long n) {
    this.permission = f.BITS.combine(n, permission);
  }

  @Override
  final String getUserName(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getUserName();
    }
    return PermissionStatusFormat.getUser(permission);
  }

  @Override
  final void setUser(String user) {
    int n = SerialNumberManager.USER.getSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }

  @Override
  final String getGroupName(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getGroupName();
    }
    return PermissionStatusFormat.getGroup(permission);
  }

  @Override
  final void setGroup(String group) {
    int n = SerialNumberManager.GROUP.getSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }

  @Override
  final FsPermission getFsPermission(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getFsPermission();
    }

    return new FsPermission(getFsPermissionShort());
  }

  @Override
  public final short getFsPermissionShort() {
    return PermissionStatusFormat.getMode(permission);
  }
  @Override
  void setPermission(FsPermission permission) {
    final short mode = permission.toShort();
    updatePermissionStatus(PermissionStatusFormat.MODE, mode);
  }

  @Override
  public long getPermissionLong() {
    return permission;
  }

  @Override
  public final AclFeature getAclFeature(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getAclFeature();
    }

    return getFeature(AclFeature.class);
  }

  @Override
  final long getModificationTime(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getModificationTime();
    }

    return this.modificationTime;
  }


  /** Update modification time if it is larger than the current value. */
  @Override
  public final INode updateModificationTime(long mtime, int latestSnapshotId) {
    Preconditions.checkState(isDirectory());
    if (mtime <= modificationTime) {
      return this;
    }
    return setModificationTime(mtime, latestSnapshotId);
  }

  final void cloneModificationTime(INodeWithAdditionalFields that) {
    this.modificationTime = that.modificationTime;
  }

  @Override
  public final void setModificationTime(long modificationTime) {
    this.modificationTime = modificationTime;
  }

  @Override
  final long getAccessTime(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getAccessTime();
    }
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  @Override
  public final void setAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }

  /**
   * addFeature()方法向features数组中添加了一个新的Feature元 素
   * @param f
   */
  protected void addFeature(Feature f) {
    int size = features.length;
    //申请一个更大的数组
    Feature[] arr = new Feature[size + 1];
    if (size != 0) {
      System.arraycopy(features, 0, arr, 0, size);
    }
    //将新的Feature对象添加到数组中
    arr[size] = f;
    //用新数组替换features引用
    features = arr;
  }

  protected void removeFeature(Feature f) {
    int size = features.length;
    if (size == 0) {
      throwFeatureNotFoundException(f);
    }

    if (size == 1) {
      if (features[0] != f) {
        throwFeatureNotFoundException(f);
      }
      features = EMPTY_FEATURE;
      return;
    }

    Feature[] arr = new Feature[size - 1];
    int j = 0;
    boolean overflow = false;
    for (Feature f1 : features) {
      if (f1 != f) {
        if (j == size - 1) {
          overflow = true;
          break;
        } else {
          arr[j++] = f1;
        }
      }
    }

    if (overflow || j != size - 1) {
      throwFeatureNotFoundException(f);
    }
    features = arr;
  }

  private void throwFeatureNotFoundException(Feature f) {
    throw new IllegalStateException(
        "Feature " + f.getClass().getSimpleName() + " not found.");
  }

  protected <T extends Feature> T getFeature(Class<? extends Feature> clazz) {
    Preconditions.checkArgument(clazz != null);
    final int size = features.length;
    for (int i=0; i < size; i++) {
      Feature f = features[i];
      if (clazz.isAssignableFrom(f.getClass())) {
        @SuppressWarnings("unchecked")
        T ret = (T) f;
        return ret;
      }
    }
    return null;
  }

  public void removeAclFeature() {
    AclFeature f = getAclFeature();
    Preconditions.checkNotNull(f);
    removeFeature(f);
    AclStorage.removeAclFeature(f);
  }

  public void addAclFeature(AclFeature f) {
    AclFeature f1 = getAclFeature();
    if (f1 != null)
      throw new IllegalStateException("Duplicated ACLFeature");

    addFeature(AclStorage.addAclFeature(f));
  }
  
  @Override
  XAttrFeature getXAttrFeature(int snapshotId) {
    if (snapshotId != Snapshot.CURRENT_STATE_ID) {
      return getSnapshotINode(snapshotId).getXAttrFeature();
    }

    return getFeature(XAttrFeature.class);
  }
  
  @Override
  public void removeXAttrFeature() {
    XAttrFeature f = getXAttrFeature();
    Preconditions.checkNotNull(f);
    removeFeature(f);
  }
  
  @Override
  public void addXAttrFeature(XAttrFeature f) {
    XAttrFeature f1 = getXAttrFeature();
    Preconditions.checkState(f1 == null, "Duplicated XAttrFeature");
    
    addFeature(f);
  }

  public final Feature[] getFeatures() {
    return features;
  }
}
