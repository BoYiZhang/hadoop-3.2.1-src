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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CacheDirectiveInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.CachePoolInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.ErasureCodingPolicyProto;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.CacheManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.SecretManagerSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.StringTableSection;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.ErasureCodingSection;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FSImageFormatPBSnapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Time;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.CodedOutputStream;

/**
 * Utility class to read / write fsimage in protobuf format.
 */
@InterfaceAudience.Private
public final class FSImageFormatProtobuf {
  private static final Logger LOG = LoggerFactory
      .getLogger(FSImageFormatProtobuf.class);

  public static final class LoaderContext {
    private SerialNumberManager.StringTable stringTable;
    private final ArrayList<INodeReference> refList = Lists.newArrayList();

    public SerialNumberManager.StringTable getStringTable() {
      return stringTable;
    }

    public ArrayList<INodeReference> getRefList() {
      return refList;
    }
  }

  public static final class SaverContext {
    public static class DeduplicationMap<E> {
      private final Map<E, Integer> map = Maps.newHashMap();
      private DeduplicationMap() {}

      static <T> DeduplicationMap<T> newMap() {
        return new DeduplicationMap<T>();
      }

      int getId(E value) {
        if (value == null) {
          return 0;
        }
        Integer v = map.get(value);
        if (v == null) {
          int nv = map.size() + 1;
          map.put(value, nv);
          return nv;
        }
        return v;
      }

      int size() {
        return map.size();
      }

      Set<Entry<E, Integer>> entrySet() {
        return map.entrySet();
      }
    }
    private final ArrayList<INodeReference> refList = Lists.newArrayList();

    public ArrayList<INodeReference> getRefList() {
      return refList;
    }
  }

  public static final class Loader implements FSImageFormat.AbstractLoader {
    static final int MINIMUM_FILE_LENGTH = 8;
    private final Configuration conf;
    private final FSNamesystem fsn;
    private final LoaderContext ctx;
    /** The MD5 sum of the loaded file */
    private MD5Hash imgDigest;
    /** The transaction ID of the last edit represented by the loaded file */
    private long imgTxId;
    /**
     * Whether the image's layout version must be the same with
     * {@link HdfsServerConstants#NAMENODE_LAYOUT_VERSION}. This is only set to true
     * when we're doing (rollingUpgrade rollback).
     */
    private final boolean requireSameLayoutVersion;

    Loader(Configuration conf, FSNamesystem fsn,
        boolean requireSameLayoutVersion) {
      this.conf = conf;
      this.fsn = fsn;
      this.ctx = new LoaderContext();
      this.requireSameLayoutVersion = requireSameLayoutVersion;
    }

    @Override
    public MD5Hash getLoadedImageMd5() {
      return imgDigest;
    }

    @Override
    public long getLoadedImageTxId() {
      return imgTxId;
    }

    public LoaderContext getLoaderContext() {
      return ctx;
    }

    /**
     * Thread to compute the MD5 of a file as this can be in parallel while
     * loading the image without interfering much.
     */
    private static class DigestThread extends Thread {

      /**
       * Exception thrown when computing the digest if it cannot be calculated.
       */
      private volatile IOException ioe = null;

      /**
       * Calculated digest if there are no error.
       */
      private volatile MD5Hash digest = null;

      /**
       * FsImage file computed MD5.
       */
      private final File file;

      DigestThread(File inFile) {
        file = inFile;
        setName(inFile.getName() + " MD5 compute");
        setDaemon(true);
      }

      public MD5Hash getDigest() throws IOException {
        if (ioe != null) {
          throw ioe;
        }
        return digest;
      }

      public IOException getException() {
        return ioe;
      }

      @Override
      public void run() {
        try {
          digest = MD5FileUtils.computeMd5ForFile(file);
        } catch (IOException e) {
          ioe = e;
        } catch (Throwable t) {
          ioe = new IOException(t);
        }
      }

      @Override
      public String toString() {
        return "DigestThread{ ThreadName=" + getName() + ", digest=" + digest
            + ", file=" + file + '}';
      }
    }

    void load(File file) throws IOException {
      long start = Time.monotonicNow();
      DigestThread dt = new DigestThread(file);
      dt.start();
      RandomAccessFile raFile = new RandomAccessFile(file, "r");
      FileInputStream fin = new FileInputStream(file);
      try {

        // 加载文件
        loadInternal(raFile, fin);

        try {
          dt.join();
          imgDigest = dt.getDigest();
        } catch (InterruptedException ie) {
          throw new IOException(ie);
        }
        long end = Time.monotonicNow();
        LOG.info("Loaded FSImage in {} seconds.", (end - start) / 1000);
      } finally {
        fin.close();
        raFile.close();
      }
    }

    // loadInternal()方法会打开fsimage文件通道，
    // 然后读取fsimage文件中的FileSummary对象，
    // FileSummary对象中记录了fsimage中保存的所有section的信息。
    // loadInternal()会对FileSummary对象中保存的section排序，
    // 然后遍历每个section并调用对应的方法从fsimage文件中加载这个section。
    private void loadInternal(RandomAccessFile raFile, FileInputStream fin)
        throws IOException {
      if (!FSImageUtil.checkFileFormat(raFile)) {
        throw new IOException("Unrecognized file format");
      }

      // 从fsimage文件末尾加载FileSummary， 也就是fsimage文件内容的描述
      FileSummary summary = FSImageUtil.loadSummary(raFile);

      if (requireSameLayoutVersion && summary.getLayoutVersion() !=
          HdfsServerConstants.NAMENODE_LAYOUT_VERSION) {
        throw new IOException("Image version " + summary.getLayoutVersion() +
            " is not equal to the software version " +
            HdfsServerConstants.NAMENODE_LAYOUT_VERSION);
      }

      //获取通道
      FileChannel channel = fin.getChannel();

      // 构造FSImageFormatPBINode.Loader和FSImageFormatPBSnapshot.
      // Loader加载INode以及Snapshot
      FSImageFormatPBINode.Loader inodeLoader = new FSImageFormatPBINode.Loader(
          fsn, this);
      FSImageFormatPBSnapshot.Loader snapshotLoader = new FSImageFormatPBSnapshot.Loader(
          fsn, this);

      //对fsimage文件描述中记录的sections进行排序
      ArrayList<FileSummary.Section> sections = Lists.newArrayList(summary
          .getSectionsList());
      Collections.sort(sections, new Comparator<FileSummary.Section>() {
        @Override
        public int compare(FileSummary.Section s1, FileSummary.Section s2) {
          SectionName n1 = SectionName.fromString(s1.getName());
          SectionName n2 = SectionName.fromString(s2.getName());
          if (n1 == null) {
            return n2 == null ? 0 : -1;
          } else if (n2 == null) {
            return -1;
          } else {
            return n1.ordinal() - n2.ordinal();
          }
        }
      });



      StartupProgress prog = NameNode.getStartupProgress();
      /**
       * beginStep() and the endStep() calls do not match the boundary of the
       * sections. This is because that the current implementation only allows
       * a particular step to be started for once.
       */
      Step currentStep = null;

      //遍历每个section， 并调用对应的方法加载这个section
      for (FileSummary.Section s : sections) {

        //在通道中定位这个section的起始位置
        channel.position(s.getOffset());
        InputStream in = new BufferedInputStream(new LimitInputStream(fin,
            s.getLength()));

        in = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), in);

        String n = s.getName();

        //调用对应的方法加载不同的section
        switch (SectionName.fromString(n)) {
        case NS_INFO:
          loadNameSystemSection(in);
          break;
        case STRING_TABLE:
          loadStringTableSection(in);
          break;
        case INODE: {
          currentStep = new Step(StepType.INODES);
          prog.beginStep(Phase.LOADING_FSIMAGE, currentStep);
          inodeLoader.loadINodeSection(in, prog, currentStep);
        }
          break;
        case INODE_REFERENCE:
          snapshotLoader.loadINodeReferenceSection(in);
          break;
        case INODE_DIR:
          inodeLoader.loadINodeDirectorySection(in);
          break;
        case FILES_UNDERCONSTRUCTION:
          inodeLoader.loadFilesUnderConstructionSection(in);
          break;
        case SNAPSHOT:
          snapshotLoader.loadSnapshotSection(in);
          break;
        case SNAPSHOT_DIFF:
          snapshotLoader.loadSnapshotDiffSection(in);
          break;
        case SECRET_MANAGER: {
          prog.endStep(Phase.LOADING_FSIMAGE, currentStep);
          Step step = new Step(StepType.DELEGATION_TOKENS);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadSecretManagerSection(in, prog, step);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
        }
          break;
        case CACHE_MANAGER: {
          Step step = new Step(StepType.CACHE_POOLS);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadCacheManagerSection(in, prog, step);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
        }
          break;
        case ERASURE_CODING:
          Step step = new Step(StepType.ERASURE_CODING_POLICIES);
          prog.beginStep(Phase.LOADING_FSIMAGE, step);
          loadErasureCodingSection(in);
          prog.endStep(Phase.LOADING_FSIMAGE, step);
          break;
        default:
          LOG.warn("Unrecognized section {}", n);
          break;
        }
      }
    }

    private void loadNameSystemSection(InputStream in) throws IOException {
      NameSystemSection s = NameSystemSection.parseDelimitedFrom(in);
      BlockIdManager blockIdManager = fsn.getBlockManager().getBlockIdManager();
      blockIdManager.setLegacyGenerationStamp(s.getGenstampV1());
      blockIdManager.setGenerationStamp(s.getGenstampV2());
      blockIdManager.setLegacyGenerationStampLimit(s.getGenstampV1Limit());
      blockIdManager.setLastAllocatedContiguousBlockId(s.getLastAllocatedBlockId());
      if (s.hasLastAllocatedStripedBlockId()) {
        blockIdManager.setLastAllocatedStripedBlockId(
            s.getLastAllocatedStripedBlockId());
      }
      imgTxId = s.getTransactionId();
      if (s.hasRollingUpgradeStartTime()
          && fsn.getFSImage().hasRollbackFSImage()) {
        // we set the rollingUpgradeInfo only when we make sure we have the
        // rollback image
        fsn.setRollingUpgradeInfo(true, s.getRollingUpgradeStartTime());
      }
    }

    private void loadStringTableSection(InputStream in) throws IOException {
      StringTableSection s = StringTableSection.parseDelimitedFrom(in);
      ctx.stringTable =
          SerialNumberManager.newStringTable(s.getNumEntry(), s.getMaskBits());
      for (int i = 0; i < s.getNumEntry(); ++i) {
        StringTableSection.Entry e = StringTableSection.Entry
            .parseDelimitedFrom(in);
        ctx.stringTable.put(e.getId(), e.getStr());
      }
    }

    private void loadSecretManagerSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      SecretManagerSection s = SecretManagerSection.parseDelimitedFrom(in);
      int numKeys = s.getNumKeys(), numTokens = s.getNumTokens();
      ArrayList<SecretManagerSection.DelegationKey> keys = Lists
          .newArrayListWithCapacity(numKeys);
      ArrayList<SecretManagerSection.PersistToken> tokens = Lists
          .newArrayListWithCapacity(numTokens);

      for (int i = 0; i < numKeys; ++i)
        keys.add(SecretManagerSection.DelegationKey.parseDelimitedFrom(in));

      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numTokens);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numTokens; ++i) {
        tokens.add(SecretManagerSection.PersistToken.parseDelimitedFrom(in));
        counter.increment();
      }

      fsn.loadSecretManagerState(s, keys, tokens);
    }

    private void loadCacheManagerSection(InputStream in, StartupProgress prog,
        Step currentStep) throws IOException {
      CacheManagerSection s = CacheManagerSection.parseDelimitedFrom(in);
      int numPools = s.getNumPools();
      ArrayList<CachePoolInfoProto> pools = Lists
          .newArrayListWithCapacity(numPools);
      ArrayList<CacheDirectiveInfoProto> directives = Lists
          .newArrayListWithCapacity(s.getNumDirectives());
      prog.setTotal(Phase.LOADING_FSIMAGE, currentStep, numPools);
      Counter counter = prog.getCounter(Phase.LOADING_FSIMAGE, currentStep);
      for (int i = 0; i < numPools; ++i) {
        pools.add(CachePoolInfoProto.parseDelimitedFrom(in));
        counter.increment();
      }
      for (int i = 0; i < s.getNumDirectives(); ++i)
        directives.add(CacheDirectiveInfoProto.parseDelimitedFrom(in));
      fsn.getCacheManager().loadState(
          new CacheManager.PersistState(s, pools, directives));
    }

    private void loadErasureCodingSection(InputStream in)
        throws IOException {
      ErasureCodingSection s = ErasureCodingSection.parseDelimitedFrom(in);
      List<ErasureCodingPolicyInfo> ecPolicies = Lists
          .newArrayListWithCapacity(s.getPoliciesCount());
      for (int i = 0; i < s.getPoliciesCount(); ++i) {
        ecPolicies.add(PBHelperClient.convertErasureCodingPolicyInfo(
            s.getPolicies(i)));
      }
      fsn.getErasureCodingPolicyManager().loadPolicies(ecPolicies, conf);
    }
  }

  public static final class Saver {
    public static final int CHECK_CANCEL_INTERVAL = 4096;

    private final SaveNamespaceContext context;
    private final SaverContext saverContext;
    private long currentOffset = FSImageUtil.MAGIC_HEADER.length;
    private MD5Hash savedDigest;

    private FileChannel fileChannel;
    // OutputStream for the section data
    private OutputStream sectionOutputStream;
    private CompressionCodec codec;
    private OutputStream underlyingOutputStream;

    Saver(SaveNamespaceContext context) {
      this.context = context;
      this.saverContext = new SaverContext();
    }

    public MD5Hash getSavedDigest() {
      return savedDigest;
    }

    public SaveNamespaceContext getContext() {
      return context;
    }

    public SaverContext getSaverContext() {
      return saverContext;
    }

    public void commitSection(FileSummary.Builder summary, SectionName name)
        throws IOException {
      long oldOffset = currentOffset;
      flushSectionOutputStream();

      if (codec != null) {
        sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
      } else {
        sectionOutputStream = underlyingOutputStream;
      }
      long length = fileChannel.position() - oldOffset;
      summary.addSections(FileSummary.Section.newBuilder().setName(name.name)
          .setLength(length).setOffset(currentOffset));
      currentOffset += length;
    }

    private void flushSectionOutputStream() throws IOException {
      if (codec != null) {
        ((CompressionOutputStream) sectionOutputStream).finish();
      }
      sectionOutputStream.flush();
    }

    /**
     * save()方法会打开fsimage文件的输出流并且获得文件通道，
     * 然后调用saveInternal()方法将命名空间保存到fsimage文件中。
     *
     * @return number of non-fatal errors detected while writing the image.
     * @throws IOException on fatal error.
     */
    long save(File file, FSImageCompression compression) throws IOException {


      FileOutputStream fout = new FileOutputStream(file);

      fileChannel = fout.getChannel();

      try {
        LOG.info("Saving image file {} using {}", file, compression);
        long startTime = monotonicNow();

        // 保存到fsimage文件
        long numErrors = saveInternal( fout, compression, file.getAbsolutePath());


        LOG.info("Image file {} of size {} bytes saved in {} seconds {}.", file,
            file.length(), (monotonicNow() - startTime) / 1000,
            (numErrors > 0 ? (" with" + numErrors + " errors") : ""));
        return numErrors;
      } finally {
        fout.close();
      }
    }

    private static void saveFileSummary(OutputStream out, FileSummary summary)
        throws IOException {
      summary.writeDelimitedTo(out);
      int length = getOndiskTrunkSize(summary);
      byte[] lengthBytes = new byte[4];
      ByteBuffer.wrap(lengthBytes).asIntBuffer().put(length);
      out.write(lengthBytes);
    }

    // saveINodes()方法构造了一个FSImageFormatPBINode.Saver对象，
    // 并调用这个对象对应的方法保存文件系统目录树中的INode信息、 INodeDirectory信息，
    // 以及处于构建状态的文件信息
    private long saveInodes(FileSummary.Builder summary) throws IOException {

      FSImageFormatPBINode.Saver saver = new FSImageFormatPBINode.Saver(this,summary);

      // 保存INode信息是由FSImageFormatPBINode.Saver.serializeINodeSection()方法实现的
      saver.serializeINodeSection(sectionOutputStream);

      saver.serializeINodeDirectorySection(sectionOutputStream);

      saver.serializeFilesUCSection(sectionOutputStream);

      return saver.getNumImageErrors();
    }

    /**
     * @return number of non-fatal errors detected while saving the image.
     * @throws IOException on fatal error.
     */
    private long saveSnapshots(FileSummary.Builder summary) throws IOException {
      FSImageFormatPBSnapshot.Saver snapshotSaver = new FSImageFormatPBSnapshot.Saver(
          this, summary, context, context.getSourceNamesystem());

      snapshotSaver.serializeSnapshotSection(sectionOutputStream);
      // Skip snapshot-related sections when there is no snapshot.
      if (context.getSourceNamesystem().getSnapshotManager()
          .getNumSnapshots() > 0) {
        snapshotSaver.serializeSnapshotDiffSection(sectionOutputStream);
      }
      snapshotSaver.serializeINodeReferenceSection(sectionOutputStream);
      return snapshotSaver.getNumImageErrors();
    }

    /**
     * saveInternal()方法首先构造底层fsimage文件的输出流，
     * 构造fsimage文件的描述类 FileSummary ，
     *
     * 然后在FileSummary中记录ondiskVersion、 layoutVersion、 codec等信息。
     * 接下来saveInternal()方法依次向fsimage文件中写入
     *     1.命名空间信息、
     *     2.inode信息、
     *     3.快照信息、
     *     4.安全信息、
     *     5.缓存信息、
     *     6.StringTable
     * 信息等。
     *
     * 注意上述信息都是以section为单位写入的， 每个section的格式定义请参考fsimage.proto文件。
     * saveInternal()方法以section为单位写入元数据信息时，
     * 还会在FileSummary中记录这个section的长度，
     * 以及section在fsimage文件中的起始位置等信息。
     *
     * 当完成了所有section的写入后，
     * FileSummary对象也就构造完毕了，
     * saveInternal()最后会将
     * FileSummary对象写入fsimage文件中。
     *
     * @return number of non-fatal errors detected while writing the FsImage.
     * @throws IOException on fatal error.
     *
     */
    private long saveInternal(FileOutputStream fout,
        FSImageCompression compression, String filePath) throws IOException {


      StartupProgress prog = NameNode.getStartupProgress();

      //构造输出流， 一边写入数据， 一边写入校验值
      MessageDigest digester = MD5Hash.getDigester();

      int layoutVersion = context.getSourceNamesystem().getEffectiveLayoutVersion();

      underlyingOutputStream = new DigestOutputStream(new BufferedOutputStream(fout), digester);

      underlyingOutputStream.write(FSImageUtil.MAGIC_HEADER);

      fileChannel = fout.getChannel();

      // FileSummary为fsimage文件的描述部分， 也是protobuf定义的
      FileSummary.Builder b = FileSummary.newBuilder()
          .setOndiskVersion(FSImageUtil.FILE_VERSION)
          .setLayoutVersion(
              context.getSourceNamesystem().getEffectiveLayoutVersion());

      //获取压缩格式， 并装饰输出流
      codec = compression.getImageCodec();


      if (codec != null) {
        b.setCodec(codec.getClass().getCanonicalName());
        sectionOutputStream = codec.createOutputStream(underlyingOutputStream);
      } else {
        sectionOutputStream = underlyingOutputStream;
      }

      //保存命名空间信息
      saveNameSystemSection(b);

      // Check for cancellation right after serializing the name system section.
      // Some unit tests, such as TestSaveNamespace#testCancelSaveNameSpace
      // depends on this behavior.

      // 检查是否取消了保存操作
      context.checkCancelled();

      Step step;

      // Erasure coding policies should be saved before inodes
      if (NameNodeLayoutVersion.supports(
          NameNodeLayoutVersion.Feature.ERASURE_CODING, layoutVersion)) {

        step = new Step(StepType.ERASURE_CODING_POLICIES, filePath);
        prog.beginStep(Phase.SAVING_CHECKPOINT, step);

        //保存 ErasureCoding 信息
        saveErasureCodingSection(b);
        prog.endStep(Phase.SAVING_CHECKPOINT, step);

      }


      //保存命名空间中的inode信息
      step = new Step(StepType.INODES, filePath);

      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      // Count number of non-fatal errors when saving inodes and snapshots.
      // 保存命名空间中的inode信息
      long numErrors = saveInodes(b);

      // 保存快照信息
      numErrors += saveSnapshots(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);


      // 保存安全信息
      step = new Step(StepType.DELEGATION_TOKENS, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      saveSecretManagerSection(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);


      // 保存缓存信息
      step = new Step(StepType.CACHE_POOLS, filePath);
      prog.beginStep(Phase.SAVING_CHECKPOINT, step);
      saveCacheManagerSection(b);
      prog.endStep(Phase.SAVING_CHECKPOINT, step);

      // 保存StringTable
      saveStringTableSection(b);

      // We use the underlyingOutputStream to write the header. Therefore flush
      // the buffered stream (which is potentially compressed) first.

      // flush输出流
      flushSectionOutputStream();

      FileSummary summary = b.build();
      //将FileSummary写入文件
      saveFileSummary(underlyingOutputStream, summary);

      //关闭底层输出流
      underlyingOutputStream.close();

      savedDigest = new MD5Hash(digester.digest());
      return numErrors;


    }

    private void saveSecretManagerSection(FileSummary.Builder summary)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      DelegationTokenSecretManager.SecretManagerState state = fsn
          .saveSecretManagerState();
      state.section.writeDelimitedTo(sectionOutputStream);
      for (SecretManagerSection.DelegationKey k : state.keys)
        k.writeDelimitedTo(sectionOutputStream);

      for (SecretManagerSection.PersistToken t : state.tokens)
        t.writeDelimitedTo(sectionOutputStream);

      commitSection(summary, SectionName.SECRET_MANAGER);
    }

    private void saveCacheManagerSection(FileSummary.Builder summary)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      CacheManager.PersistState state = fsn.getCacheManager().saveState();
      state.section.writeDelimitedTo(sectionOutputStream);

      for (CachePoolInfoProto p : state.pools)
        p.writeDelimitedTo(sectionOutputStream);

      for (CacheDirectiveInfoProto p : state.directives)
        p.writeDelimitedTo(sectionOutputStream);

      commitSection(summary, SectionName.CACHE_MANAGER);
    }

    private void saveErasureCodingSection(
        FileSummary.Builder summary) throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      ErasureCodingPolicyInfo[] ecPolicies =
          fsn.getErasureCodingPolicyManager().getPersistedPolicies();
      ArrayList<ErasureCodingPolicyProto> ecPolicyProtoes =
          new ArrayList<ErasureCodingPolicyProto>();
      for (ErasureCodingPolicyInfo p : ecPolicies) {
        ecPolicyProtoes.add(PBHelperClient.convertErasureCodingPolicy(p));
      }

      ErasureCodingSection section = ErasureCodingSection.newBuilder().
          addAllPolicies(ecPolicyProtoes).build();
      section.writeDelimitedTo(sectionOutputStream);
      commitSection(summary, SectionName.ERASURE_CODING);
    }

    private void saveNameSystemSection(FileSummary.Builder summary)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      OutputStream out = sectionOutputStream;
      BlockIdManager blockIdManager = fsn.getBlockManager().getBlockIdManager();
      NameSystemSection.Builder b = NameSystemSection.newBuilder()
          .setGenstampV1(blockIdManager.getLegacyGenerationStamp())
          .setGenstampV1Limit(blockIdManager.getLegacyGenerationStampLimit())
          .setGenstampV2(blockIdManager.getGenerationStamp())
          .setLastAllocatedBlockId(blockIdManager.getLastAllocatedContiguousBlockId())
          .setLastAllocatedStripedBlockId(blockIdManager.getLastAllocatedStripedBlockId())
          .setTransactionId(context.getTxId());

      // We use the non-locked version of getNamespaceInfo here since
      // the coordinating thread of saveNamespace already has read-locked
      // the namespace for us. If we attempt to take another readlock
      // from the actual saver thread, there's a potential of a
      // fairness-related deadlock. See the comments on HDFS-2223.
      b.setNamespaceId(fsn.unprotectedGetNamespaceInfo().getNamespaceID());
      if (fsn.isRollingUpgrade()) {
        b.setRollingUpgradeStartTime(fsn.getRollingUpgradeInfo().getStartTime());
      }
      NameSystemSection s = b.build();
      s.writeDelimitedTo(out);

      commitSection(summary, SectionName.NS_INFO);
    }

    private void saveStringTableSection(FileSummary.Builder summary)
        throws IOException {
      OutputStream out = sectionOutputStream;

      SerialNumberManager.StringTable stringTable =
          SerialNumberManager.getStringTable();
      StringTableSection.Builder b = StringTableSection.newBuilder()
          .setNumEntry(stringTable.size())
          .setMaskBits(stringTable.getMaskBits());
      b.build().writeDelimitedTo(out);
      for (Entry<Integer, String> e : stringTable) {
        StringTableSection.Entry.Builder eb = StringTableSection.Entry
            .newBuilder().setId(e.getKey()).setStr(e.getValue());
        eb.build().writeDelimitedTo(out);
      }
      commitSection(summary, SectionName.STRING_TABLE);
    }
  }

  /**
   * Supported section name. The order of the enum determines the order of
   * loading.
   */
  public enum SectionName {
    NS_INFO("NS_INFO"),
    STRING_TABLE("STRING_TABLE"),
    EXTENDED_ACL("EXTENDED_ACL"),
    ERASURE_CODING("ERASURE_CODING"),
    INODE("INODE"),
    INODE_REFERENCE("INODE_REFERENCE"),
    SNAPSHOT("SNAPSHOT"),
    INODE_DIR("INODE_DIR"),
    FILES_UNDERCONSTRUCTION("FILES_UNDERCONSTRUCTION"),
    SNAPSHOT_DIFF("SNAPSHOT_DIFF"),
    SECRET_MANAGER("SECRET_MANAGER"),
    CACHE_MANAGER("CACHE_MANAGER");

    private static final SectionName[] values = SectionName.values();

    public static SectionName fromString(String name) {
      for (SectionName n : values) {
        if (n.name.equals(name))
          return n;
      }
      return null;
    }

    private final String name;

    private SectionName(String name) {
      this.name = name;
    }
  }

  private static int getOndiskTrunkSize(com.google.protobuf.GeneratedMessage s) {
    return CodedOutputStream.computeRawVarint32Size(s.getSerializedSize())
        + s.getSerializedSize();
  }

  private FSImageFormatProtobuf() {
  }
}
