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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.WindowsSecureContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerExitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerPrepareContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.AuxiliaryServiceHelper;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.tools.packager.Log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContainerLaunch implements Callable<Integer> {

  private static final Logger LOG =  LoggerFactory.getLogger(ContainerLaunch.class);

  // 日志名称
  private static final String CONTAINER_PRE_LAUNCH_PREFIX = "prelaunch";
  // 正常日志输出扩展名
  public static final String CONTAINER_PRE_LAUNCH_STDOUT = CONTAINER_PRE_LAUNCH_PREFIX + ".out";
  // 异常文件名称.
  public static final String CONTAINER_PRE_LAUNCH_STDERR = CONTAINER_PRE_LAUNCH_PREFIX + ".err";

  // 启动任务脚本名称 launch_container
  public static final String CONTAINER_SCRIPT =  Shell.appendScriptExtension("launch_container");

  // token文件名称
  public static final String FINAL_CONTAINER_TOKENS_FILE = "container_tokens";

  // 系统目录
  public static final String SYSFS_DIR = "sysfs";

  // pid 文件
  private static final String PID_FILE_NAME_FMT = "%s.pid";

  // 进程退出文件扩展名
  static final String EXIT_CODE_FILE_SUFFIX = ".exitcode";

  // Dispatcher
  protected final Dispatcher dispatcher;
  
  // ContainerExecutor 的实现   DefaultContainerExecutor  or   LinuxContainerExecutor
  protected final ContainerExecutor exec;

  // Application信息
  protected final Application app;
  
  // Container信息
  protected final Container container;

  // 配置相关信息
  private final Configuration conf;

  // RM context信息
  private final Context context;

  // ContainerManager 对象
  private final ContainerManagerImpl containerManager;

  // 原子标识 Container 是否应启动
  protected AtomicBoolean containerAlreadyLaunched = new AtomicBoolean(false);

  // 原子标识 Container是否已经暂停
  protected AtomicBoolean shouldPauseContainer = new AtomicBoolean(false);

  // 原子标识 Container是否已经完成
  protected AtomicBoolean completed = new AtomicBoolean(false);

  // 启动之前是否需要kill
  private volatile boolean killedBeforeStart = false;

  // kill最大等待时间  yarn.nodemanager.process-kill-wait.ms : 5000
  private long maxKillWaitTime = 2000;

  // pid 文件的路径
  protected Path pidFilePath = null;

  // 本地存储服务.
  protected final LocalDirsHandlerService dirsHandler;

  // 启动的锁.
  private final Lock launchLock = new ReentrantLock();



  public ContainerLaunch(Context context, Configuration configuration,
      Dispatcher dispatcher, ContainerExecutor exec, Application app,
      Container container, LocalDirsHandlerService dirsHandler,
      ContainerManagerImpl containerManager) {

    this.context = context;

    this.conf = configuration;

    this.app = app;

    this.exec = exec;

    this.container = container;

    this.dispatcher = dispatcher;

    this.dirsHandler = dirsHandler;

    this.containerManager = containerManager;

    // yarn.nodemanager.process-kill-wait.ms : 5000
    this.maxKillWaitTime =
        conf.getLong(YarnConfiguration.NM_PROCESS_KILL_WAIT_MS,
            YarnConfiguration.DEFAULT_NM_PROCESS_KILL_WAIT_MS);
  }

  @VisibleForTesting
  public static String expandEnvironment(String var, Path containerLogDir) {



    var = var.replace(ApplicationConstants.LOG_DIR_EXPANSION_VAR,  containerLogDir.toString());


    var = var.replace(ApplicationConstants.CLASS_PATH_SEPARATOR, File.pathSeparator);


    // 替换特殊符号
    // replace parameter expansion marker. e.g. {{VAR}} on Windows is replaced as %VAR% and on Linux replaced as "$VAR"
    if (Shell.WINDOWS) {
      var = var.replaceAll("(\\{\\{)|(\\}\\})", "%");
    } else {
      var = var.replace(ApplicationConstants.PARAMETER_EXPANSION_LEFT, "$");
      var = var.replace(ApplicationConstants.PARAMETER_EXPANSION_RIGHT, "");
    }
    return var;
  }

  // 展开所有环境变量
  private Map<String, String> expandAllEnvironmentVars(
      ContainerLaunchContext launchContext, Path containerLogDir) {
    // 获取所有的环境变量





    Map<String, String> environment = launchContext.getEnvironment();
    for (Entry<String, String> entry : environment.entrySet()) {
      String value = entry.getValue();
      value = expandEnvironment(value, containerLogDir);
      entry.setValue(value);
    }
    return environment;
  }

  @Override
  public Integer call() {
    if (!validateContainerState()) {
      return 0;
    }
    // 获取ContainerLaunchContext 信息 : localResources { key: "__spark_libs__/ehcache-3.3.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/ehcache-3.3.1.jar" } size: 1726527 timestamp: 1611680551515 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spire-macros_2.12-0.13.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spire-macros_2.12-0.13.0.jar" } size: 69702 timestamp: 1611680602437 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/zstd-jni-1.3.2-2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/zstd-jni-1.3.2-2.jar" } size: 2333186 timestamp: 1611680608797 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-common-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-common-1.0.1.jar" } size: 65464 timestamp: 1611680572751 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/arrow-memory-0.10.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/arrow-memory-0.10.0.jar" } size: 79283 timestamp: 1611680537638 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-jaxrs-base-2.7.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-jaxrs-base-2.7.8.jar" } size: 29947 timestamp: 1611680559101 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/orc-mapreduce-1.5.5-nohive.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/orc-mapreduce-1.5.5-nohive.jar" } size: 812313 timestamp: 1611680587482 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/super-csv-2.2.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/super-csv-2.2.0.jar" } size: 93210 timestamp: 1611680604788 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-catalyst_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-catalyst_2.12-2.4.5.jar" } size: 7108871 timestamp: 1611680597003 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/snakeyaml-1.15.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/snakeyaml-1.15.jar" } size: 269295 timestamp: 1611680595951 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/dnsjava-2.1.7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/dnsjava-2.1.7.jar" } size: 307637 timestamp: 1611680551415 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/calcite-core-1.2.0-incubating.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/calcite-core-1.2.0-incubating.jar" } size: 3519262 timestamp: 1611680541984 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/okhttp-2.7.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/okhttp-2.7.5.jar" } size: 331034 timestamp: 1611680585638 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/scala-library-2.12.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/scala-library-2.12.8.jar" } size: 5272591 timestamp: 1611680593951 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/metrics-jvm-3.1.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/metrics-jvm-3.1.5.jar" } size: 39283 timestamp: 1611680582886 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-lang3-3.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-lang3-3.5.jar" } size: 479881 timestamp: 1611680547459 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/curator-recipes-2.12.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/curator-recipes-2.12.0.jar" } size: 283598 timestamp: 1611680549706 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-yarn-registry-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-yarn-registry-3.1.3.jar" } size: 225626 timestamp: 1611680557307 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-module-scala_2.12-2.6.7.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-module-scala_2.12-2.6.7.1.jar" } size: 351802 timestamp: 1611680560556 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hive-cli-1.2.1.spark2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hive-cli-1.2.1.spark2.jar" } size: 40821 timestamp: 1611680557494 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/javax.servlet-api-3.1.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/javax.servlet-api-3.1.0.jar" } size: 95806 timestamp: 1611680562429 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/javax.inject-2.4.0-b34.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/javax.inject-2.4.0-b34.jar" } size: 5950 timestamp: 1611680561978 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-pool-1.5.4.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-pool-1.5.4.jar" } size: 96221 timestamp: 1611680548949 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/validation-api-1.1.0.Final.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/validation-api-1.1.0.Final.jar" } size: 63777 timestamp: 1611680606100 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/htrace-core4-4.1.0-incubating.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/htrace-core4-4.1.0-incubating.jar" } size: 1502280 timestamp: 1611680558176 type: FILE visibility: PRIVATE } } localResources { key: "__app__.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-examples_2.11-2.4.5.jar" } size: 1475072 timestamp: 1611680609310 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/breeze_2.12-0.13.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/breeze_2.12-0.13.2.jar" } size: 13319481 timestamp: 1611680541385 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/animal-sniffer-annotations-1.17.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/animal-sniffer-annotations-1.17.jar" } size: 3448 timestamp: 1611680536547 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/parquet-format-2.4.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/parquet-format-2.4.0.jar" } size: 723203 timestamp: 1611680590491 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jcl-over-slf4j-1.7.16.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jcl-over-slf4j-1.7.16.jar" } size: 16430 timestamp: 1611680564255 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/pyrolite-4.13.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/pyrolite-4.13.jar" } size: 94796 timestamp: 1611680592382 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-common-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-common-3.1.3.jar" } size: 4095712 timestamp: 1611680555174 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/avro-ipc-1.8.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/avro-ipc-1.8.2.jar" } size: 132989 timestamp: 1611680539643 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-mapreduce-client-jobclient-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-mapreduce-client-jobclient-3.1.3.jar" } size: 85395 timestamp: 1611680557046 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/netty-3.9.9.Final.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/netty-3.9.9.Final.jar" } size: 1330219 timestamp: 1611680584233 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jersey-container-servlet-core-2.22.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jersey-container-servlet-core-2.22.2.jar" } size: 66270 timestamp: 1611680566116 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kryo-shaded-4.0.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kryo-shaded-4.0.2.jar" } size: 410874 timestamp: 1611680576991 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/macro-compat_2.12-1.1.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/macro-compat_2.12-1.1.1.jar" } size: 3180 timestamp: 1611680581725 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/shims-0.7.45.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/shims-0.7.45.jar" } size: 4028 timestamp: 1611680595423 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-math3-3.4.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-math3-3.4.1.jar" } size: 2035066 timestamp: 1611680548040 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/generex-1.0.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/generex-1.0.2.jar" } size: 14395 timestamp: 1611680553054 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/parquet-hadoop-bundle-1.6.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/parquet-hadoop-bundle-1.6.0.jar" } size: 2796935 timestamp: 1611680591008 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/j2objc-annotations-1.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/j2objc-annotations-1.1.jar" } size: 8782 timestamp: 1611680558421 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-yarn-client-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-yarn-client-3.1.3.jar" } size: 305534 timestamp: 1611680557169 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/curator-framework-2.12.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/curator-framework-2.12.0.jar" } size: 201953 timestamp: 1611680549650 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/JavaEWAH-0.3.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/JavaEWAH-0.3.2.jar" } size: 16993 timestamp: 1611680535986 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/RoaringBitmap-0.7.45.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/RoaringBitmap-0.7.45.jar" } size: 325335 timestamp: 1611680536109 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/gson-2.2.4.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/gson-2.2.4.jar" } size: 190432 timestamp: 1611680553135 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/json4s-scalap_2.12-3.5.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/json4s-scalap_2.12-3.5.3.jar" } size: 346628 timestamp: 1611680570449 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/antlr4-runtime-4.7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/antlr4-runtime-4.7.jar" } size: 334662 timestamp: 1611680536870 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/shapeless_2.12-2.3.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/shapeless_2.12-2.3.2.jar" } size: 2829843 timestamp: 1611680595395 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jersey-server-2.22.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jersey-server-2.22.2.jar" } size: 951701 timestamp: 1611680566732 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/logging-interceptor-3.12.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/logging-interceptor-3.12.0.jar" } size: 12486 timestamp: 1611680580729 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-dbcp-1.4.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-dbcp-1.4.jar" } size: 160519 timestamp: 1611680546449 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-mapreduce-client-common-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-mapreduce-client-common-3.1.3.jar" } size: 803720 timestamp: 1611680556136 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-compiler-3.0.9.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-compiler-3.0.9.jar" } size: 37871 timestamp: 1611680545729 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/flatbuffers-1.2.0-3f79e055.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/flatbuffers-1.2.0-3f79e055.jar" } size: 10166 timestamp: 1611680552606 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/scala-xml_2.12-1.0.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/scala-xml_2.12-1.0.5.jar" } size: 548430 timestamp: 1611680594932 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kubernetes-model-4.6.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kubernetes-model-4.6.1.jar" } size: 11061771 timestamp: 1611680577970 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-mesos_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-mesos_2.12-2.4.5.jar" } size: 292606 timestamp: 1611680599291 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/parquet-column-1.10.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/parquet-column-1.10.1.jar" } size: 1097799 timestamp: 1611680589442 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hk2-api-2.4.0-b34.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hk2-api-2.4.0-b34.jar" } size: 178947 timestamp: 1611680557940 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-hive_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-hive_2.12-2.4.5.jar" } size: 670086 timestamp: 1611680598196 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-kvstore_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-kvstore_2.12-2.4.5.jar" } size: 57071 timestamp: 1611680598792 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-server-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-server-1.0.1.jar" } size: 82756 timestamp: 1611680573359 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/woodstox-core-5.0.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/woodstox-core-5.0.3.jar" } size: 512742 timestamp: 1611680606552 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-core-asl-1.9.13.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-core-asl-1.9.13.jar" } size: 232248 timestamp: 1611680558552 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/scala-reflect-2.12.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/scala-reflect-2.12.8.jar" } size: 3645095 timestamp: 1611680594483 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/opencsv-2.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/opencsv-2.3.jar" } size: 19827 timestamp: 1611680586965 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jersey-container-servlet-2.22.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jersey-container-servlet-2.22.2.jar" } size: 18098 timestamp: 1611680566075 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/curator-client-2.12.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/curator-client-2.12.0.jar" } size: 2423138 timestamp: 1611680549596 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/error_prone_annotations-2.2.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/error_prone_annotations-2.2.0.jar" } size: 13694 timestamp: 1611680552030 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-module-paranamer-2.7.9.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-module-paranamer-2.7.9.jar" } size: 42858 timestamp: 1611680560126 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/datanucleus-core-3.2.10.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/datanucleus-core-3.2.10.jar" } size: 1890075 timestamp: 1611680549895 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/core-1.1.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/core-1.1.2.jar" } size: 164422 timestamp: 1611680549464 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/lz4-java-1.4.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/lz4-java-1.4.0.jar" } size: 370119 timestamp: 1611680580768 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-codec-1.10.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-codec-1.10.jar" } size: 284184 timestamp: 1611680544792 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/httpclient-4.5.6.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/httpclient-4.5.6.jar" } size: 767140 timestamp: 1611680558296 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/slf4j-api-1.7.16.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/slf4j-api-1.7.16.jar" } size: 40509 timestamp: 1611680595857 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-mapper-asl-1.9.13.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-mapper-asl-1.9.13.jar" } size: 780664 timestamp: 1611680559644 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-identity-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-identity-1.0.1.jar" } size: 20046 timestamp: 1611680572927 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/ivy-2.4.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/ivy-2.4.0.jar" } size: 1282424 timestamp: 1611680558388 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/slf4j-log4j12-1.7.16.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/slf4j-log4j12-1.7.16.jar" } size: 9939 timestamp: 1611680595897 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/automaton-1.11-8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/automaton-1.11-8.jar" } size: 176285 timestamp: 1611680538653 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/bonecp-0.8.0.RELEASE.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/bonecp-0.8.0.RELEASE.jar" } size: 110600 timestamp: 1611680540571 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-yarn-common-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-yarn-common-3.1.3.jar" } size: 2842656 timestamp: 1611680557259 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/zjsonpatch-0.3.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/zjsonpatch-0.3.0.jar" } size: 35518 timestamp: 1611680607873 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/javax.inject-1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/javax.inject-1.jar" } size: 2497 timestamp: 1611680561943 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/compress-lzf-1.0.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/compress-lzf-1.0.3.jar" } size: 79845 timestamp: 1611680549409 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/paranamer-2.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/paranamer-2.8.jar" } size: 34654 timestamp: 1611680588992 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-crypto-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-crypto-1.0.1.jar" } size: 116120 timestamp: 1611680572890 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar" } size: 2199 timestamp: 1611680579851 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/mssql-jdbc-6.2.1.jre7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/mssql-jdbc-6.2.1.jre7.jar" } size: 792442 timestamp: 1611680583783 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/zookeeper-3.4.9.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/zookeeper-3.4.9.jar" } size: 807277 timestamp: 1611680608339 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/ST4-4.0.4.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/ST4-4.0.4.jar" } size: 236660 timestamp: 1611680536214 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/machinist_2.12-0.6.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/machinist_2.12-0.6.1.jar" } size: 33650 timestamp: 1611680581228 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/leveldbjni-all-1.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/leveldbjni-all-1.8.jar" } size: 1045744 timestamp: 1611680578501 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/orc-core-1.5.5-nohive.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/orc-core-1.5.5-nohive.jar" } size: 1565700 timestamp: 1611680587028 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hive-exec-1.2.1.spark2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hive-exec-1.2.1.spark2.jar" } size: 11494362 timestamp: 1611680557694 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/aopalliance-1.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/aopalliance-1.0.jar" } size: 4467 timestamp: 1611680537010 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-auth-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-auth-3.1.3.jar" } size: 138812 timestamp: 1611680554616 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/parquet-encoding-1.10.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/parquet-encoding-1.10.1.jar" } size: 848750 timestamp: 1611680590035 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/scala-compiler-2.12.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/scala-compiler-2.12.8.jar" } size: 10508278 timestamp: 1611680593376 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/json-smart-2.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/json-smart-2.3.jar" } size: 120316 timestamp: 1611680569061 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jaxb-api-2.2.11.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jaxb-api-2.2.11.jar" } size: 102244 timestamp: 1611680563362 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hk2-locator-2.4.0-b34.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hk2-locator-2.4.0-b34.jar" } size: 181271 timestamp: 1611680558014 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-core_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-core_2.12-2.4.5.jar" } size: 8694617 timestamp: 1611680597508 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/HikariCP-java7-2.4.12.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/HikariCP-java7-2.4.12.jar" } size: 134308 timestamp: 1611680535806 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/calcite-linq4j-1.2.0-incubating.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/calcite-linq4j-1.2.0-incubating.jar" } size: 442406 timestamp: 1611680542437 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/chill_2.12-0.9.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/chill_2.12-0.9.3.jar" } size: 195811 timestamp: 1611680543809 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/calcite-avatica-1.2.0-incubating.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/calcite-avatica-1.2.0-incubating.jar" } size: 258370 timestamp: 1611680541437 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/metrics-graphite-3.1.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/metrics-graphite-3.1.5.jar" } size: 21247 timestamp: 1611680582811 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jcip-annotations-1.0-1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jcip-annotations-1.0-1.jar" } size: 4722 timestamp: 1611680563821 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jersey-media-jaxb-2.22.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jersey-media-jaxb-2.22.2.jar" } size: 72733 timestamp: 1611680566645 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-net-3.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-net-3.1.jar" } size: 273370 timestamp: 1611680548498 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/protobuf-java-2.5.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/protobuf-java-2.5.0.jar" } size: 533455 timestamp: 1611680591507 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-sql_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-sql_2.12-2.4.5.jar" } size: 5772768 timestamp: 1611680600149 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/antlr-2.7.7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/antlr-2.7.7.jar" } size: 445288 timestamp: 1611680536653 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/parquet-hadoop-1.10.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/parquet-hadoop-1.10.1.jar" } size: 285732 timestamp: 1611680590532 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/antlr-runtime-3.4.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/antlr-runtime-3.4.jar" } size: 164368 timestamp: 1611680536747 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-io-2.4.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-io-2.4.jar" } size: 185140 timestamp: 1611680546547 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-network-shuffle_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-network-shuffle_2.12-2.4.5.jar" } size: 70897 timestamp: 1611680599850 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/mesos-1.4.0-shaded-protobuf.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/mesos-1.4.0-shaded-protobuf.jar" } size: 7343426 timestamp: 1611680582221 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/snappy-java-1.1.7.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/snappy-java-1.1.7.3.jar" } size: 2021167 timestamp: 1611680596871 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-streaming_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-streaming_2.12-2.4.5.jar" } size: 1154604 timestamp: 1611680600592 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-tags_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-tags_2.12-2.4.5.jar" } size: 15490 timestamp: 1611680601470 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/geronimo-jcache_1.0_spec-1.0-alpha-1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/geronimo-jcache_1.0_spec-1.0-alpha-1.jar" } size: 55236 timestamp: 1611680553091 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-kubernetes_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-kubernetes_2.12-2.4.5.jar" } size: 323158 timestamp: 1611680598311 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/datanucleus-api-jdo-3.2.6.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/datanucleus-api-jdo-3.2.6.jar" } size: 339666 timestamp: 1611680549772 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/aopalliance-repackaged-2.4.0-b34.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/aopalliance-repackaged-2.4.0-b34.jar" } size: 14766 timestamp: 1611680537176 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/json4s-core_2.12-3.5.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/json4s-core_2.12-3.5.3.jar" } size: 489399 timestamp: 1611680569558 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-lang-2.6.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-lang-2.6.jar" } size: 284220 timestamp: 1611680546995 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-crypto-1.0.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-crypto-1.0.0.jar" } size: 134595 timestamp: 1611680546346 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/stringtemplate-3.2.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/stringtemplate-3.2.1.jar" } size: 148627 timestamp: 1611680604348 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jsp-api-2.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jsp-api-2.1.jar" } size: 100636 timestamp: 1611680570486 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/avro-1.8.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/avro-1.8.2.jar" } size: 1556863 timestamp: 1611680539188 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hive-metastore-1.2.1.spark2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hive-metastore-1.2.1.spark2.jar" } size: 5471322 timestamp: 1611680557873 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-yarn-server-web-proxy-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-yarn-server-web-proxy-3.1.3.jar" } size: 80417 timestamp: 1611680557410 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jtransforms-2.4.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jtransforms-2.4.0.jar" } size: 764569 timestamp: 1611680571410 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/arrow-vector-0.10.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/arrow-vector-0.10.0.jar" } size: 1318940 timestamp: 1611680538169 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jersey-common-2.22.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jersey-common-2.22.2.jar" } size: 698375 timestamp: 1611680565628 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/json4s-jackson_2.12-3.5.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/json4s-jackson_2.12-3.5.3.jar" } size: 40086 timestamp: 1611680570008 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/snappy-0.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/snappy-0.2.jar" } size: 48720 timestamp: 1611680596407 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jersey-guava-2.22.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jersey-guava-2.22.2.jar" } size: 971310 timestamp: 1611680566203 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/httpcore-4.4.10.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/httpcore-4.4.10.jar" } size: 326356 timestamp: 1611680558344 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-yarn-api-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-yarn-api-3.1.3.jar" } size: 3103441 timestamp: 1611680557119 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/javax.ws.rs-api-2.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/javax.ws.rs-api-2.0.1.jar" } size: 115534 timestamp: 1611680562878 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/log4j-1.2.17.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/log4j-1.2.17.jar" } size: 489884 timestamp: 1611680580295 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jpam-1.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jpam-1.1.jar" } size: 12131 timestamp: 1611680569032 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hive-jdbc-1.2.1.spark2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hive-jdbc-1.2.1.spark2.jar" } size: 101065 timestamp: 1611680557736 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/janino-3.0.9.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/janino-3.0.9.jar" } size: 801369 timestamp: 1611680561015 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-jaxrs-json-provider-2.7.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-jaxrs-json-provider-2.7.8.jar" } size: 16776 timestamp: 1611680559144 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/okhttp-3.12.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/okhttp-3.12.0.jar" } size: 422786 timestamp: 1611680586085 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerby-pkix-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerby-pkix-1.0.1.jar" } size: 204650 timestamp: 1611680575613 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/token-provider-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/token-provider-1.0.1.jar" } size: 18763 timestamp: 1611680605235 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/re2j-1.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/re2j-1.1.jar" } size: 128414 timestamp: 1611680592819 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-core-2.6.7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-core-2.6.7.jar" } size: 258919 timestamp: 1611680558506 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-hdfs-client-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-hdfs-client-3.1.3.jar" } size: 5059506 timestamp: 1611680555696 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-collections-3.2.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-collections-3.2.2.jar" } size: 588337 timestamp: 1611680545291 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerby-asn1-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerby-asn1-1.0.1.jar" } size: 102174 timestamp: 1611680574719 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-httpclient-3.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-httpclient-3.1.jar" } size: 305001 timestamp: 1611680546502 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jdo-api-3.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jdo-api-3.0.1.jar" } size: 201124 timestamp: 1611680564707 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jul-to-slf4j-1.7.16.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jul-to-slf4j-1.7.16.jar" } size: 4596 timestamp: 1611680571444 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-daemon-1.0.13.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-daemon-1.0.13.jar" } size: 24239 timestamp: 1611680546385 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-tags_2.12-2.4.5-tests.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-tags_2.12-2.4.5-tests.jar" } size: 8837 timestamp: 1611680601025 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-annotations-2.6.7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-annotations-2.6.7.jar" } size: 46986 timestamp: 1611680558454 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/minlog-1.3.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/minlog-1.3.0.jar" } size: 5711 timestamp: 1611680583327 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/javax.annotation-api-1.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/javax.annotation-api-1.2.jar" } size: 26366 timestamp: 1611680561909 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/objenesis-2.5.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/objenesis-2.5.1.jar" } size: 54391 timestamp: 1611680585193 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/json4s-ast_2.12-3.5.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/json4s-ast_2.12-3.5.3.jar" } size: 83376 timestamp: 1611680569512 type: FILE visibility: PRIVATE } } localResources { key: "__spark_conf__" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/__spark_conf__.zip" } size: 295728 timestamp: 1611680609955 type: ARCHIVE visibility: PRIVATE } } localResources { key: "__spark_libs__/nimbus-jose-jwt-4.41.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/nimbus-jose-jwt-4.41.1.jar" } size: 299508 timestamp: 1611680585153 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-module-jaxb-annotations-2.6.7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-module-jaxb-annotations-2.6.7.jar" } size: 32612 timestamp: 1611680559690 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-yarn_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-yarn_2.12-2.4.5.jar" } size: 326224 timestamp: 1611680601994 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-client-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-client-1.0.1.jar" } size: 113017 timestamp: 1611680572315 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hive-beeline-1.2.1.spark2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hive-beeline-1.2.1.spark2.jar" } size: 138981 timestamp: 1611680557446 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kubernetes-model-common-4.6.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kubernetes-model-common-4.6.1.jar" } size: 3956 timestamp: 1611680578018 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-admin-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-admin-1.0.1.jar" } size: 80980 timestamp: 1611680571883 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/libfb303-0.9.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/libfb303-0.9.3.jar" } size: 313702 timestamp: 1611680578965 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-simplekdc-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-simplekdc-1.0.1.jar" } size: 20409 timestamp: 1611680573810 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/chill-java-0.9.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/chill-java-0.9.3.jar" } size: 58633 timestamp: 1611680543343 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/derby-10.12.1.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/derby-10.12.1.1.jar" } size: 3224708 timestamp: 1611680550908 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerby-config-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerby-config-1.0.1.jar" } size: 30674 timestamp: 1611680575160 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/libthrift-0.9.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/libthrift-0.9.3.jar" } size: 234201 timestamp: 1611680579410 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/datanucleus-rdbms-3.2.9.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/datanucleus-rdbms-3.2.9.jar" } size: 1809447 timestamp: 1611680550408 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/univocity-parsers-2.7.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/univocity-parsers-2.7.3.jar" } size: 404970 timestamp: 1611680605665 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-compress-1.8.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-compress-1.8.1.jar" } size: 365552 timestamp: 1611680546207 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-sketch_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-sketch_2.12-2.4.5.jar" } size: 30057 timestamp: 1611680600048 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/py4j-0.10.7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/py4j-0.10.7.jar" } size: 122774 timestamp: 1611680591942 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/joda-time-2.9.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/joda-time-2.9.3.jar" } size: 627814 timestamp: 1611680568545 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-databind-2.6.7.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-databind-2.6.7.3.jar" } size: 1166637 timestamp: 1611680558606 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jackson-dataformat-yaml-2.6.7.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jackson-dataformat-yaml-2.6.7.jar" } size: 320444 timestamp: 1611680558662 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/aircompressor-0.10.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/aircompressor-0.10.jar" } size: 134044 timestamp: 1611680536470 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jsr305-1.3.9.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jsr305-1.3.9.jar" } size: 33015 timestamp: 1611680570923 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-hive-thriftserver_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-hive-thriftserver_2.12-2.4.5.jar" } size: 1736296 timestamp: 1611680597990 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-mapreduce-client-core-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-mapreduce-client-core-3.1.3.jar" } size: 1655240 timestamp: 1611680556607 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/orc-shims-1.5.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/orc-shims-1.5.5.jar" } size: 27745 timestamp: 1611680587913 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/avro-mapred-1.8.2-hadoop2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/avro-mapred-1.8.2-hadoop2.jar" } size: 187052 timestamp: 1611680540111 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/oro-2.0.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/oro-2.0.8.jar" } size: 65261 timestamp: 1611680587964 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hppc-0.7.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hppc-0.7.2.jar" } size: 1671083 timestamp: 1611680558111 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/javassist-3.18.1-GA.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/javassist-3.18.1-GA.jar" } size: 714194 timestamp: 1611680561460 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-configuration2-2.1.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-configuration2-2.1.1.jar" } size: 616888 timestamp: 1611680546292 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/eigenbase-properties-1.1.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/eigenbase-properties-1.1.5.jar" } size: 18482 timestamp: 1611680551990 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/xz-1.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/xz-1.5.jar" } size: 99555 timestamp: 1611680607438 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jetty-webapp-9.3.24.v20180605.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jetty-webapp-9.3.24.v20180605.jar" } size: 114983 timestamp: 1611680567204 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jline-2.14.6.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jline-2.14.6.jar" } size: 268780 timestamp: 1611680568101 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/osgi-resource-locator-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/osgi-resource-locator-1.0.1.jar" } size: 20235 timestamp: 1611680588470 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-launcher_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-launcher_2.12-2.4.5.jar" } size: 75932 timestamp: 1611680599253 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-annotations-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-annotations-3.1.3.jar" } size: 59823 timestamp: 1611680554166 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-mllib_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-mllib_2.12-2.4.5.jar" } size: 5305175 timestamp: 1611680599481 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-logging-1.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-logging-1.1.3.jar" } size: 62050 timestamp: 1611680547906 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-yarn-server-common-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-yarn-server-common-3.1.3.jar" } size: 1334937 timestamp: 1611680557359 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/metrics-core-3.1.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/metrics-core-3.1.5.jar" } size: 120465 timestamp: 1611680582305 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/metrics-json-3.1.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/metrics-json-3.1.5.jar" } size: 15824 timestamp: 1611680582854 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-beanutils-1.9.4.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-beanutils-1.9.4.jar" } size: 246918 timestamp: 1611680544265 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/scala-parser-combinators_2.12-1.1.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/scala-parser-combinators_2.12-1.1.0.jar" } size: 225042 timestamp: 1611680594396 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/guice-servlet-4.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/guice-servlet-4.0.jar" } size: 76983 timestamp: 1611680553722 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-graphx_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-graphx_2.12-2.4.5.jar" } size: 429742 timestamp: 1611680597939 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hadoop-client-3.1.3.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hadoop-client-3.1.3.jar" } size: 43875 timestamp: 1611680555063 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/parquet-common-1.10.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/parquet-common-1.10.1.jar" } size: 94995 timestamp: 1611680589975 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerby-util-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerby-util-1.0.1.jar" } size: 40554 timestamp: 1611680576047 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-network-common_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-network-common_2.12-2.4.5.jar" } size: 2393456 timestamp: 1611680599809 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/arpack_combined_all-0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/arpack_combined_all-0.1.jar" } size: 1194003 timestamp: 1611680537380 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-util-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-util-1.0.1.jar" } size: 36708 timestamp: 1611680574282 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/apache-log4j-extras-1.2.17.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/apache-log4j-extras-1.2.17.jar" } size: 448794 timestamp: 1611680537317 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/activation-1.1.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/activation-1.1.1.jar" } size: 69409 timestamp: 1611680536363 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerb-core-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerb-core-1.0.1.jar" } size: 226672 timestamp: 1611680572824 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jodd-core-3.5.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jodd-core-3.5.2.jar" } size: 427780 timestamp: 1611680568591 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/parquet-jackson-1.10.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/parquet-jackson-1.10.1.jar" } size: 1048171 timestamp: 1611680591063 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kerby-xdr-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kerby-xdr-1.0.1.jar" } size: 29134 timestamp: 1611680576481 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/arrow-format-0.10.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/arrow-format-0.10.0.jar" } size: 52037 timestamp: 1611680537481 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/netty-all-4.1.42.Final.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/netty-all-4.1.42.Final.jar" } size: 4080249 timestamp: 1611680584720 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jta-1.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jta-1.1.jar" } size: 15071 timestamp: 1611680571361 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/javolution-5.5.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/javolution-5.5.1.jar" } size: 395195 timestamp: 1611680563319 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/guice-4.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/guice-4.0.jar" } size: 668235 timestamp: 1611680553278 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/stax2-api-3.1.4.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/stax2-api-3.1.4.jar" } size: 161867 timestamp: 1611680603863 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/kubernetes-client-4.6.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/kubernetes-client-4.6.1.jar" } size: 686815 timestamp: 1611680577442 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-unsafe_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-unsafe_2.12-2.4.5.jar" } size: 49930 timestamp: 1611680601533 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spire_2.12-0.13.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spire_2.12-0.13.0.jar" } size: 9660469 timestamp: 1611680602985 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/breeze-macros_2.12-0.13.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/breeze-macros_2.12-0.13.2.jar" } size: 125330 timestamp: 1611680541032 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/hk2-utils-2.4.0-b34.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/hk2-utils-2.4.0-b34.jar" } size: 118973 timestamp: 1611680558054 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/failureaccess-1.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/failureaccess-1.0.jar" } size: 3727 timestamp: 1611680552093 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jetty-xml-9.3.24.v20180605.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jetty-xml-9.3.24.v20180605.jar" } size: 51734 timestamp: 1611680567650 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/stax-api-1.0.1.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/stax-api-1.0.1.jar" } size: 26514 timestamp: 1611680603431 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/accessors-smart-1.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/accessors-smart-1.2.jar" } size: 30035 timestamp: 1611680536301 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/commons-cli-1.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/commons-cli-1.2.jar" } size: 41123 timestamp: 1611680544718 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/xbean-asm6-shaded-4.8.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/xbean-asm6-shaded-4.8.jar" } size: 282930 timestamp: 1611680606989 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-mllib-local_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-mllib-local_2.12-2.4.5.jar" } size: 101649 timestamp: 1611680599356 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/stream-2.7.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/stream-2.7.0.jar" } size: 174351 timestamp: 1611680604307 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/spark-repl_2.12-2.4.5.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/spark-repl_2.12-2.4.5.jar" } size: 51164 timestamp: 1611680599971 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/guava-27.0-jre.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/guava-27.0-jre.jar" } size: 2747878 timestamp: 1611680553221 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/jersey-client-2.22.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/jersey-client-2.22.2.jar" } size: 167421 timestamp: 1611680565170 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/okio-1.15.0.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/okio-1.15.0.jar" } size: 88732 timestamp: 1611680586530 type: FILE visibility: PRIVATE } } localResources { key: "__spark_libs__/checker-qual-2.5.2.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611680479488_0001/checker-qual-2.5.2.jar" } size: 193322 timestamp: 1611680542892 type: FILE visibility: PRIVATE } } tokens: "HDTS\000\001\000\032\n\r\n\t\b\001\020\200\362\221\375\363.\020\001\020\255\245\205\270\376\377\377\377\377\001\024l\\j\034\262\202R^\275\261\235W^\341\247ju~\302\377\020YARN_AM_RM_TOKEN\000\000" environment { key: "SPARK_YARN_STAGING_DIR" value: "hdfs://localhost:8020/user/henghe/.sparkStaging/application_1611680479488_0001" } environment { key: "APPLICATION_WEB_PROXY_BASE" value: "/proxy/application_1611680479488_0001" } environment { key: "SPARK_USER" value: "henghe" } environment { key: "CLASSPATH" value: "{{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>$HADOOP_CONF_DIR<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/*<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/lib/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*<CPS>{{PWD}}/__spark_conf__/__hadoop_conf__" } environment { key: "PYTHONHASHSEED" value: "0" } environment { key: "APP_SUBMIT_TIME_ENV" value: "1611680611847" } command: "{{JAVA_HOME}}/bin/java" command: "-server" command: "-Xmx1024m" command: "-Djava.io.tmpdir={{PWD}}/tmp" command: "-Dspark.yarn.app.container.log.dir=<LOG_DIR>" command: "org.apache.spark.deploy.yarn.ApplicationMaster" command: "--class" command: "\'org.apache.spark.examples.SparkPi\'" command: "--jar" command: "file:/opt/tools/spark-2.4.5/examples/jars/spark-examples_2.11-2.4.5.jar" command: "--arg" command: "\'10\'" command: "--properties-file" command: "{{PWD}}/__spark_conf__/__spark_conf__.properties" command: "1>" command: "<LOG_DIR>/stdout" command: "2>" command: "<LOG_DIR>/stderr" application_ACLs { accessType: APPACCESS_MODIFY_APP acl: "sysadmin,henghe " } application_ACLs { accessType: APPACCESS_VIEW_APP acl: "sysadmin,henghe " }
    final ContainerLaunchContext launchContext = container.getLaunchContext();

    // 获取containerID container_1611680479488_0001_01_000001
    ContainerId containerID = container.getContainerId();
    String containerIdStr = containerID.toString();

    // 获取命令
    final List<String> command = launchContext.getCommands();
    int ret = -1;

    Path containerLogDir;
    try {
      // 获取本地资源文件 : {Path@9096} "/opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/usercache/henghe/filecache/62/logging-interceptor-3.12.0.jar" -> {ArrayList@9097}  size = 1
      Map<Path, List<String>> localResources = getLocalizedResources();

      // 获取用户 henghe
      final String user = container.getUser();

      // ****************   变量解析,替换变量.  生成新的命令  # 变量解析开始   ****************
      // /////////////////////////// Variable expansion
      // Before the container script gets written out.
      List<String> newCmds = new ArrayList<String>(command.size());

      // 获取appId  application_1611680479488_0001
      String appIdStr = app.getAppId().toString();



      // 获取相对于容器的日志目录 : application_1611680479488_0001/container_1611680479488_0001_01_000001
      String relativeContainerLogDir = ContainerLaunch.getRelativeContainerLogDir(appIdStr, containerIdStr);

      // container日志目录
      // /opt/tools/hadoop-3.2.1/logs/userlogs/application_1611680479488_0001/container_1611680479488_0001_01_000001
      containerLogDir =  dirsHandler.getLogPathForWrite(relativeContainerLogDir, false);

      // 设置容器日志目录
      recordContainerLogDir(containerID, containerLogDir.toString());



      for (String str : command) {
        // TODO: Should we instead work via symlinks without this grammar?
        newCmds.add(expandEnvironment(str, containerLogDir));
      }

      // 设置命令
      launchContext.setCommands(newCmds);
      // 获取环境变量
      //    "SPARK_YARN_STAGING_DIR" -> "hdfs://localhost:8020/user/henghe/.sparkStaging/application_1611680479488_0001"
      //    "APPLICATION_WEB_PROXY_BASE" -> "/proxy/application_1611680479488_0001"
      //    "SPARK_USER" -> "henghe"
      //    "CLASSPATH" -> "$PWD:$PWD/__spark_conf__:$PWD/__spark_libs__/*:$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$PWD/__spark_conf__/__hadoop_conf__"
      //    "PYTHONHASHSEED" -> "0"
      //    "APP_SUBMIT_TIME_ENV" -> "1611680611847"
      Map<String, String> environment = expandAllEnvironmentVars( launchContext, containerLogDir);
      // /////////////////////////// End of variable expansion
      // ****************   变量解析结束   ****************


      // 通过 NodeManger 将 track 变量加载到环境变量中
      // Use this to track variables that are added to the environment by nm.
      LinkedHashSet<String> nmEnvVars = new LinkedHashSet<String>();

      // 获取本地文件系统上下文.
      // defaultFS : LocalFs@9710
      // workingDir : file:/opt/workspace/apache/hadoop-3.2.1-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager
      FileContext lfs = FileContext.getLocalFSFileContext();



      // 获取私有Container 启动脚本路径 [ nmPrivateContainerScriptPath ]
      // /opt/tools/hadoop-3.2.1/local-dirs/nmPrivate/application_1611681788558_0001/container_1611681788558_0001_01_000001/launch_container.sh
      Path nmPrivateContainerScriptPath = dirsHandler.getLocalPathForWrite( getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR  + CONTAINER_SCRIPT);


      // 获取私有 Tokens 脚本路径 [ nmPrivateTokensPath ]
      // /opt/tools/hadoop-3.2.1/local-dirs/nmPrivate/application_1611681788558_0001/container_1611681788558_0001_01_000001/container_1611681788558_0001_01_000001.tokens
      Path nmPrivateTokensPath = dirsHandler.getLocalPathForWrite( getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR   + String.format(ContainerLocalizer.TOKEN_FILE_NAME_FMT,  containerIdStr));


      // class jar 路径 [ nmPrivateClasspathJarDir ]
      // /opt/tools/hadoop-3.2.1/local-dirs/nmPrivate/application_1611681788558_0001/container_1611681788558_0001_01_000001
      Path nmPrivateClasspathJarDir = dirsHandler.getLocalPathForWrite( getContainerPrivateDir(appIdStr, containerIdStr));

      // Select the working directory for the container
      // 获取container的工作空间路径 [ containerWorkDir ]
      // /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache/application_1611681788558_0001/container_1611681788558_0001_01_000001
      Path containerWorkDir = deriveContainerWorkDir();


      // 记录容器工作目录
      recordContainerWorkDir(containerID, containerWorkDir.toString());

      // pid文件相对路径 [pidFileSubpath]
      // nmPrivate/application_1611680479488_0001/container_1611680479488_0001_01_000001//container_1611680479488_0001_01_000001.pid
      String pidFileSubpath = getPidFileSubpath(appIdStr, containerIdStr);

      // pid文件绝对路径 [pidFilePath]
      // pid文件应该在nm private dir中，这样用户就不能访问它
      // /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/nmPrivate/application_1611680479488_0001/container_1611680479488_0001_01_000001/container_1611680479488_0001_01_000001.pid
      // pid file should be in nm private dir so that it is not accessible by users
      pidFilePath = dirsHandler.getLocalPathForWrite(pidFileSubpath);

      // 本地目录 [localDirs]
      //  /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe
      List<String> localDirs = dirsHandler.getLocalDirs();

      // /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe
      List<String> localDirsForRead = dirsHandler.getLocalDirsForRead();

      // 日志目录
      // logDirs :  /opt/tools/hadoop-3.2.1/logs/userlogs
      List<String> logDirs = dirsHandler.getLogDirs();

      // 文件缓存目录
      // /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/filecache
      List<String> filecacheDirs = getNMFilecacheDirs(localDirsForRead);

      // 用户本地目录
      // /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/
      List<String> userLocalDirs = getUserLocalDirs(localDirs);

      // containerLocalDirs 容器目录
      // /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache/application_1611681788558_0001/
      List<String> containerLocalDirs = getContainerLocalDirs(localDirs);

      // 容器日志目录
      // /opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001
      List<String> containerLogDirs = getContainerLogDirs(logDirs);

      // 用户文件缓存目录
      // /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/filecache
      List<String> userFilecacheDirs = getUserFilecacheDirs(localDirsForRead);

      // application 本地目录
      // /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache/application_1611681788558_0001
      List<String> applicationLocalDirs = getApplicationLocalDirs(localDirs, appIdStr);



      // 磁盘检查
      if (!dirsHandler.areDisksHealthy()) {
        ret = ContainerExitStatus.DISKS_FAILED;
        throw new IOException("Most of the disks failed. "
            + dirsHandler.getDisksHealthReport(false));
      }



      List<Path> appDirs = new ArrayList<Path>(localDirs.size());


      for (String localDir : localDirs) {

        // usercache : /opt/tools/hadoop-3.2.1/local-dirs/usercache
        Path usersdir = new Path(localDir, ContainerLocalizer.USERCACHE);


        // 用户工作空间 : /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe
        Path userdir = new Path(usersdir, user);

        // appcache : /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache
        Path appsdir = new Path(userdir, ContainerLocalizer.APPCACHE);
        appDirs.add(new Path(appsdir, appIdStr));
      }





      // 设置token位置..
      // HADOOP_TOKEN_FILE_LOCATION -> /opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache/application_1611681788558_0001/container_1611681788558_0001_01_000001/container_tokens

      addToEnvMap(environment, nmEnvVars,  ApplicationConstants.CONTAINER_TOKEN_FILE_ENV_NAME, new Path(containerWorkDir, FINAL_CONTAINER_TOKENS_FILE).toUri().getPath());





      // 在私有nmPrivate 空间中   写入container 脚本
      // /////////// Write out the container-script in the nmPrivate space.




      try (DataOutputStream containerScriptOutStream =  lfs.create(nmPrivateContainerScriptPath, EnumSet.of(CREATE, OVERWRITE))) {





        // 整理 container's environment
        // Sanitize the container's environment
        sanitizeEnv(environment, containerWorkDir, appDirs, userLocalDirs, containerLogDirs, localResources, nmPrivateClasspathJarDir,  nmEnvVars);



        // 准备环境
        prepareContainer(localResources, containerLocalDirs);




        // 输出环境变量.
        // Write out the environment
        exec.writeLaunchEnv(containerScriptOutStream, environment,
            localResources, launchContext.getCommands(),
            containerLogDir, user, nmEnvVars);



      }
      // /////////// End of writing out container-script






      // /////////// Write out the container-tokens in the nmPrivate space.
      try (DataOutputStream tokensOutStream = lfs.create(nmPrivateTokensPath, EnumSet.of(CREATE, OVERWRITE))) {
        Credentials creds = container.getCredentials();
        creds.writeTokenStorageToStream(tokensOutStream);
      }
      // /////////// End of writing out container-tokens





      //    启动Container
      ret = launchContainer(new ContainerStartContext.Builder()
          .setContainer(container)
          .setLocalizedResources(localResources)
          .setNmPrivateContainerScriptPath(nmPrivateContainerScriptPath)
          .setNmPrivateTokensPath(nmPrivateTokensPath)
          .setUser(user)
          .setAppId(appIdStr)
          .setContainerWorkDir(containerWorkDir)
          .setLocalDirs(localDirs)
          .setLogDirs(logDirs)
          .setFilecacheDirs(filecacheDirs)
          .setUserLocalDirs(userLocalDirs)
          .setContainerLocalDirs(containerLocalDirs)
          .setContainerLogDirs(containerLogDirs)
          .setUserFilecacheDirs(userFilecacheDirs)
          .setApplicationLocalDirs(applicationLocalDirs).build());




    } catch (ConfigurationException e) {
      LOG.error("Failed to launch container due to configuration error.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerID, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      // Mark the node as unhealthy
      context.getNodeStatusUpdater().reportException(e);
      return ret;
    } catch (Throwable e) {
      LOG.warn("Failed to launch container.", e);
      dispatcher.getEventHandler().handle(new ContainerExitEvent(
          containerID, ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
          e.getMessage()));
      return ret;
    } finally {
      // 设置执行结果.
      setContainerCompletedStatus(ret);
    }
    // 根据返回码处理请求.
    handleContainerExitCode(ret, containerLogDir);
    return ret;
  }

  private Path deriveContainerWorkDir() throws IOException {

    final String containerWorkDirPath =
        ContainerLocalizer.USERCACHE +
        Path.SEPARATOR +
        container.getUser() +
        Path.SEPARATOR +
        ContainerLocalizer.APPCACHE +
        Path.SEPARATOR +
        app.getAppId().toString() +
        Path.SEPARATOR +
        container.getContainerId().toString();

    final Path containerWorkDir =
        dirsHandler.getLocalPathForWrite(
          containerWorkDirPath,
          LocalDirAllocator.SIZE_UNKNOWN, false);

    return containerWorkDir;
  }

  private void prepareContainer(Map<Path, List<String>> localResources,
      List<String> containerLocalDirs) throws IOException {

    exec.prepareContainer(new ContainerPrepareContext.Builder()
        .setContainer(container)
        .setLocalizedResources(localResources)
        .setUser(container.getUser())
        .setContainerLocalDirs(containerLocalDirs)
        .setCommands(container.getLaunchContext().getCommands())
        .build());
  }

  protected boolean validateContainerState() {
    // CONTAINER_KILLED_ON_REQUEST should not be missed if the container
    // is already at KILLING
    if (container.getContainerState() == ContainerState.KILLING) {
      dispatcher.getEventHandler().handle(
          new ContainerExitEvent(container.getContainerId(),
              ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
              Shell.WINDOWS ? ExitCode.FORCE_KILLED.getExitCode() :
                  ExitCode.TERMINATED.getExitCode(),
              "Container terminated before launch."));
      return false;
    }

    return true;
  }

  protected List<String> getContainerLogDirs(List<String> logDirs) {
    List<String> containerLogDirs = new ArrayList<>(logDirs.size());
    String appIdStr = app.getAppId().toString();
    String containerIdStr = container.getContainerId().toString();
    String relativeContainerLogDir = ContainerLaunch
        .getRelativeContainerLogDir(appIdStr, containerIdStr);

    for (String logDir : logDirs) {
      containerLogDirs.add(logDir + Path.SEPARATOR + relativeContainerLogDir);
    }

    return containerLogDirs;
  }

  protected List<String> getContainerLocalDirs(List<String> localDirs) {
    List<String> containerLocalDirs = new ArrayList<>(localDirs.size());
    String user = container.getUser();
    String appIdStr = app.getAppId().toString();
    String relativeContainerLocalDir = ContainerLocalizer.USERCACHE
        + Path.SEPARATOR + user + Path.SEPARATOR + ContainerLocalizer.APPCACHE
        + Path.SEPARATOR + appIdStr + Path.SEPARATOR;

    for (String localDir : localDirs) {
      containerLocalDirs.add(localDir + Path.SEPARATOR
          + relativeContainerLocalDir);
    }

    return containerLocalDirs;
  }

  protected List<String> getUserLocalDirs(List<String> localDirs) {
    List<String> userLocalDirs = new ArrayList<>(localDirs.size());
    String user = container.getUser();

    for (String localDir : localDirs) {
      String userLocalDir = localDir + Path.SEPARATOR +
          ContainerLocalizer.USERCACHE + Path.SEPARATOR + user
          + Path.SEPARATOR;

      userLocalDirs.add(userLocalDir);
    }

    return userLocalDirs;
  }

  protected List<String> getNMFilecacheDirs(List<String> localDirs) {
    List<String> filecacheDirs = new ArrayList<>(localDirs.size());

    for (String localDir : localDirs) {
      String filecacheDir = localDir + Path.SEPARATOR +
          ContainerLocalizer.FILECACHE;

      filecacheDirs.add(filecacheDir);
    }

    return filecacheDirs;
  }

  protected List<String> getUserFilecacheDirs(List<String> localDirs) {
    List<String> userFilecacheDirs = new ArrayList<>(localDirs.size());
    String user = container.getUser();
    for (String localDir : localDirs) {
      String userFilecacheDir = localDir + Path.SEPARATOR +
          ContainerLocalizer.USERCACHE + Path.SEPARATOR + user
          + Path.SEPARATOR + ContainerLocalizer.FILECACHE;
      userFilecacheDirs.add(userFilecacheDir);
    }
    return userFilecacheDirs;
  }

  protected List<String> getApplicationLocalDirs(List<String> localDirs,
      String appIdStr) {
    List<String> applicationLocalDirs = new ArrayList<>(localDirs.size());
    String user = container.getUser();
    for (String localDir : localDirs) {
      String appLocalDir = localDir + Path.SEPARATOR +
          ContainerLocalizer.USERCACHE + Path.SEPARATOR + user
          + Path.SEPARATOR + ContainerLocalizer.APPCACHE
          + Path.SEPARATOR + appIdStr;
      applicationLocalDirs.add(appLocalDir);
    }
    return applicationLocalDirs;
  }

  protected Map<Path, List<String>> getLocalizedResources()
      throws YarnException {
    Map<Path, List<String>> localResources = container.getLocalizedResources();
    if (localResources == null) {
      throw RPCUtil.getRemoteException(
          "Unable to get local resources when Container " + container
              + " is at " + container.getContainerState());
    }
    return localResources;
  }

  protected int launchContainer(ContainerStartContext ctx) throws IOException, ConfigurationException {



    int launchPrep = prepareForLaunch(ctx);



    if (launchPrep == 0) {
      launchLock.lock();
      try {




        return exec.launchContainer(ctx);
      } finally {
        launchLock.unlock();
      }
    }



    return launchPrep;
  }

  protected int relaunchContainer(ContainerStartContext ctx)
      throws IOException, ConfigurationException {
    int launchPrep = prepareForLaunch(ctx);
    if (launchPrep == 0) {
      launchLock.lock();
      try {
        return exec.relaunchContainer(ctx);
      } finally {
        launchLock.unlock();
      }
    }
    return launchPrep;
  }

  void reapContainer() throws IOException {
    launchLock.lock();
    try {
      // Reap the container
      boolean result = exec.reapContainer(
          new ContainerReapContext.Builder()
              .setContainer(container)
              .setUser(container.getUser())
              .build());
      if (!result) {
        throw new IOException("Reap container failed for container " +
            container.getContainerId());
      }
      cleanupContainerFiles(getContainerWorkDir());
    } finally {
      launchLock.unlock();
    }
  }

  protected int prepareForLaunch(ContainerStartContext ctx) throws IOException {
    ContainerId containerId = container.getContainerId();
    if (container.isMarkedForKilling()) {
      LOG.info("Container " + containerId + " not launched as it has already " + "been marked for Killing");
      this.killedBeforeStart = true;

      return ExitCode.TERMINATED.getExitCode();
    }




    // LaunchContainer是一个阻塞调用。
    // 执行到这里意味着container已经启动,所以我们发送这个事件.


    // LaunchContainer is a blocking call.
    // We are here almost means the  container is launched, so send out the event.



    dispatcher.getEventHandler().handle(new ContainerEvent( containerId,  ContainerEventType.CONTAINER_LAUNCHED));



    context.getNMStateStore().storeContainerLaunched(containerId);


    // 检查container是否被通知 killed
    // Check if the container is signalled to be killed.
    if (!containerAlreadyLaunched.compareAndSet(false, true)) {


      LOG.info("Container " + containerId + " not launched as " + "cleanup already called");



      return ExitCode.TERMINATED.getExitCode();
    } else {




      exec.activateContainer(containerId, pidFilePath);
    }


    return ExitCode.SUCCESS.getExitCode();
  }

  protected void setContainerCompletedStatus(int exitCode) {
    ContainerId containerId = container.getContainerId();


    completed.set(true);


    exec.deactivateContainer(containerId);


    try {


      if (!container.shouldRetry(exitCode)) {


        context.getNMStateStore().storeContainerCompleted(containerId,  exitCode);


      }


    } catch (IOException e) {
      LOG.error("Unable to set exit code for container " + containerId);
    }
  }

  protected void handleContainerExitCode(int exitCode, Path containerLogDir) {
    ContainerId containerId = container.getContainerId();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Container " + containerId + " completed with exit code "
          + exitCode);
    }

    StringBuilder diagnosticInfo =  new StringBuilder("Container exited with a non-zero exit code ");


    diagnosticInfo.append(exitCode);


    diagnosticInfo.append(". ");


    if (exitCode == ExitCode.FORCE_KILLED.getExitCode()
        || exitCode == ExitCode.TERMINATED.getExitCode()) {
      // If the process was killed, Send container_cleanedup_after_kill and
      // just break out of this method.
      dispatcher.getEventHandler().handle(
          new ContainerExitEvent(containerId,
              ContainerEventType.CONTAINER_KILLED_ON_REQUEST, exitCode,
              diagnosticInfo.toString()));
    } else if (exitCode != 0) {



      handleContainerExitWithFailure(containerId, exitCode, containerLogDir,  diagnosticInfo);
    } else {


      LOG.info("Container " + containerId + " succeeded ");



      dispatcher.getEventHandler().handle(
          new ContainerEvent(containerId,
              ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
    }
  }

  /**
   * Tries to tail and fetch TAIL_SIZE_IN_BYTES of data from the error log.
   * ErrorLog filename is not fixed and depends upon app, hence file name
   * pattern is used.
   *
   * @param containerID
   * @param ret
   * @param containerLogDir
   * @param diagnosticInfo
   */
  protected void handleContainerExitWithFailure(ContainerId containerID,
      int ret, Path containerLogDir, StringBuilder diagnosticInfo) {
    LOG.warn("Container launch failed : " + diagnosticInfo.toString());

    FileSystem fileSystem = null;
    long tailSizeInBytes =
        conf.getLong(YarnConfiguration.NM_CONTAINER_STDERR_BYTES,
            YarnConfiguration.DEFAULT_NM_CONTAINER_STDERR_BYTES);

    // Append container prelaunch stderr to diagnostics
    try {
      fileSystem = FileSystem.getLocal(conf).getRaw();
      FileStatus preLaunchErrorFileStatus = fileSystem
          .getFileStatus(new Path(containerLogDir, ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR));

      Path errorFile = preLaunchErrorFileStatus.getPath();
      long fileSize = preLaunchErrorFileStatus.getLen();

      diagnosticInfo.append("Error file: ")
          .append(ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR).append(".\n");
      ;

      byte[] tailBuffer = tailFile(errorFile, fileSize, tailSizeInBytes);
      diagnosticInfo.append("Last ").append(tailSizeInBytes)
          .append(" bytes of ").append(errorFile.getName()).append(" :\n")
          .append(new String(tailBuffer, StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.error("Failed to get tail of the container's prelaunch error log file", e);
    }

    // Append container stderr to diagnostics
    String errorFileNamePattern =
        conf.get(YarnConfiguration.NM_CONTAINER_STDERR_PATTERN,
            YarnConfiguration.DEFAULT_NM_CONTAINER_STDERR_PATTERN);

    try {
      if (fileSystem == null) {
        fileSystem = FileSystem.getLocal(conf).getRaw();
      }
      FileStatus[] errorFileStatuses = fileSystem
          .globStatus(new Path(containerLogDir, errorFileNamePattern));
      if (errorFileStatuses != null && errorFileStatuses.length != 0) {
        Path errorFile = errorFileStatuses[0].getPath();
        long fileSize = errorFileStatuses[0].getLen();

        // if more than one file matches the stderr pattern, take the latest
        // modified file, and also append the file names in the diagnosticInfo
        if (errorFileStatuses.length > 1) {
          String[] errorFileNames = new String[errorFileStatuses.length];
          long latestModifiedTime = errorFileStatuses[0].getModificationTime();
          errorFileNames[0] = errorFileStatuses[0].getPath().getName();
          for (int i = 1; i < errorFileStatuses.length; i++) {
            errorFileNames[i] = errorFileStatuses[i].getPath().getName();
            if (errorFileStatuses[i]
                .getModificationTime() > latestModifiedTime) {
              latestModifiedTime = errorFileStatuses[i].getModificationTime();
              errorFile = errorFileStatuses[i].getPath();
              fileSize = errorFileStatuses[i].getLen();
            }
          }
          diagnosticInfo.append("Error files: ")
              .append(StringUtils.join(", ", errorFileNames)).append(".\n");
        }

        byte[] tailBuffer = tailFile(errorFile, fileSize, tailSizeInBytes);
        String tailBufferMsg = new String(tailBuffer, StandardCharsets.UTF_8);
        diagnosticInfo.append("Last ").append(tailSizeInBytes)
            .append(" bytes of ").append(errorFile.getName()).append(" :\n")
            .append(tailBufferMsg).append("\n")
            .append(analysesErrorMsgOfContainerExitWithFailure(tailBufferMsg));

      }
    } catch (IOException e) {
      LOG.error("Failed to get tail of the container's error log file", e);
    }
    this.dispatcher.getEventHandler()
        .handle(new ContainerExitEvent(containerID,
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE, ret,
            diagnosticInfo.toString()));
  }

  private byte[] tailFile(Path filePath, long fileSize, long tailSizeInBytes) throws IOException {
    FSDataInputStream errorFileIS = null;
    FileSystem fileSystem = FileSystem.getLocal(conf).getRaw();
    try {
      long startPosition =
          (fileSize < tailSizeInBytes) ? 0 : fileSize - tailSizeInBytes;
      int bufferSize =
          (int) ((fileSize < tailSizeInBytes) ? fileSize : tailSizeInBytes);
      byte[] tailBuffer = new byte[bufferSize];
      errorFileIS = fileSystem.open(filePath);
      errorFileIS.readFully(startPosition, tailBuffer);
      return tailBuffer;
    } finally {
      IOUtils.cleanupWithLogger(LOG, errorFileIS);
    }
  }

  private String analysesErrorMsgOfContainerExitWithFailure(String errorMsg) {
    StringBuilder analysis = new StringBuilder();
    if (errorMsg.indexOf("Error: Could not find or load main class"
        + " org.apache.hadoop.mapreduce") != -1) {
      analysis.append("Please check whether your etc/hadoop/mapred-site.xml "
          + "contains the below configuration:\n");
      analysis.append("<property>\n")
          .append("  <name>yarn.app.mapreduce.am.env</name>\n")
          .append("  <value>HADOOP_MAPRED_HOME=${full path of your hadoop "
              + "distribution directory}</value>\n")
          .append("</property>\n<property>\n")
          .append("  <name>mapreduce.map.env</name>\n")
          .append("  <value>HADOOP_MAPRED_HOME=${full path of your hadoop "
              + "distribution directory}</value>\n")
          .append("</property>\n<property>\n")
          .append("  <name>mapreduce.reduce.env</name>\n")
          .append("  <value>HADOOP_MAPRED_HOME=${full path of your hadoop "
              + "distribution directory}</value>\n")
          .append("</property>\n");
    }
    return analysis.toString();
  }

  protected String getPidFileSubpath(String appIdStr, String containerIdStr) {
    return getContainerPrivateDir(appIdStr, containerIdStr) + Path.SEPARATOR
        + String.format(ContainerLaunch.PID_FILE_NAME_FMT, containerIdStr);
  }

  /**
   * Send a signal to the container.
   *
   *
   * @throws IOException
   */
  public void signalContainer(SignalContainerCommand command)
      throws IOException {
    ContainerId containerId =  container.getContainerTokenIdentifier().getContainerID();
    String containerIdStr = containerId.toString();
    String user = container.getUser();
    Signal signal = translateCommandToSignal(command);
    if (signal.equals(Signal.NULL)) {
      LOG.info("ignore signal command " + command);
      return;
    }

    LOG.info("Sending signal " + command + " to container " + containerIdStr);

    boolean alreadyLaunched =
        !containerAlreadyLaunched.compareAndSet(false, true);
    if (!alreadyLaunched) {
      LOG.info("Container " + containerIdStr + " not launched."
          + " Not sending the signal");
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting pid for container " + containerIdStr
          + " to send signal to from pid file "
          + (pidFilePath != null ? pidFilePath.toString() : "null"));
    }

    try {
      // get process id from pid file if available
      // else if shell is still active, get it from the shell
      String processId = getContainerPid();
      if (processId != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Sending signal to pid " + processId
              + " as user " + user
              + " for container " + containerIdStr);
        }

        boolean result = exec.signalContainer(
            new ContainerSignalContext.Builder()
                .setContainer(container)
                .setUser(user)
                .setPid(processId)
                .setSignal(signal)
                .build());

        String diagnostics = "Sent signal " + command
            + " (" + signal + ") to pid " + processId
            + " as user " + user
            + " for container " + containerIdStr
            + ", result=" + (result ? "success" : "failed");
        LOG.info(diagnostics);

        dispatcher.getEventHandler().handle(
            new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
      }
    } catch (Exception e) {
      String message =
          "Exception when sending signal to container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.warn(message);
    }
  }

  @VisibleForTesting
  public static Signal translateCommandToSignal(
      SignalContainerCommand command) {
    Signal signal = Signal.NULL;
    switch (command) {
      case OUTPUT_THREAD_DUMP:
        // TODO for windows support.
        signal = Shell.WINDOWS ? Signal.NULL: Signal.QUIT;
        break;
      case GRACEFUL_SHUTDOWN:
        signal = Signal.TERM;
        break;
      case FORCEFUL_SHUTDOWN:
        signal = Signal.KILL;
        break;
    }
    return signal;
  }

  /**
   * Pause the container.
   * Cancels the launch if the container isn't launched yet. Otherwise asks the
   * executor to pause the container.
   * @throws IOException in case of errors.
   */
  public void pauseContainer() throws IOException {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    LOG.info("Pausing the container " + containerIdStr);

    // The pause event is only handled if the container is in the running state
    // (the container state machine), so we don't check for
    // shouldLaunchContainer over here

    if (!shouldPauseContainer.compareAndSet(false, true)) {
      LOG.info("Container " + containerId + " not paused as "
          + "resume already called");
      return;
    }

    try {
      // Pause the container
      exec.pauseContainer(container);

      // PauseContainer is a blocking call. We are here almost means the
      // container is paused, so send out the event.
      dispatcher.getEventHandler().handle(new ContainerEvent(
          containerId,
          ContainerEventType.CONTAINER_PAUSED));

      try {
        this.context.getNMStateStore().storeContainerPaused(
            container.getContainerId());
      } catch (IOException e) {
        LOG.warn("Could not store container [" + container.getContainerId()
            + "] state. The Container has been paused.", e);
      }
    } catch (Exception e) {
      String message =
          "Exception when trying to pause container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.info(message);
      container.handle(new ContainerKillEvent(container.getContainerId(),
          ContainerExitStatus.PREEMPTED, "Container preempted as there was "
          + " an exception in pausing it."));
    }
  }

  /**
   * Resume the container.
   * Cancels the launch if the container isn't launched yet. Otherwise asks the
   * executor to pause the container.
   * @throws IOException in case of error.
   */
  public void resumeContainer() throws IOException {
    ContainerId containerId = container.getContainerId();
    String containerIdStr = containerId.toString();
    LOG.info("Resuming the container " + containerIdStr);

    // The resume event is only handled if the container is in a paused state
    // so we don't check for the launched flag here.

    // paused flag will be set to true if process already paused
    boolean alreadyPaused = !shouldPauseContainer.compareAndSet(false, true);
    if (!alreadyPaused) {
      LOG.info("Container " + containerIdStr + " not paused."
          + " No resume necessary");
      return;
    }

    // If the container has already started
    try {
      exec.resumeContainer(container);
      // ResumeContainer is a blocking call. We are here almost means the
      // container is resumed, so send out the event.
      dispatcher.getEventHandler().handle(new ContainerEvent(
          containerId,
          ContainerEventType.CONTAINER_RESUMED));

      try {
        this.context.getNMStateStore().removeContainerPaused(
            container.getContainerId());
      } catch (IOException e) {
        LOG.warn("Could not store container [" + container.getContainerId()
            + "] state. The Container has been resumed.", e);
      }
    } catch (Exception e) {
      String message =
          "Exception when trying to resume container " + containerIdStr
              + ": " + StringUtils.stringifyException(e);
      LOG.info(message);
      container.handle(new ContainerKillEvent(container.getContainerId(),
          ContainerExitStatus.PREEMPTED, "Container preempted as there was "
          + " an exception in pausing it."));
    }
  }

  /**
   * Loop through for a time-bounded interval waiting to
   * read the process id from a file generated by a running process.
   * @return Process ID; null when pidFilePath is null
   * @throws Exception
   */
  String getContainerPid() throws Exception {
    if (pidFilePath == null) {
      return null;
    }
    String containerIdStr = 
        container.getContainerId().toString();
    String processId;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Accessing pid for container " + containerIdStr
          + " from pid file " + pidFilePath);
    }
    int sleepCounter = 0;
    final int sleepInterval = 100;

    // loop waiting for pid file to show up 
    // until our timer expires in which case we admit defeat
    while (true) {
      processId = ProcessIdFileReader.getProcessId(pidFilePath);
      if (processId != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Got pid " + processId + " for container " + containerIdStr);
        }
        break;
      }
      else if ((sleepCounter*sleepInterval) > maxKillWaitTime) {
        LOG.info("Could not get pid for " + containerIdStr
        		+ ". Waited for " + maxKillWaitTime + " ms.");
        break;
      }
      else {
        ++sleepCounter;
        Thread.sleep(sleepInterval);
      }
    }
    return processId;
  }

  public static String getRelativeContainerLogDir(String appIdStr,
      String containerIdStr) {
    return appIdStr + Path.SEPARATOR + containerIdStr;
  }

  protected String getContainerPrivateDir(String appIdStr,
      String containerIdStr) {
    return getAppPrivateDir(appIdStr) + Path.SEPARATOR + containerIdStr
        + Path.SEPARATOR;
  }

  private String getAppPrivateDir(String appIdStr) {
    return ResourceLocalizationService.NM_PRIVATE_DIR + Path.SEPARATOR
        + appIdStr;
  }

  Context getContext() {
    return context;
  }

  public static abstract class ShellScriptBuilder {
    public static ShellScriptBuilder create() {
      return create(Shell.osType);
    }

    @VisibleForTesting
    public static ShellScriptBuilder create(Shell.OSType osType) {
      return (osType == Shell.OSType.OS_TYPE_WIN) ?
          new WindowsShellScriptBuilder() :
          new UnixShellScriptBuilder();
    }

    private static final String LINE_SEPARATOR =
        System.getProperty("line.separator");
    private final StringBuilder sb = new StringBuilder();

    public abstract void command(List<String> command) throws IOException;

    protected static final String ENV_PRELAUNCH_STDOUT = "PRELAUNCH_OUT";
    protected static final String ENV_PRELAUNCH_STDERR = "PRELAUNCH_ERR";

    private boolean redirectStdOut = false;
    private boolean redirectStdErr = false;

    /**
     * Set stdout for the shell script
     * @param stdoutDir stdout must be an absolute path
     * @param stdOutFile stdout file name
     * @throws IOException thrown when stdout path is not absolute
     */
    public final void stdout(Path stdoutDir, String stdOutFile) throws IOException {
      if (!stdoutDir.isAbsolute()) {
        throw new IOException("Stdout path must be absolute");
      }
      redirectStdOut = true;
      setStdOut(new Path(stdoutDir, stdOutFile));
    }

    /**
     * Set stderr for the shell script
     * @param stderrDir stderr must be an absolute path
     * @param stdErrFile stderr file name
     * @throws IOException thrown when stderr path is not absolute
     */
    public final void stderr(Path stderrDir, String stdErrFile) throws IOException {
      if (!stderrDir.isAbsolute()) {
        throw new IOException("Stdout path must be absolute");
      }
      redirectStdErr = true;
      setStdErr(new Path(stderrDir, stdErrFile));
    }

    protected abstract void setStdOut(Path stdout) throws IOException;

    protected abstract void setStdErr(Path stdout) throws IOException;

    public abstract void env(String key, String value) throws IOException;

    public abstract void whitelistedEnv(String key, String value)
        throws IOException;

    public abstract void echo(String echoStr) throws IOException;

    public final void symlink(Path src, Path dst) throws IOException {
      if (!src.isAbsolute()) {
        throw new IOException("Source must be absolute");
      }
      if (dst.isAbsolute()) {
        throw new IOException("Destination must be relative");
      }
      if (dst.toUri().getPath().indexOf('/') != -1) {
        mkdir(dst.getParent());
      }
      link(src, dst);
    }

    /**
     * Method to copy files that are useful for debugging container failures.
     * This method will be called by ContainerExecutor when setting up the
     * container launch script. The method should take care to make sure files
     * are read-able by the yarn user if the files are to undergo
     * log-aggregation.
     * @param src path to the source file
     * @param dst path to the destination file - should be absolute
     * @throws IOException
     */
    public abstract void copyDebugInformation(Path src, Path dst)
        throws IOException;

    /**
     * Method to dump debug information to a target file. This method will
     * be called by ContainerExecutor when setting up the container launch
     * script.
     * @param output the file to which debug information is to be written
     * @throws IOException
     */
    public abstract void listDebugInformation(Path output) throws IOException;

    @Override
    public String toString() {
      return sb.toString();
    }

    public final void write(PrintStream out) throws IOException {
      out.append(sb);
    }

    protected final void buildCommand(String... command) {
      for (String s : command) {
        sb.append(s);
      }
    }

    protected final void linebreak(String... command) {
      sb.append(LINE_SEPARATOR);
    }

    protected final void line(String... command) {
      buildCommand(command);
      linebreak();
    }

    public void setExitOnFailure() {
      // Dummy implementation
    }

    protected abstract void link(Path src, Path dst) throws IOException;

    protected abstract void mkdir(Path path) throws IOException;

    boolean doRedirectStdOut() {
      return redirectStdOut;
    }

    boolean doRedirectStdErr() {
      return redirectStdErr;
    }

    /**
     * Parse an environment value and returns all environment keys it uses.
     * @param envVal an environment variable's value
     * @return all environment variable names used in <code>envVal</code>.
     */
    public Set<String> getEnvDependencies(final String envVal) {
      return Collections.emptySet();
    }

    /**
     * Returns a dependency ordered version of <code>envs</code>. Does not alter
     * input <code>envs</code> map.
     * @param envs environment map
     * @return a dependency ordered version of <code>envs</code>
     */
    public final Map<String, String> orderEnvByDependencies(
        Map<String, String> envs) {
      if (envs == null || envs.size() < 2) {
        return envs;
      }
      final Map<String, String> ordered = new LinkedHashMap<String, String>();
      class Env {
        private boolean resolved = false;
        private final Collection<Env> deps = new ArrayList<>();
        private final String name;
        private final String value;
        Env(String name, String value) {
          this.name = name;
          this.value = value;
        }
        void resolve() {
          resolved = true;
          for (Env dep : deps) {
            if (!dep.resolved) {
              dep.resolve();
            }
          }
          ordered.put(name, value);
        }
      }
      final Map<String, Env> singletons = new HashMap<>();
      for (Map.Entry<String, String> e : envs.entrySet()) {
        Env env = singletons.get(e.getKey());
        if (env == null) {
          env = new Env(e.getKey(), e.getValue());
          singletons.put(env.name, env);
        }
        for (String depStr : getEnvDependencies(env.value)) {
          if (!envs.containsKey(depStr)) {
            continue;
          }
          Env depEnv = singletons.get(depStr);
          if (depEnv == null) {
            depEnv = new Env(depStr, envs.get(depStr));
            singletons.put(depStr, depEnv);
          }
          env.deps.add(depEnv);
        }
      }
      for (Env env : singletons.values()) {
        if (!env.resolved) {
          env.resolve();
        }
      }
      return ordered;
    }
  }

  private static final class UnixShellScriptBuilder extends ShellScriptBuilder {
    @SuppressWarnings("unused")
    private void errorCheck() {
      line("hadoop_shell_errorcode=$?");
      line("if [[ \"$hadoop_shell_errorcode\" -ne 0 ]]");
      line("then");
      line("  exit $hadoop_shell_errorcode");
      line("fi");
    }

    public UnixShellScriptBuilder() {
      line("#!/bin/bash");
      line();
    }

    @Override
    public void command(List<String> command) {
      line("exec /bin/bash -c \"", StringUtils.join(" ", command), "\"");
    }

    @Override
    public void setStdOut(final Path stdout) throws IOException {
      line("export ", ENV_PRELAUNCH_STDOUT, "=\"", stdout.toString(), "\"");
      // tee is needed for DefaultContainerExecutor error propagation to stdout
      // Close stdout of subprocess to prevent it from writing to the stdout file
      line("exec >\"${" + ENV_PRELAUNCH_STDOUT + "}\"");
    }

    @Override
    public void setStdErr(final Path stderr) throws IOException {
      line("export ", ENV_PRELAUNCH_STDERR, "=\"", stderr.toString(), "\"");
      // tee is needed for DefaultContainerExecutor error propagation to stderr
      // Close stdout of subprocess to prevent it from writing to the stdout file
      line("exec 2>\"${" + ENV_PRELAUNCH_STDERR + "}\"");
    }

    @Override
    public void env(String key, String value) throws IOException {
      line("export ", key, "=\"", value, "\"");
    }

    @Override
    public void whitelistedEnv(String key, String value) throws IOException {
      line("export ", key, "=${", key, ":-", "\"", value, "\"}");
    }

    @Override
    public void echo(final String echoStr) throws IOException {
      line("echo \"" + echoStr + "\"");
    }

    @Override
    protected void link(Path src, Path dst) throws IOException {
      line("ln -sf -- \"", src.toUri().getPath(), "\" \"", dst.toString(), "\"");
    }

    @Override
    protected void mkdir(Path path) throws IOException {
      line("mkdir -p ", path.toString());
    }

    @Override
    public void copyDebugInformation(Path src, Path dest) throws IOException {
      line("# Creating copy of launch script");
      line("cp \"", src.toUri().getPath(), "\" \"", dest.toUri().getPath(),
          "\"");
      // set permissions to 640 because we need to be able to run
      // log aggregation in secure mode as well
      if(dest.isAbsolute()) {
        line("chmod 640 \"", dest.toUri().getPath(), "\"");
      }
    }

    @Override
    public void listDebugInformation(Path output) throws  IOException {
      line("# Determining directory contents");
      line("echo \"ls -l:\" 1>\"", output.toString(), "\"");
      line("ls -l 1>>\"", output.toString(), "\"");

      // don't run error check because if there are loops
      // find will exit with an error causing container launch to fail
      // find will follow symlinks outside the work dir if such sylimks exist
      // (like public/app local resources)
      line("echo \"find -L . -maxdepth 5 -ls:\" 1>>\"", output.toString(),
          "\"");
      line("find -L . -maxdepth 5 -ls 1>>\"", output.toString(), "\"");
      line("echo \"broken symlinks(find -L . -maxdepth 5 -type l -ls):\" 1>>\"",
          output.toString(), "\"");
      line("find -L . -maxdepth 5 -type l -ls 1>>\"", output.toString(), "\"");
    }

    @Override
    public void setExitOnFailure() {
      line("set -o pipefail -e");
    }

    /**
     * Parse <code>envVal</code> using bash-like syntax to extract env variables
     * it depends on.
     */
    @Override
    public Set<String> getEnvDependencies(final String envVal) {
      if (envVal == null || envVal.isEmpty()) {
        return Collections.emptySet();
      }
      final Set<String> deps = new HashSet<>();
      // env/whitelistedEnv dump values inside double quotes
      boolean inDoubleQuotes = true;
      char c;
      int i = 0;
      final int len = envVal.length();
      while (i < len) {
        c = envVal.charAt(i);
        if (c == '"') {
          inDoubleQuotes = !inDoubleQuotes;
        } else if (c == '\'' && !inDoubleQuotes) {
          i++;
          // eat until closing simple quote
          while (i < len) {
            c = envVal.charAt(i);
            if (c == '\\') {
              i++;
            }
            if (c == '\'') {
              break;
            }
            i++;
          }
        } else if (c == '\\') {
          i++;
        } else if (c == '$') {
          i++;
          if (i >= len) {
            break;
          }
          c = envVal.charAt(i);
          if (c == '{') { // for ${... bash like syntax
            i++;
            if (i >= len) {
              break;
            }
            c = envVal.charAt(i);
            if (c == '#') { // for ${#... bash array syntax
              i++;
              if (i >= len) {
                break;
              }
            }
          }
          final int start = i;
          while (i < len) {
            c = envVal.charAt(i);
            if (c != '$' && (
                (i == start && Character.isJavaIdentifierStart(c)) ||
                    (i > start && Character.isJavaIdentifierPart(c)))) {
              i++;
            } else {
              break;
            }
          }
          if (i > start) {
            deps.add(envVal.substring(start, i));
          }
        }
        i++;
      }
      return deps;
    }
  }

  private static final class WindowsShellScriptBuilder
      extends ShellScriptBuilder {

    private void errorCheck() {
      line("@if %errorlevel% neq 0 exit /b %errorlevel%");
    }

    private void lineWithLenCheck(String... commands) throws IOException {
      Shell.checkWindowsCommandLineLength(commands);
      line(commands);
    }

    public WindowsShellScriptBuilder() {
      line("@setlocal");
      line();
    }

    @Override
    public void command(List<String> command) throws IOException {
      lineWithLenCheck("@call ", StringUtils.join(" ", command));
      errorCheck();
    }

    //Dummy implementation
    @Override
    protected void setStdOut(final Path stdout) throws IOException {
    }

    //Dummy implementation
    @Override
    protected void setStdErr(final Path stderr) throws IOException {
    }

    @Override
    public void env(String key, String value) throws IOException {
      lineWithLenCheck("@set ", key, "=", value);
      errorCheck();
    }

    @Override
    public void whitelistedEnv(String key, String value) throws IOException {
      env(key, value);
    }

    @Override
    public void echo(final String echoStr) throws IOException {
      lineWithLenCheck("@echo \"", echoStr, "\"");
    }

    @Override
    protected void link(Path src, Path dst) throws IOException {
      File srcFile = new File(src.toUri().getPath());
      String srcFileStr = srcFile.getPath();
      String dstFileStr = new File(dst.toString()).getPath();
      lineWithLenCheck(String.format("@%s symlink \"%s\" \"%s\"",
          Shell.getWinUtilsPath(), dstFileStr, srcFileStr));
      errorCheck();
    }

    @Override
    protected void mkdir(Path path) throws IOException {
      lineWithLenCheck(String.format("@if not exist \"%s\" mkdir \"%s\"",
          path.toString(), path.toString()));
      errorCheck();
    }

    @Override
    public void copyDebugInformation(Path src, Path dest)
        throws IOException {
      // no need to worry about permissions - in secure mode
      // WindowsSecureContainerExecutor will set permissions
      // to allow NM to read the file
      line("rem Creating copy of launch script");
      lineWithLenCheck(String.format("copy \"%s\" \"%s\"", src.toString(),
          dest.toString()));
    }

    @Override
    public void listDebugInformation(Path output) throws IOException {
      line("rem Determining directory contents");
      lineWithLenCheck(
          String.format("@echo \"dir:\" > \"%s\"", output.toString()));
      lineWithLenCheck(String.format("dir >> \"%s\"", output.toString()));
    }

    /**
     * Parse <code>envVal</code> using cmd/bat-like syntax to extract env
     * variables it depends on.
     */
    public Set<String> getEnvDependencies(final String envVal) {
      if (envVal == null || envVal.isEmpty()) {
        return Collections.emptySet();
      }
      final Set<String> deps = new HashSet<>();
      final int len = envVal.length();
      int i = 0;
      while (i < len) {
        i = envVal.indexOf('%', i); // find beginning of variable
        if (i < 0 || i == (len - 1)) {
          break;
        }
        i++;
        // 3 cases: %var%, %var:...% or %%
        final int j = envVal.indexOf('%', i); // find end of variable
        if (j == i) {
          // %% case, just skip it
          i++;
          continue;
        }
        if (j < 0) {
          break; // even %var:...% syntax ends with a %, so j cannot be negative
        }
        final int k = envVal.indexOf(':', i);
        if (k >= 0 && k < j) {
          // %var:...% syntax
          deps.add(envVal.substring(i, k));
        } else {
          // %var% syntax
          deps.add(envVal.substring(i, j));
        }
        i = j + 1;
      }
      return deps;
    }
  }

  private static void addToEnvMap(  Map<String, String> envMap, Set<String> envSet,  String envName, String envValue) {

    Log.info("ContainerLaunch#envMap ==>  key : "+envName+"  value : "+ envValue);
    envMap.put(envName, envValue);
    envSet.add(envName);

  }

  public void sanitizeEnv(Map<String, String> environment, Path pwd,
      List<Path> appDirs, List<String> userLocalDirs, List<String>
      containerLogDirs, Map<Path, List<String>> resources,
      Path nmPrivateClasspathJarDir,
      Set<String> nmVars) throws IOException {



    // Based on discussion in YARN-7654, for ENTRY_POINT enabled
    // docker container, we forward user defined environment variables
    // without node manager environment variables.  This is the reason
    // that we skip sanitizeEnv method.

    // YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE :
    boolean overrideDisable = Boolean.parseBoolean(
        environment.get(
            Environment.
                YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE.
                    name()));



    if (overrideDisable) {


      environment.remove("WORK_DIR");
      return;

    }

    /**
     * Non-modifiable environment variables
     */



    addToEnvMap(environment, nmVars, Environment.CONTAINER_ID.name(), container.getContainerId().toString());



    addToEnvMap(environment, nmVars, Environment.NM_PORT.name(),  String.valueOf(this.context.getNodeId().getPort()));



    addToEnvMap(environment, nmVars, Environment.NM_HOST.name(), this.context.getNodeId().getHost());



    addToEnvMap(environment, nmVars, Environment.NM_HTTP_PORT.name(),  String.valueOf(this.context.getHttpPort()));



    addToEnvMap(environment, nmVars, Environment.LOCAL_DIRS.name(),  StringUtils.join(",", appDirs));



    addToEnvMap(environment, nmVars, Environment.LOCAL_USER_DIRS.name(),  StringUtils.join(",", userLocalDirs));




    addToEnvMap(environment, nmVars, Environment.LOG_DIRS.name(), StringUtils.join(",", containerLogDirs));



    addToEnvMap(environment, nmVars, Environment.USER.name(), container.getUser());



    addToEnvMap(environment, nmVars, Environment.LOGNAME.name(), container.getUser());



    addToEnvMap(environment, nmVars, Environment.HOME.name(),  conf.get(  YarnConfiguration.NM_USER_HOME_DIR,   YarnConfiguration.DEFAULT_NM_USER_HOME_DIR    )  );



    addToEnvMap(environment, nmVars, Environment.PWD.name(), pwd.toString());



    if (!Shell.WINDOWS) {


      addToEnvMap(environment, nmVars, "JVM_PID", "$$");
    }





    // variables here will be forced in, even if the container has specified them.
    String defEnvStr = conf.get(YarnConfiguration.DEFAULT_NM_ADMIN_USER_ENV);



    Apps.setEnvFromInputProperty(environment,
        YarnConfiguration.NM_ADMIN_USER_ENV, defEnvStr, conf,
        File.pathSeparator);



    nmVars.addAll(Apps.getEnvVarsFromInputProperty( YarnConfiguration.NM_ADMIN_USER_ENV, defEnvStr, conf));



    // TODO: Remove Windows check and use this approach on all platforms after additional testing.  See YARN-358.
    if (Shell.WINDOWS) {

      sanitizeWindowsEnv(environment, pwd,
          resources, nmPrivateClasspathJarDir);
    }




    // put AuxiliaryService data to environment
    for (Map.Entry<String, ByteBuffer> meta : containerManager .getAuxServiceMetaData().entrySet()) {

      LOG.info(" key : "+meta.getKey()+"  value :  " + meta.getValue() ) ;




























      AuxiliaryServiceHelper.setServiceDataIntoEnv( meta.getKey(), meta.getValue(), environment);



      nmVars.add(AuxiliaryServiceHelper.getPrefixServiceName(meta.getKey()));
    }
  }

  private void sanitizeWindowsEnv(Map<String, String> environment, Path pwd,
      Map<Path, List<String>> resources, Path nmPrivateClasspathJarDir)
      throws IOException {

    String inputClassPath = environment.get(Environment.CLASSPATH.name());

    if (inputClassPath != null && !inputClassPath.isEmpty()) {

      //On non-windows, localized resources
      //from distcache are available via the classpath as they were placed
      //there but on windows they are not available when the classpath
      //jar is created and so they "are lost" and have to be explicitly
      //added to the classpath instead.  This also means that their position
      //is lost relative to other non-distcache classpath entries which will
      //break things like mapreduce.job.user.classpath.first.  An environment
      //variable can be set to indicate that distcache entries should come
      //first

      boolean preferLocalizedJars = Boolean.parseBoolean(
              environment.get(Environment.CLASSPATH_PREPEND_DISTCACHE.name())
      );

      boolean needsSeparator = false;
      StringBuilder newClassPath = new StringBuilder();
      if (!preferLocalizedJars) {
        newClassPath.append(inputClassPath);
        needsSeparator = true;
      }

      // Localized resources do not exist at the desired paths yet, because the
      // container launch script has not run to create symlinks yet.  This
      // means that FileUtil.createJarWithClassPath can't automatically expand
      // wildcards to separate classpath entries for each file in the manifest.
      // To resolve this, append classpath entries explicitly for each
      // resource.
      for (Map.Entry<Path, List<String>> entry : resources.entrySet()) {
        boolean targetIsDirectory = new File(entry.getKey().toUri().getPath())
                .isDirectory();

        for (String linkName : entry.getValue()) {
          // Append resource.
          if (needsSeparator) {
            newClassPath.append(File.pathSeparator);
          } else {
            needsSeparator = true;
          }
          newClassPath.append(pwd.toString())
                  .append(Path.SEPARATOR).append(linkName);

          // FileUtil.createJarWithClassPath must use File.toURI to convert
          // each file to a URI to write into the manifest's classpath.  For
          // directories, the classpath must have a trailing '/', but
          // File.toURI only appends the trailing '/' if it is a directory that
          // already exists.  To resolve this, add the classpath entries with
          // explicit trailing '/' here for any localized resource that targets
          // a directory.  Then, FileUtil.createJarWithClassPath will guarantee
          // that the resulting entry in the manifest's classpath will have a
          // trailing '/', and thus refer to a directory instead of a file.
          if (targetIsDirectory) {
            newClassPath.append(Path.SEPARATOR);
          }
        }
      }
      if (preferLocalizedJars) {
        if (needsSeparator) {
          newClassPath.append(File.pathSeparator);
        }
        newClassPath.append(inputClassPath);
      }

      // When the container launches, it takes the parent process's environment
      // and then adds/overwrites with the entries from the container launch
      // context.  Do the same thing here for correct substitution of
      // environment variables in the classpath jar manifest.
      Map<String, String> mergedEnv = new HashMap<String, String>(
              System.getenv());
      mergedEnv.putAll(environment);

      // this is hacky and temporary - it's to preserve the windows secure
      // behavior but enable non-secure windows to properly build the class
      // path for access to job.jar/lib/xyz and friends (see YARN-2803)
      Path jarDir;
      if (exec instanceof WindowsSecureContainerExecutor) {
        jarDir = nmPrivateClasspathJarDir;
      } else {
        jarDir = pwd;
      }
      String[] jarCp = FileUtil.createJarWithClassPath(
              newClassPath.toString(), jarDir, pwd, mergedEnv);
      // In a secure cluster the classpath jar must be localized to grant access
      Path localizedClassPathJar = exec.localizeClasspathJar(
              new Path(jarCp[0]), pwd, container.getUser());
      String replacementClassPath = localizedClassPathJar.toString() + jarCp[1];
      environment.put(Environment.CLASSPATH.name(), replacementClassPath);
    }
  }

  public static String getExitCodeFile(String pidFile) {
    return pidFile + EXIT_CODE_FILE_SUFFIX;
  }

  private void recordContainerLogDir(ContainerId containerId,
      String logDir) throws IOException{
    container.setLogDir(logDir);
    if (container.isRetryContextSet()) {
      context.getNMStateStore().storeContainerLogDir(containerId, logDir);
    }
  }

  private void recordContainerWorkDir(ContainerId containerId,
      String workDir) throws IOException{
    container.setWorkDir(workDir);
    if (container.isRetryContextSet()) {
      context.getNMStateStore().storeContainerWorkDir(containerId, workDir);
    }
  }

  protected Path getContainerWorkDir() throws IOException {
    String containerWorkDir = container.getWorkDir();
    if (containerWorkDir == null
        || !dirsHandler.isGoodLocalDir(containerWorkDir)) {
      throw new IOException(
          "Could not find a good work dir " + containerWorkDir
              + " for container " + container);
    }

    return new Path(containerWorkDir);
  }

  /**
   * Clean up container's files for container relaunch or cleanup.
   */
  protected void cleanupContainerFiles(Path containerWorkDir) {
    LOG.debug("cleanup container {} files", containerWorkDir);
    // delete ContainerScriptPath
    deleteAsUser(new Path(containerWorkDir, CONTAINER_SCRIPT));
    // delete TokensPath
    deleteAsUser(new Path(containerWorkDir, FINAL_CONTAINER_TOKENS_FILE));
    // delete sysfs dir
    deleteAsUser(new Path(containerWorkDir, SYSFS_DIR));

    // delete symlinks because launch script will create symlinks again
    try {
      exec.cleanupBeforeRelaunch(container);
    } catch (IOException | InterruptedException e) {
      LOG.warn("{} exec failed to cleanup", container.getContainerId(), e);
    }
  }

  private void deleteAsUser(Path path) {
    try {
      exec.deleteAsUser(new DeletionAsUserContext.Builder()
          .setUser(container.getUser())
          .setSubDir(path)
          .build());
    } catch (Exception e) {
      LOG.warn("Failed to delete " + path, e);
    }
  }

  /**
   * Returns the PID File Path.
   */
  Path getPidFilePath() {
    return pidFilePath;
  }

  /**
   * Marks the container to be launched only if it was not launched.
   *
   * @return true if successful; false otherwise.
   */
  boolean markLaunched() {
    return containerAlreadyLaunched.compareAndSet(false, true);
  }

  /**
   * Returns if the launch is completed or not.
   */
  boolean isLaunchCompleted() {
    return completed.get();
  }

}
