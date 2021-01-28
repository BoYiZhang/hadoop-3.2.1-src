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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.ShellScriptBuilder;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerPrepareContext;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReacquisitionContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.util.ProcessIdFileReader;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.CONTAINER_PRE_LAUNCH_STDERR;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch.CONTAINER_PRE_LAUNCH_STDOUT;

/**
 * 用于在底层操作系统上启动container的机制的抽象类。
 * 所有的executor 必须继承ContainerExecutor
 *
 * This class is abstraction of the mechanism used to launch a container on the underlying OS.
 * All executor implementations must extend ContainerExecutor.
 */
public abstract class ContainerExecutor implements Configurable {

  private static final Logger LOG =  LoggerFactory.getLogger(ContainerExecutor.class);

  // 通配符
  protected static final String WILDCARD = "*";

  /**
   * 创建启动脚本时要使用的权限。 : 700
   * The permissions to use when creating the launch script.
   */
  public static final FsPermission TASK_LAUNCH_SCRIPT_PERMISSION = FsPermission.createImmutable((short)0700);

  /**
   *
   * 调试信息将写入的相对路径。
   *
   * The relative path to which debug information will be written.
   *
   * @see ShellScriptBuilder#listDebugInformation
   */
  public static final String DIRECTORY_CONTENTS = "directory.info";

  // 配置信息
  private Configuration conf;


  // ContainerId 对应的 pid 存储文件路径
  private final ConcurrentMap<ContainerId, Path> pidFiles =  new ConcurrentHashMap<>();

  // 可重入读写锁
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReadLock readLock = lock.readLock();
  private final WriteLock writeLock = lock.writeLock();

  // 白名单 变量 : 用户可以自定义设置的环境变量, 当用户指定的时候, 不再使用NodeManager环境的默认值.
  private String[] whitelistVars;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    if (conf != null) {
      // Environment variables that containers may override rather than use NodeManager's default
      //
      // 用户可以自定义设置的环境变量, 当用户指定的时候, 不再使用NodeManager环境的默认值.
      // yarn.nodemanager.env-whitelist
      // JAVA_HOME , HADOOP_COMMON_HOME , HADOOP_HDFS_HOME , HADOOP_CONF_DIR ,  CLASSPATH_PREPEND_DISTCACHE , HADOOP_YARN_HOME
      whitelistVars = conf.get(YarnConfiguration.NM_ENV_WHITELIST,
          YarnConfiguration.DEFAULT_NM_ENV_WHITELIST).split(",");
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Run the executor initialization steps.
   * Verify that the necessary configs and permissions are in place.
   *
   * @param nmContext Context of NM
   * @throws IOException if initialization fails
   */
  public abstract void init(Context nmContext) throws IOException;

  /**
   * This function localizes the JAR file on-demand.
   * On Windows the ContainerLaunch creates a temporary special JAR manifest of other JARs to workaround the CLASSPATH length.
   * In a secure cluster this JAR must be localized so that the container has access to it.
   * The default implementation returns the classpath passed to it, which is expected to have been created in the node manager's <i>fprivate</i> folder, which will not work with secure Windows clusters.
   *
   * @param jarPath the path to the JAR to localize
   * @param target the directory where the JAR file should be localized
   * @param owner the name of the user who should own the localized file
   * @return the path to the localized JAR file
   * @throws IOException if localization fails
   */
  public Path localizeClasspathJar(Path jarPath, Path target, String owner)
      throws IOException {
    return jarPath;
  }


  /**
   * Prepare the environment for containers in this application to execute.
   * <pre>
   * For $x in local.dirs
   *   create $x/$user/$appId
   * Copy $nmLocal/appTokens {@literal ->} $N/$user/$appId
   * For $rsrc in private resources
   *   Copy $rsrc {@literal ->} $N/$user/filecache/[idef]
   * For $rsrc in job resources
   *   Copy $rsrc {@literal ->} $N/$user/$appId/filecache/idef
   * </pre>
   *
   * @param ctx LocalizerStartContext that encapsulates necessary information
   *            for starting a localizer.
   * @throws IOException for most application init failures
   * @throws InterruptedException if application init thread is halted by NM
   */
  public abstract void startLocalizer(LocalizerStartContext ctx)
    throws IOException, InterruptedException;

  /**
   * Prepare the container prior to the launch environment being written.
   * @param ctx Encapsulates information necessary for launching containers.
   * @throws IOException if errors occur during container preparation
   */
  public void prepareContainer(ContainerPrepareContext ctx) throws
      IOException{
  }

  /**
   * Launch the container on the node. This is a blocking call and returns only
   * when the container exits.
   * @param ctx Encapsulates information necessary for launching containers.
   * @return the return status of the launch
   * @throws IOException if the container launch fails
   * @throws ConfigurationException if config error was found
   */
  public abstract int launchContainer(ContainerStartContext ctx) throws
      IOException, ConfigurationException;

  /**
   * Relaunch the container on the node. This is a blocking call and returns
   * only when the container exits.
   * @param ctx Encapsulates information necessary for relaunching containers.
   * @return the return status of the relaunch
   * @throws IOException if the container relaunch fails
   * @throws ConfigurationException if config error was found
   */
  public abstract int relaunchContainer(ContainerStartContext ctx) throws
      IOException, ConfigurationException;

  /**
   * Signal container with the specified signal.
   *
   * @param ctx Encapsulates information necessary for signaling containers.
   * @return returns true if the operation succeeded
   * @throws IOException if signaling the container fails
   */
  public abstract boolean signalContainer(ContainerSignalContext ctx)
      throws IOException;

  /**
   * Perform the steps necessary to reap the container.
   *
   * @param ctx Encapsulates information necessary for reaping containers.
   * @return returns true if the operation succeeded.
   * @throws IOException if reaping the container fails.
   */
  public abstract boolean reapContainer(ContainerReapContext ctx)
      throws IOException;

  /**
   * Delete specified directories as a given user.
   *
   * @param ctx Encapsulates information necessary for deletion.
   * @throws IOException if delete fails
   * @throws InterruptedException if interrupted while waiting for the deletion
   * operation to complete
   */
  public abstract void deleteAsUser(DeletionAsUserContext ctx)
      throws IOException, InterruptedException;

  /**
   * Create a symlink file which points to the target.
   * @param target The target for symlink
   * @param symlink the symlink file
   * @throws IOException Error when creating symlinks
   */
  public abstract void symLink(String target, String symlink)
      throws IOException;

  /**
   * Check if a container is alive.
   * @param ctx Encapsulates information necessary for container liveness check.
   * @return true if container is still alive
   * @throws IOException if there is a failure while checking the container
   * status
   */
  public abstract boolean isContainerAlive(ContainerLivenessContext ctx)
      throws IOException;

  /**
   * Recover an already existing container. This is a blocking call and returns
   * only when the container exits.  Note that the container must have been
   * activated prior to this call.
   *
   * @param ctx encapsulates information necessary to reacquire container
   * @return The exit code of the pre-existing container
   * @throws IOException if there is a failure while reacquiring the container
   * @throws InterruptedException if interrupted while waiting to reacquire
   * the container
   */
  public int reacquireContainer(ContainerReacquisitionContext ctx)
      throws IOException, InterruptedException {

    // 获取容器
    Container container = ctx.getContainer();

    // 获取用户
    String user = ctx.getUser();

    // 获取ContainerId
    ContainerId containerId = ctx.getContainerId();

    // 获取pid 路径
    Path pidPath = getPidFilePath(containerId);

    if (pidPath == null) {
      LOG.warn(containerId + " is not active, returning terminated error");

      return ExitCode.TERMINATED.getExitCode();
    }

    // 获取pid
    String pid = ProcessIdFileReader.getProcessId(pidPath);

    if (pid == null) {
      throw new IOException("Unable to determine pid for " + containerId);
    }

    LOG.info("Reacquiring " + containerId + " with pid " + pid);

    // 构建ContainerLivenessContext
    ContainerLivenessContext livenessContext = new ContainerLivenessContext
        .Builder()
        .setContainer(container)
        .setUser(user)
        .setPid(pid)
        .build();

    while (isContainerAlive(livenessContext)) {
      Thread.sleep(1000);
    }

    // 等待退出代码文件出现
    // wait for exit code file to appear
    final int sleepMsec = 100;

    // 最大等待2s
    int msecLeft = 2000;
    // 获取pid进程文件路径
    String exitCodeFile = ContainerLaunch.getExitCodeFile(pidPath.toString());
    File file = new File(exitCodeFile);

    while (!file.exists() && msecLeft >= 0) {
      if (!isContainerActive(containerId)) {
        LOG.info(containerId + " was deactivated");

        return ExitCode.TERMINATED.getExitCode();
      }

      Thread.sleep(sleepMsec);

      msecLeft -= sleepMsec;
    }

    if (msecLeft < 0) {
      throw new IOException("Timeout while waiting for exit code from "  + containerId);
    }

    try {
      return Integer.parseInt( FileUtils.readFileToString(file, Charset.defaultCharset()).trim());
    } catch (NumberFormatException e) {
      throw new IOException("Error parsing exit code from pid " + pid, e);
    }
  }

  /**
   * 向container启动脚本写入环境信息.
   *
   * This method writes out the launch environment of a container to the
   * default container launch script. For the default container script path see
   * {@link ContainerLaunch#CONTAINER_SCRIPT}.
   *
   * @param out the output stream to which the environment is written (usually
   * a script file which will be executed by the Launcher)
   * @param environment the environment variables and their values
   * @param resources the resources which have been localized for this
   * container. Symlinks will be created to these localized resources
   * @param command the command that will be run
   * @param logDir the log dir to which to copy debugging information
   * @param user the username of the job owner
   * @param nmVars the set of environment vars that are explicitly set by NM
   * @throws IOException if any errors happened writing to the OutputStream,
   * while creating symlinks
   */
  public void writeLaunchEnv(OutputStream out, Map<String, String> environment,
      Map<Path, List<String>> resources, List<String> command, Path logDir,
      String user, LinkedHashSet<String> nmVars) throws IOException {

    this.writeLaunchEnv(out, environment, resources, command, logDir, user,  ContainerLaunch.CONTAINER_SCRIPT, nmVars);
  }

  /**
   * 此方法将容器的启动环境写出到指定的路径。
   * This method writes out the launch environment of a container to a specified path.
   *
   * @param out the output stream to which the environment is written (usually
   * a script file which will be executed by the Launcher)
   * @param environment the environment variables and their values
   * @param resources the resources which have been localized for this
   * container. Symlinks will be created to these localized resources
   * @param command the command that will be run
   * @param logDir the log dir to which to copy debugging information
   * @param user the username of the job owner
   * @param outFilename the path to which to write the launch environment
   * @param nmVars the set of environment vars that are explicitly set by NM
   * @throws IOException if any errors happened writing to the OutputStream,
   * while creating symlinks
   */
  @VisibleForTesting
  public void writeLaunchEnv(OutputStream out, Map<String, String> environment,
      Map<Path, List<String>> resources, List<String> command, Path logDir,
      String user, String outFilename, LinkedHashSet<String> nmVars)
      throws IOException {


    ContainerLaunch.ShellScriptBuilder sb =  ContainerLaunch.ShellScriptBuilder.create();
//    # 输出脚本的头信息
//    #!/bin/bash

    // Add "set -o pipefail -e" to validate launch_container script.
    sb.setExitOnFailure();
//    #快速失败并检查退出状态(exit codes).
//    set -o pipefail -e

    //Redirect stdout and stderr for launch_container script
    sb.stdout(logDir, CONTAINER_PRE_LAUNCH_STDOUT);
//    #输出运行日志
//    export PRELAUNCH_OUT="/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/prelaunch.out"
//    exec >"${PRELAUNCH_OUT}"

    sb.stderr(logDir, CONTAINER_PRE_LAUNCH_STDERR);
//    #输出异常日志
//    export PRELAUNCH_ERR="/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/prelaunch.err"
//    exec 2>"${PRELAUNCH_ERR}"


    if (environment != null) {

      sb.echo("Setting up env variables");
//      # 设置环境变量
//      echo "Setting up env variables"


      // 白名单环境变量被特别处理。
      // 仅当环境中尚未定义它们时才添加它们。
      // 使用特殊的语法添加它们，以防止它们掩盖可能在容器映像（例如docker映像）中显式设置的变量。
      // 将这些放在其他之前，以确保使用正确的使用。

      // Whitelist environment variables are treated specially.
      // Only add them if they are not already defined in the environment.
      // Add them using special syntax to prevent them from eclipsing variables that may be set explicitly in the container image (e.g, in a docker image).
      // Put these before the others to ensure the correct expansion is used.


      sb.echo("Setting up env variables#whitelistVars");
      for(String var : whitelistVars) {
        if (!environment.containsKey(var)) {

          String val = getNMEnvVar(var);

          if (val != null) {


            sb.whitelistedEnv(var, val);
          }
        }
      }
//      # 设置环境变量#白名单变量
//      echo "Setting up env variables#whitelistVars"
//      export JAVA_HOME=${JAVA_HOME:-"/Library/java/JavaVirtualMachines/jdk1.8.0_271.jdk/Contents/Home"}
//      export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME:-"/opt/workspace/apache/hadoop-3.2.1-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/../../../../hadoop-common-project/hadoop-common/target"}
//      export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/opt/tools/hadoop-3.2.1/etc/hadoop"}
//      export HADOOP_HOME=${HADOOP_HOME:-"/opt/workspace/apache/hadoop-3.2.1-src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/../../../../hadoop-common-project/hadoop-common/target"}
//      export PATH=${PATH:-"/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/tools/apache-maven-3.6.3/bin:/opt/tools/scala-2.12.10/bin:/usr/local/mysql-5.7.28-macos10.14-x86_64/bin:/Library/java/JavaVirtualMachines/jdk1.8.0_271.jdk/Contents/Home/bin:/opt/tools/hadoop-3.2.1/bin:/opt/tools/hadoop-3.2.1/etc/hadoop:henghe:/opt/tools/ozone-1.0.0/bin:/opt/tools/spark-2.4.5/bin:/opt/tools/spark-2.4.5/conf:/opt/tools/redis-5.0.7/src:/opt/tools/datax/bin:/opt/tools/apache-ant-1.9.6/bin:/opt/tools/hbase-2.0.2/bin"}



      sb.echo("Setting up env variables#env");

      // 现在编写由nodemanager显式设置的变量，保留它们的写入顺序。
      // Now write vars that were set explicitly by nodemanager, preserving the order they were written in.
      for (String nmEnvVar : nmVars) {
        sb.env(nmEnvVar, environment.get(nmEnvVar));
      }
//      # 设置环境变量#环境变量
//      echo "Setting up env variables#env"
//      export HADOOP_TOKEN_FILE_LOCATION="/opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache/application_1611681788558_0001/container_1611681788558_0001_01_000001/container_tokens"
//      export CONTAINER_ID="container_1611681788558_0001_01_000001"
//      export NM_PORT="62016"
//      export NM_HOST="boyi-pro.lan"
//      export NM_HTTP_PORT="8042"
//      export LOCAL_DIRS="/opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache/application_1611681788558_0001"
//      export LOCAL_USER_DIRS="/opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/"
//      export LOG_DIRS="/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001"
//      export USER="henghe"
//      export LOGNAME="henghe"
//      export HOME="/home/"
//      export PWD="/opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache/application_1611681788558_0001/container_1611681788558_0001_01_000001"
//      export JVM_PID="$$"
//      export MALLOC_ARENA_MAX="4"


      sb.echo("Setting up env variables#remaining");
      // 现在写入剩余的环境变量
      // Now write the remaining environment variables.

      for (Map.Entry<String, String> env :  sb.orderEnvByDependencies(environment).entrySet()) {
        if (!nmVars.contains(env.getKey())) {
          sb.env(env.getKey(), env.getValue());
        }
      }
//      # 设置环境变量#剩余环境变量
//      echo "Setting up env variables#remaining"
//      export SPARK_YARN_STAGING_DIR="hdfs://localhost:8020/user/henghe/.sparkStaging/application_1611681788558_0001"
//      export APPLICATION_WEB_PROXY_BASE="/proxy/application_1611681788558_0001"
//      export CLASSPATH="$PWD:$PWD/__spark_conf__:$PWD/__spark_libs__/*:$HADOOP_CONF_DIR:$HADOOP_COMMON_HOME/share/hadoop/common/*:$HADOOP_COMMON_HOME/share/hadoop/common/lib/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/*:$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$PWD/__spark_conf__/__hadoop_conf__"
//      export APP_SUBMIT_TIME_ENV="1611681915166"
//      export SPARK_USER="henghe"
//      export PYTHONHASHSEED="0"

    }



    if (resources != null) {

      sb.echo("Setting up job resources");


      Map<Path, Path> symLinks = resolveSymLinks(resources, user);
      for (Map.Entry<Path, Path> symLink : symLinks.entrySet()) {
        // 链接环境变量
        sb.symlink(symLink.getKey(), symLink.getValue());
      }
//      # 设置资源文件[通过ln -sf 构建所需jar文件/配置文件的软连接. ]
//      echo "Setting up job resources"
//      mkdir -p __spark_libs__
//      ln -sf -- "/opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/filecache/35/spark-examples_2.11-2.4.5.jar" "__app__.jar"
//      mkdir -p __spark_libs__
//      ln -sf -- "/opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/filecache/180/__spark_conf__.zip" "__spark_conf__"
//      # 此处省略N多
//      # mkdir -p __spark_libs__
//      # ln -sf --"xxxxx"  "__spark_libs__/xxxxx.jar"
    }


    // dump 调试信息（如果已配置）
    // dump debugging information if configured


    if (getConf() != null && getConf().getBoolean(YarnConfiguration.NM_LOG_CONTAINER_DEBUG_INFO,  YarnConfiguration.DEFAULT_NM_LOG_CONTAINER_DEBUG_INFO)) {

//    # 设置debug 信息
      sb.echo("Copying debugging information");

      sb.copyDebugInformation(new Path(outFilename),  new Path(logDir, outFilename));
      sb.listDebugInformation(new Path(logDir, DIRECTORY_CONTENTS));

//      # 设置debug 信息
//      echo "Copying debugging information"

//      # Creating copy of launch script
//      cp "launch_container.sh" "/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/launch_container.sh"
//      chmod 640 "/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/launch_container.sh"

//      # 确定目录内容
//      # Determining directory contents
//      echo "ls -l:" 1>"/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/directory.info"
//      ls -l 1>>"/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/directory.info"
//      echo "find -L . -maxdepth 5 -ls:" 1>>"/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/directory.info"
//      find -L . -maxdepth 5 -ls 1>>"/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/directory.info"
//      echo "broken symlinks(find -L . -maxdepth 5 -type l -ls):" 1>>"/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/directory.info"
//      find -L . -maxdepth 5 -type l -ls 1>>"/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/directory.info"


    }


    sb.echo("Launching container");
//    echo "Launching container"


    // 启动container
    sb.command(command);
//    #输出启动脚本
//    exec /bin/bash -c "
//      $JAVA_HOME/bin/java
//      -server
//      -Xmx1024m
//      -Djava.io.tmpdir=$PWD/tmp
//      -Dspark.yarn.app.container.log.dir=/opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001
//      org.apache.spark.deploy.yarn.ApplicationMaster
//        --class 'org.apache.spark.examples.SparkPi'
//        --jar file:/opt/tools/spark-2.4.5/examples/jars/spark-examples_2.11-2.4.5.jar
//        --arg '10'
//        --properties-file $PWD/__spark_conf__/__spark_conf__.properties
//      1> /opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/stdout
//      2> /opt/tools/hadoop-3.2.1/logs/userlogs/application_1611681788558_0001/container_1611681788558_0001_01_000001/stderr"



    //最终输入内容
    LOG.warn("ContainerExecutor#writeLaunchEnv : " + sb.toString());

    PrintStream pout = null;
    try {
      pout = new PrintStream(out, false, "UTF-8");
      sb.write(pout);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  /**
   * Return the files in the target directory. If retrieving the list of files
   * requires specific access rights, that access will happen as the
   * specified user. The list will not include entries for "." or "..".
   *
   * @param user the user as whom to access the target directory
   * @param dir the target directory
   * @return a list of files in the target directory
   */
  protected File[] readDirAsUser(String user, Path dir) {
    return new File(dir.toString()).listFiles();
  }

  /**
   * The container exit code.
   */
  public enum ExitCode {
    SUCCESS(0),
    FORCE_KILLED(137),
    TERMINATED(143),
    LOST(154);

    private final int code;

    private ExitCode(int exitCode) {
      this.code = exitCode;
    }

    /**
     * Get the exit code as an int.
     * @return the exit code as an int
     */
    public int getExitCode() {
      return code;
    }

    @Override
    public String toString() {
      return String.valueOf(code);
    }
  }

  /**
   * The constants for the signals.
   */
  public enum Signal {
    NULL(0, "NULL"),
    QUIT(3, "SIGQUIT"),
    KILL(9, "SIGKILL"),
    TERM(15, "SIGTERM");

    private final int value;
    private final String str;

    private Signal(int value, String str) {
      this.str = str;
      this.value = value;
    }

    /**
     * Get the signal number.
     * @return the signal number
     */
    public int getValue() {
      return value;
    }

    @Override
    public String toString() {
      return str;
    }
  }

  /**
   * Log each line of the output string as INFO level log messages.
   *
   * @param output the output string to log
   */
  protected void logOutput(String output) {
    String shExecOutput = output;

    if (shExecOutput != null) {
      for (String str : shExecOutput.split("\n")) {
        LOG.info(str);
      }
    }
  }

  /**
   * Get the pidFile of the container.
   *
   * @param containerId the container ID
   * @return the path of the pid-file for the given containerId.
   */
  protected Path getPidFilePath(ContainerId containerId) {
    try {
      readLock.lock();
      return (this.pidFiles.get(containerId));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Return a command line to execute the given command in the OS shell.
   * On Windows, the {code}groupId{code} parameter can be used to launch
   * and associate the given GID with a process group. On
   * non-Windows hosts, the {code}groupId{code} parameter is ignored.
   *
   * @param command the command to execute
   * @param groupId the job owner's GID
   * @param userName the job owner's username
   * @param pidFile the path to the container's PID file
   * @param config the configuration
   * @return the command line to execute
   */
  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration config) {
    return getRunCommand(command, groupId, userName, pidFile, config, null);
  }

  /**
   * Return a command line to execute the given command in the OS shell.
   * On Windows, the {code}groupId{code} parameter can be used to launch
   * and associate the given GID with a process group. On
   * non-Windows hosts, the {code}groupId{code} parameter is ignored.
   *
   * @param command the command to execute
   * @param groupId the job owner's GID for Windows. On other operating systems
   * it is ignored.
   * @param userName the job owner's username for Windows. On other operating
   * systems it is ignored.
   * @param pidFile the path to the container's PID file on Windows. On other
   * operating systems it is ignored.
   * @param config the configuration
   * @param resource on Windows this parameter controls memory and CPU limits.
   * If null, no limits are set. On other operating systems it is ignored.
   * @return the command line to execute
   */
  protected String[] getRunCommand(String command, String groupId,
      String userName, Path pidFile, Configuration config, Resource resource) {
    if (Shell.WINDOWS) {
      return getRunCommandForWindows(command, groupId, userName, pidFile,
          config, resource);
    } else {
      return getRunCommandForOther(command, config);
    }

  }

  /**
   * Return a command line to execute the given command in the OS shell.
   * The {code}groupId{code} parameter can be used to launch
   * and associate the given GID with a process group.
   *
   * @param command the command to execute
   * @param groupId the job owner's GID
   * @param userName the job owner's username
   * @param pidFile the path to the container's PID file
   * @param config the configuration
   * @param resource this parameter controls memory and CPU limits.
   * If null, no limits are set.
   * @return the command line to execute
   */
  protected String[] getRunCommandForWindows(String command, String groupId,
      String userName, Path pidFile, Configuration config, Resource resource) {
    int cpuRate = -1;
    int memory = -1;

    if (resource != null) {
      if (config.getBoolean(
          YarnConfiguration.NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED,
          YarnConfiguration.
            DEFAULT_NM_WINDOWS_CONTAINER_MEMORY_LIMIT_ENABLED)) {
        memory = (int) resource.getMemorySize();
      }

      if (config.getBoolean(
          YarnConfiguration.NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED,
          YarnConfiguration.DEFAULT_NM_WINDOWS_CONTAINER_CPU_LIMIT_ENABLED)) {
        int containerVCores = resource.getVirtualCores();
        int nodeVCores = NodeManagerHardwareUtils.getVCores(config);
        int nodeCpuPercentage =
            NodeManagerHardwareUtils.getNodeCpuPercentage(config);

        float containerCpuPercentage =
            (float)(nodeCpuPercentage * containerVCores) / nodeVCores;

        // CPU should be set to a percentage * 100, e.g. 20% cpu rate limit
        // should be set as 20 * 100.
        cpuRate = Math.min(10000, (int)(containerCpuPercentage * 100));
      }
    }

    return new String[] {
        Shell.getWinUtilsPath(),
        "task",
        "create",
        "-m",
        String.valueOf(memory),
        "-c",
        String.valueOf(cpuRate),
        groupId,
        "cmd /c " + command
    };
  }

  /**
   *
   * 返回命令行以在OS shell中执行给定的命令。
   *
   * Return a command line to execute the given command in the OS shell.
   *
   * @param command the command to execute
   * @param config the configuration
   * @return the command line to execute
   */
  protected String[] getRunCommandForOther(String command,
      Configuration config) {
    List<String> retCommand = new ArrayList<>();
    boolean containerSchedPriorityIsSet = false;

    // 0
    int containerSchedPriorityAdjustment = YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY;

    // 对容器操作系统调度优先级进行调整。
    // 有效值可能因平台而异。
    // 在Linux上，较高的值意味着运行容器的优先级低于NM。
    // 指定的值是一个整数

    // yarn.nodemanager.container-executor.os.sched.priority.adjustment : 0
    if (config.get(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY) !=  null) {
      containerSchedPriorityIsSet = true;
      containerSchedPriorityAdjustment = config
          .getInt(YarnConfiguration.NM_CONTAINER_EXECUTOR_SCHED_PRIORITY,
          YarnConfiguration.DEFAULT_NM_CONTAINER_EXECUTOR_SCHED_PRIORITY);
    }

    if (containerSchedPriorityIsSet) {
      // 设置优先级
      retCommand.addAll(Arrays.asList("nice", "-n", Integer.toString(containerSchedPriorityAdjustment)));
    }

    retCommand.addAll(Arrays.asList("bash", command));

//    0 = "nice"
//    1 = "-n"
//    2 = "6"
//    3 = "bash"
//    4 = "/opt/tools/hadoop-3.2.1/local-dirs/usercache/henghe/appcache/application_1611769550924_0001/container_1611769550924_0001_01_000001/default_container_executor.sh"
    String[] commands =  retCommand.toArray(new String[retCommand.size()]);
    return commands ;
  }

  /**
   * Return whether the container is still active.
   *
   * @param containerId the target container's ID
   * @return true if the container is active
   */
  protected boolean isContainerActive(ContainerId containerId) {
    try {
      readLock.lock();

      return (this.pidFiles.containsKey(containerId));
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  protected String getNMEnvVar(String varname) {
    return System.getenv(varname);
  }

  /**
   * Mark the container as active.
   *
   * @param containerId the container ID
   * @param pidFilePath the path where the executor should write the PID
   * of the launched process
   */
  public void activateContainer(ContainerId containerId, Path pidFilePath) {
    try {
      writeLock.lock();
      this.pidFiles.put(containerId, pidFilePath);
    } finally {
      writeLock.unlock();
    }
  }

  // LinuxContainerExecutor overrides this method and behaves differently.
  public String[] getIpAndHost(Container container)
      throws ContainerExecutionException {
    return getLocalIpAndHost(container);
  }

  // ipAndHost[0] contains ip.
  // ipAndHost[1] contains hostname.
  public static String[] getLocalIpAndHost(Container container) {
    String[] ipAndHost = new String[2];
    try {
      InetAddress address = InetAddress.getLocalHost();
      ipAndHost[0] = address.getHostAddress();
      ipAndHost[1] = address.getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to get Local hostname and ip for " + container
          .getContainerId(), e);
    }
    return ipAndHost;
  }

  /**
   * Mark the container as inactive. For inactive containers this method has no effect.
   *
   * @param containerId the container ID
   */
  public void deactivateContainer(ContainerId containerId) {
    try {
      writeLock.lock();
      this.pidFiles.remove(containerId);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Pause the container. The default implementation is to raise a kill event.
   * Specific executor implementations can override this behavior.
   * @param container
   *          the Container
   */
  public void pauseContainer(Container container) {
    LOG.warn(container.getContainerId() + " doesn't support pausing.");
    throw new UnsupportedOperationException();
  }

  /**
   * Resume the container from pause state. The default implementation ignores
   * this event. Specific implementations can override this behavior.
   * @param container
   *          the Container
   */
  public void resumeContainer(Container container) {
    LOG.warn(container.getContainerId() + " doesn't support resume.");
    throw new UnsupportedOperationException();
  }

  /**
   * Perform any cleanup before the next launch of the container.
   * @param container         container
   */
  public void cleanupBeforeRelaunch(Container container)
      throws IOException, InterruptedException {

    if (container.getLocalizedResources() != null) {

      // 获取资源软链
      Map<Path, Path> symLinks = resolveSymLinks( container.getLocalizedResources(), container.getUser());

      for (Map.Entry<Path, Path> symLink : symLinks.entrySet()) {
        LOG.debug("{} deleting {}", container.getContainerId(),  symLink.getValue());

        // 删除资源
        deleteAsUser(new DeletionAsUserContext.Builder()
            .setUser(container.getUser())
            .setSubDir(symLink.getValue())
            .build());
      }
    }
  }

  /**
   * Get the process-identifier for the container.
   *
   * @param containerID the container ID
   * @return the process ID of the container if it has already launched,
   * or null otherwise
   */
  public String getProcessId(ContainerId containerID) {
    String pid = null;
    // 获取pid 文件路径
    Path pidFile = pidFiles.get(containerID);

    // If PID is null, this container hasn't launched yet.
    if (pidFile != null) {
      try {
        //读取pid
        pid = ProcessIdFileReader.getProcessId(pidFile);
      } catch (IOException e) {
        LOG.error("Got exception reading pid from pid-file " + pidFile, e);
      }
    }

    return pid;
  }

  /**
   * 此类将在指定的延迟后向目标容器发送信号。
   * This class will signal a target container after a specified delay.
   * @see #signalContainer
   */
  public static class DelayedProcessKiller extends Thread {
    // 容器
    private final Container container;
    // 用户
    private final String user;
    // 进程ID
    private final String pid;
    // 延迟时长
    private final long delay;
    // 信号
    private final Signal signal;
    // 容器的 Executor
    private final ContainerExecutor containerExecutor;

    /**
     * Basic constructor.
     *
     * @param container the container to signal
     * @param user the user as whow to send the signal
     * @param pid the PID of the container process
     * @param delayMS the period of time to wait in millis before signaling
     * the container
     * @param signal the signal to send
     * @param containerExecutor the executor to use to send the signal
     */
    public DelayedProcessKiller(Container container, String user, String pid,
        long delayMS, Signal signal, ContainerExecutor containerExecutor) {
      this.container = container;
      this.user = user;
      this.pid = pid;
      this.delay = delayMS;
      this.signal = signal;
      this.containerExecutor = containerExecutor;
      setName("Task killer for " + pid);
      setDaemon(false);
    }

    @Override
    public void run() {
      try {
        // 休眠指定时长 : 单位 毫秒
        Thread.sleep(delay);

        // 发送信号
        containerExecutor.signalContainer(new ContainerSignalContext.Builder()
            .setContainer(container)
            .setUser(user)
            .setPid(pid)
            .setSignal(signal)
            .build());
      } catch (InterruptedException e) {
        interrupt();
      } catch (IOException e) {
        String message = "Exception when user " + user + " killing task " + pid
            + " in DelayedProcessKiller: " + StringUtils.stringifyException(e);
        LOG.warn(message);
        // 处理容器状态的变更
        container.handle(new ContainerDiagnosticsUpdateEvent(
            container.getContainerId(), message));
      }
    }
  }

  private Map<Path, Path> resolveSymLinks(Map<Path,  List<String>> resources, String user) {
    Map<Path, Path> symLinks = new HashMap<>();

    for (Map.Entry<Path, List<String>> resourceEntry : resources.entrySet()) {

      for (String linkName : resourceEntry.getValue()) {
        if (new Path(linkName).getName().equals(WILDCARD)) {
          // If this is a wildcarded path, link to everything in the  directory from the working directory
          // 如果这是一个通配符路径，请从工作目录链接到目录中的所有内容
          for (File wildLink : readDirAsUser(user, resourceEntry.getKey())) {
            symLinks.put(new Path(wildLink.toString()), new Path(wildLink.getName()));
          }
        } else {
          symLinks.put(resourceEntry.getKey(), new Path(linkName));
        }
      }
    }

    return symLinks;
  }
}
