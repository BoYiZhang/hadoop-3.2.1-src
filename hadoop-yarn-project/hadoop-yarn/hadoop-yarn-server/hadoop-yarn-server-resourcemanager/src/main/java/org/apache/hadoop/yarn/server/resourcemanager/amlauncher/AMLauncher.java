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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * The launch of the AM itself.
 */
public class AMLauncher implements Runnable {

  private static final Log LOG = LogFactory.getLog(AMLauncher.class);

  private ContainerManagementProtocol containerMgrProxy;

  private final RMAppAttempt application;
  private final Configuration conf;
  private final AMLauncherEventType eventType;
  private final RMContext rmContext;
  private final Container masterContainer;
  private boolean timelineServiceV2Enabled;

  @SuppressWarnings("rawtypes")
  private final EventHandler handler;

  public AMLauncher(RMContext rmContext, RMAppAttempt application,
      AMLauncherEventType eventType, Configuration conf) {
    this.application = application;
    this.conf = conf;
    this.eventType = eventType;
    this.rmContext = rmContext;
    this.handler = rmContext.getDispatcher().getEventHandler();
    // application 中获取的
    this.masterContainer = application.getMasterContainer();
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(conf);
  }
  //nodeId : boyi-pro.lan:50456        resource : <memory:2048, vCores:1>
  private void connect() throws IOException {
    ContainerId masterContainerID = masterContainer.getId();  // container_1611506953824_0001_01_000001
    // org.apache.hadoop.yarn.api.impl.pb.client.ContainerManagementProtocolPBClientImpl@66cdec65
    containerMgrProxy = getContainerMgrProxy(masterContainerID);
  }

  private void launch() throws IOException, YarnException {
    // 建立连接
    connect();

    // 获取ContainerId:   container_1611506953824_0001_01_000001
    ContainerId masterContainerID = masterContainer.getId();

    // 获取applicationContext信息
    // application_id { id: 1 cluster_timestamp: 1611506953824 }
    // application_name: "org.apache.spark.examples.SparkPi"
    // queue: "default"
    // priority { priority: 0 }
    // am_container_spec { localResources { key: "__spark_conf__" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611506953824_0001/__spark_conf__.zip" } size: 250024 timestamp: 1611512474054 type: ARCHIVE visibility: PRIVATE } }
    // localResources { key: "__app__.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611506953824_0001/spark-examples_2.11-2.4.5.jar" } size: 1475072 timestamp: 1611512473631 type: FILE visibility: PRIVATE } } tokens: "HDTS\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000" environment { key: "SPARK_YARN_STAGING_DIR" value: "hdfs://localhost:8020/user/henghe/.sparkStaging/application_1611506953824_0001" }
    // environment { key: "SPARK_USER" value: "henghe" }
    // environment { key: "CLASSPATH" value: "{{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>$HADOOP_CONF_DIR<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/*<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/lib/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*<CPS>{{PWD}}/__spark_conf__/__hadoop_conf__" }
    // environment { key: "PYTHONHASHSEED" value: "0" } command: "{{JAVA_HOME}}/bin/java" command: "-server" command: "-Xmx1024m" command: "-Djava.io.tmpdir={{PWD}}/tmp" command: "-Dspark.yarn.app.container.log.dir=<LOG_DIR>" command: "org.apache.spark.deploy.yarn.ApplicationMaster" command: "--class" command: "\'org.apache.spark.examples.SparkPi\'" command: "--jar" command: "file:/opt/tools/spark-2.4.5/examples/jars/spark-examples_2.11-2.4.5.jar" command: "--arg" command: "\'10\'" command: "--properties-file" command: "{{PWD}}/__spark_conf__/__spark_conf__.properties" command: "1>" command: "<LOG_DIR>/stdout" command: "2>" command: "<LOG_DIR>/stderr" application_ACLs { accessType: APPACCESS_VIEW_APP acl: "sysadmin,henghe " } application_ACLs { accessType: APPACCESS_MODIFY_APP acl: "sysadmin,henghe " } } resource { memory: 2048 virtual_cores: 1 resource_value_map { key: "memory-mb" value: 2048 units: "Mi" type: COUNTABLE } resource_value_map { key: "vcores" value: 1 units: "" type: COUNTABLE } } applicationType: "SPARK"
    ApplicationSubmissionContext applicationContext =    application.getSubmissionContext();
    //  Setting up container Container:  [ContainerId: container_1611513130854_0001_01_000001,  AllocationRequestId: -1,  Version: 0,
    //  NodeId: boyi-pro.lan:56960,
    //  NodeHttpAddress: boyi-pro.lan:8042,
    //  Resource: <memory:2048, vCores:1>, Priority: 0, Token: Token { kind: ContainerToken, service: boyi-pro.lan:56960 }, ExecutionType: GUARANTEED, ] for AM appattempt_1611513130854_0001_000001
    LOG.info("Setting up container " + masterContainer  + " for AM " + application.getAppAttemptId());
    ContainerLaunchContext launchContext = createAMContainerLaunchContext(applicationContext, masterContainerID);




    // container_launch_context { localResources { key: "__spark_conf__" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611513130854_0001/__spark_conf__.zip" } size: 250024 timestamp: 1611513158992 type: ARCHIVE visibility: PRIVATE } } localResources { key: "__app__.jar" value { resource { scheme: "hdfs" host: "localhost" port: 8020 file: "/user/henghe/.sparkStaging/application_1611513130854_0001/spark-examples_2.11-2.4.5.jar" } size: 1475072 timestamp: 1611513158274 type: FILE visibility: PRIVATE } } tokens: "HDTS\000\001\000\032\n\r\n\t\b\001\020\346\336\253\255\363.\020\001\020\312\207\343\314\372\377\377\377\377\001\024\256X\203p\026\324\327\356^;\212z\314\333\276\361\263\227\246\236\020YARN_AM_RM_TOKEN\000\000"
    // environment { key: "SPARK_YARN_STAGING_DIR" value: "hdfs://localhost:8020/user/henghe/.sparkStaging/application_1611513130854_0001" }
    // environment { key: "APPLICATION_WEB_PROXY_BASE" value: "/proxy/application_1611513130854_0001" }
    // environment { key: "SPARK_USER" value: "henghe" }
    // environment { key: "CLASSPATH" value: "{{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>$HADOOP_CONF_DIR<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/*<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/lib/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*<CPS>{{PWD}}/__spark_conf__/__hadoop_conf__" }
    // environment { key: "PYTHONHASHSEED" value: "0" }
    // environment { key: "APP_SUBMIT_TIME_ENV" value: "1611513161745" } command: "{{JAVA_HOME}}/bin/java" command: "-server" command: "-Xmx1024m" command: "-Djava.io.tmpdir={{PWD}}/tmp" command: "-Dspark.yarn.app.container.log.dir=<LOG_DIR>" command: "org.apache.spark.deploy.yarn.ApplicationMaster" command: "--class" command: "\'org.apache.spark.examples.SparkPi\'" command: "--jar" command: "file:/opt/tools/spark-2.4.5/examples/jars/spark-examples_2.11-2.4.5.jar" command: "--arg" command: "\'10\'" command: "--properties-file" command: "{{PWD}}/__spark_conf__/__spark_conf__.properties" command: "1>" command: "<LOG_DIR>/stdout" command: "2>" command: "<LOG_DIR>/stderr" application_ACLs { accessType: APPACCESS_MODIFY_APP acl: "sysadmin,henghe " } application_ACLs { accessType: APPACCESS_VIEW_APP acl: "sysadmin,henghe " } } container_token { identifier: "\n\021\022\r\n\t\b\001\020\346\336\253\255\363.\020\001\030\001\022\022boyi-pro.lan:56960\032\006henghe\"+\b\200\020\020\001\032\024\n\tmemory-mb\020\200\020\032\002Mi \000\032\016\n\006vcores\020\001\032\000 \000(\361\251\322\255\363.0\265\254\237\350\0068\346\336\253\255\363.B\002\b\000H\245\332\255\255\363.Z\000`\001h\001p\000x\377\377\377\377\377\377\377\377\377\001" password: "$g\341\316Mw\377\fC\270\v\026<\242a\325\027\354\331\354" kind: "ContainerToken" service: "boyi-pro.lan:56960" }
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(launchContext,
          masterContainer.getContainerToken());

    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();


    list.add(scRequest);



    StartContainersRequest allRequests =  StartContainersRequest.newInstance(list);


    // 获取响应信息
    StartContainersResponse response =  containerMgrProxy.startContainers(allRequests);

    if (response.getFailedRequests() != null
        && response.getFailedRequests().containsKey(masterContainerID)) {
      Throwable t =
          response.getFailedRequests().get(masterContainerID).deSerialize();
      parseAndThrowException(t);
    } else {

      // succeeded_requests { app_attempt_id { application_id { id: 1 cluster_timestamp: 1611514283537 } attemptId: 1 } id: 1 }
      LOG.info("Done launching container " + masterContainer + " for AM "  + application.getAppAttemptId());
    }
  }

  private void cleanup() throws IOException, YarnException {

    // 建立连接
    connect();

    // 获取容器的id
    ContainerId containerId = masterContainer.getId();
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(containerId);

    // 构建请求
    StopContainersRequest stopRequest =  StopContainersRequest.newInstance(containerIds);

    // 发送请求
    StopContainersResponse response =  containerMgrProxy.stopContainers(stopRequest);

    // 处理响应信息
    if (response.getFailedRequests() != null
        && response.getFailedRequests().containsKey(containerId)) {
      Throwable t = response.getFailedRequests().get(containerId).deSerialize();
      parseAndThrowException(t);
    }
  }

  // Protected. For tests.
  protected ContainerManagementProtocol getContainerMgrProxy(
      final ContainerId containerId) {
    // boyi-pro.lan:56960
    final NodeId node = masterContainer.getNodeId();
    // containerManagerConnectAddress :  boyi-pro.lan/192.168.8.156:56960
    final InetSocketAddress containerManagerConnectAddress =
        NetUtils.createSocketAddrForHost(node.getHost(), node.getPort());
    final YarnRPC rpc = getYarnRPC();
    // appattempt_1611513130854_0001_000001 (auth:SIMPLE)
    UserGroupInformation currentUser =
        UserGroupInformation.createRemoteUser(containerId
            .getApplicationAttemptId().toString());
    // henghe
    String user =
        rmContext.getRMApps()
            .get(containerId.getApplicationAttemptId().getApplicationId())
            .getUser();

    // Token { kind: NMToken, service: boyi-pro.lan:56960 }
    org.apache.hadoop.yarn.api.records.Token token =
        rmContext.getNMTokenSecretManager().createNMToken(
            containerId.getApplicationAttemptId(), node, user);
    currentUser.addToken(ConverterUtils.convertFromYarn(token,
        containerManagerConnectAddress));

    return NMProxy.createNMProxy(conf, ContainerManagementProtocol.class,
        currentUser, rpc, containerManagerConnectAddress);
  }

  @VisibleForTesting
  protected YarnRPC getYarnRPC() {
    return YarnRPC.create(conf);  // TODO: Don't create again and again.
  }

  private ContainerLaunchContext createAMContainerLaunchContext(
      ApplicationSubmissionContext applicationMasterContext,
      ContainerId containerID) throws IOException {

    // Construct the actual Container
    ContainerLaunchContext container =  applicationMasterContext.getAMContainerSpec();

    if (container == null){
      throw new IOException(containerID +
            " has been cleaned before launched");
    }


    // Finalize the container
    setupTokens(container, containerID);


    // set the flow context optionally for timeline service v.2
    setFlowContext(container);


    //  container 信息
    //  localResources {
    //    key: "__spark_conf__" value {
    //      resource {
    //        scheme: "hdfs"
    //        host: "localhost"
    //        port: 8020
    //        file: "/user/henghe/.sparkStaging/application_1611513130854_0001/__spark_conf__.zip"
    //      }
    //      size: 250024
    //      timestamp: 1611513158992
    //      type: ARCHIVE visibility:
    //      PRIVATE
    //    }
    //  }
    //  localResources {
    //    key: "__app__.jar"
    //    value {
    //      resource {
    //        scheme: "hdfs"
    //        host: "localhost"
    //        port: 8020 file: "/user/henghe/.sparkStaging/application_1611513130854_0001/spark-examples_2.11-2.4.5.jar"
    //      }
    //      size: 1475072
    //      timestamp: 1611513158274
    //      type: FILE
    //      visibility: PRIVATE
    //    }
    //  }
    //  tokens: "HDTS\000\001\000\032\n\r\n\t\b\001\020\346\336\253\255\363.\020\001\020\312\207\343\314\372\377\377\377\377\001\024\256X\203p\026\324\327\356^;\212z\314\333\276\361\263\227\246\236\020YARN_AM_RM_TOKEN\000\000"
    //  environment {
    //    key: "SPARK_YARN_STAGING_DIR"
    //    value: "hdfs://localhost:8020/user/henghe/.sparkStaging/application_1611513130854_0001"
    //  }
    //  environment {
    //    key: "APPLICATION_WEB_PROXY_BASE"
    //    value: "/proxy/application_1611513130854_0001"
    //  }
    //  environment {
    //    key: "SPARK_USER"
    //    value: "henghe"
    //  }
    //  environment {
    //    key: "CLASSPATH" value: "{{PWD}}<CPS>{{PWD}}/__spark_conf__<CPS>{{PWD}}/__spark_libs__/*<CPS>$HADOOP_CONF_DIR<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/*<CPS>$HADOOP_COMMON_HOME/share/hadoop/common/lib/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/*<CPS>$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/*<CPS>$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*<CPS>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*<CPS>{{PWD}}/__spark_conf__/__hadoop_conf__"
    //  }
    //  environment { key: "PYTHONHASHSEED" value: "0" }
    //  environment { key: "APP_SUBMIT_TIME_ENV" value: "1611513161745" }
    //  command: "{{JAVA_HOME}}/bin/java"
    //  command: "-server"
    //  command: "-Xmx1024m"
    //  command: "-Djava.io.tmpdir={{PWD}}/tmp"
    //  command: "-Dspark.yarn.app.container.log.dir=<LOG_DIR>"
    //  command: "org.apache.spark.deploy.yarn.ApplicationMaster"
    //  command: "--class"
    //  command: "\'org.apache.spark.examples.SparkPi\'"
    //  command: "--jar"
    //  command: "file:/opt/tools/spark-2.4.5/examples/jars/spark-examples_2.11-2.4.5.jar"
    //  command: "--arg"
    //  command: "\'10\'"
    //  command: "--properties-file"
    //  command: "{{PWD}}/__spark_conf__/__spark_conf__.properties"
    //  command: "1>"
    //  command: "<LOG_DIR>/stdout"
    //  command: "2>"
    //  command: "<LOG_DIR>/stderr"
    //  application_ACLs { accessType: APPACCESS_MODIFY_APP acl: "sysadmin,henghe " }
    //  application_ACLs { accessType: APPACCESS_VIEW_APP acl: "sysadmin,henghe " }


    return container;
  }

  @Private
  @VisibleForTesting
  protected void setupTokens(
      ContainerLaunchContext container, ContainerId containerID)
      throws IOException {
    Map<String, String> environment = container.getEnvironment();

    // APPLICATION_WEB_PROXY_BASE
    environment.put(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV,  application.getWebProxyBase());


    // Set AppSubmitTime to be consumable by the AM.
    ApplicationId applicationId = application.getAppAttemptId().getApplicationId();


    environment.put(
        ApplicationConstants.APP_SUBMIT_TIME_ENV,
        String.valueOf(rmContext.getRMApps()
            .get(applicationId)
            .getSubmitTime()));

    Credentials credentials = new Credentials();


    DataInputByteBuffer dibb = new DataInputByteBuffer();
    ByteBuffer tokens = container.getTokens();
    if (tokens != null) {
      // TODO: Don't do this kind of checks everywhere.
      dibb.reset(tokens);
      credentials.readTokenStorageStream(dibb);
      tokens.rewind();
    }

    // Add AMRMToken
    Token<AMRMTokenIdentifier> amrmToken = createAndSetAMRMToken();
    if (amrmToken != null) {
      credentials.addToken(amrmToken.getService(), amrmToken);
    }

    DataOutputBuffer dob = new DataOutputBuffer();

    credentials.writeTokenStorageToStream(dob);

    container.setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
  }

  private void setFlowContext(ContainerLaunchContext container) {
    if (timelineServiceV2Enabled) {

      Map<String, String> environment = container.getEnvironment();


      ApplicationId applicationId = application.getAppAttemptId().getApplicationId();


      RMApp app = rmContext.getRMApps().get(applicationId);


      // initialize the flow in the environment with default values for those
      // that do not specify the flow tags
      // flow name: app name (or app id if app name is missing),
      // flow version: "1", flow run id: start time
      setFlowTags(environment, TimelineUtils.FLOW_NAME_TAG_PREFIX,
          TimelineUtils.generateDefaultFlowName(app.getName(), applicationId));


      setFlowTags(environment, TimelineUtils.FLOW_VERSION_TAG_PREFIX,
          TimelineUtils.DEFAULT_FLOW_VERSION);


      setFlowTags(environment, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX,
          String.valueOf(app.getStartTime()));

      // Set flow context info: the flow context is received via the application
      // tags
      for (String tag : app.getApplicationTags()) {

        String[] parts = tag.split(":", 2);
        if (parts.length != 2 || parts[1].isEmpty()) {
          continue;
        }
        switch (parts[0].toUpperCase()) {


        case TimelineUtils.FLOW_NAME_TAG_PREFIX:
          setFlowTags(environment, TimelineUtils.FLOW_NAME_TAG_PREFIX,
              parts[1]);
          break;


        case TimelineUtils.FLOW_VERSION_TAG_PREFIX:
          setFlowTags(environment, TimelineUtils.FLOW_VERSION_TAG_PREFIX,
              parts[1]);
          break;


        case TimelineUtils.FLOW_RUN_ID_TAG_PREFIX:
          setFlowTags(environment, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX,
              parts[1]);
          break;
        default:
          break;
        }
      }
    }
  }

  private static void setFlowTags(
      Map<String, String> environment, String tagPrefix, String value) {
    if (!value.isEmpty()) {
      environment.put(tagPrefix, value);
    }
  }

  @VisibleForTesting
  protected Token<AMRMTokenIdentifier> createAndSetAMRMToken() {
    Token<AMRMTokenIdentifier> amrmToken =
        this.rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
          application.getAppAttemptId());
    ((RMAppAttemptImpl)application).setAMRMToken(amrmToken);
    return amrmToken;
  }

  @SuppressWarnings("unchecked")
  public void run() {

    switch (eventType) {
    // 启动AM
    case LAUNCH:
      try {
        LOG.info("Launching master" + application.getAppAttemptId());

        // 尝试启动Container
        launch();

        // 处理RMAppAttemptEvent启动事件
        handler.handle(new RMAppAttemptEvent(application.getAppAttemptId(),
            RMAppAttemptEventType.LAUNCHED, System.currentTimeMillis()));
      } catch(Exception ie) {
        onAMLaunchFailed(masterContainer.getId(), ie);
      }
      break;
    // 执行请求操作.
    case CLEANUP:
      try {
        LOG.info("Cleaning master " + application.getAppAttemptId());

        // 执行清理操作
        cleanup();
      } catch(IOException ie) {
        LOG.info("Error cleaning master ", ie);
      } catch (YarnException e) {
        StringBuilder sb = new StringBuilder("Container ");
        sb.append(masterContainer.getId().toString());
        sb.append(" is not handled by this NodeManager");
        if (!e.getMessage().contains(sb.toString())) {
          // Ignoring if container is already killed by Node Manager.
          LOG.info("Error cleaning master ", e);
        }
      }
      break;
    default:
      LOG.warn("Received unknown event-type " + eventType + ". Ignoring.");
      break;
    }
  }

  private void parseAndThrowException(Throwable t) throws YarnException,
      IOException {
    if (t instanceof YarnException) {
      throw (YarnException) t;
    } else if (t instanceof InvalidToken) {
      throw (InvalidToken) t;
    } else {
      throw (IOException) t;
    }
  }

  @SuppressWarnings("unchecked")
  protected void onAMLaunchFailed(ContainerId containerId, Exception ie) {
    String message = "Error launching " + application.getAppAttemptId()
            + ". Got exception: " + StringUtils.stringifyException(ie);
    LOG.info(message);
    handler.handle(new RMAppAttemptEvent(application
           .getAppAttemptId(), RMAppAttemptEventType.LAUNCH_FAILED, message));
  }
}
