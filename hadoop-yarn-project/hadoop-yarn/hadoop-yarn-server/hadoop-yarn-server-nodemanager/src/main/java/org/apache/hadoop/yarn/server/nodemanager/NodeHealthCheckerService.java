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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.NodeHealthScriptRunner;

import java.util.Arrays;
import java.util.Collections;

/**
 * The class which provides functionality of checking the health of the node and
 * reporting back to the service for which the health checker has been asked to
 * report.
 */
public class NodeHealthCheckerService extends CompositeService {

  // 健康检查线程类 : 周期性执行节点健康状况检测脚本
  private NodeHealthScriptRunner nodeHealthScriptRunner;

  // 提供检查节点本地目录运行状况的功能的类。
  private LocalDirsHandlerService dirsHandler;

  // 异常信息
  private Exception nodeHealthException;
  // 异常事件.
  private long nodeHealthExceptionReportTime;

  // 分隔符
  static final String SEPARATOR = ";";

  public NodeHealthCheckerService(NodeHealthScriptRunner scriptRunner,
      LocalDirsHandlerService dirHandlerService) {
    super(NodeHealthCheckerService.class.getName());
    nodeHealthScriptRunner = scriptRunner;
    dirsHandler = dirHandlerService;
    nodeHealthException = null;
    nodeHealthExceptionReportTime = 0;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (nodeHealthScriptRunner != null) {
      addService(nodeHealthScriptRunner);
    }
    addService(dirsHandler);
    super.serviceInit(conf);
  }

  /**
   * @return the reporting string of health of the node
   */
  String getHealthReport() {
    // 脚本运行报告.
    String scriptReport = Strings.emptyToNull(
        nodeHealthScriptRunner == null ? null :
        nodeHealthScriptRunner.getHealthReport());

    // 文件目录报过.
    String discReport =
        Strings.emptyToNull(
            dirsHandler.getDisksHealthReport(false));

    // 异常报告.
    String exceptionReport = Strings.emptyToNull(
        nodeHealthException == null ? null :
        nodeHealthException.getMessage());

    // 输出报告结果... 以  分号 " ; " 分隔..
    return Joiner.on(SEPARATOR).skipNulls()
        .join(scriptReport, discReport, exceptionReport);
  }

  /**
   * @return <em>true</em> if the node is healthy
   */
  boolean isHealthy() {
    boolean scriptHealthy = nodeHealthScriptRunner == null ||
        nodeHealthScriptRunner.isHealthy();
    return nodeHealthException == null &&
        scriptHealthy && dirsHandler.areDisksHealthy();
  }

  /**
   * @return when the last time the node health status is reported
   */
  long getLastHealthReportTime() {
    return Collections.max(Arrays.asList(
        dirsHandler.getLastDisksCheckTime(),
        nodeHealthScriptRunner == null ? 0 :
            nodeHealthScriptRunner.getLastReportedTime(),
        nodeHealthExceptionReportTime));
  }

  /**
   * @return the disk handler
   */
  public LocalDirsHandlerService getDiskHandler() {
    return dirsHandler;
  }

  /**
   * @return the node health script runner
   */
  NodeHealthScriptRunner getNodeHealthScriptRunner() {
    return nodeHealthScriptRunner;
  }

  /**
   * Report an exception to mark the node as unhealthy.
   * @param ex the exception that makes the node unhealthy
   */
  void reportException(Exception ex) {
    nodeHealthException = ex;
    nodeHealthExceptionReportTime = System.currentTimeMillis();
  }
}
