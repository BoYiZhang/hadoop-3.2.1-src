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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;


public class ApplicationMasterLauncher extends AbstractService implements
    EventHandler<AMLauncherEvent> {
  private static final Log LOG = LogFactory.getLog(  ApplicationMasterLauncher.class);

  // ApplicationMasterLauncher 线程池
  private ThreadPoolExecutor launcherPool;

  // [ 主 ]工作线程, 用于监控masterEvents队列, 并将任务下发给launcherPool执行.
  private LauncherThread launcherHandlingThread;

  // 阻塞式队列
  private final BlockingQueue<Runnable> masterEvents  = new LinkedBlockingQueue<Runnable>();

  // RM的 context信息 : RMContextImpl
  protected final RMContext context;
  
  public ApplicationMasterLauncher(RMContext context) {
    super(ApplicationMasterLauncher.class.getName());
    this.context = context;
    this.launcherHandlingThread = new LauncherThread();
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {

    // 默认线程数量 : 50
    int threadCount = conf.getInt(
        YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT);
    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("ApplicationMasterLauncher #%d")
        .build();

    // 构架线程池
    launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());

    // 构建线程工厂类
    launcherPool.setThreadFactory(tf);

    Configuration newConf = new YarnConfiguration(conf);

    // 最大重试次数: 10
    newConf.setInt(CommonConfigurationKeysPublic.
            IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETRIES,
            YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES));
    setConfig(newConf);
    super.serviceInit(newConf);
  }

  @Override
  protected void serviceStart() throws Exception {
    launcherHandlingThread.start();
    super.serviceStart();
  }
  
  protected Runnable createRunnableLauncher(RMAppAttempt application, 
      AMLauncherEventType event) {
    Runnable launcher =
        new AMLauncher(context, application, event, getConfig());
    return launcher;
  }
  
  private void launch(RMAppAttempt application) {
    Runnable launcher = createRunnableLauncher(application, 
        AMLauncherEventType.LAUNCH);
    masterEvents.add(launcher);
  }
  

  @Override
  protected void serviceStop() throws Exception {
    launcherHandlingThread.interrupt();
    try {
      launcherHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info(launcherHandlingThread.getName() + " interrupted during join ", 
          ie);    }
    launcherPool.shutdown();
  }

  private class LauncherThread extends Thread {
    
    public LauncherThread() {
      super("ApplicationMaster Launcher");
    }

    @Override
    public void run() {
      while (!this.isInterrupted()) {
        Runnable toLaunch;
        try {
          // 从队列汇总获取任务.
          toLaunch = masterEvents.take();
          // 线程池用于处理 masterEvents 队列中的事件
          launcherPool.execute(toLaunch);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }
  }    

  private void cleanup(RMAppAttempt application) {
    Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.CLEANUP);
    masterEvents.add(launcher);
  } 
  
  @Override
  public synchronized void  handle(AMLauncherEvent appEvent) {
    AMLauncherEventType event = appEvent.getType();
    RMAppAttempt application = appEvent.getAppAttempt();
    switch (event) {
    case LAUNCH:
      launch(application);
      break;
    case CLEANUP:
      cleanup(application);
      break;
    default:
      break;
    }
  }
}
