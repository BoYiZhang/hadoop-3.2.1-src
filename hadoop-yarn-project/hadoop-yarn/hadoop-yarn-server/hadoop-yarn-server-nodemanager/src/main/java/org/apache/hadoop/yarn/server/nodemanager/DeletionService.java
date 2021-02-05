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

import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.hadoop.yarn.server.nodemanager.recovery.RecoveryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto;
import org.apache.hadoop.yarn.server.nodemanager.api.impl.pb.NMProtoUtils;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.recovery.DeletionTaskRecoveryInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.DeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMNullStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class DeletionService extends AbstractService {

  private static final Logger LOG =  LoggerFactory.getLogger(DeletionService.class);

  // 延迟
  private int debugDelay;

  // ContainerExecutor
  private final ContainerExecutor containerExecutor;

  //存储
  private final NMStateStoreService stateStore;

  // 线程池
  private ScheduledThreadPoolExecutor sched;

  // 原子类, 获取 任务的id
  private AtomicInteger nextTaskId = new AtomicInteger(0);



  public DeletionService(ContainerExecutor exec) {
    this(exec, new NMNullStateStoreService());
  }


  public DeletionService(ContainerExecutor containerExecutor,  NMStateStoreService stateStore) {
    super(DeletionService.class.getName());
    this.containerExecutor = containerExecutor;
    this.debugDelay = 0;
    this.stateStore = stateStore;
  }

  public int getDebugDelay() {
    return debugDelay;
  }

  public ContainerExecutor getContainerExecutor() {
    return containerExecutor;
  }

  public NMStateStoreService getStateStore() {
    return stateStore;
  }

  public void delete(DeletionTask deletionTask) {
    if (debugDelay != -1) {
      if (LOG.isDebugEnabled()) {
        String msg = String.format("Scheduling DeletionTask (delay %d) : %s",
            debugDelay, deletionTask.toString());
        LOG.debug(msg);
      }
      // 删除存储
      recordDeletionTaskInStateStore(deletionTask);
      // 执行删除任务..
      sched.schedule(deletionTask, debugDelay, TimeUnit.SECONDS);
    }
  }

  private void recover(NMStateStoreService.RecoveredDeletionServiceState state)  throws IOException {
    Map<Integer, DeletionTaskRecoveryInfo> idToInfoMap =   new HashMap<Integer, DeletionTaskRecoveryInfo>();
    Set<Integer> successorTasks = new HashSet<Integer>();

    try (RecoveryIterator<DeletionServiceDeleteTaskProto> it =  state.getIterator()) {
      while (it.hasNext()) {
        DeletionServiceDeleteTaskProto proto = it.next();
        DeletionTaskRecoveryInfo info =
            NMProtoUtils.convertProtoToDeletionTaskRecoveryInfo(proto, this);
        idToInfoMap.put(info.getTask().getTaskId(), info);
        nextTaskId.set(Math.max(nextTaskId.get(), info.getTask().getTaskId()));
        successorTasks.addAll(info.getSuccessorTaskIds());
      }
    }

    // restore the task dependencies and schedule the deletion tasks that
    // have no predecessors
    final long now = System.currentTimeMillis();
    for (DeletionTaskRecoveryInfo info : idToInfoMap.values()) {
      for (Integer successorId : info.getSuccessorTaskIds()){
        DeletionTaskRecoveryInfo successor = idToInfoMap.get(successorId);
        if (successor != null) {
          info.getTask().addDeletionTaskDependency(successor.getTask());
        } else {
          LOG.error("Unable to locate dependency task for deletion task "
              + info.getTask().getTaskId());
        }
      }
      if (!successorTasks.contains(info.getTask().getTaskId())) {
        long msecTilDeletion = info.getDeletionTimestamp() - now;
        sched.schedule(info.getTask(), msecTilDeletion, TimeUnit.MILLISECONDS);
      }
    }
  }

  private int generateTaskId() {
    // get the next ID but avoid an invalid ID
    int taskId = nextTaskId.incrementAndGet();
    while (taskId == DeletionTask.INVALID_TASK_ID) {
      taskId = nextTaskId.incrementAndGet();
    }
    return taskId;
  }

  private void recordDeletionTaskInStateStore(DeletionTask task) {
    if (!stateStore.canRecover()) {
      // optimize the case where we aren't really recording
      return;
    }
    if (task.getTaskId() != DeletionTask.INVALID_TASK_ID) {
      return;  // task already recorded
    }
    // 设置TaskId
    task.setTaskId(generateTaskId());

    // store successors first to ensure task IDs have been generated for them
    DeletionTask[] successors = task.getSuccessorTasks();
    for (DeletionTask successor : successors) {
      recordDeletionTaskInStateStore(successor);
    }

    try {
      stateStore.storeDeletionTask(task.getTaskId(),
          task.convertDeletionTaskToProto());
    } catch (IOException e) {
      LOG.error("Unable to store deletion task " + task.getTaskId(), e);
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("DeletionService #%d")
        .build();



    if (conf != null) {

      // 从配置中获取参数
      // yarn.nodemanager.delete.thread-count : 4
      sched = new HadoopScheduledThreadPoolExecutor(
          conf.getInt(YarnConfiguration.NM_DELETE_THREAD_COUNT,
              YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT), tf);

      // yarn.nodemanager.delete.debug-delay-sec : 0
      debugDelay = conf.getInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 0);

    } else {

      sched = new HadoopScheduledThreadPoolExecutor(
          YarnConfiguration.DEFAULT_NM_DELETE_THREAD_COUNT, tf);
    }

    // true时，在执行shutdown方法后，当前正在等待的任务的和正在运行的任务需要被执行完，然后进程被销毁；
    // false时，表示放弃等待的任务，正在运行的任务一旦完成，则进程被销毁对与cheduleAtFisedRate方法和scheduleWithFixedDelay方法，则会打断循环，只执行当前正在执行的任务，不会对他们在进行循环执行
    sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

    //
    sched.setKeepAliveTime(60L, SECONDS);
    if (stateStore.canRecover()) {
      recover(stateStore.loadDeletionServiceState());
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    if (sched != null) {
      sched.shutdown();
      boolean terminated = false;
      try {
        terminated = sched.awaitTermination(10, SECONDS);
      } catch (InterruptedException e) { }
      if (!terminated) {
        sched.shutdownNow();
      }
    }
    super.serviceStop();
  }

  /**
   * Determine if the service has completely stopped.
   * Used only by unit tests
   * @return true if service has completely stopped
   */
  @Private
  public boolean isTerminated() {
    return getServiceState() == STATE.STOPPED && sched.isTerminated();
  }
}