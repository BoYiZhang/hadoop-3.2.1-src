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

package org.apache.hadoop.yarn.event;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * This is a specialized EventHandler to be used by Services that are expected
 * handle a large number of events efficiently by ensuring that the caller
 * thread is not blocked. Events are immediately stored in a BlockingQueue and
 * a separate dedicated Thread consumes events from the queue and handles
 * appropriately
 * @param <T> Type of Event
 */
public class EventDispatcher<T extends Event> extends
    AbstractService implements EventHandler<T> {

  private final EventHandler<T> handler;

  // 他也有一个事件阻塞队列
  private final BlockingQueue<T> eventQueue =  new LinkedBlockingDeque<>();

  // 处理事件的队列
  private final Thread eventProcessor;

  // 线程应该停止与否的标志
  private volatile boolean stopped = false;

  // 在执行事件过程中如果遇到异常是否应该导致程序退出
  private boolean shouldExitOnError = true;

  private static final Log LOG = LogFactory.getLog(EventDispatcher.class);


  // 处理事件的线程，跟前面的 createThread 线程类似
  private final class EventProcessor implements Runnable {
    @Override
    public void run() {

      T event;

      while (!stopped && !Thread.currentThread().isInterrupted()) {
        try {
          event = eventQueue.take();
        } catch (InterruptedException e) {
          LOG.error("Returning, interrupted : " + e);
          return; // TODO: Kill RM.
        }

        try {
          // 注意这里，把事件直接交给了我们的主角-EventHandler ！！
          handler.handle(event);
        } catch (Throwable t) {
          // An error occurred, but we are shutting down anyway.
          // If it was an InterruptedException, the very act of
          // shutdown could have caused it and is probably harmless.
          if (stopped) {
            LOG.warn("Exception during shutdown: ", t);
            break;
          }
          LOG.fatal("Error in handling event type " + event.getType()
              + " to the Event Dispatcher", t);
          if (shouldExitOnError
              && !ShutdownHookManager.get().isShutdownInProgress()) {
            LOG.info("Exiting, bbye..");
            System.exit(-1);
          }
        }
      }
    }
  }

  // 构造方法，在前面介绍过，是RM在serviceInit方法中调用
  public EventDispatcher(EventHandler<T> handler, String name) {
    super(name);
    this.handler = handler;
    this.eventProcessor = new Thread(new EventProcessor());
    this.eventProcessor.setName(getName() + ":Event Processor");
  }

  @Override
  protected void serviceStart() throws Exception {
    this.eventProcessor.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    this.stopped = true;
    this.eventProcessor.interrupt();
    try {
      this.eventProcessor.join();
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceStop();
  }

  @Override
  public void handle(T event) {
    try {
      int qSize = eventQueue.size();
      if (qSize !=0 && qSize %1000 == 0) {
        LOG.info("Size of " + getName() + " event-queue is " + qSize);
      }
      int remCapacity = eventQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.info("Very low remaining capacity on " + getName() + "" +
            "event queue: " + remCapacity);
      }
      // 处理事件就是放入自己的阻塞队列，让处理线程去处理
      this.eventQueue.put(event);
    } catch (InterruptedException e) {
      LOG.info("Interrupted. Trying to exit gracefully.");
    }
  }

  @VisibleForTesting
  public void disableExitOnError() {
    shouldExitOnError = false;
  }
}
