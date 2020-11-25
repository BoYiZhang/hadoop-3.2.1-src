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
package org.apache.hadoop.hdfs.util;

import static org.apache.hadoop.util.Time.monotonicNow;

/**
 *
 * 用于限制数据传输的类。
 *
 * 这个类是线程安全的。它可以被多个线程共享。
 *
 * 参数bandwidthPerSec指定线程共享的总带宽。
 *
 * a class to throttle the data transfers.
 * This class is thread safe. It can be shared by multiple threads.
 * The parameter bandwidthPerSec specifies the total bandwidth shared by
 * threads.
 */
public class DataTransferThrottler {
  // 描述一个发送周期的时长， 单位是毫秒   500
  // period over which bw is imposed
  private final long period;

  // 周期最大延长的时间范围 , 为 period 的三倍   1500
  // Max period over which bw accumulates.
  private final long periodExtension;

  // 每周期可以发送的总字节数
  // total number of bytes can be sent in each period
  private long bytesPerPeriod;

  // 本周期开始时间
  // current period starting time
  private long curPeriodStart;

  // 在该周期内还有多少字节可以发送   5242880
  // remaining bytes can be sent in the period
  private long curReserve;

  // 在该周期内使用的字节数
  private long bytesAlreadyUsed;

  /** Constructor 
   * @param bandwidthPerSec bandwidth allowed in bytes per second. 
   */
  public DataTransferThrottler(long bandwidthPerSec) {
    this(500, bandwidthPerSec);  // by default throttling period is 500ms 
  }

  /**
   * Constructor
   * @param period in milliseconds. Bandwidth is enforced over this period.
   * @param bandwidthPerSec bandwidth allowed in bytes per second. 
   */
  public DataTransferThrottler(long period, long bandwidthPerSec) {
    this.curPeriodStart = monotonicNow();
    this.period = period;
    this.curReserve = this.bytesPerPeriod = bandwidthPerSec*period/1000;
    this.periodExtension = period*3;
  }

  /**
   * @return current throttle bandwidth in bytes per second.
   */
  public synchronized long getBandwidth() {
    return bytesPerPeriod*1000/period;
  }
  
  /**
   * Sets throttle bandwidth. This takes affect latest by the end of current
   * period.
   */
  public synchronized void setBandwidth(long bytesPerSecond) {
    if ( bytesPerSecond <= 0 ) {
      throw new IllegalArgumentException("" + bytesPerSecond);
    }
    bytesPerPeriod = bytesPerSecond*period/1000;
  }
  
  /** Given the numOfBytes sent/received since last time throttle was called,
   * make the current thread sleep if I/O rate is too fast
   * compared to the given bandwidth.
   *
   * @param numOfBytes
   *     number of bytes sent/received since last time throttle was called
   */
  public synchronized void throttle(long numOfBytes) {
    throttle(numOfBytes, null);
  }

  /** Given the numOfBytes sent/received since last time throttle was called,
   * make the current thread sleep if I/O rate is too fast
   * compared to the given bandwidth.  Allows for optional external cancelation.
   *
   * @param numOfBytes
   *     number of bytes sent/received since last time throttle was called
   * @param canceler
   *     optional canceler to check for abort of throttle
   */
  public synchronized void throttle(long numOfBytes, Canceler canceler) {
    if ( numOfBytes <= 0 ) {
      return;
    }

    curReserve -= numOfBytes;
    bytesAlreadyUsed += numOfBytes;

    while (curReserve <= 0) {
      if (canceler != null && canceler.isCancelled()) {
        return;
      }
      long now = monotonicNow();
      long curPeriodEnd = curPeriodStart + period;

      if ( now < curPeriodEnd ) {
        //等待至下一个周期， curReserve可以增加
        // Wait for next period so that curReserve can be increased.
        try {
          wait( curPeriodEnd - now );
        } catch (InterruptedException e) {
          // Abort throttle and reset interrupted status to make sure other
          // interrupt handling higher in the call stack executes.
          Thread.currentThread().interrupt();
          break;
        }
      } else if ( now <  (curPeriodStart + periodExtension)) {
        curPeriodStart = curPeriodEnd;
        //增加剩余请求量
        curReserve += bytesPerPeriod;
      } else {
        // discard the prev period. Throttler might not have been used for a long time.
        // 长时间没有使用节流器， 重置节流器
        curPeriodStart = now;
        curReserve = bytesPerPeriod - bytesAlreadyUsed;
      }
    }

    bytesAlreadyUsed -= numOfBytes;
  }
}
