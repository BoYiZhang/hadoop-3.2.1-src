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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ByteBufferPool;

import com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ByteBufferUtil {

  /**
   * Determine if a stream can do a byte buffer read via read(ByteBuffer buf)
   */
  private static boolean streamHasByteBufferRead(InputStream stream) {
    if (!(stream instanceof ByteBufferReadable)) {
      return false;
    }
    if (!(stream instanceof FSDataInputStream)) {
      return true;
    }
    return ((FSDataInputStream)stream).getWrappedStream() 
        instanceof ByteBufferReadable;
  }

  /**
   * 当read()方法执行零拷贝读操作失败后， 会调用ByteBufferUtil.fallbackRead()退化为一
   * 个普通的读操作。 ByteBufferUtil.fallbackRead()方法非常简单， 判断传入参数的
   * InputStream(DFSInputStream)是否支持ByteBuffer Read（实现了ByteBufferReadable接
   * 口） 。 如果支持则直接将数据读取至ByteBuffer中， 否则读取到ByteBuffer.array()字节数组
   * 中。
   * Perform a fallback read.
   */
  public static ByteBuffer fallbackRead(
      InputStream stream, ByteBufferPool bufferPool, int maxLength)
          throws IOException {
    if (bufferPool == null) {
      throw new UnsupportedOperationException("zero-copy reads " +
          "were not available, and you did not provide a fallback " +
          "ByteBufferPool.");
    }


    //判断stream是否支持将数据读入ByteBuffer
    boolean useDirect = streamHasByteBufferRead(stream);

    //调用ByteBufferPool构造一个ByteBuffer
    ByteBuffer buffer = bufferPool.getBuffer(useDirect, maxLength);
    if (buffer == null) {
      throw new UnsupportedOperationException("zero-copy reads " +
          "were not available, and the ByteBufferPool did not provide " +
          "us with " + (useDirect ? "a direct" : "an indirect") +
          "buffer.");
    }
    Preconditions.checkState(buffer.capacity() > 0);
    Preconditions.checkState(buffer.isDirect() == useDirect);
    maxLength = Math.min(maxLength, buffer.capacity());
    boolean success = false;
    try {
      if (useDirect) {
        buffer.clear();
        buffer.limit(maxLength);
        ByteBufferReadable readable = (ByteBufferReadable)stream;
        int totalRead = 0;
        while (true) {
          if (totalRead >= maxLength) {
            success = true;
            break;
          }
          //直接调用stream上支持ByteBuffer Read的函数
          int nRead = readable.read(buffer);
          if (nRead < 0) {
            if (totalRead > 0) {
              success = true;
            }
            break;
          }
          totalRead += nRead;
        }
        buffer.flip();
      } else {
        buffer.clear();
        //调用InputStream.read(byte[])方法
        int nRead = stream.read(buffer.array(),
            buffer.arrayOffset(), maxLength);
        if (nRead >= 0) {
          buffer.limit(nRead);
          success = true;
        }
      }
    } finally {
      if (!success) {
        // If we got an error while reading, or if we are at EOF, we 
        // don't need the buffer any more.  We can give it back to the
        // bufferPool.
        bufferPool.putBuffer(buffer);
        buffer = null;
      }
    }
    return buffer;
  }
}
