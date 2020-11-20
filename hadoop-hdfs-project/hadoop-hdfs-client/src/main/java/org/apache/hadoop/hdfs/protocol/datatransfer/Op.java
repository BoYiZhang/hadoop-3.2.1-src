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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** Operation */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum Op {
  //操作码80， 对应DataTransferProtocol.writeBlock()方法
  WRITE_BLOCK((byte)80),
  //操作码81， 对应DataTransferProtocol.readBlock()方法
  READ_BLOCK((byte)81),

  READ_METADATA((byte)82),

  //操作码83， 对应DataTransferProtocol.replaceBlock()方法
  REPLACE_BLOCK((byte)83),

  //操作码84， 对应DataTransferProtocol.copyBlock()方法
  COPY_BLOCK((byte)84),

  //操作码85， 对应DataTransferProtocol.blockChecksum()方法
  BLOCK_CHECKSUM((byte)85),

  //操作码86， 对应DataTransferProtocol.transferBlock()方法
  TRANSFER_BLOCK((byte)86),

  REQUEST_SHORT_CIRCUIT_FDS((byte)87),
  RELEASE_SHORT_CIRCUIT_FDS((byte)88),
  REQUEST_SHORT_CIRCUIT_SHM((byte)89),
  BLOCK_GROUP_CHECKSUM((byte)90),
  CUSTOM((byte)127);

  /** The code for this operation. */
  public final byte code;

  Op(byte code) {
    this.code = code;
  }

  private static final int FIRST_CODE = values()[0].code;
  /** Return the object represented by the code. */
  private static Op valueOf(byte code) {
    final int i = (code & 0xff) - FIRST_CODE;
    return i < 0 || i >= values().length? null: values()[i];
  }

  /** Read from in */
  public static Op read(DataInput in) throws IOException {
    return valueOf(in.readByte());
  }

  /** Write to out */
  public void write(DataOutput out) throws IOException {
    out.write(code);
  }
}
