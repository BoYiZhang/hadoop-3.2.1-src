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

package org.apache.hadoop.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.SocketFactory;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;

/** An RPC implementation. */
@InterfaceStability.Evolving
public interface RpcEngine {

  /** Construct a client-side proxy object. 
   * @param <T>*/
  /**
   * 客户端会调用RpcEngine.getProxy()方法获取一个本地接口的代理对 象，
   * 然后在这个代理对象上调用本地接口的方法。
   *
   * getProxy()方法的实现采用了 Java动态代理机制，
   * 客户端调用程序在代理对象上的调用会由一个
   * RpcInvocationHandler(java.lang.reflect.InvocationHandler的子类，
   * 在RpcEngine的 实现类中定义)对象处理，
   * 这个RpcInvocationHandler会将请求序列化
   * (使用 RpcEngine实现类定义的序列化方式)
   * 并调用Client.call()方法将请求发送到远程 服务器。
   * 当远程服务器发回响应信息后，RpcInvocationHandler会将响应信息反 序列化并返回给调用程序，
   * 这一切通过Java动态代理机制对于调用程序是完全透 明的，就像本地调用一样。
   *
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param ticket
   * @param conf
   * @param factory
   * @param rpcTimeout
   * @param connectionRetryPolicy
   * @param <T>
   * @return
   * @throws IOException
   */
  <T> ProtocolProxy<T> getProxy(Class<T> protocol,
                  long clientVersion, InetSocketAddress addr,
                  UserGroupInformation ticket, Configuration conf,
                  SocketFactory factory, int rpcTimeout,
                  RetryPolicy connectionRetryPolicy) throws IOException;

  /** Construct a client-side proxy object. */
  <T> ProtocolProxy<T> getProxy(Class<T> protocol,
                  long clientVersion, InetSocketAddress addr,
                  UserGroupInformation ticket, Configuration conf,
                  SocketFactory factory, int rpcTimeout,
                  RetryPolicy connectionRetryPolicy,
                  AtomicBoolean fallbackToSimpleAuth,
                  AlignmentContext alignmentContext) throws IOException;

  /**
   *
   * 该方法用于产生一个RPC Server对象，服务器会启动这个Server对象 监听从客户端发来的请求。
   * 成功从网络接收请求数据后，
   * Server对象会调用 Rpclnvoker(在RpcEngine的实现类中定义)对象处理这个请求。
   *
   * Construct a server for a protocol implementation instance.
   * 
   * @param protocol the class of protocol to use
   * @param instance the instance of protocol whose methods will be called
   * @param conf the configuration to use
   * @param bindAddress the address to bind on to listen for connection
   * @param port the port to listen for connections on
   * @param numHandlers the number of method handler threads to run
   * @param numReaders the number of reader threads to run
   * @param queueSizePerHandler the size of the queue per hander thread
   * @param verbose whether each call should be logged
   * @param secretManager The secret manager to use to validate incoming requests.
   * @param portRangeConfig A config parameter that can be used to restrict
   *        the range of ports used when port is 0 (an ephemeral port)
   * @param alignmentContext provides server state info on client responses
   * @return The Server instance
   * @throws IOException on any error
   */
  RPC.Server getServer(Class<?> protocol, Object instance, String bindAddress,
                       int port, int numHandlers, int numReaders,
                       int queueSizePerHandler, boolean verbose,
                       Configuration conf, 
                       SecretManager<? extends TokenIdentifier> secretManager,
                       String portRangeConfig,
                       AlignmentContext alignmentContext) throws IOException;

  /**
   * Returns a proxy for ProtocolMetaInfoPB, which uses the given connection
   * id.
   * @param connId, ConnectionId to be used for the proxy.
   * @param conf, Configuration.
   * @param factory, Socket factory.
   * @return Proxy object.
   * @throws IOException
   */
  ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException;
}
