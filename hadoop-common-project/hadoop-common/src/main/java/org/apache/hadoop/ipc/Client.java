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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import javax.security.sasl.Sasl;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
@Public
@InterfaceStability.Evolving
public class Client implements AutoCloseable {
  
  public static final Logger LOG = LoggerFactory.getLogger(Client.class);

  /** A counter for generating call IDs. */
  private static final AtomicInteger callIdCounter = new AtomicInteger();

  private static final ThreadLocal<Integer> callId = new ThreadLocal<Integer>();
  private static final ThreadLocal<Integer> retryCount = new ThreadLocal<Integer>();
  private static final ThreadLocal<Object> EXTERNAL_CALL_HANDLER
      = new ThreadLocal<>();
  private static final ThreadLocal<AsyncGet<? extends Writable, IOException>>
      ASYNC_RPC_RESPONSE = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> asynchronousMode =
      new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
          return false;
        }
      };

  @SuppressWarnings("unchecked")
  @Unstable
  public static <T extends Writable> AsyncGet<T, IOException>
      getAsyncRpcResponse() {
    return (AsyncGet<T, IOException>) ASYNC_RPC_RESPONSE.get();
  }

  /** Set call id and retry count for the next call. */
  public static void setCallIdAndRetryCount(int cid, int rc,
                                            Object externalHandler) {
    Preconditions.checkArgument(cid != RpcConstants.INVALID_CALL_ID);
    Preconditions.checkState(callId.get() == null);
    Preconditions.checkArgument(rc != RpcConstants.INVALID_RETRY_COUNT);

    callId.set(cid);
    retryCount.set(rc);
    EXTERNAL_CALL_HANDLER.set(externalHandler);
  }

  private ConcurrentMap<ConnectionId, Connection> connections =
      new ConcurrentHashMap<>();

  private Class<? extends Writable> valueClass;   // class of call values
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs
  final private Configuration conf;

  private SocketFactory socketFactory;           // how to create sockets
  private int refCount = 1;

  private final int connectionTimeout;

  private final boolean fallbackAllowed;
  private final boolean bindToWildCardAddress;
  private final byte[] clientId;
  private final int maxAsyncCalls;
  private final AtomicInteger asyncCallCounter = new AtomicInteger(0);

  /**
   * Executor on which IPC calls' parameters are sent.
   * Deferring the sending of parameters to a separate
   * thread isolates them from thread interruptions in the
   * calling code.
   */
  private final ExecutorService sendParamsExecutor;
  private final static ClientExecutorServiceFactory clientExcecutorFactory =
      new ClientExecutorServiceFactory();

  private static class ClientExecutorServiceFactory {
    private int executorRefCount = 0;
    private ExecutorService clientExecutor = null;
    
    /**
     * Get Executor on which IPC calls' parameters are sent.
     * If the internal reference counter is zero, this method
     * creates the instance of Executor. If not, this method
     * just returns the reference of clientExecutor.
     * 
     * @return An ExecutorService instance
     */
    synchronized ExecutorService refAndGetInstance() {
      if (executorRefCount == 0) {
        clientExecutor = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("IPC Parameter Sending Thread #%d")
            .build());
      }
      executorRefCount++;
      
      return clientExecutor;
    }
    
    /**
     * Cleanup Executor on which IPC calls' parameters are sent.
     * If reference counter is zero, this method discards the
     * instance of the Executor. If not, this method
     * just decrements the internal reference counter.
     * 
     * @return An ExecutorService instance if it exists.
     *   Null is returned if not.
     */
    synchronized ExecutorService unrefAndCleanup() {
      executorRefCount--;
      assert(executorRefCount >= 0);
      
      if (executorRefCount == 0) {
        clientExecutor.shutdown();
        try {
          if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
            clientExecutor.shutdownNow();
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for clientExecutor" +
              " to stop");
          clientExecutor.shutdownNow();
          Thread.currentThread().interrupt();
        }
        clientExecutor = null;
      }
      
      return clientExecutor;
    }
  };
  
  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  public static final void setPingInterval(Configuration conf,
      int pingInterval) {
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  public static final int getPingInterval(Configuration conf) {
    return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
  }

  /**
   * The time after which a RPC will timeout.
   * If ping is not enabled (via ipc.client.ping), then the timeout value is the 
   * same as the pingInterval.
   * If ping is enabled, then there is no timeout value.
   * 
   * @param conf Configuration
   * @return the timeout period in milliseconds. -1 if no timeout value is set
   * @deprecated use {@link #getRpcTimeout(Configuration)} instead
   */
  @Deprecated
  final public static int getTimeout(Configuration conf) {
    int timeout = getRpcTimeout(conf);
    if (timeout > 0)  {
      return timeout;
    }
    if (!conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
        CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT)) {
      return getPingInterval(conf);
    }
    return -1;
  }

  /**
   * The time after which a RPC will timeout.
   *
   * @param conf Configuration
   * @return the timeout period in milliseconds.
   */
  public static final int getRpcTimeout(Configuration conf) {
    int timeout =
        conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
            CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);
    return (timeout < 0) ? 0 : timeout;
  }
  /**
   * set the connection timeout value in configuration
   * 
   * @param conf Configuration
   * @param timeout the socket connect timeout value
   */
  public static final void setConnectTimeout(Configuration conf, int timeout) {
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY, timeout);
  }

  @VisibleForTesting
  public static final ExecutorService getClientExecutor() {
    return Client.clientExcecutorFactory.clientExecutor;
  }
  /**
   * Increment this client's reference count
   *
   */
  synchronized void incCount() {
    refCount++;
  }
  
  /**
   * Decrement this client's reference count
   *
   */
  synchronized void decCount() {
    refCount--;
  }
  
  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount==0;
  }

  /** Check the rpc response header. */
  void checkResponse(RpcResponseHeaderProto header) throws IOException {
    if (header == null) {
      throw new EOFException("Response is null.");
    }
    if (header.hasClientId()) {
      // check client IDs
      final byte[] id = header.getClientId().toByteArray();
      if (!Arrays.equals(id, RpcConstants.DUMMY_CLIENT_ID)) {
        if (!Arrays.equals(id, clientId)) {
          throw new IOException("Client IDs not matched: local ID="
              + StringUtils.byteToHexString(clientId) + ", ID in response="
              + StringUtils.byteToHexString(header.getClientId().toByteArray()));
        }
      }
    }
  }


  /**
   *
   * @param rpcKind  rpcKind参数用于描述RPC请求的序列化工具类型
   * @param rpcRequest rpcRequest参数则用于记录序列化后的RPC请求
   * @return
   */
  Call createCall(RPC.RpcKind rpcKind, Writable rpcRequest) {
    return new Call(rpcKind, rpcRequest);
  }

  /** 
   * Class that represents an RPC call
   */
  static class Call {

    //RPC请求的id
    final int id;               // call id

    //重试次数
    final int retry;           // retry count

    //序列化的RPC请求
    final Writable rpcRequest;  // the serialized rpc request

    //序列化的RPC响应，如果调用发生错误，则这个字段为空
    Writable rpcResponse;       // null if rpc has error

    //如果发生错误，则保存远程的异常
    IOException error;          // exception, null if success

    //Rpc引擎类型, 使用ProtobufRpcEngine值固定为: RPC_PROTOCOL_BUFFER
    //使用 : ProtobufRpcEngine ==> RPC_PROTOCOL_BUFFER ((short) 3)
    final RPC.RpcKind rpcKind;      // Rpc EngineKind

    //调用操作是否完成
    boolean done;               // true when call is done


    private final Object externalHandler;
    private AlignmentContext alignmentContext;


    private Call(RPC.RpcKind rpcKind, Writable param) {
      this.rpcKind = rpcKind;
      this.rpcRequest = param;

      //获取 callId
      final Integer id = callId.get();
      if (id == null) {
        // AtomicInteger  callIdCounter  自增
        this.id = nextCallId();
      } else {
        callId.set(null);
        this.id = id;
      }
      
      final Integer rc = retryCount.get();
      if (rc == null) {
        this.retry = 0;
      } else {
        this.retry = rc;
      }
      // 设置异常处理类
      this.externalHandler = EXTERNAL_CALL_HANDLER.get();

    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + id;
    }

    /**
     * //调用callComplete()方法唤醒在Call对象上等待的线程
     *
     * Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    protected synchronized void callComplete() {
      //设置done字段为true，表明当前请求
      this.done = true;

      //唤醒在Call对象上等待的线程
      notify();                                 // notify caller

      if (externalHandler != null) {
        synchronized (externalHandler) {
          externalHandler.notify();
        }
      }
    }

    /**
     * Set an AlignmentContext for the call to update when call is done.
     *
     * @param ac alignment context to update.
     */
    public synchronized void setAlignmentContext(AlignmentContext ac) {
      this.alignmentContext = ac;
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(IOException error) {
      this.error = error;
      callComplete();
    }
    
    /**
     * 当Server成功地执行了RPC调用，并发回响应到接 收线程后，接收线程会调用Call.
     * setRpcResponse() 方法保存 Server 发回的响应信息。
     *
     * Set the return value when there is no error.
     * Notify the caller the call is done.
     * 
     * @param rpcResponse return value of the rpc call.
     */
    public synchronized void setRpcResponse(Writable rpcResponse) {
      this.rpcResponse = rpcResponse;
      callComplete();
    }
    
    public synchronized Writable getRpcResponse() {
      return rpcResponse;
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  private class Connection extends Thread {

    //Server IP端口
    private InetSocketAddress server;             // server ip:port
    private final ConnectionId remoteId;                // connection id
    private AuthMethod authMethod; // authentication method
    private AuthProtocol authProtocol;
    private int serviceClass;
    private SaslRpcClient saslRpcClient;

    //到Server的Socket连接
    private Socket socket = null;                 // connected socket
    private IpcStreams ipcStreams;
    private final int maxResponseLength;
    private final int rpcTimeout;

    //最长空闲时间
    private int maxIdleTime; //connections will be culled if it was idle for


    //maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private final int maxRetriesOnSasl;
    private int maxRetriesOnSocketTimeouts;
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private final boolean tcpLowLatency; // if T then use low-delay QoS
    private final boolean doPing; //do we need to send ping message
    private final int pingInterval; // how often sends ping to the server
    private final int soTimeout; // used by ipc ping and rpc timeout
    private byte[] pingRequest; // ping message

    // 当前活跃的 calls currently active calls
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();


    private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
    private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
    private IOException closeException; // close reason
    
    private final Object sendRpcRequestLock = new Object();

    private AtomicReference<Thread> connectingThread = new AtomicReference<>();

    public Connection(ConnectionId remoteId, int serviceClass) throws IOException {

      this.remoteId = remoteId;

      // 设置server 配置
      this.server = remoteId.getAddress();
      if (server.isUnresolved()) {
        throw NetUtils.wrapException(server.getHostName(),
            server.getPort(),
            null,
            0,
            new UnknownHostException());
      }

      // 最大响应大小 128
      this.maxResponseLength = remoteId.conf.getInt(
          CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
          CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);

      //超时时间
      this.rpcTimeout = remoteId.getRpcTimeout();
      this.maxIdleTime = remoteId.getMaxIdleTime();

      //设置重试策略  默认: 重试10次, 每次间隔1秒
      this.connectionRetryPolicy = remoteId.connectionRetryPolicy;

      this.maxRetriesOnSasl = remoteId.getMaxRetriesOnSasl();
      this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();

      this.tcpNoDelay = remoteId.getTcpNoDelay();

      this.tcpLowLatency = remoteId.getTcpLowLatency();
      this.doPing = remoteId.getDoPing();
      if (doPing) {
        //如果开启ping模式,构造ping的请求对象.
        // construct a RPC header with the callId as the ping callId
        ResponseBuffer buf = new ResponseBuffer();
        RpcRequestHeaderProto pingHeader = ProtoUtil
            .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
                OperationProto.RPC_FINAL_PACKET, PING_CALL_ID,
                RpcConstants.INVALID_RETRY_COUNT, clientId);
        pingHeader.writeDelimitedTo(buf);
        pingRequest = buf.toByteArray();
      }


      this.pingInterval = remoteId.getPingInterval();

      if (rpcTimeout > 0) {
        // effective rpc timeout is rounded up to multiple of pingInterval
        // if pingInterval < rpcTimeout.
        this.soTimeout = (doPing && pingInterval < rpcTimeout) ?
            pingInterval : rpcTimeout;
      } else {
        this.soTimeout = pingInterval;
      }

      this.serviceClass = serviceClass;
      if (LOG.isDebugEnabled()) {
        LOG.debug("The ping interval is " + this.pingInterval + " ms.");
      }


      UserGroupInformation ticket = remoteId.getTicket();
      // try SASL if security is enabled or if the ugi contains tokens.
      // this causes a SIMPLE client with tokens to attempt SASL
      boolean trySasl = UserGroupInformation.isSecurityEnabled() ||
                        (ticket != null && !ticket.getTokens().isEmpty());
      this.authProtocol = trySasl ? AuthProtocol.SASL : AuthProtocol.NONE;
      
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
          server.toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch() {
      lastActivity.set(Time.now());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    private synchronized boolean addCall(Call call) {
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends FilterInputStream {
      /* constructor */
      protected PingInputStream(InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed or 
       * the RPC is not timed out yet, send a ping.
       */
      private void handleTimeout(SocketTimeoutException e, int waiting)
          throws IOException {
        if (shouldCloseConnection.get() || !running.get() ||
            (0 < rpcTimeout && rpcTimeout <= waiting)) {
          throw e;
        } else {
          sendPing();
        }
      }
      
      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      @Override
      public int read() throws IOException {
        int waiting = 0;
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            waiting += soTimeout;
            handleTimeout(e, waiting);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * 
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public int read(byte[] buf, int off, int len) throws IOException {
        int waiting = 0;
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            waiting += soTimeout;
            handleTimeout(e, waiting);
          }
        } while (true);
      }
    }
    
    private synchronized void disposeSasl() {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
          saslRpcClient = null;
        } catch (IOException ignored) {
        }
      }
    }
    
    private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser = currentUser.getRealUser();
      if (authMethod == AuthMethod.KERBEROS && loginUser != null &&
      // Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser))) {
        return true;
      }
      return false;
    }

    private synchronized AuthMethod setupSaslConnection(IpcStreams streams)
        throws IOException {
      // Do not use Client.conf here! We must use ConnectionId.conf, since the
      // Client object is cached and shared between all RPC clients, even those
      // for separate services.
      saslRpcClient = new SaslRpcClient(remoteId.getTicket(),
          remoteId.getProtocol(), remoteId.getAddress(), remoteId.conf);
      return saslRpcClient.saslConnect(streams);
    }

    /**
     * Update the server address if the address corresponding to the host
     * name has changed.
     *
     * @return true if an addr change was detected.
     * @throws IOException when the hostname cannot be resolved.
     */
    private synchronized boolean updateAddress() throws IOException {
      // Do a fresh lookup with the old host name.
      InetSocketAddress currentAddr = NetUtils.createSocketAddrForHost(
                               server.getHostName(), server.getPort());

      if (!server.equals(currentAddr)) {
        LOG.warn("Address change detected. Old: " + server.toString() +
                                 " New: " + currentAddr.toString());
        server = currentAddr;
        return true;
      }
      return false;
    }
    
    private synchronized void setupConnection(
        UserGroupInformation ticket) throws IOException {
      short ioFailures = 0;
      short timeoutFailures = 0;
      while (true) {
        try {
          // socketFactory  采用默认的 SocketFactory =>  DefaultSocketFactory
          this.socket = socketFactory.createSocket();
          // 禁用 Nagle's 算法, 减少延迟
          this.socket.setTcpNoDelay(tcpNoDelay);
          // 设置为长连接
          this.socket.setKeepAlive(true);


          //是否启用低延迟的客户端 ???? 默认 false
          if (tcpLowLatency) {
            /*
             * This allows intermediate switches to shape IPC traffic
             * differently from Shuffle/HDFS DataStreamer traffic.
             *
             * IPTOS_RELIABILITY (0x04) | IPTOS_LOWDELAY (0x10)
             *
             * Prefer to optimize connect() speed & response latency over net
             * throughput.
             */

            // 为从此Socket发送的数据包的IP标头设置流量类或服务类型八位字节。
            this.socket.setTrafficClass(0x04 | 0x10);

            //设置此套接字的性能首选项。
            //套接字默认使用TCP / IP协议。 一些实现可以提供具有与TCP / IP不同的性能特征的替代协议。
            // 该方法允许应用程序表达自己的偏好，以便在实现从可用协议中选择时如何进行这些权衡。
            //
            // 性能首选项由三个整数描述，
            // ------------------------------------------
            // 其值表示短  [ 连接时间，低延迟 和 高带宽 ]     的相对重要性。
            // ------------------------------------------
            // 整数的绝对值是无关紧要的;
            // 为了选择协议，简单地比较值，较大的值表示较强的偏好。 负值表示比正值低的优先级。
            // 例如，
            // 如果应用程序优先考虑低延迟和高带宽的短连接时间，
            // 那么它可以使用值(1, 0, 0)调用此方法。
            // 如果应用程序更喜欢低延迟以上的高带宽，以及短连接时间之上的低延迟，
            // 那么它可以使用值(0, 1, 2)
            // 调用此方法。
            //
            //连接此套接字后调用此方法将不起作用。
            // 默认的 java8 自定义的  Socket 没有实现这个方法.

            // 如果要用的话, 更换 SocketFactory 的实现类.默认的肯定不支持!!!
            this.socket.setPerformancePreferences(1, 2, 0);
          }

          /*
           * Bind the socket to the host specified in the principal name of the
           * client, to ensure Server matching address of the client connection
           * to host name in principal passed.
           */
          InetSocketAddress bindAddr = null;
          if (ticket != null && ticket.hasKerberosCredentials()) {
            KerberosInfo krbInfo = 
              remoteId.getProtocol().getAnnotation(KerberosInfo.class);
            if (krbInfo != null) {
              String principal = ticket.getUserName();
              String host = SecurityUtil.getHostFromPrincipal(principal);
              // If host name is a valid local address then bind socket to it
              // 绑定的端口是多少 ???????
              InetAddress localAddr = NetUtils.getLocalInetAddress(host);
              if (localAddr != null) {
                this.socket.setReuseAddress(true);
                localAddr = NetUtils.bindToLocalAddress(localAddr,
                    bindToWildCardAddress);
                LOG.debug("Binding {} to {}", principal,
                    (bindToWildCardAddress) ? "0.0.0.0" : localAddr);
                this.socket.bind(new InetSocketAddress(localAddr, 0));
              }
            }
          }

          //建立连接
          NetUtils.connect(this.socket, server, bindAddr, connectionTimeout);

          this.socket.setSoTimeout(soTimeout);
          return;
        } catch (ConnectTimeoutException toe) {
          /* Check for an address change and update the local reference.
           * Reset the failure counter if the address was changed
           */
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionTimeout(timeoutFailures++,
              maxRetriesOnSocketTimeouts, toe);
        } catch (IOException ie) {
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionFailure(ioFailures++, ie);
        }
      }
    }

    /**
     * If multiple clients with the same principal try to connect to the same
     * server at the same time, the server assumes a replay attack is in
     * progress. This is a feature of kerberos. In order to work around this,
     * what is done is that the client backs off randomly and tries to initiate
     * the connection again. The other problem is to do with ticket expiry. To
     * handle that, a relogin is attempted.
     */
    private synchronized void handleSaslConnectionFailure(
        final int currRetries, final int maxRetries, final Exception ex,
        final Random rand, final UserGroupInformation ugi) throws IOException,
        InterruptedException {
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException, InterruptedException {
          final short MAX_BACKOFF = 5000;
          closeConnection();
          disposeSasl();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              if(LOG.isDebugEnabled()) {
                LOG.debug("Exception encountered while connecting to "
                    + "the server : " + ex);
              }
              // try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else if (UserGroupInformation.isLoginTicketBased()) {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              // have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
              return null;
            } else {
              String msg = "Couldn't setup connection for "
                  + UserGroupInformation.getLoginUser().getUserName() + " to "
                  + remoteId;
              LOG.warn(msg, ex);
              throw (IOException) new IOException(msg).initCause(ex);
            }
          } else {
            LOG.warn("Exception encountered while connecting to "
                + "the server : " + ex);
          }
          if (ex instanceof RemoteException)
            throw (RemoteException) ex;
          throw new IOException(ex);
        }
      });
    }

    
    /**
     *
     *
     * Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     *
     * Client.call()方法调用 Connection.setupIOstreams() 方法建立与Server的Socket连接。
     * setupIOstreams() 方法还会启动Connection线程，
     * Connection线程会监听Socket并读取Server发回的响应信息。
     *
     */
    private synchronized void setupIOstreams( AtomicBoolean fallbackToSimpleAuth) {
      if (socket != null || shouldCloseConnection.get()) {
        return;
      }

      UserGroupInformation ticket = remoteId.getTicket();

      if (ticket != null) {
        final UserGroupInformation realUser = ticket.getRealUser();
        if (realUser != null) {
          ticket = realUser;
        }
      }

      try {
        connectingThread.set(Thread.currentThread());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        Span span = Tracer.getCurrentSpan();
        if (span != null) {
          span.addTimelineAnnotation("IPC client connecting to " + server);
        }
        short numRetries = 0;
        Random rand = null;
        while (true) {

          //建立到Server的Socket连接，
          // 并且在这个Socket连接上 获得InputStream和OutputStream对象。
          setupConnection(ticket);


          ipcStreams = new IpcStreams(socket, maxResponseLength);

          // 调用writeConnectionHeader()方法在连接建立时发送连接头域。
          writeConnectionHeader(ipcStreams);

          if (authProtocol == AuthProtocol.SASL) {
            try {
              authMethod = ticket
                  .doAs(new PrivilegedExceptionAction<AuthMethod>() {
                    @Override
                    public AuthMethod run()
                        throws IOException, InterruptedException {
                      return setupSaslConnection(ipcStreams);
                    }
                  });
            } catch (IOException ex) {
              if (saslRpcClient == null) {
                // whatever happened -it can't be handled, so rethrow
                throw ex;
              }
              // otherwise, assume a connection problem
              authMethod = saslRpcClient.getAuthMethod();
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, maxRetriesOnSasl, ex,
                  rand, ticket);
              continue;
            }
            if (authMethod != AuthMethod.SIMPLE) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              ipcStreams.setSaslClient(saslRpcClient);
              // for testing
              remoteId.saslQop =
                  (String)saslRpcClient.getNegotiatedProperty(Sasl.QOP);
              LOG.debug("Negotiated QOP is :" + remoteId.saslQop);
              if (fallbackToSimpleAuth != null) {
                fallbackToSimpleAuth.set(false);
              }
            } else if (UserGroupInformation.isSecurityEnabled()) {
              if (!fallbackAllowed) {
                throw new IOException("Server asks us to fall back to SIMPLE " +
                    "auth, but this client is configured to only allow secure " +
                    "connections.");
              }
              if (fallbackToSimpleAuth != null) {
                fallbackToSimpleAuth.set(true);
              }
            }
          }

          if (doPing) {
            ipcStreams.setInputStream(new PingInputStream(ipcStreams.in));
          }

          //调用writeConnectionContext()方法写入连接上下文。
          writeConnectionContext(remoteId, authMethod);

          // update last activity time
          //调用touch()方法更新上次活跃时间。
          touch();

          span = Tracer.getCurrentSpan();
          if (span != null) {
            span.addTimelineAnnotation("IPC client connected to " + server);
          }


          // 开启接收线程
          // 调用start()方法启动Connection线程监听并接收Server发回的响应信息。
          // start the receiver thread after the socket connection has been set up
          start();


          return;
        }
      } catch (Throwable t) {
        if (t instanceof IOException) {
          markClosed((IOException)t);
        } else {
          markClosed(new IOException("Couldn't set up IO streams: " + t, t));
        }
        close();
      } finally {
        connectingThread.set(null);
      }
    }
    
    private void closeConnection() {
      if (socket == null) {
        return;
      }
      // close the current connection
      try {
        socket.close();
      } catch (IOException e) {
        LOG.warn("Not able to close a socket", e);
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /* Handle connection failures due to timeout on connect
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff 1 second and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    private void handleConnectionTimeout(
        int curRetries, int maxRetries, IOException ioe) throws IOException {

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); maxRetries=" + maxRetries);
    }

    private void handleConnectionFailure(int curRetries, IOException ioe
        ) throws IOException {
      closeConnection();

      final RetryAction action;
      try {
        action = connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);
      } catch(Exception e) {
        throw e instanceof IOException? (IOException)e: new IOException(e);
      }
      if (action.action == RetryAction.RetryDecision.FAIL) {
        if (action.reason != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to connect to server: " + server + ": "
                    + action.reason, ioe);
          }
        }
        throw ioe;
      }

      // Throw the exception if the thread is interrupted
      if (Thread.currentThread().isInterrupted()) {
        LOG.warn("Interrupted while trying for connection");
        throw ioe;
      }

      try {
        Thread.sleep(action.delayMillis);
      } catch (InterruptedException e) {
        throw (IOException)new InterruptedIOException("Interrupted: action="
            + action + ", retry policy=" + connectionRetryPolicy).initCause(e);
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
    }

    /**
     * Write the connection header - this is sent when connection is established
     * +----------------------------------+
     * |  "hrpc" 4 bytes                  |      
     * +----------------------------------+
     * |  Version (1 byte)                |
     * +----------------------------------+
     * |  Service Class (1 byte)          |
     * +----------------------------------+
     * |  AuthProtocol (1 byte)           |      
     * +----------------------------------+
     */
    private void writeConnectionHeader(IpcStreams streams)
        throws IOException {
      // Write out the header, version and authentication method.
      // The output stream is buffered but we must not flush it yet.  The
      // connection setup protocol requires the client to send multiple
      // messages before reading a response.
      //
      //   insecure: send header+context+call, read
      //   secure  : send header+negotiate, read, (sasl), context+call, read
      //
      // The client must flush only when it's prepared to read.  Otherwise
      // "broken pipe" exceptions occur if the server closes the connection
      // before all messages are sent.
      final DataOutputStream out = streams.out;
      synchronized (out) {
        out.write(RpcConstants.HEADER.array());
        out.write(RpcConstants.CURRENT_VERSION);
        out.write(serviceClass);
        out.write(authProtocol.callId);
      }
    }

    /* Write the connection context header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    private void writeConnectionContext(ConnectionId remoteId,
                                        AuthMethod authMethod)
                                            throws IOException {
      // Write out the ConnectionHeader
      IpcConnectionContextProto message = ProtoUtil.makeIpcConnectionContext(
          RPC.getProtocolName(remoteId.getProtocol()),
          remoteId.getTicket(),
          authMethod);
      RpcRequestHeaderProto connectionContextHeader = ProtoUtil
          .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
              OperationProto.RPC_FINAL_PACKET, CONNECTION_CONTEXT_CALL_ID,
              RpcConstants.INVALID_RETRY_COUNT, clientId);
      // do not flush.  the context and first ipc call request must be sent
      // together to avoid possibility of broken pipes upon authz failure.
      // see writeConnectionHeader
      final ResponseBuffer buf = new ResponseBuffer();
      connectionContextHeader.writeDelimitedTo(buf);
      message.writeDelimitedTo(buf);


      synchronized (ipcStreams.out) {
        ipcStreams.sendRequest(buf.toByteArray());

      }
    }

    /**
     * waitForWork()方法会判断当前 Connection的calls队列当中是否有Call对象，
     * 如果没有则等待maxTimeOut时长(calls队列 没有对象则表明Connection空闲，没有待发送请求)。
     * 等待之后如果还没有数据，则返回 false,并且关闭Connection对象。
     *
     * wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed, 
     * or the client is marked as not running.
     * 
     * Return true if it is time to read a response; false otherwise.
     *
     */
    private synchronized boolean waitForWork() {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        long timeout = maxIdleTime-
              (Time.now()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException e) {}
        }
      }
      
      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        //空闲的连接，则关闭
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests

        //仍有等待处理的请求，但是连接被关闭了
        markClosed((IOException)new IOException().initCause(
            new InterruptedException()));
        return false;
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return server;
    }

    /* Send a ping to the server if the time elapsed 
     * since last I/O activity is equal to or greater than the ping interval
     */
    private synchronized void sendPing() throws IOException {
      long curTime = Time.now();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        synchronized (ipcStreams.out) {
          ipcStreams.sendRequest(pingRequest);
          ipcStreams.flush();
        }
      }
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections " 
            + connections.size());

      try {
        while (waitForWork()) {

          //wait here for work - read or close connection
          //接收到返回信息
          receiveRpcResponse();

        }
      } catch (Throwable t) {
        // This truly is unexpected, since we catch IOException in receiveResponse
        // -- this is only to be really sure that we don't leave a client hanging
        // forever.
        LOG.warn("Unexpected error reading responses on connection " + this, t);
        markClosed(new IOException("Error reading responses", t));
      }
      
      close();
      
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /**
     *
     * 发送RPC请求到Server。
     *
     * Initiates a rpc call by sending the rpc request to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     * @param call - the rpc request
     *
     * RPC发送请求线程会调用Connection.sendRpcRequest()方法发送RPC请求到Server，
     * 这 里要特别注意，这个方法不是由Connection线程调用的，
     *             而是由发起RPC请求的线程调用 的。
     *
     */
    public void sendRpcRequest(final Call call)
        throws InterruptedException, IOException {
      if (shouldCloseConnection.get()) {
        return;
      }

      // Serialize the call to be sent. This is done from the actual
      // caller thread, rather than the sendParamsExecutor thread,
      
      // so that if the serialization throws an error, it is reported
      // properly. This also parallelizes the serialization.
      //
      // Format of a call on the wire:
      // 0) Length of rest below (1 + 2)
      // 1) RpcRequestHeader  - is serialized Delimited hence contains length
      // 2) RpcRequest
      //
      // Items '1' and '2' are prepared here.

      //先构造RPC请求头
      RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
          call.rpcKind, OperationProto.RPC_FINAL_PACKET, call.id, call.retry,
          clientId, call.alignmentContext);


      final ResponseBuffer buf = new ResponseBuffer();

      //将RPC请求头写入  输出流 ResponseBuffer
      header.writeDelimitedTo(buf);


      //将RPC请求(包括请求元数据和请求参数)封装成 ProtobufWrapper  ==> 写入输出流
      RpcWritable.wrap(call.rpcRequest).writeTo(buf);


      //这里使用 线程池 将请求发送出去，
      // 请求包括三个部分:
      // 1  长度;
      // 2  RPC请求头;
      // 3  RPC请求(包括 请求元数据以及请求参数)
      synchronized (sendRpcRequestLock) {
        Future<?> senderFuture = sendParamsExecutor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              synchronized (ipcStreams.out) {
                if (shouldCloseConnection.get()) {
                  return;
                }
                if (LOG.isDebugEnabled()) {
                  LOG.debug(getName() + " sending #" + call.id
                      + " " + call.rpcRequest);
                }
                // RpcRequestHeader + RpcRequest
                ipcStreams.sendRequest(buf.toByteArray());
                ipcStreams.flush();
              }
            } catch (IOException e) {
              // exception at this point would leave the connection in an
              // unrecoverable state (eg half a call left on the wire).
              // So, close the connection, killing any outstanding calls
              //如果发生发送异常，则直接关闭连接
              markClosed(e);
            } finally {
              //the buffer is just an in-memory buffer, but it is still polite to
              // close early
              //之前申请的buffer给关闭了，比较优雅
              IOUtils.closeStream(buf);
            }
          }
        });
      
        try {
          //尝试获取响应,但是只是看看有没有报错而已, 真正的处理相应不在这里.

          senderFuture.get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          
          // cause should only be a RuntimeException as the Runnable above
          // catches IOException
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          } else {
            throw new RuntimeException("unexpected checked exception", cause);
          }
        }
      }
    }

    /**
       接收到返回信息
       Receive a response.
     * Because only one receiver, so no synchronization on in.
     * receiveRpcResponse()方法接收RPC响应。
     * receiveRpcResponse()方法 会从输入流中读取序列化对象RpcResponseHeaderProto，
     * 然后根据RpcResponseHeaderProto 中记录的callid字段获取对应的Call的对象。
     *
     * 接下来receiveRpcResponse()方法会从输入流中 读取响应消息，
     * 然后调用Call.setRpcResponse()将响应消息保存在Call对象中。
     * 如果服务器 在处理RPC请求时抛出异常，
     * 则receiveRpcResponse()会从输入流中读取异常信息，并构造 异常对象，
     * 然后调用Call.setException()将异常保存在Call对象中。
     *
     */
    private void receiveRpcResponse() {
      if (shouldCloseConnection.get()) {
        return;
      }
      //更新请求时间的
      touch();
      
      try {
        //通过ipcStreams 获取响应对象
        ByteBuffer bb = ipcStreams.readResponse();

        //将响应对象,采用RpcWritable 进行包装.
        RpcWritable.Buffer packet = RpcWritable.Buffer.wrap(bb);

        // 获取响应的响应头.
        RpcResponseHeaderProto header = packet.getValue(RpcResponseHeaderProto.getDefaultInstance());

        // 检测头信息.
        checkResponse(header);

        // 获取call唯一标识callId
        int callId = header.getCallId();

        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + callId);

        // 获取头信息里面的响应状态.
        RpcStatusProto status = header.getStatus();

        //如果调用成功，则读取响应消息，在call实例中设置
        if (status == RpcStatusProto.SUCCESS) {

          //构建实例
          Writable value = packet.newInstance(valueClass, conf);

          //将Call 从 正在执行的calls缓存队列中移除.
          final Call call = calls.remove(callId);

          //将Call 设置请求信息
          call.setRpcResponse(value);


          if (call.alignmentContext != null) {
            call.alignmentContext.receiveResponseState(header);
          }
        }


        // verify that packet length was correct
        if (packet.remaining() > 0) {
          throw new RpcClientException("RPC response length mismatch");
        }

        //RPC调用失败
        if (status != RpcStatusProto.SUCCESS) { // Rpc Request failed

          //取出响应中的异常消息
          final String exceptionClassName = header.hasExceptionClassName() ?
                header.getExceptionClassName() : 
                  "ServerDidNotSetExceptionClassName";

          final String errorMsg = header.hasErrorMsg() ? 
                header.getErrorMsg() : "ServerDidNotSetErrorMsg" ;
          final RpcErrorCodeProto erCode = 
                    (header.hasErrorDetail() ? header.getErrorDetail() : null);


          if (erCode == null) {
             LOG.warn("Detailed error code not set by server on rpc error");
          }

          RemoteException re = new RemoteException(exceptionClassName, errorMsg, erCode);

          //在Call对象中设置异常
          if (status == RpcStatusProto.ERROR) {
            final Call call = calls.remove(callId);
            call.setException(re);
          } else if (status == RpcStatusProto.FATAL) {
            // Close the connection
            markClosed(re);
          }
        }
      } catch (IOException e) {
        markClosed(e);
      }
    }
    
    private synchronized void markClosed(IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }

    private void interruptConnectingThread() {
      Thread connThread = connectingThread.get();
      if (connThread != null) {
        connThread.interrupt();
      }
    }
    
    /** Close the connection. */
    private synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // We have marked this connection as closed. Other thread could have
      // already known it and replace this closedConnection with a new one.
      // We should only remove this closedConnection.
      connections.remove(remoteId, this);

      // close the streams and therefore the socket
      IOUtils.closeStream(ipcStreams);
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      closeConnection();
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue(); 
        itor.remove();
        c.setException(closeException); // local exception
      }
    }
  }

  /**
   *
   * valueClass  ==> 创建时指定 RpcWritable.Buffer.class
   * 根据给定的value 构造一个 IPC client
   * Construct an IPC client whose values are of the given {@link Writable} class.
   * */
  public Client(Class<? extends Writable> valueClass, Configuration conf, 
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
    this.connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
    this.fallbackAllowed = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.bindToWildCardAddress = conf
        .getBoolean(CommonConfigurationKeys.IPC_CLIENT_BIND_WILDCARD_ADDR_KEY,
            CommonConfigurationKeys.IPC_CLIENT_BIND_WILDCARD_ADDR_DEFAULT);

    this.clientId = ClientId.getClientId();
    this.sendParamsExecutor = clientExcecutorFactory.refAndGetInstance();
    this.maxAsyncCalls = conf.getInt(
        CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY,
        CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_DEFAULT);
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public Client(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }
 
  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }
    
    // wake up all connections
    for (Connection conn : connections.values()) {
      conn.interrupt();
      conn.interruptConnectingThread();
    }
    
    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
    
    clientExcecutorFactory.unrefAndCleanup();
  }

  /** 
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   */
  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT,
      fallbackToSimpleAuth, null);
  }

  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, AtomicBoolean fallbackToSimpleAuth,
      AlignmentContext alignmentContext)
      throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT,
        fallbackToSimpleAuth, alignmentContext);
  }

  private void checkAsyncCall() throws IOException {
    if (isAsynchronousMode()) {
      if (asyncCallCounter.incrementAndGet() > maxAsyncCalls) {
        asyncCallCounter.decrementAndGet();
        String errMsg = String.format(
            "Exceeded limit of max asynchronous calls: %d, " +
            "please configure %s to adjust it.",
            maxAsyncCalls,
            CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY);
        throw new AsyncCallLimitExceededException(errMsg);
      }
    }
  }

  Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
                ConnectionId remoteId, int serviceClass,
                AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    return call(rpcKind, rpcRequest, remoteId, serviceClass,
        fallbackToSimpleAuth, null);
  }

  /**
   *
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc response.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param serviceClass - service class for RPC
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @param alignmentContext - state alignment context
   * @return the rpc response
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   */
  Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, int serviceClass,
      AtomicBoolean fallbackToSimpleAuth, AlignmentContext alignmentContext)
      throws IOException {


    /**
     * 创建 Call 对象
     * Client.call()方法将 RPC 请求封装成一个Call对象，
     * Call对象中保存了RPC调用的完 成标志、返回值信息以及异常信息;
     * 随后，Client.cal()方法会创建一个 Connection对象，
     * Connection对象用于管理Client与Server的Socket连接。
     */
    final Call call = createCall(rpcKind, rpcRequest);


    call.setAlignmentContext(alignmentContext);


    //获取&建立连接
    //用ConnectionId作为key，
    // 将新建的Connection对象放入Client.connections字段中保 存
    // (
    // 对于Connection对象，
    // 由于涉及了与Server建立Socket连接，会比较耗费资 源，
    // 所以Client类使用一个HashTable对象connections保存那些没有过期的 Connection，
    // 如果可以复用，则复用这些Connection对象
    // );
    // 以callId作为key，将 构造的Call对象放入Connection.calls字段中保存。

    final Connection connection = getConnection(remoteId, call, serviceClass,
        fallbackToSimpleAuth);

    try {
      //检测是否是异步请求. 默认 false .
      checkAsyncCall();
      try {
        //发送RPC请求
        connection.sendRpcRequest(call);                 // send the rpc request


      } catch (RejectedExecutionException e) {
        throw new IOException("connection has been closed", e);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        IOException ioe = new InterruptedIOException(
            "Interrupted waiting to send RPC request to server");
        ioe.initCause(ie);
        throw ioe;
      }
    } catch(Exception e) {
      if (isAsynchronousMode()) {
        releaseAsyncCall();
      }
      throw e;
    }

    if (isAsynchronousMode()) {
      final AsyncGet<Writable, IOException> asyncGet
          = new AsyncGet<Writable, IOException>() {
        @Override
        public Writable get(long timeout, TimeUnit unit)
            throws IOException, TimeoutException{
          boolean done = true;
          try {
            final Writable w = getRpcResponse(call, connection, timeout, unit);
            if (w == null) {
              done = false;
              throw new TimeoutException(call + " timed out "
                  + timeout + " " + unit);
            }
            return w;
          } finally {
            if (done) {
              releaseAsyncCall();
            }
          }
        }

        @Override
        public boolean isDone() {
          synchronized (call) {
            return call.done;
          }
        }
      };

      ASYNC_RPC_RESPONSE.set(asyncGet);
      return null;
    } else {

      //服务器成功发回响应信息，返回RPC响应
      return getRpcResponse(call, connection, -1, null);
    }
  }

  /**
   * Check if RPC is in asynchronous mode or not.
   *
   * @returns true, if RPC is in asynchronous mode, otherwise false for
   *          synchronous mode.
   */
  @Unstable
  public static boolean isAsynchronousMode() {
    boolean rs = asynchronousMode.get();
    return rs;
  }

  /**
   * Set RPC to asynchronous or synchronous mode.
   *
   * @param async
   *          true, RPC will be in asynchronous mode, otherwise false for
   *          synchronous mode
   */
  @Unstable
  public static void setAsynchronousMode(boolean async) {
    asynchronousMode.set(async);
  }

  private void releaseAsyncCall() {
    asyncCallCounter.decrementAndGet();
  }

  @VisibleForTesting
  int getAsyncCallCount() {
    return asyncCallCounter.get();
  }

  /** @return the rpc response or, in case of timeout, null. */
  private Writable getRpcResponse(final Call call, final Connection connection,
      final long timeout, final TimeUnit unit) throws IOException {
    synchronized (call) {
      while (!call.done) {
        try {

          //等待RPC响应
          AsyncGet.Util.wait(call, timeout, unit);
          if (timeout >= 0 && !call.done) {
            return null;
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Call interrupted");
        }
      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          InetSocketAddress address = connection.getRemoteAddress();
          throw NetUtils.wrapException(address.getHostName(),
                  address.getPort(),
                  NetUtils.getHostname(),
                  0,
                  call.error);
        }
      } else {
        return call.getRpcResponse();
      }
    }
  }

  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  Set<ConnectionId> getConnectionIds() {
    return connections.keySet();
  }


  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given ConnectionId are reused.
   * 获取连接
   *
   * */
  private Connection getConnection(ConnectionId remoteId,
      Call call, int serviceClass, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    if (!running.get()) {
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    while (true) {
      // These lines below can be shorten with computeIfAbsent in Java8
      //首先尝试从Client.connections队列中获取Connection对象
      connection = connections.get(remoteId);
      if (connection == null) {

        //如果 connections 队列中没有保存，则构造新的对象
        connection = new Connection(remoteId, serviceClass);

        // putIfAbsent 如果对应的 key 已经有值了,
        // 则忽略本次操作,直接返回旧值,新值直接丢弃

        Connection existing = connections.putIfAbsent(remoteId, connection);

        if (existing != null) {
          connection = existing;
        }
      }

      //将待发送请求对应的Call对象放入Connection.calls队列
      if (connection.addCall(call)) {
        break;
      } else {
        // This connection is closed, should be removed. But other thread could
        // have already known this closedConnection, and replace it with a new
        // connection. So we should call conditional remove to make sure we only
        // remove this closedConnection.

        connections.remove(remoteId, connection);
      }
    }

    // If the server happens to be slow, the method below will take longer to
    // establish a connection.

    //调用setupIOstreams()方法，初始化Connection对象并获取IO流
    connection.setupIOstreams(fallbackToSimpleAuth);
    return connection;
  }
  
  /**
   *
   * 这个类 持有 请求地址 和 用户的ticket
   * clietn 连接 server 的唯一凭证 :  [remoteAddress, protocol, ticket]
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by <remoteAddress, protocol, ticket>
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class ConnectionId {
    InetSocketAddress address;
    UserGroupInformation ticket;
    final Class<?> protocol;
    private static final int PRIME = 16777619;
    private final int rpcTimeout;
    private final int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private final int maxRetriesOnSasl;
    // the max. no. of retries for socket connections on time out exceptions
    private final int maxRetriesOnSocketTimeouts;
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private final boolean tcpLowLatency; // if T then use low-delay QoS
    private final boolean doPing; //do we need to send ping message
    private final int pingInterval; // how often sends ping to the server in msecs
    private String saslQop; // here for testing
    private final Configuration conf; // used to get the expected kerberos principal name
    
    ConnectionId(InetSocketAddress address, Class<?> protocol, 
                 UserGroupInformation ticket, int rpcTimeout,
                 RetryPolicy connectionRetryPolicy, Configuration conf) {

      // 协议
      this.protocol = protocol;

      // 请求地址
      this.address = address;

      //用户 ticket
      this.ticket = ticket;

      //设置超时时间
      this.rpcTimeout = rpcTimeout;

      //设置重试策略 默认: 重试10次, 每次间隔1秒
      this.connectionRetryPolicy = connectionRetryPolicy;

      // 单位 10秒
      this.maxIdleTime = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);

      // sasl client最大重试次数 5 次
      this.maxRetriesOnSasl = conf.getInt(
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY,
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT);

      //指示客户端将在套接字超时时进行重试的次数，以建立服务器连接。 默认值: 45
      this.maxRetriesOnSocketTimeouts = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);

      //使用TCP_NODELAY标志绕过Nagle的算法传输延迟。 默认值: true
      this.tcpNoDelay = conf.getBoolean(
          CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);

      // 从客户端启用低延迟连接 默认 false
      this.tcpLowLatency = conf.getBoolean(
          CommonConfigurationKeysPublic.IPC_CLIENT_LOW_LATENCY,
          CommonConfigurationKeysPublic.IPC_CLIENT_LOW_LATENCY_DEFAULT
          );

      // 启用从RPC客户端到服务器的ping操作 默认值: true
      this.doPing = conf.getBoolean(
          CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
          CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);

      // 设置ping 操作的间隔, 默认值 : 1分钟
      this.pingInterval = (doPing ? Client.getPingInterval(conf) : 0);
      this.conf = conf;
    }
    
    InetSocketAddress getAddress() {
      return address;
    }
    
    Class<?> getProtocol() {
      return protocol;
    }
    
    UserGroupInformation getTicket() {
      return ticket;
    }
    
    private int getRpcTimeout() {
      return rpcTimeout;
    }
    
    int getMaxIdleTime() {
      return maxIdleTime;
    }
    
    public int getMaxRetriesOnSasl() {
      return maxRetriesOnSasl;
    }

    /** max connection retries on socket time outs */
    public int getMaxRetriesOnSocketTimeouts() {
      return maxRetriesOnSocketTimeouts;
    }

    /** disable nagle's algorithm */
    boolean getTcpNoDelay() {
      return tcpNoDelay;
    }

    /** use low-latency QoS bits over TCP */
    boolean getTcpLowLatency() {
      return tcpLowLatency;
    }

    boolean getDoPing() {
      return doPing;
    }
    
    int getPingInterval() {
      return pingInterval;
    }
    
    @VisibleForTesting
    String getSaslQop() {
      return saslQop;
    }
    
    /**
     * Returns a ConnectionId object. 
     * @param addr Remote address for the connection.
     * @param protocol Protocol for RPC.
     * @param ticket UGI
     * @param rpcTimeout timeout
     * @param conf Configuration object
     * @return A ConnectionId instance
     * @throws IOException
     */
    static ConnectionId getConnectionId(InetSocketAddress addr,
        Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
        RetryPolicy connectionRetryPolicy, Configuration conf) throws IOException {


      //构建重试策略
      if (connectionRetryPolicy == null) {
        //设置最大重试次数 默认值: 10
        final int max = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);

        // 设置重试间隔: 1 秒
        final int retryInterval = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
            CommonConfigurationKeysPublic
                .IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT);

        //创建重试策略实例 RetryUpToMaximumCountWithFixedSleep
        //              重试10次, 每次间隔1秒
        connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            max, retryInterval, TimeUnit.MILLISECONDS);
      }

      //创建ConnectionId :
      //  这个类 持有 请求地址 和 用户的ticket
      //  clietn 连接 server 的唯一凭证 :  [remoteAddress, protocol, ticket]

      return new ConnectionId(addr, protocol, ticket, rpcTimeout,
          connectionRetryPolicy, conf);
    }
    
    static boolean isEqual(Object a, Object b) {
      return a == null ? b == null : a.equals(b);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof ConnectionId) {
        ConnectionId that = (ConnectionId) obj;
        return isEqual(this.address, that.address)
            && this.doPing == that.doPing
            && this.maxIdleTime == that.maxIdleTime
            && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
            && this.pingInterval == that.pingInterval
            && isEqual(this.protocol, that.protocol)
            && this.rpcTimeout == that.rpcTimeout
            && this.tcpNoDelay == that.tcpNoDelay
            && isEqual(this.ticket, that.ticket);
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      int result = connectionRetryPolicy.hashCode();
      result = PRIME * result + ((address == null) ? 0 : address.hashCode());
      result = PRIME * result + (doPing ? 1231 : 1237);
      result = PRIME * result + maxIdleTime;
      result = PRIME * result + pingInterval;
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + rpcTimeout;
      result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
      result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
      return result;
    }
    
    @Override
    public String toString() {
      return address.toString();
    }
  }  

  /**
   * Returns the next valid sequential call ID by incrementing an atomic counter
   * and masking off the sign bit.  Valid call IDs are non-negative integers in
   * the range [ 0, 2^31 - 1 ].  Negative numbers are reserved for special
   * purposes.  The values can overflow back to 0 and be reused.  Note that prior
   * versions of the client did not mask off the sign bit, so a server may still
   * see a negative call ID if it receives connections from an old client.
   * 
   * @return next call ID
   */
  public static int nextCallId() {
    return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
  }

  @Override
  @Unstable
  public void close() throws Exception {
    stop();
  }

  /** Manages the input and output streams for an IPC connection.
   *  Only exposed for use by SaslRpcClient.
   */
  @InterfaceAudience.Private
  public static class IpcStreams implements Closeable, Flushable {
    private DataInputStream in;
    public DataOutputStream out;
    private int maxResponseLength;
    private boolean firstResponse = true;

    IpcStreams(Socket socket, int maxResponseLength) throws IOException {
      this.maxResponseLength = maxResponseLength;
      setInputStream(
          new BufferedInputStream(NetUtils.getInputStream(socket)));
      setOutputStream(
          new BufferedOutputStream(NetUtils.getOutputStream(socket)));
    }

    void setSaslClient(SaslRpcClient client) throws IOException {
      // Wrap the input stream in a BufferedInputStream to fill the buffer
      // before reading its length (HADOOP-14062).
      setInputStream(new BufferedInputStream(client.getInputStream(in)));
      setOutputStream(client.getOutputStream(out));
    }

    private void setInputStream(InputStream is) {
      this.in = (is instanceof DataInputStream)
          ? (DataInputStream)is : new DataInputStream(is);
    }

    private void setOutputStream(OutputStream os) {
      this.out = (os instanceof DataOutputStream)
          ? (DataOutputStream)os : new DataOutputStream(os);
    }

    public ByteBuffer readResponse() throws IOException {
      int length = in.readInt();
      if (firstResponse) {
        firstResponse = false;
        // pre-rpcv9 exception, almost certainly a version mismatch.
        if (length == -1) {
          in.readInt(); // ignore fatal/error status, it's fatal for us.
          throw new RemoteException(WritableUtils.readString(in),
                                    WritableUtils.readString(in));
        }
      }
      if (length <= 0) {
        throw new RpcException("RPC response has invalid length");
      }
      if (maxResponseLength > 0 && length > maxResponseLength) {
        throw new RpcException("RPC response exceeds maximum data length");
      }
      ByteBuffer bb = ByteBuffer.allocate(length);
      in.readFully(bb.array());
      return bb;
    }

    public void sendRequest(byte[] buf) throws IOException {
      out.write(buf);
      out.flush();
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() {
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
    }
  }
}
