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

import static org.apache.hadoop.ipc.ProcessingDetails.Timing;
import static org.apache.hadoop.ipc.RpcConstants.AUTHORIZATION_FAILED_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION;
import static org.apache.hadoop.ipc.RpcConstants.HEADER_LEN_AFTER_HRPC_PART;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.CallQueueManager.CallQueueOverflowException;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.metrics.RpcDetailedMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Client
 */
@Public
@InterfaceStability.Evolving
public abstract class Server {
  private final boolean authorize;
  private List<AuthMethod> enabledAuthMethods;
  private RpcSaslProto negotiateResponse;
  private ExceptionsHandler exceptionsHandler = new ExceptionsHandler();
  private Tracer tracer;
  private AlignmentContext alignmentContext;
  /**
   * Logical name of the server used in metrics and monitor.
   */
  private final String serverName;

  /**
   * Add exception classes for which server won't log stack traces.
   *
   * @param exceptionClass exception classes
   */
  public void addTerseExceptions(Class<?>... exceptionClass) {
    exceptionsHandler.addTerseLoggingExceptions(exceptionClass);
  }

  /**
   * Add exception classes which server won't log at all.
   *
   * @param exceptionClass exception classes
   */
  public void addSuppressedLoggingExceptions(Class<?>... exceptionClass) {
    exceptionsHandler.addSuppressedLoggingExceptions(exceptionClass);
  }

  /**
   * Set alignment context to pass state info thru RPC.
   *
   * @param alignmentContext alignment state context
   */
  public void setAlignmentContext(AlignmentContext alignmentContext) {
    this.alignmentContext = alignmentContext;
  }

  /**
   * ExceptionsHandler manages Exception groups for special handling
   * e.g., terse exception group for concise logging messages
   */
  static class ExceptionsHandler {
    private volatile Set<String> terseExceptions = new HashSet<>();
    private volatile Set<String> suppressedExceptions = new HashSet<>();

    /**
     * Add exception classes for which server won't log stack traces.
     * Optimized for infrequent invocation.
     * @param exceptionClass exception classes 
     */
    void addTerseLoggingExceptions(Class<?>... exceptionClass) {
      // Thread-safe replacement of terseExceptions.
      terseExceptions = addExceptions(terseExceptions, exceptionClass);
    }

    /**
     * Add exception classes which server won't log at all.
     * Optimized for infrequent invocation.
     * @param exceptionClass exception classes
     */
    void addSuppressedLoggingExceptions(Class<?>... exceptionClass) {
      // Thread-safe replacement of suppressedExceptions.
      suppressedExceptions = addExceptions(
          suppressedExceptions, exceptionClass);
    }

    boolean isTerseLog(Class<?> t) {
      return terseExceptions.contains(t.toString());
    }

    boolean isSuppressedLog(Class<?> t) {
      return suppressedExceptions.contains(t.toString());
    }

    /**
     * Return a new set containing all the exceptions in exceptionsSet
     * and exceptionClass.
     * @return
     */
    private static Set<String> addExceptions(
        final Set<String> exceptionsSet, Class<?>[] exceptionClass) {
      // Make a copy of the exceptionSet for performing modification
      final HashSet<String> newSet = new HashSet<>(exceptionsSet);

      // Add all class names into the HashSet
      for (Class<?> name : exceptionClass) {
        newSet.add(name.toString());
      }

      return Collections.unmodifiableSet(newSet);
    }
  }

  
  /**
   * If the user accidentally sends an HTTP GET to an IPC port, we detect this
   * and send back a nicer response.
   */
  private static final ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap(
      "GET ".getBytes(StandardCharsets.UTF_8));
  
  /**
   * An HTTP response to send back if we detect an HTTP request to our IPC
   * port.
   */
  static final String RECEIVED_HTTP_REQ_RESPONSE =
    "HTTP/1.1 404 Not Found\r\n" +
    "Content-type: text/plain\r\n\r\n" +
    "It looks like you are making an HTTP request to a Hadoop IPC port. " +
    "This is not the correct port for the web interface on this daemon.\r\n";

  /**
   * Initial and max size of response buffer
   */
  static int INITIAL_RESP_BUF_SIZE = 10240;
  
  static class RpcKindMapValue {
    final Class<? extends Writable> rpcRequestWrapperClass;
    final RpcInvoker rpcInvoker;

    RpcKindMapValue (Class<? extends Writable> rpcRequestWrapperClass,
          RpcInvoker rpcInvoker) {
      this.rpcInvoker = rpcInvoker;
      this.rpcRequestWrapperClass = rpcRequestWrapperClass;
    }   
  }
  static Map<RPC.RpcKind, RpcKindMapValue> rpcKindMap = new HashMap<>(4);
  
  

  /**
   * Register a RPC kind and the class to deserialize the rpc request.
   * 
   * Called by static initializers of rpcKind Engines
   * @param rpcKind
   * @param rpcRequestWrapperClass - this class is used to deserialze the
   *  the rpc request.
   *  @param rpcInvoker - use to process the calls on SS.
   */
  
  public static void registerProtocolEngine(RPC.RpcKind rpcKind, 
          Class<? extends Writable> rpcRequestWrapperClass,
          RpcInvoker rpcInvoker) {
    RpcKindMapValue  old = 
        rpcKindMap.put(rpcKind, new RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker));
    if (old != null) {
      rpcKindMap.put(rpcKind, old);
      throw new IllegalArgumentException("ReRegistration of rpcKind: " +
          rpcKind);      
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("rpcKind=" + rpcKind +
          ", rpcRequestWrapperClass=" + rpcRequestWrapperClass +
          ", rpcInvoker=" + rpcInvoker);
    }
  }
  
  public Class<? extends Writable> getRpcRequestWrapper(
      RpcKindProto rpcKind) {
    if (rpcRequestClass != null)
       return rpcRequestClass;
    RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convert(rpcKind));
    return (val == null) ? null : val.rpcRequestWrapperClass; 
  }
  
  public static RpcInvoker  getRpcInvoker(RPC.RpcKind rpcKind) {
    RpcKindMapValue val = rpcKindMap.get(rpcKind);
    return (val == null) ? null : val.rpcInvoker; 
  }
  

  public static final Logger LOG = LoggerFactory.getLogger(Server.class);
  public static final Logger AUDITLOG =
      LoggerFactory.getLogger("SecurityLogger."+Server.class.getName());
  private static final String AUTH_FAILED_FOR = "Auth failed for ";
  private static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  
  private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

  private static final Map<String, Class<?>> PROTOCOL_CACHE = 
    new ConcurrentHashMap<String, Class<?>>();
  
  static Class<?> getProtocolClass(String protocolName, Configuration conf) 
  throws ClassNotFoundException {
    Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
    if (protocol == null) {
      protocol = conf.getClassByName(protocolName);
      PROTOCOL_CACHE.put(protocolName, protocol);
    }
    return protocol;
  }
  
  /** Returns the server instance called under or null.  May be called under
   * {@link #call(Writable, long)} implementations, and under {@link Writable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static Server get() {
    return SERVER.get();
  }
 
  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();
  
  /** Get the current call */
  @VisibleForTesting
  public static ThreadLocal<Call> getCurCall() {
    return CurCall;
  }
  
  /**
   * Returns the currently active RPC call's sequential ID number.  A negative
   * call ID indicates an invalid value, such as if there is no currently active
   * RPC call.
   * 
   * @return int sequential ID number of currently active RPC call
   */
  public static int getCallId() {
    Call call = CurCall.get();
    return call != null ? call.callId : RpcConstants.INVALID_CALL_ID;
  }
  
  /**
   * @return The current active RPC call's retry count. -1 indicates the retry
   *         cache is not supported in the client side.
   */
  public static int getCallRetryCount() {
    Call call = CurCall.get();
    return call != null ? call.retryCount : RpcConstants.INVALID_RETRY_COUNT;
  }

  /** Returns the remote side ip address when invoked inside an RPC 
   *  Returns null incase of an error.
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    return (call != null ) ? call.getHostInetAddress() : null;
  }

  /**
   * Returns the SASL qop for the current call, if the current call is
   * set, and the SASL negotiation is done. Otherwise return null. Note
   * that CurCall is thread local object. So in fact, different handler
   * threads will process different CurCall object.
   *
   * Also, only return for RPC calls, not supported for other protocols.
   * @return the QOP of the current connection.
   */
  public static String getEstablishedQOP() {
    Call call = CurCall.get();
    if (call == null || !(call instanceof RpcCall)) {
      return null;
    }
    RpcCall rpcCall = (RpcCall)call;
    return rpcCall.connection.getEstablishedQOP();
  }

  /**
   * Returns the clientId from the current RPC request
   */
  public static byte[] getClientId() {
    Call call = CurCall.get();
    return call != null ? call.clientId : RpcConstants.DUMMY_CLIENT_ID;
  }
  
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }

  /** Returns the RPC remote user when invoked inside an RPC.  Note this
   *  may be different than the current user if called within another doAs
   *  @return connection's UGI or null if not an RPC
   */
  public static UserGroupInformation getRemoteUser() {
    Call call = CurCall.get();
    return (call != null) ? call.getRemoteUser() : null;
  }

  public static String getProtocol() {
    Call call = CurCall.get();
    return (call != null) ? call.getProtocol() : null;
  }

  /** Return true if the invocation was through an RPC.
   */
  public static boolean isRpcInvocation() {
    return CurCall.get() != null;
  }

  /**
   * Return the priority level assigned by call queue to an RPC
   * Returns 0 in case no priority is assigned.
   */
  public static int getPriorityLevel() {
    Call call = CurCall.get();
    return call != null? call.getPriorityLevel() : 0;
  }

  private String bindAddress; 
  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private int readThreads;                        // number of read threads
  private int readerPendingConnectionQueue;         // number of connections to queue per read thread
  private Class<? extends Writable> rpcRequestClass;   // class used for deserializing the rpc request
  final protected RpcMetrics rpcMetrics;
  final protected RpcDetailedMetrics rpcDetailedMetrics;
  
  private Configuration conf;
  private String portRangeConfig = null;
  private SecretManager<TokenIdentifier> secretManager;
  private SaslPropertiesResolver saslPropsResolver;
  private ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager();

  private int maxQueueSize;
  private final int maxRespSize;
  private final ThreadLocal<ResponseBuffer> responseBuffer =
      new ThreadLocal<ResponseBuffer>(){
        @Override
        protected ResponseBuffer initialValue() {
          return new ResponseBuffer(INITIAL_RESP_BUF_SIZE);
        }
  };
  private int socketSendBufferSize;
  private final int maxDataLength;
  private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm

  volatile private boolean running = true;         // true while server runs
  private CallQueueManager<Call> callQueue;

  // maintains the set of client connections and handles idle timeouts
  private ConnectionManager connectionManager;
  private Listener listener = null;
  // Auxiliary listeners maintained as in a map, to allow
  // arbitrary number of of auxiliary listeners. A map from
  // the port to the listener binding to it.
  private Map<Integer, Listener> auxiliaryListenerMap;
  private Responder responder = null;
  private Handler[] handlers = null;

  private boolean logSlowRPC = false;

  /**
   * Checks if LogSlowRPC is set true.
   * @return true, if LogSlowRPC is set true, false, otherwise.
   */
  protected boolean isLogSlowRPC() {
    return logSlowRPC;
  }

  /**
   * Sets slow RPC flag.
   * @param logSlowRPCFlag
   */
  @VisibleForTesting
  protected void setLogSlowRPC(boolean logSlowRPCFlag) {
    this.logSlowRPC = logSlowRPCFlag;
  }


  /**
   * Logs a Slow RPC Request.
   *
   * @param methodName - RPC Request method name
   * @param processingTime - Processing Time.
   *
   * if this request took too much time relative to other requests
   * we consider that as a slow RPC. 3 is a magic number that comes
   * from 3 sigma deviation. A very simple explanation can be found
   * by searching for 68-95-99.7 rule. We flag an RPC as slow RPC
   * if and only if it falls above 99.7% of requests. We start this logic
   * only once we have enough sample size.
   */
  void logSlowRpcCalls(String methodName, Call call, long processingTime) {
    final int deviation = 3;

    // 1024 for minSampleSize just a guess -- not a number computed based on
    // sample size analysis. It is chosen with the hope that this
    // number is high enough to avoid spurious logging, yet useful
    // in practice.
    final int minSampleSize = 1024;
    final double threeSigma = rpcMetrics.getProcessingMean() +
        (rpcMetrics.getProcessingStdDev() * deviation);

    if ((rpcMetrics.getProcessingSampleCount() > minSampleSize) &&
        (processingTime > threeSigma)) {
      LOG.warn("Slow RPC : {} took {} {} to process from client {}",
          methodName, processingTime, RpcMetrics.TIMEUNIT, call);
      rpcMetrics.incrSlowRpc();
    }
  }

  void updateMetrics(Call call, long startTime, boolean connDropped) {
    // delta = handler + processing + response
    long deltaNanos = Time.monotonicNowNanos() - startTime;
    long timestampNanos = call.timestampNanos;

    ProcessingDetails details = call.getProcessingDetails();
    // queue time is the delta between when the call first arrived and when it
    // began being serviced, minus the time it took to be put into the queue
    details.set(Timing.QUEUE,
        startTime - timestampNanos - details.get(Timing.ENQUEUE));
    deltaNanos -= details.get(Timing.PROCESSING);
    deltaNanos -= details.get(Timing.RESPONSE);
    details.set(Timing.HANDLER, deltaNanos);

    long queueTime = details.get(Timing.QUEUE, RpcMetrics.TIMEUNIT);
    rpcMetrics.addRpcQueueTime(queueTime);

    if (call.isResponseDeferred() || connDropped) {
      // call was skipped; don't include it in processing metrics
      return;
    }

    long processingTime =
        details.get(Timing.PROCESSING, RpcMetrics.TIMEUNIT);
    long waitTime =
        details.get(Timing.LOCKWAIT, RpcMetrics.TIMEUNIT);
    rpcMetrics.addRpcLockWaitTime(waitTime);
    rpcMetrics.addRpcProcessingTime(processingTime);
    // don't include lock wait for detailed metrics.
    processingTime -= waitTime;
    String name = call.getDetailedMetricsName();
    rpcDetailedMetrics.addProcessingTime(name, processingTime);
    callQueue.addResponseTime(name, call, details);
    if (isLogSlowRPC()) {
      logSlowRpcCalls(name, call, processingTime);
    }
  }

  void updateDeferredMetrics(String name, long processingTime) {
    rpcMetrics.addDeferredRpcProcessingTime(processingTime);
    rpcDetailedMetrics.addDeferredProcessingTime(name, processingTime);
  }

  /**
   * A convenience method to bind to a given address and report 
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address, 
                          int backlog) throws IOException {
    bind(socket, address, backlog, null, null);
  }

  public static void bind(ServerSocket socket, InetSocketAddress address, 
      int backlog, Configuration conf, String rangeConf) throws IOException {
    try {
      IntegerRanges range = null;
      if (rangeConf != null) {
        range = conf.getRange(rangeConf, "");
      }
      if (range == null || range.isEmpty() || (address.getPort() != 0)) {
        socket.bind(address, backlog);
      } else {
        for (Integer port : range) {
          if (socket.isBound()) break;
          try {
            InetSocketAddress temp = new InetSocketAddress(address.getAddress(),
                port);
            socket.bind(temp, backlog);
          } catch(BindException e) {
            //Ignored
          }
        }
        if (!socket.isBound()) {
          throw new BindException("Could not find a free port in "+range);
        }
      }
    } catch (SocketException e) {
      throw NetUtils.wrapException(null,
          0,
          address.getHostName(),
          address.getPort(), e);
    }
  }
  
  /**
   * Returns a handle to the rpcMetrics (required in tests)
   * @return rpc metrics
   */
  @VisibleForTesting
  public RpcMetrics getRpcMetrics() {
    return rpcMetrics;
  }

  @VisibleForTesting
  public RpcDetailedMetrics getRpcDetailedMetrics() {
    return rpcDetailedMetrics;
  }
  
  @VisibleForTesting
  Iterable<? extends Thread> getHandlers() {
    return Arrays.asList(handlers);
  }

  @VisibleForTesting
  Connection[] getConnections() {
    return connectionManager.toArray();
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server.
   */
  public void refreshServiceAcl(Configuration conf, PolicyProvider provider) {
    serviceAuthorizationManager.refresh(conf, provider);
  }

  /**
   * Refresh the service authorization ACL for the service handled by this server
   * using the specified Configuration.
   */
  @Private
  public void refreshServiceAclWithLoadedConfiguration(Configuration conf,
      PolicyProvider provider) {
    serviceAuthorizationManager.refreshWithLoadedConfiguration(conf, provider);
  }
  /**
   * Returns a handle to the serviceAuthorizationManager (required in tests)
   * @return instance of ServiceAuthorizationManager for this server
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public ServiceAuthorizationManager getServiceAuthorizationManager() {
    return serviceAuthorizationManager;
  }

  private String getQueueClassPrefix() {
    return CommonConfigurationKeys.IPC_NAMESPACE + "." + port;
  }

  static Class<? extends BlockingQueue<Call>> getQueueClass(
      String prefix, Configuration conf) {
    //  ipc.[端口号].callqueue.impl LinkedBlockingQueue
    String name = prefix + "." + CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
    Class<?> queueClass = conf.getClass(name, LinkedBlockingQueue.class);

    //这里需要看一下, 到底是啥
    Class<? extends BlockingQueue<Call>> call = CallQueueManager.convertQueueClass(queueClass, Call.class);

    return call;
  }

  static Class<? extends RpcScheduler> getSchedulerClass(
      String prefix, Configuration conf) {

    //   ipc.[端口号].scheduler.impl
    String schedulerKeyname = prefix + "." + CommonConfigurationKeys
        .IPC_SCHEDULER_IMPL_KEY;

    Class<?> schedulerClass = conf.getClass(schedulerKeyname, null);
    // Patch the configuration for legacy fcq configuration that does not have
    // a separate scheduler setting
    if (schedulerClass == null) {

      String queueKeyName = prefix + "." + CommonConfigurationKeys
          .IPC_CALLQUEUE_IMPL_KEY;
      Class<?> queueClass = conf.getClass(queueKeyName, null);

      if (queueClass != null) {

        // FairCallQueue , 当进行大量rpc操作时,默认的FIFO会使后面的操作失灵.  可以使用DecayRpcScheduler 策略.
        if (queueClass.getCanonicalName().equals(FairCallQueue.class.getCanonicalName())) {
          conf.setClass(schedulerKeyname, DecayRpcScheduler.class, RpcScheduler.class);
        }

      }
    }


    schedulerClass = conf.getClass(schedulerKeyname,
        DefaultRpcScheduler.class);

    Class<? extends RpcScheduler> scheduler = CallQueueManager.convertSchedulerClass(schedulerClass);
    return scheduler;
  }

  /*
   * Refresh the call queue
   */
  public synchronized void refreshCallQueue(Configuration conf) {
    // Create the next queue
    String prefix = getQueueClassPrefix();
    this.maxQueueSize = handlerCount * conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
    callQueue.swapQueue(getSchedulerClass(prefix, conf),
        getQueueClass(prefix, conf), maxQueueSize, prefix, conf);
    callQueue.setClientBackoffEnabled(getClientBackoffEnable(prefix, conf));
  }

  /**
   * Get from config if client backoff is enabled on that port.
   */
  static boolean getClientBackoffEnable(
      String prefix, Configuration conf) {
    String name = prefix + "." +
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
    return conf.getBoolean(name,
        CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT);
  }

  /** A generic call queued for handling. */
  public static class Call implements Schedulable, PrivilegedExceptionAction<Void> {


    private final ProcessingDetails processingDetails =
        new ProcessingDetails(TimeUnit.NANOSECONDS);


    // the method name to use in metrics
    private volatile String detailedMetricsName = "";
    final int callId;            // the client's call id
    final int retryCount;        // the retry count of the call
    long timestampNanos;         // time received when response is null
                                 // time served when response is not null
    private AtomicInteger responseWaitCount = new AtomicInteger(1);
    final RPC.RpcKind rpcKind;
    final byte[] clientId;
    private final TraceScope traceScope; // the HTrace scope on the server side
    private final CallerContext callerContext; // the call context
    private boolean deferredResponse = false;
    private int priorityLevel;
    // the priority level assigned by scheduler, 0 by default
    private long clientStateId;
    private boolean isCallCoordinated;

    Call() {
      this(RpcConstants.INVALID_CALL_ID, RpcConstants.INVALID_RETRY_COUNT,
        RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID);
    }

    Call(Call call) {
      this(call.callId, call.retryCount, call.rpcKind, call.clientId,
          call.traceScope, call.callerContext);
    }

    Call(int id, int retryCount, RPC.RpcKind kind, byte[] clientId) {
      this(id, retryCount, kind, clientId, null, null);
    }

    @VisibleForTesting // primarily TestNamenodeRetryCache
    public Call(int id, int retryCount, Void ignore1, Void ignore2,
        RPC.RpcKind kind, byte[] clientId) {
      this(id, retryCount, kind, clientId, null, null);
    }

    Call(int id, int retryCount, RPC.RpcKind kind, byte[] clientId,
        TraceScope traceScope, CallerContext callerContext) {
      this.callId = id;
      this.retryCount = retryCount;
      this.timestampNanos = Time.monotonicNowNanos();
      this.rpcKind = kind;
      this.clientId = clientId;
      this.traceScope = traceScope;
      this.callerContext = callerContext;
      this.clientStateId = Long.MIN_VALUE;
      this.isCallCoordinated = false;
    }

    /**
     * Indicates whether the call has been processed. Always true unless
     * overridden.
     *
     * @return true
     */
    boolean isOpen() {
      return true;
    }

    String getDetailedMetricsName() {
      return detailedMetricsName;
    }

    void setDetailedMetricsName(String name) {
      detailedMetricsName = name;
    }

    public ProcessingDetails getProcessingDetails() {
      return processingDetails;
    }

    @Override
    public String toString() {
      return "Call#" + callId + " Retry#" + retryCount;
    }

    @Override
    public Void run() throws Exception {
      return null;
    }
    // should eventually be abstract but need to avoid breaking tests
    public UserGroupInformation getRemoteUser() {
      return null;
    }
    public InetAddress getHostInetAddress() {
      return null;
    }
    public String getHostAddress() {
      InetAddress addr = getHostInetAddress();
      return (addr != null) ? addr.getHostAddress() : null;
    }

    public String getProtocol() {
      return null;
    }

    /**
     * Allow a IPC response to be postponed instead of sent immediately
     * after the handler returns from the proxy method.  The intended use
     * case is freeing up the handler thread when the response is known,
     * but an expensive pre-condition must be satisfied before it's sent
     * to the client.
     */
    @InterfaceStability.Unstable
    @InterfaceAudience.LimitedPrivate({"HDFS"})
    public final void postponeResponse() {
      int count = responseWaitCount.incrementAndGet();
      assert count > 0 : "response has already been sent";
    }

    @InterfaceStability.Unstable
    @InterfaceAudience.LimitedPrivate({"HDFS"})
    public final void sendResponse() throws IOException {
      int count = responseWaitCount.decrementAndGet();
      assert count >= 0 : "response has already been sent";
      if (count == 0) {
        doResponse(null);
      }
    }

    @InterfaceStability.Unstable
    @InterfaceAudience.LimitedPrivate({"HDFS"})
    public final void abortResponse(Throwable t) throws IOException {
      // don't send response if the call was already sent or aborted.
      if (responseWaitCount.getAndSet(-1) > 0) {
        doResponse(t);
      }
    }

    void doResponse(Throwable t) throws IOException {
      doResponse(t, RpcStatusProto.FATAL);
    }

    void doResponse(Throwable t, RpcStatusProto proto) throws IOException {}

    // For Schedulable
    @Override
    public UserGroupInformation getUserGroupInformation() {
      return getRemoteUser();
    }

    @Override
    public int getPriorityLevel() {
      return this.priorityLevel;
    }

    public void setPriorityLevel(int priorityLevel) {
      this.priorityLevel = priorityLevel;
    }

    public long getClientStateId() {
      return this.clientStateId;
    }

    public void setClientStateId(long stateId) {
      this.clientStateId = stateId;
    }

    public void markCallCoordinated(boolean flag) {
      this.isCallCoordinated = flag;
    }

    public boolean isCallCoordinated() {
      return this.isCallCoordinated;
    }

    @InterfaceStability.Unstable
    public void deferResponse() {
      this.deferredResponse = true;
    }

    @InterfaceStability.Unstable
    public boolean isResponseDeferred() {
      return this.deferredResponse;
    }

    public void setDeferredResponse(Writable response) {
    }

    public void setDeferredError(Throwable t) {
    }
  }

  /** A RPC extended call queued for handling. */
  private class RpcCall extends Call {
    final Connection connection;  // connection to client
    final Writable rpcRequest;    // Serialized Rpc request from client
    ByteBuffer rpcResponse;       // the response for this call

    private ResponseParams responseParams; // the response params
    private Writable rv;                   // the byte response

    RpcCall(RpcCall call) {
      super(call);
      this.connection = call.connection;
      this.rpcRequest = call.rpcRequest;
      this.rv = call.rv;
      this.responseParams = call.responseParams;
    }

    RpcCall(Connection connection, int id) {
      this(connection, id, RpcConstants.INVALID_RETRY_COUNT);
    }

    RpcCall(Connection connection, int id, int retryCount) {
      this(connection, id, retryCount, null,
          RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID,
          null, null);
    }

    RpcCall(Connection connection, int id, int retryCount,
        Writable param, RPC.RpcKind kind, byte[] clientId,
        TraceScope traceScope, CallerContext context) {
      super(id, retryCount, kind, clientId, traceScope, context);
      this.connection = connection;
      this.rpcRequest = param;
    }

    @Override
    boolean isOpen() {
      return connection.channel.isOpen();
    }

    void setResponseFields(Writable returnValue,
                           ResponseParams responseParams) {
      this.rv = returnValue;
      this.responseParams = responseParams;
    }

    @Override
    public String getProtocol() {
      return "rpc";
    }

    @Override
    public UserGroupInformation getRemoteUser() {
      return connection.user;
    }

    @Override
    public InetAddress getHostInetAddress() {
      return connection.getHostInetAddress();
    }

    @Override
    public Void run() throws Exception {
      if (!connection.channel.isOpen()) {
        Server.LOG.info(Thread.currentThread().getName() + ": skipped " + this);
        return null;
      }

      long startNanos = Time.monotonicNowNanos();
      Writable value = null;
      ResponseParams responseParams = new ResponseParams();

      try {

        //通过call()发起本地调用，并返回结果
        value = call(
            rpcKind, connection.protocolName, rpcRequest, timestampNanos);



      } catch (Throwable e) {
        populateResponseParamsOnError(e, responseParams);
      }
      if (!isResponseDeferred()) {
        long deltaNanos = Time.monotonicNowNanos() - startNanos;
        ProcessingDetails details = getProcessingDetails();

        details.set(Timing.PROCESSING, deltaNanos, TimeUnit.NANOSECONDS);
        deltaNanos -= details.get(Timing.LOCKWAIT, TimeUnit.NANOSECONDS);
        deltaNanos -= details.get(Timing.LOCKSHARED, TimeUnit.NANOSECONDS);
        deltaNanos -= details.get(Timing.LOCKEXCLUSIVE, TimeUnit.NANOSECONDS);
        details.set(Timing.LOCKFREE, deltaNanos, TimeUnit.NANOSECONDS);
        startNanos = Time.monotonicNowNanos();

        setResponseFields(value, responseParams);
        //todo 发送响应
        sendResponse();

        deltaNanos = Time.monotonicNowNanos() - startNanos;
        details.set(Timing.RESPONSE, deltaNanos, TimeUnit.NANOSECONDS);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Deferring response for callId: " + this.callId);
        }
      }
      return null;
    }

    /**
     * @param t              the {@link java.lang.Throwable} to use to set
     *                       errorInfo
     * @param responseParams the {@link ResponseParams} instance to populate
     */
    private void populateResponseParamsOnError(Throwable t,
                                               ResponseParams responseParams) {
      if (t instanceof UndeclaredThrowableException) {
        t = t.getCause();
      }
      logException(Server.LOG, t, this);
      if (t instanceof RpcServerException) {
        RpcServerException rse = ((RpcServerException) t);
        responseParams.returnStatus = rse.getRpcStatusProto();
        responseParams.detailedErr = rse.getRpcErrorCodeProto();
      } else {
        responseParams.returnStatus = RpcStatusProto.ERROR;
        responseParams.detailedErr = RpcErrorCodeProto.ERROR_APPLICATION;
      }
      responseParams.errorClass = t.getClass().getName();
      responseParams.error = StringUtils.stringifyException(t);
      // Remove redundant error class name from the beginning of the
      // stack trace
      String exceptionHdr = responseParams.errorClass + ": ";
      if (responseParams.error.startsWith(exceptionHdr)) {
        responseParams.error =
            responseParams.error.substring(exceptionHdr.length());
      }
    }

    void setResponse(ByteBuffer response) throws IOException {
      this.rpcResponse = response;
    }

    @Override
    void doResponse(Throwable t, RpcStatusProto status) throws IOException {
      RpcCall call = this;
      if (t != null) {
        if (status == null) {
          status = RpcStatusProto.FATAL;
        }
        // clone the call to prevent a race with another thread stomping
        // on the response while being sent.  the original call is
        // effectively discarded since the wait count won't hit zero
        call = new RpcCall(this);
        setupResponse(call, status, RpcErrorCodeProto.ERROR_RPC_SERVER,
            null, t.getClass().getName(), StringUtils.stringifyException(t));
      } else {
        setupResponse(call, call.responseParams.returnStatus,
            call.responseParams.detailedErr, call.rv,
            call.responseParams.errorClass,
            call.responseParams.error);
      }
      connection.sendResponse(call);
    }

    /**
     * Send a deferred response, ignoring errors.
     */
    private void sendDeferedResponse() {
      try {
        connection.sendResponse(this);
      } catch (Exception e) {
        // For synchronous calls, application code is done once it's returned
        // from a method. It does not expect to receive an error.
        // This is equivalent to what happens in synchronous calls when the
        // Responder is not able to send out the response.
        LOG.error("Failed to send deferred response. ThreadName=" + Thread
            .currentThread().getName() + ", CallId="
            + callId + ", hostname=" + getHostAddress());
      }
    }

    @Override
    public void setDeferredResponse(Writable response) {
      if (this.connection.getServer().running) {
        try {
          setupResponse(this, RpcStatusProto.SUCCESS, null, response,
              null, null);
        } catch (IOException e) {
          // For synchronous calls, application code is done once it has
          // returned from a method. It does not expect to receive an error.
          // This is equivalent to what happens in synchronous calls when the
          // response cannot be sent.
          LOG.error(
              "Failed to setup deferred successful response. ThreadName=" +
                  Thread.currentThread().getName() + ", Call=" + this);
          return;
        }
        sendDeferedResponse();
      }
    }

    @Override
    public void setDeferredError(Throwable t) {
      if (this.connection.getServer().running) {
        if (t == null) {
          t = new IOException(
              "User code indicated an error without an exception");
        }
        try {
          ResponseParams responseParams = new ResponseParams();
          populateResponseParamsOnError(t, responseParams);
          setupResponse(this, responseParams.returnStatus,
              responseParams.detailedErr,
              null, responseParams.errorClass, responseParams.error);
        } catch (IOException e) {
          // For synchronous calls, application code is done once it has
          // returned from a method. It does not expect to receive an error.
          // This is equivalent to what happens in synchronous calls when the
          // response cannot be sent.
          LOG.error(
              "Failed to setup deferred error response. ThreadName=" +
                  Thread.currentThread().getName() + ", Call=" + this);
        }
        sendDeferedResponse();
      }
    }

    /**
     * Holds response parameters. Defaults set to work for successful
     * invocations
     */
    private class ResponseParams {
      String errorClass = null;
      String error = null;
      RpcErrorCodeProto detailedErr = null;
      RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;
    }

    @Override
    public String toString() {
      return super.toString() + " " + rpcRequest + " from " + connection;
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {

    // socket 接收服务的channel 这是一个无阻塞的socker服务.
    private ServerSocketChannel acceptChannel = null; //the accept channel

    // 注册一个 Selector 用于服务的监控
    private Selector selector = null; //the selector that we use for the server

    // 注册Reader 服务的缓冲池.用于读取client的服务.
    private Reader[] readers = null;

    private int currentReader = 0;

    // Socket 地址的实体对象
    private InetSocketAddress address; //the address we bind at

    // 监听的端口
    private int listenPort; //the port we bind at

    //服务监听队列的长度, 默认 128
    private int backlogLength = conf.getInt(
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);
    
    Listener(int port) throws IOException {

      //创建InetSocketAddress 实例
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode

      // 创建一个无阻塞的socket服务
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      // 绑定服务和端口
      bind(acceptChannel.socket(), address, backlogLength, conf, portRangeConfig);

      //Could be an ephemeral port
      // 可能是一个临时端口
      this.listenPort = acceptChannel.socket().getLocalPort();

      //设置当前线程的名字
      Thread.currentThread().setName("Listener at " +  bindAddress + "/" + this.listenPort);

      // create a selector;
      // 创建一个selector
      selector= Selector.open();

      // 创建 Reader
      readers = new Reader[readThreads];
      for (int i = 0; i < readThreads; i++) {
        Reader reader = new Reader(
            "Socket Reader #" + (i + 1) + " for port " + port);
        readers[i] = reader;
        reader.start();
      }

      // Register accepts on the server socket with the selector.
      /// 注册 SelectionKey.OP_ACCEPT 事件到 selector
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);

      //设置线程名字
      this.setName("IPC Server listener on " + port);
      //设置守护模式.
      this.setDaemon(true);

    }
    
    private class Reader extends Thread {
      // 队列
      final private BlockingQueue<Connection> pendingConnections;

      //Selector 用于注册 channel
      private final Selector readSelector;

      Reader(String name) throws IOException {
        //设置线程名字
        super(name);

        //reader的队列长度, 默认 100
        this.pendingConnections =
            new LinkedBlockingQueue<Connection>(readerPendingConnectionQueue);
        this.readSelector = Selector.open();
      }
      
      @Override
      public void run() {
        LOG.info("Starting " + Thread.currentThread().getName());
        try {
          //Reader ... 进行轮询操作...
          doRunLoop();

        } finally {
          try {
            readSelector.close();
          } catch (IOException ioe) {
            LOG.error("Error closing read selector in " + Thread.currentThread().getName(), ioe);
          }
        }
      }

      private synchronized void doRunLoop() {
        while (running) {
          SelectionKey key = null;
          try {
            // consume as many connections as currently queued to avoid
            // unbridled acceptance of connections that starves the select

            int size = pendingConnections.size();
            for (int i=size; i>0; i--) {
              Connection conn = pendingConnections.take();
              conn.channel.register(readSelector, SelectionKey.OP_READ, conn);
            }

            //等待请求接入
            readSelector.select();

            //在当前的readSelector上等待可读事件，也就是有客户端RPC请求到达
            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              key = iter.next();
              iter.remove();
              try {
                if (key.isReadable()) {
                  //有可读事件时，调用doRead()方法处理
                  doRead(key);
                }
              } catch (CancelledKeyException cke) {
                // something else closed the connection, ex. responder or
                // the listener doing an idle scan.  ignore it and let them
                // clean up.
                LOG.info(Thread.currentThread().getName() +
                    ": connection aborted from " + key.attachment());
              }
              key = null;
            }
          } catch (InterruptedException e) {
            if (running) {                      // unexpected -- log it
              LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
            }
          } catch (IOException ex) {
            LOG.error("Error in Reader", ex);
          } catch (Throwable re) {
            LOG.error("Bug in read selector!", re);
            ExitUtil.terminate(1, "Bug in read selector!");
          }
        }
      }

      /**
       * Updating the readSelector while it's being used is not thread-safe,
       * so the connection must be queued.  The reader will drain the queue
       * and update its readSelector before performing the next select
       */
      public void addConnection(Connection conn) throws InterruptedException {
        pendingConnections.put(conn);
        readSelector.wakeup();
      }

      void shutdown() {
        assert !running;
        readSelector.wakeup();
        try {
          super.interrupt();
          super.join();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public void run() {
      LOG.info(Thread.currentThread().getName() + ": starting");

      SERVER.set(Server.this);

      //创建线程,定时扫描connection, 关闭超时,无效的连接
      connectionManager.startIdleScan();

      while (running) {
        SelectionKey key = null;
        try {

          //如果没有请求进来的话,会阻塞.
          getSelector().select();

          //循环判断是否有新的连接建立请求
          Iterator<SelectionKey> iter = getSelector().selectedKeys().iterator();

          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable()){
                  //如果有，则调用doAccept()方法响应
                  doAccept(key);
                }
              }
            } catch (IOException e) {
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          //这里可能出现内存溢出的情况，要特别注意
          // 如果内存溢出了,会关闭当前连接, 休眠 60 秒
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give 
          // some thread(s) a chance to finish
          LOG.warn("Out of Memory in server select", e);
          closeCurrentConnection(key, e);
          connectionManager.closeIdle(true);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          //捕获到其他异常，也关闭当前连接
          closeCurrentConnection(key, e);
        }
      }


      LOG.info("Stopping " + Thread.currentThread().getName());


      // 关闭请求. 停止所有服务.
      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException e) { }

        selector= null;
        acceptChannel= null;
        
        // close all connections
        connectionManager.stopIdleScan();
        connectionManager.closeAll();
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          closeConnection(c);
          c = null;
        }
      }
    }

    InetSocketAddress getAddress() {
      return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }


    //doAccept()方法会接收来自客户端的Socket连接请 求并初始化Socket连接。
    //
    // 之后doAccept()方法会从readers线程池中选出一个Reader线程读取来自这个客户端的RPC请求。
    // 每个Reader线程都会有一个自己的readSelector，用于监听是 否有新的RPC请求到达。
    // 所以doAccept()方法在建立连接并选出 Reader对象后，
    // 会在这个 Reader 对象的 readSelector上注册OP_READ事件。
    // 那么这里就有一个问题了，

    // Reader对象 在被通知时是怎么知道从哪个Socket输入流上读取数据呢?
    // 这里就用到了Connection类，
    // Connection类封装了 Server 与 Client之间的Socket连接，
    // doAccept()方法会通过 SelectionKey 将新构造的 Connection对象传给Reader，
    // 这样Reader线程在被唤醒时就可以通过Connection 对象读取RPC请求了。

    void doAccept(SelectionKey key) throws InterruptedException, IOException,  OutOfMemoryError {

      //接收请求，建立连接
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      SocketChannel channel;

      while ((channel = server.accept()) != null) {

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(tcpNoDelay);
        channel.socket().setKeepAlive(true);

        // 获取 reader , 通过 % 取余的方式获取reader
        Reader reader = getReader();


        //构造Connection对象， 添加到readKey的附件传递给Reader对象
        Connection c = connectionManager.register(channel, this.listenPort);

        // If the connectionManager can't take it, close the connection.
        // 如果connectionManager获取不到Connection, 关闭当前连接
        if (c == null) {
          if (channel.isOpen()) {
            IOUtils.cleanup(null, channel);
          }
          connectionManager.droppedConnections.getAndIncrement();
          continue;
        }

        //todo 这个是干啥用????  4307
        key.attach(c);  // so closeCurrentConnection can get the object


        //reader 增加连接,处理 connection 里面的数据
        reader.addConnection(c);

      }
    }
    // doRead()方法负责读取RPC请求，
    // 虽然readSelector监听到了RPC请求的可读事件，
    // 但 是doRead()方法此时并不知道这个RPC请求是由哪个客户端发送来的，
    // 所以doRead()方法首先会调用SelectionKey.attachment() 方法获取 Listener 对象构造的 Connection 对象，
    // Connection对象中封装了Server与Client之间的网络连接，之后doRead()方法只需调用
    // Connection.readAndProcess()方法就可以读取RPC请求了，这里的设计非常的巧妙。

    void doRead(SelectionKey key) throws InterruptedException {
      int count;

      //通过SelectionKey获取Connection对象
      // (Connection对象是 Listener#run方法中的doAccept 方法中绑定的  key.attach(c) )
      Connection c = (Connection)key.attachment();

      if (c == null) {
        return;  
      }

      c.setLastContact(Time.now());
      
      try {

        //调用Connection.readAndProcess处理读取请求
        count = c.readAndProcess();


      } catch (InterruptedException ieo) {
        LOG.info(Thread.currentThread().getName() + ": readAndProcess caught InterruptedException", ieo);
        throw ieo;
      } catch (Exception e) {
        // Any exceptions that reach here are fatal unexpected internal errors
        // that could not be sent to the client.
        LOG.info(Thread.currentThread().getName() +
            ": readAndProcess from client " + c +
            " threw exception [" + e + "]", e);
        count = -1; //so that the (count < 0) block is executed
      }
      // setupResponse will signal the connection should be closed when a
      // fatal response is sent.
      if (count < 0 || c.shouldClose()) {
        closeConnection(c);
        c = null;
      } else {
        c.setLastContact(Time.now());
      }
    }   

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.info(Thread.currentThread().getName() + ":Exception in closing listener socket. " + e);
        }
      }
      for (Reader r : readers) {
        r.shutdown();
      }
    }
    
    synchronized Selector getSelector() { return selector; }

    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  private final static long PURGE_INTERVAL_NANOS = TimeUnit.NANOSECONDS.convert(
      15, TimeUnit.MINUTES);

  // 内部类Responder也是一个线程类，
  // Server端仅有一个Responder对象，
  // Responder内部包含一个Selector对象responseSelector，
  // 用于监听SelectionKey.OP_WRITE事件。
  //
  // 当网络环 境不佳或者响应信息太大时，
  // Handler线程可能无法发送完整的响应信息到客户端，
  // 这时 Handler会在Responder.responseSelector上注册SelectionKey.OP_WRITE事件，
  // responseSelector会循环监听网络环境是否具备发送数据的条件，
  // 之后responseselector会触发Responder线程发送未完成的响应结果到客户端

  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
    private final Selector writeSelector;
    private int pending;         // connections waiting to register

    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run() {
      LOG.info(Thread.currentThread().getName() + ": starting");
      SERVER.set(Server.this);
      try {
        //Responder 处理逻辑
        doRunLoop();
      } finally {
        LOG.info("Stopping " + Thread.currentThread().getName());
        try {
          writeSelector.close();
        } catch (IOException ioe) {
          LOG.error("Couldn't close write selector in " + Thread.currentThread().getName(), ioe);
        }
      }
    }
    
    private void doRunLoop() {
      long lastPurgeTimeNanos = 0;   // last check for old calls.

      while (running) {
        try {

          waitPending();     // If a channel is being registered, wait.


          // 阻塞 15min ,如果超时的话, 会执行后面的清除长期没有发送成功的消息
          writeSelector.select(
              TimeUnit.NANOSECONDS.toMillis(PURGE_INTERVAL_NANOS));

          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isWritable()) {

                //执行写入操作
                doAsyncWrite(key);

              }
            } catch (CancelledKeyException cke) {
              // something else closed the connection, ex. reader or the
              // listener doing an idle scan.  ignore it and let them clean
              // up
              RpcCall call = (RpcCall)key.attachment();
              if (call != null) {
                LOG.info(Thread.currentThread().getName() +
                    ": connection aborted from " + call.connection);
              }
            } catch (IOException e) {
              LOG.info(Thread.currentThread().getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long nowNanos = Time.monotonicNowNanos();
          if (nowNanos < lastPurgeTimeNanos + PURGE_INTERVAL_NANOS) {
            continue;
          }
          lastPurgeTimeNanos = nowNanos;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          if(LOG.isDebugEnabled()) {
            LOG.debug("Checking for old call responses.");
          }



          ArrayList<RpcCall> calls;
          
          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<RpcCall>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              RpcCall call = (RpcCall)key.attachment();
              if (call != null && key.channel() == call.connection.channel) { 
                calls.add(call);
              }
            }
          }
          // 移除掉已经很久没有发送调的信息
          for (RpcCall call : calls) {
            doPurge(call, nowNanos);
          }
        } catch (OutOfMemoryError e) {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          LOG.warn("Out of Memory in server select", e);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          LOG.warn("Exception in Responder", e);
        }
      }
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      RpcCall call = (RpcCall)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue 
    // for a long time.
    //
    private void doPurge(RpcCall call, long now) {
      LinkedList<RpcCall> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        Iterator<RpcCall> iter = responseQueue.listIterator(0);
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestampNanos + PURGE_INTERVAL_NANOS) {
            closeConnection(call.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private boolean processResponse(LinkedList<RpcCall> responseQueue,
                                    boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements = 0;
      RpcCall call = null;
      try {
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          SocketChannel channel = call.connection.channel;
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + ": responding to " + call);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          int numBytes = channelWrite(channel, call.rpcResponse);
          if (numBytes < 0) {
            return true;
          }
          if (!call.rpcResponse.hasRemaining()) {
            //Clear out the response buffer so it can be collected
            call.rpcResponse = null;
            call.connection.decRpcCount();
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                  + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then 
            // insert in Selector queue. 
            //
            call.connection.responseQueue.addFirst(call);
            
            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestampNanos = Time.monotonicNowNanos();
              
              incPending();
              try {
                // Wakeup the thread blocked on select, only then can the call 
                // to channel.register() complete.
                writeSelector.wakeup();

                //注册并将 RpcCall 对象 附在 key 上面
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (ClosedChannelException e) {
                //Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                  + " Wrote partial " + numBytes + " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(Thread.currentThread().getName()+", call " + call + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(RpcCall call) throws IOException {
      synchronized (call.connection.responseQueue) {
        // must only wrap before adding to the responseQueue to prevent
        // postponed responses from being encrypted and sent out of order.
        if (call.connection.useWrap) {
          wrapWithSasl(call);
        }
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }

  @InterfaceAudience.Private
  public enum AuthProtocol {
    NONE(0),
    SASL(-33);
    
    public final int callId;
    AuthProtocol(int callId) {
      this.callId = callId;
    }
    
    static AuthProtocol valueOf(int callId) {
      for (AuthProtocol authType : AuthProtocol.values()) {
        if (authType.callId == callId) {
          return authType;
        }
      }
      return null;
    }
  };
  
  /**
   * Wrapper for RPC IOExceptions to be returned to the client.  Used to
   * let exceptions bubble up to top of processOneRpc where the correct
   * callId can be associated with the response.  Also used to prevent
   * unnecessary stack trace logging if it's not an internal server error. 
   */
  @SuppressWarnings("serial")
  private static class FatalRpcServerException extends RpcServerException {
    private final RpcErrorCodeProto errCode;
    public FatalRpcServerException(RpcErrorCodeProto errCode, IOException ioe) {
      super(ioe.toString(), ioe);
      this.errCode = errCode;
    }
    public FatalRpcServerException(RpcErrorCodeProto errCode, String message) {
      this(errCode, new RpcServerException(message));
    }
    @Override
    public RpcStatusProto getRpcStatusProto() {
      return RpcStatusProto.FATAL;
    }
    @Override
    public RpcErrorCodeProto getRpcErrorCodeProto() {
      return errCode;
    }
    @Override
    public String toString() {
      return getCause().toString();
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  public class Connection {
    private boolean connectionHeaderRead = false; // connection  header is read?
    private boolean connectionContextRead = false; //if connection context that
                                            //follows connection header is read

    private SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    private LinkedList<RpcCall> responseQueue;
    // number of outstanding rpcs
    private AtomicInteger rpcCount = new AtomicInteger();
    private long lastContact;
    private int dataLength;
    private Socket socket;
    // Cache the remote host & port info so that even if the socket is 
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;
    private InetAddress addr;
    
    IpcConnectionContextProto connectionContext;
    String protocolName;
    SaslServer saslServer;
    private String establishedQOP;
    private AuthMethod authMethod;
    private AuthProtocol authProtocol;
    private boolean saslContextEstablished;
    private ByteBuffer connectionHeaderBuf = null;
    private ByteBuffer unwrappedData;
    private ByteBuffer unwrappedDataLengthBuffer;
    private int serviceClass;
    private boolean shouldClose = false;
    private int ingressPort;

    UserGroupInformation user = null;
    public UserGroupInformation attemptingUser = null; // user name before auth

    // Fake 'call' for failed authorization response
    private final RpcCall authFailedCall =
        new RpcCall(this, AUTHORIZATION_FAILED_CALL_ID);

    private boolean sentNegotiate = false;

    private boolean useWrap = false;
    
    public Connection(SocketChannel channel, long lastContact,
        int ingressPort) {
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      
      // the buffer is initialized to read the "hrpc" and after that to read
      // the length of the Rpc-packet (i.e 4 bytes)
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.unwrappedData = null;
      this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
      this.addr = socket.getInetAddress();
      this.ingressPort = ingressPort;
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<RpcCall>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }   

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort; 
    }

    boolean setShouldClose() {
      return shouldClose = true;
    }

    boolean shouldClose() {
      return shouldClose;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public int getIngressPort() {
      return ingressPort;
    }

    public InetAddress getHostInetAddress() {
      return addr;
    }

    public String getEstablishedQOP() {
      return establishedQOP;
    }
    
    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    public Server getServer() {
      return Server.this;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount.get() == 0;
    }
    
    /* Decrement the outstanding RPC count */
    private void decRpcCount() {
      rpcCount.decrementAndGet();
    }
    
    /* Increment the outstanding RPC count */
    private void incRpcCount() {
      rpcCount.incrementAndGet();
    }
    
    private UserGroupInformation getAuthorizedUgi(String authorizedId)
        throws InvalidToken, AccessControlException {
      if (authMethod == AuthMethod.TOKEN) {
        TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        UserGroupInformation ugi = tokenId.getUser();
        if (ugi == null) {
          throw new AccessControlException(
              "Can't retrieve username from tokenIdentifier.");
        }
        ugi.addTokenIdentifier(tokenId);
        return ugi;
      } else {
        return UserGroupInformation.createRemoteUser(authorizedId, authMethod);
      }
    }

    private void saslReadAndProcess(RpcWritable.Buffer buffer) throws
        RpcServerException, IOException, InterruptedException {
      final RpcSaslProto saslMessage =
          getMessage(RpcSaslProto.getDefaultInstance(), buffer);
      switch (saslMessage.getState()) {
        case WRAP: {
          if (!saslContextEstablished || !useWrap) {
            throw new FatalRpcServerException(
                RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                new SaslException("Server is not wrapping data"));
          }
          // loops over decoded data and calls processOneRpc
          unwrapPacketAndProcessRpcs(saslMessage.getToken().toByteArray());
          break;
        }
        default:
          saslProcess(saslMessage);
      }
    }

    /**
     * Some exceptions ({@link RetriableException} and {@link StandbyException})
     * that are wrapped as a cause of parameter e are unwrapped so that they can
     * be sent as the true cause to the client side. In case of
     * {@link InvalidToken} we go one level deeper to get the true cause.
     * 
     * @param e the exception that may have a cause we want to unwrap.
     * @return the true cause for some exceptions.
     */
    private Throwable getTrueCause(IOException e) {
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof RetriableException) {
          return cause;
        } else if (cause instanceof StandbyException) {
          return cause;
        } else if (cause instanceof InvalidToken) {
          // FIXME: hadoop method signatures are restricting the SASL
          // callbacks to only returning InvalidToken, but some services
          // need to throw other exceptions (ex. NN + StandyException),
          // so for now we'll tunnel the real exceptions via an
          // InvalidToken's cause which normally is not set 
          if (cause.getCause() != null) {
            cause = cause.getCause();
          }
          return cause;
        }
        cause = cause.getCause();
      }
      return e;
    }
    
    /**
     * Process saslMessage and send saslResponse back
     * @param saslMessage received SASL message
     * @throws RpcServerException setup failed due to SASL negotiation
     *         failure, premature or invalid connection context, or other state 
     *         errors. This exception needs to be sent to the client. This 
     *         exception will wrap {@link RetriableException}, 
     *         {@link InvalidToken}, {@link StandbyException} or 
     *         {@link SaslException}.
     * @throws IOException if sending reply fails
     * @throws InterruptedException
     */
    private void saslProcess(RpcSaslProto saslMessage)
        throws RpcServerException, IOException, InterruptedException {
      if (saslContextEstablished) {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            new SaslException("Negotiation is already complete"));
      }
      RpcSaslProto saslResponse = null;
      try {
        try {
          saslResponse = processSaslMessage(saslMessage);
        } catch (IOException e) {
          rpcMetrics.incrAuthenticationFailures();
          if (LOG.isDebugEnabled()) {
            LOG.debug(StringUtils.stringifyException(e));
          }
          // attempting user could be null
          IOException tce = (IOException) getTrueCause(e);
          AUDITLOG.warn(AUTH_FAILED_FOR + this.toString() + ":"
              + attemptingUser + " (" + e.getLocalizedMessage()
              + ") with true cause: (" + tce.getLocalizedMessage() + ")");
          throw tce;
        }
        
        if (saslServer != null && saslServer.isComplete()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Negotiated QoP is "
                + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          user = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server successfully authenticated client: " + user);
          }
          rpcMetrics.incrAuthenticationSuccesses();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
          saslContextEstablished = true;
        }
      } catch (RpcServerException rse) { // don't re-wrap
        throw rse;
      } catch (IOException ioe) {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ioe);
      }
      // send back response if any, may throw IOException
      if (saslResponse != null) {
        doSaslReply(saslResponse);
      }
      // do NOT enable wrapping until the last auth response is sent
      if (saslContextEstablished) {
        String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
        establishedQOP = qop;
        // SASL wrapping is only used if the connection has a QOP, and
        // the value is not auth.  ex. auth-int & auth-priv
        useWrap = (qop != null && !"auth".equalsIgnoreCase(qop));
        if (!useWrap) {
          disposeSasl();
        }
      }
    }
    
    /**
     * Process a saslMessge.
     * @param saslMessage received SASL message
     * @return the sasl response to send back to client
     * @throws SaslException if authentication or generating response fails, 
     *                       or SASL protocol mixup
     * @throws IOException if a SaslServer cannot be created
     * @throws AccessControlException if the requested authentication type 
     *         is not supported or trying to re-attempt negotiation.
     * @throws InterruptedException
     */
    private RpcSaslProto processSaslMessage(RpcSaslProto saslMessage)
        throws SaslException, IOException, AccessControlException,
        InterruptedException {
      final RpcSaslProto saslResponse;
      final SaslState state = saslMessage.getState(); // required      
      switch (state) {
        case NEGOTIATE: {
          if (sentNegotiate) {
            // FIXME shouldn't this be SaslException?
            throw new AccessControlException(
                "Client already attempted negotiation");
          }
          saslResponse = buildSaslNegotiateResponse();
          // simple-only server negotiate response is success which client
          // interprets as switch to simple
          if (saslResponse.getState() == SaslState.SUCCESS) {
            switchToSimple();
          }
          break;
        }
        case INITIATE: {
          if (saslMessage.getAuthsCount() != 1) {
            throw new SaslException("Client mechanism is malformed");
          }
          // verify the client requested an advertised authType
          SaslAuth clientSaslAuth = saslMessage.getAuths(0);
          if (!negotiateResponse.getAuthsList().contains(clientSaslAuth)) {
            if (sentNegotiate) {
              throw new AccessControlException(
                  clientSaslAuth.getMethod() + " authentication is not enabled."
                      + "  Available:" + enabledAuthMethods);
            }
            saslResponse = buildSaslNegotiateResponse();
            break;
          }
          authMethod = AuthMethod.valueOf(clientSaslAuth.getMethod());
          // abort SASL for SIMPLE auth, server has already ensured that
          // SIMPLE is a legit option above.  we will send no response
          if (authMethod == AuthMethod.SIMPLE) {
            switchToSimple();
            saslResponse = null;
            break;
          }
          // sasl server for tokens may already be instantiated
          if (saslServer == null || authMethod != AuthMethod.TOKEN) {
            saslServer = createSaslServer(authMethod);
          }
          saslResponse = processSaslToken(saslMessage);
          break;
        }
        case RESPONSE: {
          saslResponse = processSaslToken(saslMessage);
          break;
        }
        default:
          throw new SaslException("Client sent unsupported state " + state);
      }
      return saslResponse;
    }

    private RpcSaslProto processSaslToken(RpcSaslProto saslMessage)
        throws SaslException {
      if (!saslMessage.hasToken()) {
        throw new SaslException("Client did not send a token");
      }
      byte[] saslToken = saslMessage.getToken().toByteArray();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Have read input token of size " + saslToken.length
            + " for processing by saslServer.evaluateResponse()");
      }
      saslToken = saslServer.evaluateResponse(saslToken);
      return buildSaslResponse(
          saslServer.isComplete() ? SaslState.SUCCESS : SaslState.CHALLENGE,
          saslToken);
    }

    private void switchToSimple() {
      // disable SASL and blank out any SASL server
      authProtocol = AuthProtocol.NONE;
      disposeSasl();
    }

    private RpcSaslProto buildSaslResponse(SaslState state, byte[] replyToken) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Will send " + state + " token of size "
            + ((replyToken != null) ? replyToken.length : null)
            + " from saslServer.");
      }
      RpcSaslProto.Builder response = RpcSaslProto.newBuilder();
      response.setState(state);
      if (replyToken != null) {
        response.setToken(ByteString.copyFrom(replyToken));
      }
      return response.build();
    }

    private void doSaslReply(Message message) throws IOException {
      final RpcCall saslCall = new RpcCall(this, AuthProtocol.SASL.callId);
      setupResponse(saslCall,
          RpcStatusProto.SUCCESS, null,
          RpcWritable.wrap(message), null, null);
      sendResponse(saslCall);
    }

    private void doSaslReply(Exception ioe) throws IOException {
      setupResponse(authFailedCall,
          RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_UNAUTHORIZED,
          null, ioe.getClass().getName(), ioe.toString());
      sendResponse(authFailedCall);
    }

    private void disposeSasl() {
      if (saslServer != null) {
        try {
          saslServer.dispose();
        } catch (SaslException ignored) {
        } finally {
          saslServer = null;
        }
      }
    }

    private void checkDataLength(int dataLength) throws IOException {
      if (dataLength < 0) {
        String error = "Unexpected data length " + dataLength +
                       "!! from " + getHostAddress();
        LOG.warn(error);
        throw new IOException(error);
      } else if (dataLength > maxDataLength) {
        String error = "Requested data length " + dataLength +
              " is longer than maximum configured RPC length " + 
            maxDataLength + ".  RPC came from " + getHostAddress();
        LOG.warn(error);
        throw new IOException(error);
      }
    }

    /**
     * This method reads in a non-blocking fashion from the channel: 
     * this method is called repeatedly when data is present in the channel; 
     * when it has enough data to process one rpc it processes that rpc.
     * 
     * On the first pass, it processes the connectionHeader, 
     * connectionContext (an outOfBand RPC) and at most one RPC request that 
     * follows that. On future passes it will process at most one RPC request.
     *  
     * Quirky things: dataLengthBuffer (4 bytes) is used to read "hrpc" OR 
     * rpc request length.
     *    
     * @return -1 in case of error, else num bytes read so far
     * @throws IOException - internal error that should not be returned to
     *         client, typically failure to respond to client
     * @throws InterruptedException
     *
     * readAndProcess()方法会首先从Socket流中读取连接头域(connectionHeader)，
     * 然后 读取一个完整的RPC请求，
     * 最后调用processOneRpc()方法处理这个RPC请求。
     * processOneRpc()方法会读取出RPC请求头域，
     * 然后调用processRpcRequest()处理RPC请求 体。
     *
     * 这里特别注意，
     * 如果在处理过程中抛出了异常，则直接通过Socket返回RPC响应(带 有Server异常信息的响应)。
     */
    public int readAndProcess() throws IOException, InterruptedException {


      int whileIndex = 0 ;
      while (!shouldClose()) {




        // stop if a fatal response has been sent.
        // dataLengthBuffer is used to read "hrpc" or the rpc-packet length
        int count = -1;


        System.out.println(" init  dataLengthBuffer.remaining  :  " + dataLengthBuffer.remaining());
        if (dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, dataLengthBuffer);       
          if (count < 0 || dataLengthBuffer.remaining() > 0) {

            System.out.println(" 数据读完了耶, 退出  readAndProcess ");
            return count;
          }else{
            System.out.println(" 数据还没有读完!!!!!!  继续下一部分处理. ");
          }

        }

        if (!connectionHeaderRead) {
          System.out.println("还没有读取 头信息  .. ");
          // Every connection is expected to send the header;
          // so far we read "hrpc" of the connection header.
          if (connectionHeaderBuf == null) {
            // for the bytes that follow "hrpc", in the connection header
            connectionHeaderBuf = ByteBuffer.allocate(HEADER_LEN_AFTER_HRPC_PART);
          }
          count = channelRead(channel, connectionHeaderBuf);
          if (count < 0 || connectionHeaderBuf.remaining() > 0) {
            System.out.println("   !connectionHeaderRead  ==> connectionHeaderBuf.remaining : "+ connectionHeaderBuf.remaining());
            return count;

          }
          int version = connectionHeaderBuf.get(0);
          // TODO we should add handler for service class later
          this.setServiceClass(connectionHeaderBuf.get(1));


          dataLengthBuffer.flip();
          
          // Check if it looks like the user is hitting an IPC port
          // with an HTTP GET - this is a common error, so we can
          // send back a simple string indicating as much.
          if (HTTP_GET_BYTES.equals(dataLengthBuffer)) {
            setupHttpRequestOnIpcPortResponse();
            return -1;
          }

          if(!RpcConstants.HEADER.equals(dataLengthBuffer)) {
            LOG.warn("Incorrect RPC Header length from {}:{} "
                + "expected length: {} got length: {}",
                hostAddress, remotePort, RpcConstants.HEADER, dataLengthBuffer);
            setupBadVersionResponse(version);
            return -1;
          }
          if (version != CURRENT_VERSION) {
            //Warning is ok since this is not supposed to happen.
            LOG.warn("Version mismatch from " +
                     hostAddress + ":" + remotePort +
                     " got version " + version + 
                     " expected version " + CURRENT_VERSION);
            setupBadVersionResponse(version);
            return -1;
          }
          
          // this may switch us into SIMPLE
          authProtocol = initializeAuthContext(connectionHeaderBuf.get(2));          
          
          dataLengthBuffer.clear(); // clear to next read rpc packet len
          connectionHeaderBuf = null;
          connectionHeaderRead = true;
          continue; // connection header read, now read  4 bytes rpc packet len
        }

        System.out.println("已经读取过头信息  .. ");
        if (data == null) { // just read 4 bytes -  length of RPC packet

          System.out.println("befroe : "  + dataLengthBuffer);

          dataLengthBuffer.flip();


          dataLength = dataLengthBuffer.getInt();


          System.out.println("after : "  + dataLengthBuffer);



          checkDataLength(dataLength);
          // Set buffer for reading EXACTLY the RPC-packet length and no more.
          data = ByteBuffer.allocate(dataLength);
        }
        // Now read the RPC packet
        count = channelRead(channel, data);
        
        if (data.remaining() == 0) {
          dataLengthBuffer.clear(); // to read length of future rpc packets
          data.flip();
          ByteBuffer requestData = data;
          data = null; // null out in case processOneRpc throws.
          boolean isHeaderRead = connectionContextRead;

          System.out.println("处理这个RPC请求#processOneRpc");
          //处理这个RPC请求。
          processOneRpc(requestData);


          // the last rpc-request we processed could have simply been the
          // connectionContext; if so continue to read the first RPC.
          if (!isHeaderRead) {
            continue;
          }
        } 
        return count;
      }
      return -1;
    }

    private AuthProtocol initializeAuthContext(int authType)
        throws IOException {
      AuthProtocol authProtocol = AuthProtocol.valueOf(authType);
      if (authProtocol == null) {
        IOException ioe = new IpcException("Unknown auth protocol:" + authType);
        doSaslReply(ioe);
        throw ioe;        
      }
      boolean isSimpleEnabled = enabledAuthMethods.contains(AuthMethod.SIMPLE);
      switch (authProtocol) {
        case NONE: {
          // don't reply if client is simple and server is insecure
          if (!isSimpleEnabled) {
            IOException ioe = new AccessControlException(
                "SIMPLE authentication is not enabled."
                    + "  Available:" + enabledAuthMethods);
            doSaslReply(ioe);
            throw ioe;
          }
          break;
        }
        default: {
          break;
        }
      }
      return authProtocol;
    }

    /**
     * Process the Sasl's Negotiate request, including the optimization of 
     * accelerating token negotiation.
     * @return the response to Negotiate request - the list of enabled 
     *         authMethods and challenge if the TOKENS are supported. 
     * @throws SaslException - if attempt to generate challenge fails.
     * @throws IOException - if it fails to create the SASL server for Tokens
     */
    private RpcSaslProto buildSaslNegotiateResponse()
        throws InterruptedException, SaslException, IOException {
      RpcSaslProto negotiateMessage = negotiateResponse;
      // accelerate token negotiation by sending initial challenge
      // in the negotiation response
      if (enabledAuthMethods.contains(AuthMethod.TOKEN)) {
        saslServer = createSaslServer(AuthMethod.TOKEN);
        byte[] challenge = saslServer.evaluateResponse(new byte[0]);
        RpcSaslProto.Builder negotiateBuilder =
            RpcSaslProto.newBuilder(negotiateResponse);
        negotiateBuilder.getAuthsBuilder(0)  // TOKEN is always first
            .setChallenge(ByteString.copyFrom(challenge));
        negotiateMessage = negotiateBuilder.build();
      }
      sentNegotiate = true;
      return negotiateMessage;
    }
    
    private SaslServer createSaslServer(AuthMethod authMethod)
        throws IOException, InterruptedException {
      final Map<String,?> saslProps =
                  saslPropsResolver.getServerProperties(addr, ingressPort);
      return new SaslRpcServer(authMethod).create(this, saslProps, secretManager);
    }
    
    /**
     * Try to set up the response to indicate that the client version
     * is incompatible with the server. This can contain special-case
     * code to speak enough of past IPC protocols to pass back
     * an exception to the caller.
     * @param clientVersion the version the caller is using 
     * @throws IOException
     */
    private void setupBadVersionResponse(int clientVersion) throws IOException {
      String errMsg = "Server IPC version " + CURRENT_VERSION +
      " cannot communicate with client version " + clientVersion;
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      
      if (clientVersion >= 9) {
        // Versions >>9  understand the normal response
        RpcCall fakeCall = new RpcCall(this, -1);
        setupResponse(fakeCall,
            RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_VERSION_MISMATCH,
            null, VersionMismatch.class.getName(), errMsg);
        sendResponse(fakeCall);
      } else if (clientVersion >= 3) {
        RpcCall fakeCall = new RpcCall(this, -1);
        // Versions 3 to 8 use older response
        setupResponseOldVersionFatal(buffer, fakeCall,
            null, VersionMismatch.class.getName(), errMsg);

        sendResponse(fakeCall);
      } else if (clientVersion == 2) { // Hadoop 0.18.3
        RpcCall fakeCall = new RpcCall(this, 0);
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(0); // call ID
        out.writeBoolean(true); // error
        WritableUtils.writeString(out, VersionMismatch.class.getName());
        WritableUtils.writeString(out, errMsg);
        fakeCall.setResponse(ByteBuffer.wrap(buffer.toByteArray()));
        sendResponse(fakeCall);
      }
    }
    
    private void setupHttpRequestOnIpcPortResponse() throws IOException {
      RpcCall fakeCall = new RpcCall(this, 0);
      fakeCall.setResponse(ByteBuffer.wrap(
          RECEIVED_HTTP_REQ_RESPONSE.getBytes(StandardCharsets.UTF_8)));
      sendResponse(fakeCall);
    }

    /** Reads the connection context following the connection header
     * @throws RpcServerException - if the header cannot be
     *         deserialized, or the user is not authorized
     */ 
    private void processConnectionContext(RpcWritable.Buffer buffer)
        throws RpcServerException {
      // allow only one connection context during a session
      if (connectionContextRead) {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Connection context already processed");
      }
      connectionContext = getMessage(IpcConnectionContextProto.getDefaultInstance(), buffer);
      protocolName = connectionContext.hasProtocol() ? connectionContext
          .getProtocol() : null;

      UserGroupInformation protocolUser = ProtoUtil.getUgi(connectionContext);
      if (authProtocol == AuthProtocol.NONE) {
        user = protocolUser;
      } else {
        // user is authenticated
        user.setAuthenticationMethod(authMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However, 
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(user.getUserName()))) {
          if (authMethod == AuthMethod.TOKEN) {
            // Not allowed to doAs if token authentication is used
            throw new FatalRpcServerException(
                RpcErrorCodeProto.FATAL_UNAUTHORIZED,
                new AccessControlException("Authenticated user (" + user
                    + ") doesn't match what the client claims to be ("
                    + protocolUser + ")"));
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = user;
            user = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
          }
        }
      }
      authorizeConnection();
      // don't set until after authz because connection isn't established
      connectionContextRead = true;
      if (user != null) {
        connectionManager.incrUserConnections(user.getShortUserName());
      }
    }
    
    /**
     * Process a wrapped RPC Request - unwrap the SASL packet and process
     * each embedded RPC request 
     * @param inBuf - SASL wrapped request of one or more RPCs
     * @throws IOException - SASL packet cannot be unwrapped
     * @throws InterruptedException
     */    
    private void unwrapPacketAndProcessRpcs(byte[] inBuf)
        throws IOException, InterruptedException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Have read input token of size " + inBuf.length
            + " for processing by saslServer.unwrap()");
      }
      inBuf = saslServer.unwrap(inBuf, 0, inBuf.length);
      ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(
          inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (!shouldClose()) { // stop if a fatal response has been sent.
        int count = -1;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          ByteBuffer requestData = unwrappedData;
          unwrappedData = null; // null out in case processOneRpc throws.
          processOneRpc(requestData);
        }
      }
    }
    
    /**
     * Process one RPC Request from buffer read from socket stream 
     *  - decode rpc in a rpc-Call
     *  - handle out-of-band RPC requests such as the initial connectionContext
     *  - A successfully decoded RpcCall will be deposited in RPC-Q and
     *    its response will be sent later when the request is processed.
     * 
     * Prior to this call the connectionHeader ("hrpc...") has been handled and
     * if SASL then SASL has been established and the buf we are passed
     * has been unwrapped from SASL.
     * 
     * @param bb - contains the RPC request header and the rpc request
     * @throws IOException - internal error that should not be returned to
     *         client, typically failure to respond to client
     * @throws InterruptedException
     */
    private void processOneRpc(ByteBuffer bb)
        throws IOException, InterruptedException {
      // exceptions that escape this method are fatal to the connection.
      // setupResponse will use the rpc status to determine if the connection
      // should be closed.
      int callId = -1;
      int retry = RpcConstants.INVALID_RETRY_COUNT;
      try {
        final RpcWritable.Buffer buffer = RpcWritable.Buffer.wrap(bb);

        //解析出RPC请求头域
        final RpcRequestHeaderProto header =
            getMessage(RpcRequestHeaderProto.getDefaultInstance(), buffer);

        //从RPC请求头域中提取出callId
        callId = header.getCallId();

        //从RPC请求头域中提取出重试次数
        retry = header.getRetryCount();
        if (LOG.isDebugEnabled()) {
          LOG.debug(" got #" + callId);
        }

        //检测头信息是否正确
        checkRpcHeaders(header);


        //处理RPC请求头域异常的情况
        if (callId < 0) {
          // callIds typically used during connection setup
          processRpcOutOfBandRequest(header, buffer);
        } else if (!connectionContextRead) {
          throw new FatalRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection context not established");
        } else {



          //如果RPC请求头域正常，则直接调用processRpcRequest处理RPC请求体
          processRpcRequest(header, buffer);



        }
      } catch (RpcServerException rse) {
        // inform client of error, but do not rethrow else non-fatal
        // exceptions will close connection!
        if (LOG.isDebugEnabled()) {
          LOG.debug(Thread.currentThread().getName() +
              ": processOneRpc from client " + this +
              " threw exception [" + rse + "]");
        }
        //通过Socket返回这个带有异常信息的RPC响应
        // use the wrapped exception if there is one.
        Throwable t = (rse.getCause() != null) ? rse.getCause() : rse;
        final RpcCall call = new RpcCall(this, callId, retry);
        setupResponse(call,
            rse.getRpcStatusProto(), rse.getRpcErrorCodeProto(), null,
            t.getClass().getName(),
            t.getMessage() != null ? t.getMessage() : t.toString());

        sendResponse(call);
      }
    }

    /**
     * Verify RPC header is valid
     * @param header - RPC request header
     * @throws RpcServerException - header contains invalid values
     */
    private void checkRpcHeaders(RpcRequestHeaderProto header)
        throws RpcServerException {
      if (!header.hasRpcOp()) {
        String err = " IPC Server: No rpc op in rpcRequestHeader";
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      if (header.getRpcOp() != 
          RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET) {
        String err = "IPC Server does not implement rpc header operation" + 
                header.getRpcOp();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
      // If we know the rpc kind, get its class so that we can deserialize
      // (Note it would make more sense to have the handler deserialize but 
      // we continue with this original design.
      if (!header.hasRpcKind()) {
        String err = " IPC Server: No rpc kind in rpcRequestHeader";
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }
    }

    /**
     *
     * processRpcRequest()会从输入流中解析出完整的请求对象(包括请求元数据以及请求 参数)，
     * 然后根据RPC请求头的信息(包括callId)构造Call对象(Call对象保存了这次调 用的所有信息)，
     * 最后将这个Call对象放入callQueue队列中保存，等待Handler线程处理。
     *
     * Process an RPC Request 
     *   - the connection headers and context must have been already read.
     *   - Based on the rpcKind, decode the rpcRequest.
     *   - A successfully decoded RpcCall will be deposited in RPC-Q and
     *     its response will be sent later when the request is processed.
     * @param header - RPC request header
     * @param buffer - stream to request payload
     * @throws RpcServerException - generally due to fatal rpc layer issues
     *   such as invalid header or deserialization error.  The call queue
     *   may also throw a fatal or non-fatal exception on overflow.
     * @throws IOException - fatal internal error that should/could not
     *   be sent to client.
     * @throws InterruptedException
     */
    private void processRpcRequest(RpcRequestHeaderProto header,
        RpcWritable.Buffer buffer) throws RpcServerException,
        InterruptedException {

      System.out.println(

              String.format("processRpcRequest#header: CallId [%s]  , ClientId [%s]  , CallerContext [%s]  , RpcKind [%s] " ,
              header.getCallId(),
              header.getClientId(),
              header.getCallerContext(),
              header.getRpcKind())
      );


      //获取协议
      Class<? extends Writable> rpcRequestClass = 
          getRpcRequestWrapper(header.getRpcKind());

      if (rpcRequestClass == null) {
        LOG.warn("Unknown rpc kind "  + header.getRpcKind() + 
            " from client " + getHostAddress());
        final String err = "Unknown rpc kind in rpc header"  + 
            header.getRpcKind();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
      }


      //读取RPC请求体
      Writable rpcRequest;
      try { //Read the rpc request
        rpcRequest = buffer.newInstance(rpcRequestClass, conf);
      } catch (RpcServerException rse) { // lets tests inject failures.
        throw rse;
      } catch (Throwable t) { // includes runtime exception from newInstance
        LOG.warn("Unable to read call parameters for client " +
                 getHostAddress() + "on connection protocol " +
            this.protocolName + " for rpcKind " + header.getRpcKind(),  t);
        String err = "IPC server unable to read call parameters: "+ t.getMessage();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
      }



      TraceScope traceScope = null;
      if (header.hasTraceInfo()) {
        if (tracer != null) {
          // If the incoming RPC included tracing info, always continue the
          // trace
          SpanId parentSpanId = new SpanId(
              header.getTraceInfo().getTraceId(),
              header.getTraceInfo().getParentId());
          traceScope = tracer.newScope(
              RpcClientUtil.toTraceName(rpcRequest.toString()),
              parentSpanId);
          traceScope.detach();
        }
      }


      CallerContext callerContext = null;
      if (header.hasCallerContext()) {
        callerContext =
            new CallerContext.Builder(header.getCallerContext().getContext())
                .setSignature(header.getCallerContext().getSignature()
                    .toByteArray())
                .build();
      }



      //构造Call对象封装RPC请求信息
      RpcCall call = new RpcCall(this, header.getCallId(),
          header.getRetryCount(), rpcRequest,
          ProtoUtil.convert(header.getRpcKind()),
          header.getClientId().toByteArray(), traceScope, callerContext);



      // Save the priority level assignment by the scheduler
      call.setPriorityLevel(callQueue.getPriorityLevel(call));
      call.markCallCoordinated(false);

      if(alignmentContext != null && call.rpcRequest != null &&
          (call.rpcRequest instanceof ProtobufRpcEngine.RpcProtobufRequest)) {
        // if call.rpcRequest is not RpcProtobufRequest, will skip the following
        // step and treat the call as uncoordinated. As currently only certain
        // ClientProtocol methods request made through RPC protobuf needs to be
        // coordinated.
        String methodName;
        String protoName;
        ProtobufRpcEngine.RpcProtobufRequest req =
            (ProtobufRpcEngine.RpcProtobufRequest) call.rpcRequest;
        try {
          methodName = req.getRequestHeader().getMethodName();
          protoName = req.getRequestHeader().getDeclaringClassProtocolName();
          if (alignmentContext.isCoordinatedCall(protoName, methodName)) {
            call.markCallCoordinated(true);
            long stateId;
            stateId = alignmentContext.receiveRequestState(
                header, getMaxIdleTime());
            call.setClientStateId(stateId);
          }
        } catch (IOException ioe) {
          throw new RpcServerException("Processing RPC request caught ", ioe);
        }
      }

      try {

        //将Call对象放入callQueue中，等待Handler处理
        internalQueueCall(call);


      } catch (RpcServerException rse) {
        throw rse;
      } catch (IOException ioe) {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.ERROR_RPC_SERVER, ioe);
      }
      incRpcCount();  // Increment the rpc count
    }

    /**
     * Establish RPC connection setup by negotiating SASL if required, then
     * reading and authorizing the connection header
     * @param header - RPC header
     * @param buffer - stream to request payload
     * @throws RpcServerException - setup failed due to SASL
     *         negotiation failure, premature or invalid connection context,
     *         or other state errors. This exception needs to be sent to the 
     *         client.
     * @throws IOException - failed to send a response back to the client
     * @throws InterruptedException
     */
    private void processRpcOutOfBandRequest(RpcRequestHeaderProto header,
        RpcWritable.Buffer buffer) throws RpcServerException,
            IOException, InterruptedException {
      final int callId = header.getCallId();
      if (callId == CONNECTION_CONTEXT_CALL_ID) {
        // SASL must be established prior to connection context
        if (authProtocol == AuthProtocol.SASL && !saslContextEstablished) {
          throw new FatalRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "Connection header sent during SASL negotiation");
        }
        // read and authorize the user
        processConnectionContext(buffer);
      } else if (callId == AuthProtocol.SASL.callId) {
        // if client was switched to simple, ignore first SASL message
        if (authProtocol != AuthProtocol.SASL) {
          throw new FatalRpcServerException(
              RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
              "SASL protocol not requested by client");
        }
        saslReadAndProcess(buffer);
      } else if (callId == PING_CALL_ID) {
        LOG.debug("Received ping message");
      } else {
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
            "Unknown out of band call #" + callId);
      }
    }    

    /**
     * Authorize proxy users to access this server
     * @throws RpcServerException - user is not allowed to proxy
     */
    private void authorizeConnection() throws RpcServerException {
      try {
        // If auth method is TOKEN, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (user != null && user.getRealUser() != null
            && (authMethod != AuthMethod.TOKEN)) {
          ProxyUsers.authorize(user, this.getHostAddress());
        }
        authorize(user, protocolName, getHostInetAddress());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Successfully authorized " + connectionContext);
        }
        rpcMetrics.incrAuthorizationSuccesses();
      } catch (AuthorizationException ae) {
        LOG.info("Connection from " + this
            + " for protocol " + connectionContext.getProtocol()
            + " is unauthorized for user " + user);
        rpcMetrics.incrAuthorizationFailures();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_UNAUTHORIZED, ae);
      }
    }
    
    /**
     * Decode the a protobuf from the given input stream 
     * @return Message - decoded protobuf
     * @throws RpcServerException - deserialization failed
     */
    @SuppressWarnings("unchecked")
    <T extends Message> T getMessage(Message message,
        RpcWritable.Buffer buffer) throws RpcServerException {
      try {
        return (T)buffer.getValue(message);
      } catch (Exception ioe) {
        Class<?> protoClass = message.getClass();
        throw new FatalRpcServerException(
            RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
            "Error decoding " + protoClass.getSimpleName() + ": "+ ioe);
      }
    }

    // ipc reader threads should invoke this directly, whereas handlers
    // must invoke call.sendResponse to allow lifecycle management of
    // external, postponed, deferred calls, etc.
    private void sendResponse(RpcCall call) throws IOException {
      responder.doRespond(call);
    }

    /**
     * Get service class for connection
     * @return the serviceClass
     */
    public int getServiceClass() {
      return serviceClass;
    }

    /**
     * Set service class for connection
     * @param serviceClass the serviceClass to set
     */
    public void setServiceClass(int serviceClass) {
      this.serviceClass = serviceClass;
    }

    private synchronized void close() {
      disposeSasl();
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception e) {
        LOG.debug("Ignoring socket shutdown exception", e);
      }
      if (channel.isOpen()) {
        IOUtils.cleanup(null, channel);
      }
      IOUtils.cleanup(null, socket);
    }
  }

  public void queueCall(Call call) throws IOException, InterruptedException {
    // external non-rpc calls don't need server exception wrapper.
    try {
      internalQueueCall(call);
    } catch (RpcServerException rse) {
      throw (IOException)rse.getCause();
    }
  }

  private void internalQueueCall(Call call)
      throws IOException, InterruptedException {
    internalQueueCall(call, true);
  }

  private void internalQueueCall(Call call, boolean blocking)
      throws IOException, InterruptedException {
    try {
      // queue the call, may be blocked if blocking is true.
      if (blocking) {
        callQueue.put(call);
      } else {
        callQueue.add(call);
      }
      long deltaNanos = Time.monotonicNowNanos() - call.timestampNanos;
      call.getProcessingDetails().set(Timing.ENQUEUE, deltaNanos,
          TimeUnit.NANOSECONDS);
    } catch (CallQueueOverflowException cqe) {
      // If rpc scheduler indicates back off based on performance degradation
      // such as response time or rpc queue is full, we will ask the client
      // to back off by throwing RetriableException. Whether the client will
      // honor RetriableException and retry depends the client and its policy.
      // For example, IPC clients using FailoverOnNetworkExceptionRetry handle
      // RetriableException.
      rpcMetrics.incrClientBackoff();
      // unwrap retriable exception.
      throw cqe.getCause();
    }
  }

  /**
   * Handles queued calls .
   * Handler类也是一个线程类，负责执行RPC请求对应的本地函数，然后将结果发回客户端。
   *
   * 在Server类中会有多个Handler线程，它们并发地处理RPC请求。
   *
   * Handler线程类的主方法会循环从共享队列callQueue中取出待处理的Call对象，
   * 然后调 用Server.call()方法执行RPC调用对应的本地函数，
   * 如果在调用过程中发生异常，则将异常 信息保存下来。
   *
   * 接下来Handler会调用setupResponse()方法构造RPC响应，
   * 并调用 responder.doRespond()方法将响应发回。
   *
   * */
  private class Handler extends Thread {
    public Handler(int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber +
          " on default port " + port);
    }

    @Override
    public void run() {
      LOG.debug(Thread.currentThread().getName() + ": starting");
      SERVER.set(Server.this);
      while (running) {
        TraceScope traceScope = null;
        Call call = null;
        long startTimeNanos = 0;
        // True iff the connection for this call has been dropped.
        // Set to true by default and update to false later if the connection
        // can be succesfully read.
        boolean connDropped = true;

        try {



          //从callQueue中取出请求
          call = callQueue.take(); // pop the queue; maybe blocked here
          startTimeNanos = Time.monotonicNowNanos();
          if (alignmentContext != null && call.isCallCoordinated() &&
              call.getClientStateId() > alignmentContext.getLastSeenStateId()) {
            /*
             * The call processing should be postponed until the client call's
             * state id is aligned (<=) with the server state id.

             * NOTE:
             * Inserting the call back to the queue can change the order of call
             * execution comparing to their original placement into the queue.
             * This is not a problem, because Hadoop RPC does not have any
             * constraints on ordering the incoming rpc requests.
             * In case of Observer, it handles only reads, which are
             * commutative.
             */
            // Re-queue the call and continue
            requeueCall(call);
            continue;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName() + ": " + call + " for RpcKind " + call.rpcKind);
          }


          //设置当前线程要处理的 call 任务
          CurCall.set(call);


          if (call.traceScope != null) {
            call.traceScope.reattach();
            traceScope = call.traceScope;
            traceScope.getSpan().addTimelineAnnotation("called");
          }

          // always update the current call context
          CallerContext.setCurrent(call.callerContext);
          UserGroupInformation remoteUser = call.getRemoteUser();
          connDropped = !call.isOpen();


          //通过调用Call对象的run()方法发起本地调用，并返回结果
          if (remoteUser != null) {
            remoteUser.doAs(call);
          } else {
            // RpcCall#run()
            call.run();
          }



        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
            if (traceScope != null) {
              traceScope.getSpan().addTimelineAnnotation("unexpectedly interrupted: " +
                  StringUtils.stringifyException(e));
            }
          }
        } catch (Exception e) {
          LOG.info(Thread.currentThread().getName() + " caught an exception", e);
          if (traceScope != null) {
            traceScope.getSpan().addTimelineAnnotation("Exception: " +
                StringUtils.stringifyException(e));
          }
        } finally {
          CurCall.set(null);
          IOUtils.cleanupWithLogger(LOG, traceScope);
          if (call != null) {
            updateMetrics(call, startTimeNanos, connDropped);
            ProcessingDetails.LOG.debug(
                "Served: [{}]{} name={} user={} details={}",
                call, (call.isResponseDeferred() ? ", deferred" : ""),
                call.getDetailedMetricsName(), call.getRemoteUser(),
                call.getProcessingDetails());
          }
        }
      }
      LOG.debug(Thread.currentThread().getName() + ": exiting");
    }

    private void requeueCall(Call call)
        throws IOException, InterruptedException {
      try {
        internalQueueCall(call, false);
      } catch (RpcServerException rse) {
        call.doResponse(rse.getCause(), rse.getRpcStatusProto());
      }
    }

  }

  @VisibleForTesting
  void logException(Logger logger, Throwable e, Call call) {
    if (exceptionsHandler.isSuppressedLog(e.getClass())) {
      return; // Log nothing.
    }

    final String logMsg = Thread.currentThread().getName() + ", call " + call;
    if (exceptionsHandler.isTerseLog(e.getClass())) {
      // Don't log the whole stack trace. Way too noisy!
      logger.info(logMsg + ": " + e);
    } else if (e instanceof RuntimeException || e instanceof Error) {
      // These exception types indicate something is probably wrong
      // on the server side, as opposed to just a normal exceptional
      // result.
      logger.warn(logMsg, e);
    } else {
      logger.info(logMsg, e);
    }
  }
  
  protected Server(String bindAddress, int port,
                  Class<? extends Writable> paramClass, int handlerCount, 
                  Configuration conf)
    throws IOException 
  {
    this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, Integer
        .toString(port), null, null);
  }
  
  protected Server(String bindAddress, int port,
      Class<? extends Writable> rpcRequestClass, int handlerCount,
      int numReaders, int queueSizePerHandler, Configuration conf,
      String serverName, SecretManager<? extends TokenIdentifier> secretManager)
    throws IOException {
    this(bindAddress, port, rpcRequestClass, handlerCount, numReaders, 
        queueSizePerHandler, conf, serverName, secretManager, null);
  }
  
  /** 
   * Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * If queueSizePerHandler or numReaders are not -1 they will be used instead of parameters
   * from configuration. Otherwise the configuration will be picked up.
   * 
   * If rpcRequestClass is null then the rpcRequestClass must have been 
   * registered via {@link #registerProtocolEngine(RPC.RpcKind,
   *  Class, RPC.RpcInvoker)}
   * This parameter has been retained for compatibility with existing tests
   * and usage.
   */
  @SuppressWarnings("unchecked")
  protected Server(String bindAddress, int port,
      Class<? extends Writable> rpcRequestClass, int handlerCount,
      int numReaders, int queueSizePerHandler, Configuration conf,
      String serverName, SecretManager<? extends TokenIdentifier> secretManager,
      String portRangeConfig)
    throws IOException {
    //绑定IP 地址 必填
    this.bindAddress = bindAddress;
    //绑定配置文件
    this.conf = conf;
    this.portRangeConfig = portRangeConfig;

    //绑定 端口 必填
    this.port = port;

    // todo  这个值 应该是为null,
    this.rpcRequestClass = rpcRequestClass;

    // handlerCount 的线程数量
    this.handlerCount = handlerCount;


    this.socketSendBufferSize = 0;

    // 服务名
    this.serverName = serverName;

    this.auxiliaryListenerMap = null;

    // server接收的最大数据长度
    // ipc.maximum.data.length  默认 :  64 * 1024 * 1024    ===>  64 MB
    this.maxDataLength = conf.getInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
        CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);

    // handler队列的最大数量 默认值为-1 , 即默认最大容量为 handler线程的数量 * 每个handler线程队列的数量 = 1 * 100 = 100
    if (queueSizePerHandler != -1) {
      //最大队列长度:  如果不是默认值为handler线程的数量 * 每个handler线程队列的数量
      this.maxQueueSize = handlerCount * queueSizePerHandler;
    } else {
      //最大队列长度:   如果设置为 -1 的话,  默认值handler队列值为 100 , 所以最大队列长度为  : handler 线程的数量 * 100
      this.maxQueueSize = handlerCount * conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
          CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);      
    }

    // 返回值的大小如果超过 1024*1024 = 1M  ,将会有告警[WARN]级别的日志输出....
    this.maxRespSize = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);


    //设置 readThread的线程数量, 默认 1
    if (numReaders != -1) {
      this.readThreads = numReaders;
    } else {
      this.readThreads = conf.getInt(
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
          CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
    }

    //设置reader的队列长度, 默认 100
    this.readerPendingConnectionQueue = conf.getInt(
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
        CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT);

    // Setup appropriate callqueue
    final String prefix = getQueueClassPrefix();

    //callQueue reader 读取client端的数据之后 . 放到这个队列里面, 等到hander进行处理

    //队列 : LinkedBlockingQueue<Call> 格式.  调度器默认: DefaultRpcScheduler
    //      clientBackOffEnabled 这个值没懂干啥用的!!!  默认false
    this.callQueue = new CallQueueManager<Call>(getQueueClass(prefix, conf),
        getSchedulerClass(prefix, conf),
        getClientBackoffEnable(prefix, conf), maxQueueSize, prefix, conf);

    // 安全相关
    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
    this.authorize = conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);

    // configure supported authentications
    this.enabledAuthMethods = getAuthMethods(secretManager, conf);
    this.negotiateResponse = buildNegotiateResponse(enabledAuthMethods);


    // Start the listener here and let it bind to the port
    //创建Listener , 绑定监听的端口, 所有client端发送的请求, 都是通过这里进行转发
    listener = new Listener(port);


    // set the server port to the default listener port.
    this.port = listener.getAddress().getPort();

    connectionManager = new ConnectionManager();
    this.rpcMetrics = RpcMetrics.create(this, conf);
    this.rpcDetailedMetrics = RpcDetailedMetrics.create(this.port);


    //打开/关闭服务器上TCP套接字连接的Nagle算法 默认值 true
    //如果设置为true，则禁用该算法，并可能会降低延迟，同时会导致更多/更小数据包的开销。
    this.tcpNoDelay = conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY,
        CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_DEFAULT);


    //如果当前的rpc服务比其他的rpc服务要慢的话, 记录日志, 默认 false
    this.setLogSlowRPC(conf.getBoolean(
        CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC,
        CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC_DEFAULT));

    // Create the responder here
    // 创建响应服务
    responder = new Responder();

    //安全相关
    if (secretManager != null || UserGroupInformation.isSecurityEnabled()) {
      SaslRpcServer.init(conf);
      saslPropsResolver = SaslPropertiesResolver.getInstance(conf);
    }

    //设置StandbyException异常处理
    this.exceptionsHandler.addTerseLoggingExceptions(StandbyException.class);
  }

  public synchronized void addAuxiliaryListener(int auxiliaryPort)
      throws IOException {
    if (auxiliaryListenerMap == null) {
      auxiliaryListenerMap = new HashMap<>();
    }
    if (auxiliaryListenerMap.containsKey(auxiliaryPort) && auxiliaryPort != 0) {
      throw new IOException(
          "There is already a listener binding to: " + auxiliaryPort);
    }
    Listener newListener = new Listener(auxiliaryPort);
    // in the case of port = 0, the listener would be on a != 0 port.
    LOG.info("Adding a server listener on port " +
        newListener.getAddress().getPort());
    auxiliaryListenerMap.put(newListener.getAddress().getPort(), newListener);
  }

  private RpcSaslProto buildNegotiateResponse(List<AuthMethod> authMethods)
      throws IOException {
    RpcSaslProto.Builder negotiateBuilder = RpcSaslProto.newBuilder();
    if (authMethods.contains(AuthMethod.SIMPLE) && authMethods.size() == 1) {
      // SIMPLE-only servers return success in response to negotiate
      negotiateBuilder.setState(SaslState.SUCCESS);
    } else {
      negotiateBuilder.setState(SaslState.NEGOTIATE);
      for (AuthMethod authMethod : authMethods) {
        SaslRpcServer saslRpcServer = new SaslRpcServer(authMethod);      
        SaslAuth.Builder builder = negotiateBuilder.addAuthsBuilder()
            .setMethod(authMethod.toString())
            .setMechanism(saslRpcServer.mechanism);
        if (saslRpcServer.protocol != null) {
          builder.setProtocol(saslRpcServer.protocol);
        }
        if (saslRpcServer.serverId != null) {
          builder.setServerId(saslRpcServer.serverId);
        }
      }
    }
    return negotiateBuilder.build();
  }

  // get the security type from the conf. implicitly include token support
  // if a secret manager is provided, or fail if token is the conf value but
  // there is no secret manager
  private List<AuthMethod> getAuthMethods(SecretManager<?> secretManager,
                                             Configuration conf) {
    AuthenticationMethod confAuthenticationMethod =
        SecurityUtil.getAuthenticationMethod(conf);        
    List<AuthMethod> authMethods = new ArrayList<AuthMethod>();
    if (confAuthenticationMethod == AuthenticationMethod.TOKEN) {
      if (secretManager == null) {
        throw new IllegalArgumentException(AuthenticationMethod.TOKEN +
            " authentication requires a secret manager");
      } 
    } else if (secretManager != null) {
      LOG.debug(AuthenticationMethod.TOKEN +
          " authentication enabled for secret manager");
      // most preferred, go to the front of the line!
      authMethods.add(AuthenticationMethod.TOKEN.getAuthMethod());
    }
    authMethods.add(confAuthenticationMethod.getAuthMethod());        
    
    LOG.debug("Server accepts auth methods:" + authMethods);
    return authMethods;
  }
  
  private void closeConnection(Connection connection) {
    connectionManager.close(connection);
  }

  /**
   * Setup response for the IPC Call.
   * 
   * @param call {@link Call} to which we are setting up the response
   * @param status of the IPC call
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponse(
      RpcCall call, RpcStatusProto status, RpcErrorCodeProto erCode,
      Writable rv, String errorClass, String error)
          throws IOException {
    // fatal responses will cause the reader to close the connection.
    if (status == RpcStatusProto.FATAL) {
      call.connection.setShouldClose();
    }
    RpcResponseHeaderProto.Builder headerBuilder =
        RpcResponseHeaderProto.newBuilder();
    headerBuilder.setClientId(ByteString.copyFrom(call.clientId));
    headerBuilder.setCallId(call.callId);
    headerBuilder.setRetryCount(call.retryCount);
    headerBuilder.setStatus(status);
    headerBuilder.setServerIpcVersionNum(CURRENT_VERSION);
    if (alignmentContext != null) {
      alignmentContext.updateResponseState(headerBuilder);
    }

    if (status == RpcStatusProto.SUCCESS) {
      RpcResponseHeaderProto header = headerBuilder.build();
      try {
        setupResponse(call, header, rv);
      } catch (Throwable t) {
        LOG.warn("Error serializing call response for call " + call, t);
        // Call back to same function - this is OK since the
        // buffer is reset at the top, and since status is changed
        // to ERROR it won't infinite loop.
        setupResponse(call, RpcStatusProto.ERROR,
            RpcErrorCodeProto.ERROR_SERIALIZING_RESPONSE,
            null, t.getClass().getName(),
            StringUtils.stringifyException(t));
        return;
      }
    } else { // Rpc Failure
      headerBuilder.setExceptionClassName(errorClass);
      headerBuilder.setErrorMsg(error);
      headerBuilder.setErrorDetail(erCode);
      setupResponse(call, headerBuilder.build(), null);
    }
  }

  private void setupResponse(RpcCall call,
      RpcResponseHeaderProto header, Writable rv) throws IOException {
    final byte[] response;
    if (rv == null || (rv instanceof RpcWritable.ProtobufWrapper)) {
      response = setupResponseForProtobuf(header, rv);
    } else {
      response = setupResponseForWritable(header, rv);
    }
    if (response.length > maxRespSize) {
      LOG.warn("Large response size " + response.length + " for call "
          + call.toString());
    }
    call.setResponse(ByteBuffer.wrap(response));
  }

  private byte[] setupResponseForWritable(
      RpcResponseHeaderProto header, Writable rv) throws IOException {
    ResponseBuffer buf = responseBuffer.get().reset();
    try {
      RpcWritable.wrap(header).writeTo(buf);
      if (rv != null) {
        RpcWritable.wrap(rv).writeTo(buf);
      }
      return buf.toByteArray();
    } finally {
      // Discard a large buf and reset it back to smaller size
      // to free up heap.
      if (buf.capacity() > maxRespSize) {
        buf.setCapacity(INITIAL_RESP_BUF_SIZE);
      }
    }
  }


  // writing to a pre-allocated array is the most efficient way to construct
  // a protobuf response.
  private byte[] setupResponseForProtobuf(
      RpcResponseHeaderProto header, Writable rv) throws IOException {
    Message payload = (rv != null)
        ? ((RpcWritable.ProtobufWrapper)rv).getMessage() : null;
    int length = getDelimitedLength(header);
    if (payload != null) {
      length += getDelimitedLength(payload);
    }
    byte[] buf = new byte[length + 4];
    CodedOutputStream cos = CodedOutputStream.newInstance(buf);
    // the stream only supports little endian ints
    cos.writeRawByte((byte)((length >>> 24) & 0xFF));
    cos.writeRawByte((byte)((length >>> 16) & 0xFF));
    cos.writeRawByte((byte)((length >>>  8) & 0xFF));
    cos.writeRawByte((byte)((length >>>  0) & 0xFF));
    cos.writeRawVarint32(header.getSerializedSize());
    header.writeTo(cos);
    if (payload != null) {
      cos.writeRawVarint32(payload.getSerializedSize());
      payload.writeTo(cos);
    }
    return buf;
  }

  private static int getDelimitedLength(Message message) {
    int length = message.getSerializedSize();
    return length + CodedOutputStream.computeRawVarint32Size(length);
  }

  /**
   * Setup response for the IPC Call on Fatal Error from a 
   * client that is using old version of Hadoop.
   * The response is serialized using the previous protocol's response
   * layout.
   * 
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param rv return value for the IPC Call, if the call was successful
   * @param errorClass error class, if the the call failed
   * @param error error message, if the call failed
   * @throws IOException
   */
  private void setupResponseOldVersionFatal(ByteArrayOutputStream response, 
                             RpcCall call,
                             Writable rv, String errorClass, String error) 
  throws IOException {
    final int OLD_VERSION_FATAL_STATUS = -1;
    response.reset();
    DataOutputStream out = new DataOutputStream(response);
    out.writeInt(call.callId);                // write call id
    out.writeInt(OLD_VERSION_FATAL_STATUS);   // write FATAL_STATUS
    WritableUtils.writeString(out, errorClass);
    WritableUtils.writeString(out, error);
    call.setResponse(ByteBuffer.wrap(response.toByteArray()));
  }

  private void wrapWithSasl(RpcCall call) throws IOException {
    if (call.connection.saslServer != null) {
      byte[] token = call.rpcResponse.array();
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (call.connection.saslServer) {
        token = call.connection.saslServer.wrap(token, 0, token.length);
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      // rebuild with sasl header and payload
      RpcResponseHeaderProto saslHeader = RpcResponseHeaderProto.newBuilder()
          .setCallId(AuthProtocol.SASL.callId)
          .setStatus(RpcStatusProto.SUCCESS)
          .build();
      RpcSaslProto saslMessage = RpcSaslProto.newBuilder()
          .setState(SaslState.WRAP)
          .setToken(ByteString.copyFrom(token))
          .build();
      setupResponse(call, saslHeader, RpcWritable.wrap(saslMessage));
    }
  }
  
  Configuration getConf() {
    return conf;
  }
  
  /** Sets the socket buffer size used for responding to RPCs */
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  public void setTracer(Tracer t) {
    this.tracer = t;
  }

  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() {
    responder.start();
    listener.start();
    if (auxiliaryListenerMap != null && auxiliaryListenerMap.size() > 0) {
      for (Listener newListener : auxiliaryListenerMap.values()) {
        newListener.start();
      }
    }

    handlers = new Handler[handlerCount];
    
    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    if (auxiliaryListenerMap != null && auxiliaryListenerMap.size() > 0) {
      for (Listener newListener : auxiliaryListenerMap.values()) {
        newListener.interrupt();
        newListener.doStop();
      }
    }
    responder.interrupt();
    notifyAll();
    this.rpcMetrics.shutdown();
    this.rpcDetailedMetrics.shutdown();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }

  /**
   * Return the set of all the configured auxiliary socket addresses NameNode
   * RPC is listening on. If there are none, or it is not configured at all, an
   * empty set is returned.
   * @return the set of all the auxiliary addresses on which the
   *         RPC server is listening on.
   */
  public synchronized Set<InetSocketAddress> getAuxiliaryListenerAddresses() {
    Set<InetSocketAddress> allAddrs = new HashSet<>();
    if (auxiliaryListenerMap != null && auxiliaryListenerMap.size() > 0) {
      for (Listener auxListener : auxiliaryListenerMap.values()) {
        allAddrs.add(auxListener.getAddress());
      }
    }
    return allAddrs;
  }
  
  /** 
   * Called for each call. 
   * @deprecated Use  {@link #call(RPC.RpcKind, String,
   *  Writable, long)} instead
   */
  @Deprecated
  public Writable call(Writable param, long receiveTime) throws Exception {
    return call(RPC.RpcKind.RPC_BUILTIN, null, param, receiveTime);
  }
  
  /** Called for each call. */
  public abstract Writable call(RPC.RpcKind rpcKind, String protocol,
      Writable param, long receiveTime) throws Exception;
  
  /**
   * Authorize the incoming client connection.
   * 
   * @param user client user
   * @param protocolName - the protocol
   * @param addr InetAddress of incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  private void authorize(UserGroupInformation user, String protocolName,
      InetAddress addr) throws AuthorizationException {
    if (authorize) {
      if (protocolName == null) {
        throw new AuthorizationException("Null protocol not authorized");
      }
      Class<?> protocol = null;
      try {
        protocol = getProtocolClass(protocolName, getConf());
      } catch (ClassNotFoundException cfne) {
        throw new AuthorizationException("Unknown protocol: " + 
                                         protocolName);
      }
      serviceAuthorizationManager.authorize(user, protocol, getConf(), addr);
    }
  }
  
  /**
   * Get the port on which the IPC Server is listening for incoming connections.
   * This could be an ephemeral port too, in which case we return the real
   * port on which the Server has bound.
   * @return port on which IPC Server is listening
   */
  public int getPort() {
    return port;
  }
  
  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  public int getNumOpenConnections() {
    return connectionManager.size();
  }

  /**
   * Get the NumOpenConnections/User.
   */
  public String getNumOpenConnectionsPerUser() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper
          .writeValueAsString(connectionManager.getUserToConnectionsMap());
    } catch (IOException ignored) {
    }
    return null;
  }

  /**
   * The number of RPC connections dropped due to
   * too many connections.
   * @return the number of dropped rpc connections
   */
  public long getNumDroppedConnections() {
    return connectionManager.getDroppedConnections();

  }

  /**
   * The number of rpc calls in the queue.
   * @return The number of rpc calls in the queue.
   */
  public int getCallQueueLen() {
    return callQueue.size();
  }

  public boolean isClientBackoffEnabled() {
    return callQueue.isClientBackoffEnabled();
  }

  public void setClientBackoffEnabled(boolean value) {
    callQueue.setClientBackoffEnabled(value);
  }

  /**
   * The maximum size of the rpc call queue of this server.
   * @return The maximum size of the rpc call queue.
   */
  public int getMaxQueueSize() {
    return maxQueueSize;
  }

  /**
   * The number of reader threads for this server.
   * @return The number of reader threads.
   */
  public int getNumReaders() {
    return readThreads;
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be 
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static int NIO_BUFFER_LIMIT = 8*1024; //should not be more than 64KB.
  
  /**
   * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large 
   * buffer.  
   *
   * @see WritableByteChannel#write(ByteBuffer)
   */
  private int channelWrite(WritableByteChannel channel, 
                           ByteBuffer buffer) throws IOException {
    
    int count =  (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                 channel.write(buffer) : channelIO(null, channel, buffer);
    if (count > 0) {
      rpcMetrics.incrSentBytes(count);
    }
    return count;
  }
  
  
  /**
   * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks. 
   * This is to avoid jdk from creating many direct buffers as the size of 
   * ByteBuffer increases. There should not be any performance degredation.
   * 
   * @see ReadableByteChannel#read(ByteBuffer)
   */
  private int channelRead(ReadableByteChannel channel, 
                          ByteBuffer buffer) throws IOException {
    
    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      rpcMetrics.incrReceivedBytes(count);
    }
    return count;
  }
  
  /**
   * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
   * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   * 
   * @see #channelRead(ReadableByteChannel, ByteBuffer)
   * @see #channelWrite(WritableByteChannel, ByteBuffer)
   */
  private static int channelIO(ReadableByteChannel readCh, 
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {
    
    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;
    
    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);
        
        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf); 
        
        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);        
      }
    }

    int nBytes = initialRemaining - buf.remaining(); 
    return (nBytes > 0) ? nBytes : ret;
  }
  
  private class ConnectionManager {

    // 现有Connection的数量
    final private AtomicInteger count = new AtomicInteger();

    final private AtomicLong droppedConnections = new AtomicLong();

    //现有的Connection连接.
    final private Set<Connection> connections;

    /* Map to maintain the statistics per User */
    final private Map<String, Integer> userToConnectionsMap;

    final private Object userToConnectionsMapLock = new Object();

    //Timer定时器, 定期检查/关闭 Connection
    final private Timer idleScanTimer;

    // 定义 空闲多久之后关闭 Connection 默认值: 4秒
    final private int idleScanThreshold;

    // 扫描间隔  默认 10秒
    final private int idleScanInterval;

    // 最大等待时间 默认值 20秒
    final private int maxIdleTime;

    // 定义一次断开连接的最大客户端数。 默认值 10
    final private int maxIdleToClose;

    // 定义最大连接数 默认值 0 , 无限制
    final private int maxConnections;
    
    ConnectionManager() {
      this.idleScanTimer = new Timer(
          "IPC Server idle connection scanner for port " + getPort(), true);
      this.idleScanThreshold = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
      this.idleScanInterval = conf.getInt(
          CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY,
          CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT);
      this.maxIdleTime = 2 * conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
      this.maxIdleToClose = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
      this.maxConnections = conf.getInt(
          CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_KEY,
          CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_DEFAULT);
      // create a set with concurrency -and- a thread-safe iterator, add 2
      // for listener and idle closer threads
      this.connections = Collections.newSetFromMap(
          new ConcurrentHashMap<Connection,Boolean>(
              maxQueueSize, 0.75f, readThreads+2));
      this.userToConnectionsMap = new ConcurrentHashMap<>();
    }

    private boolean add(Connection connection) {
      boolean added = connections.add(connection);
      if (added) {
        count.getAndIncrement();
      }
      return added;
    }
    
    private boolean remove(Connection connection) {
      boolean removed = connections.remove(connection);
      if (removed) {
        count.getAndDecrement();
      }
      return removed;
    }

    void incrUserConnections(String user) {
      synchronized (userToConnectionsMapLock) {
        Integer count = userToConnectionsMap.get(user);
        if (count == null) {
          count = 1;
        } else {
          count++;
        }
        userToConnectionsMap.put(user, count);
      }
    }

    void decrUserConnections(String user) {
      synchronized (userToConnectionsMapLock) {
        Integer count = userToConnectionsMap.get(user);
        if (count == null) {
          return;
        } else {
          count--;
        }
        if (count == 0) {
          userToConnectionsMap.remove(user);
        } else {
          userToConnectionsMap.put(user, count);
        }
      }
    }

    Map<String, Integer> getUserToConnectionsMap() {
      return userToConnectionsMap;
    }


    long getDroppedConnections() {
      return droppedConnections.get();
    }

    int size() {
      return count.get();
    }

    boolean isFull() {
      // The check is disabled when maxConnections <= 0.
      return ((maxConnections > 0) && (size() >= maxConnections));
    }

    Connection[] toArray() {
      return connections.toArray(new Connection[0]);
    }

    Connection register(SocketChannel channel, int ingressPort) {
      if (isFull()) {
        return null;
      }
      Connection connection = new Connection(channel, Time.now(), ingressPort);
      add(connection);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Server connection from " + connection +
            "; # active connections: " + size() +
            "; # queued calls: " + callQueue.size());
      }      
      return connection;
    }
    
    boolean close(Connection connection) {
      boolean exists = remove(connection);
      if (exists) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(Thread.currentThread().getName() +
              ": disconnecting client " + connection +
              ". Number of active connections: "+ size());
        }
        // only close if actually removed to avoid double-closing due
        // to possible races
        connection.close();
        // Remove authorized users only
        if (connection.user != null && connection.connectionContextRead) {
          decrUserConnections(connection.user.getShortUserName());
        }
      }
      return exists;
    }
    
    // synch'ed to avoid explicit invocation upon OOM from colliding with
    // timer task firing
    synchronized void closeIdle(boolean scanAll) {
      long minLastContact = Time.now() - maxIdleTime;
      // concurrent iterator might miss new connections added
      // during the iteration, but that's ok because they won't
      // be idle yet anyway and will be caught on next scan
      int closed = 0;
      for (Connection connection : connections) {
        // stop if connections dropped below threshold unless scanning all
        if (!scanAll && size() < idleScanThreshold) {
          break;
        }
        // stop if not scanning all and max connections are closed
        if (connection.isIdle() &&
            connection.getLastContact() < minLastContact &&
            close(connection) &&
            !scanAll && (++closed == maxIdleToClose)) {
          break;
        }
      }
    }
    
    void closeAll() {
      // use a copy of the connections to be absolutely sure the concurrent
      // iterator doesn't miss a connection
      for (Connection connection : toArray()) {
        close(connection);
      }
    }

    //创建线程,定时扫描connection, 关闭超时,无效的连接
    void startIdleScan() {
      scheduleIdleScanTask();
    }
    
    void stopIdleScan() {
      idleScanTimer.cancel();
    }
    
    private void scheduleIdleScanTask() {
      if (!running) {
        return;
      }
      //创建线程,定时扫描connection, 关闭超时,无效的连接
      TimerTask idleScanTask = new TimerTask(){
        @Override
        public void run() {
          if (!running) {
            return;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(Thread.currentThread().getName()+": task running");
          }
          try {
            closeIdle(false);
          } finally {
            // explicitly reschedule so next execution occurs relative
            // to the end of this scan, not the beginning
            scheduleIdleScanTask();
          }
        }
      };
      idleScanTimer.schedule(idleScanTask, idleScanInterval);
    }
  }

  protected int getMaxIdleTime() {
    return connectionManager.maxIdleTime;
  }

  public String getServerName() {
    return serverName;
  }
}
