package org.apache.hadoop.rpc.writable;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * 访问RPC服务
 */
public class Client {
  public static void main(String[] args) throws IOException {
    //1. 拿到RPC协议
    ClicentNameNodeProtocol proxy = RPC.getProxy(ClicentNameNodeProtocol.class, 1L,
        new InetSocketAddress("localhost", 7777), new Configuration());
    //2. 发送请求
    String metaData = proxy.getMetaData("/meta");
    //3. 打印元数据
    System.out.println(metaData);
  }
}
