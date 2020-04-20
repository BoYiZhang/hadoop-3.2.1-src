package org.apache.hadoop.rpc.protobuf;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
    public static final Logger LOG =
            LoggerFactory.getLogger(Client.class);


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        RPC.setProtocolEngine(conf, Server.MetaInfoProtocol.class,
                ProtobufRpcEngine.class);


        //1. 拿到RPC协议
        Server.MetaInfoProtocol proxy = RPC.getProxy(Server.MetaInfoProtocol.class, 1L,
                new InetSocketAddress("localhost", 7777), conf);

        //2. 发送请求
        CustomProtos.GetMetaInfoRequestProto obj =  CustomProtos.GetMetaInfoRequestProto.newBuilder().setPath("/meta").build();

        CustomProtos.GetMetaInfoResponseProto metaData = proxy.getMetaInfo(null, obj);

        //3. 打印元数据
        System.out.println(metaData.getInfo());



    }
}