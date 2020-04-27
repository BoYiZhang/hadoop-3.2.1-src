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

        System.out.println(System.currentTimeMillis());
        //1. 构建配置对象
        Configuration conf = new Configuration();

        //2. 设置协议的RpcEngine为ProtobufRpcEngine .
        RPC.setProtocolEngine(conf, Server.MetaInfoProtocol.class,
                ProtobufRpcEngine.class);


        //3. 拿到代理对象
        Server.MetaInfoProtocol proxy = RPC.getProxy(Server.MetaInfoProtocol.class, 1L,
                new InetSocketAddress("localhost", 7777), conf);

        //4. 构建发送请求对象
        CustomProtos.GetMetaInfoRequestProto obj =  CustomProtos.GetMetaInfoRequestProto.newBuilder().setPath("/meta").build();

        //5. 将请求对象传入, 获取响应信息
        CustomProtos.GetMetaInfoResponseProto metaData = proxy.getMetaInfo(null, obj);

        //6. 输出数据
        System.out.println(metaData.getInfo());

    }

}
