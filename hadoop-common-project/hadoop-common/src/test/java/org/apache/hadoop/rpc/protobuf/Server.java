package org.apache.hadoop.rpc.protobuf;


import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.rpc.protobuf.CustomProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Server {

    public static final Logger LOG =
            LoggerFactory.getLogger(Server.class);


    // 接口信息 供server 和client 端使用
    @ProtocolInfo(
            protocolName = "org.apache.hadoop.rpc.CustomProtos$MetaInfoProtocol",
            protocolVersion = 1)
    public interface MetaInfoProtocol
            extends CustomProtos.MetaInfo.BlockingInterface {
    }

    // 实现类
    public static class MetaInfoServer
            implements MetaInfoProtocol {

        @Override
        public CustomProtos.GetMetaInfoResponseProto getMetaInfo(RpcController controller,
                CustomProtos.GetMetaInfoRequestProto request) throws
                ServiceException {

            //获取请求参数
            final String path = request.getPath();

            return CustomProtos.GetMetaInfoResponseProto.newBuilder().setInfo(path + ":3 - {BLOCK_1,BLOCK_2,BLOCK_3....").build();
        }
    }



    public static void main(String[] args) throws  Exception{
        Configuration conf = new Configuration();

        MetaInfoServer serverImpl =
                new MetaInfoServer();

        BlockingService blockingService =
                CustomProtos.MetaInfo.newReflectiveBlockingService(serverImpl);

        RPC.setProtocolEngine(conf, MetaInfoProtocol.class,
                ProtobufRpcEngine.class);


        //1. 构建RPC框架
        RPC.Builder builder = new RPC.Builder(conf);
        //2. 绑定地址
        builder.setBindAddress("localhost");
        //3. 绑定端口
        builder.setPort(7777);
        //4. 绑定协议
        builder.setProtocol(MetaInfoProtocol.class);
        //5. 调用协议实现类
        builder.setInstance(blockingService);
        //6. 创建服务
        RPC.Server server = builder.build();
        //7. 启动服务
        server.start();




    }






}
