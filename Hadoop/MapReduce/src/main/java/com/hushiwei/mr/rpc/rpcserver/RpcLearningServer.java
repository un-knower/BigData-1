package com.hushiwei.mr.rpc.rpcserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by hushiwei on 2018/3/28.
 * desc :
 * <p>
 * java -classpath MapReduce-1.0-SNAPSHOT-jar-with-dependencies.jar com.hushiwei.mr.rpc.rpcserver.RpcLearningServer
 */
public class RpcLearningServer {
    public static void main(String[] args) throws IOException {

        // 创建RPC.Builder实例builder,用于构造RPC server
        RPC.Builder builder = new RPC.Builder(new Configuration());

        // 向builder传递一些必要的参数,如主机,端口号,真实业务逻辑实例,协议接口
        builder.setBindAddress("U006").setPort(45666)
                .setInstance(new RPCLearningServiceImpl())
                .setProtocol(RPCLearningServiceInterface.class);

        // 构造rpc server
        RPC.Server server = builder.build();

        // 启动server
        server.start();

    }
}
