package com.hushiwei.mr.rpc.rpcclient;

import com.hushiwei.mr.rpc.rpcserver.RPCLearningServiceInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by hushiwei on 2018/3/28.
 * desc : ClientController is a RPC client
 */
public class ClientController {
    public static void main(String[] args) throws IOException {

        // 通过rpc拿到loginServiceinterface的代理对象
        RPCLearningServiceInterface service = RPC.getProxy(RPCLearningServiceInterface.class, 1L, new InetSocketAddress("U006", 45666), new Configuration());

        // 像本地调用一样,就可以调用服务端的方法
        String result = service.loging("Hushiwei");
        System.out.println("rpc client result: " + result);

        String res = service.multip(6, 8);
        System.out.println(res);


        // 关闭连接
        RPC.stopProxy(service);
    }
}
