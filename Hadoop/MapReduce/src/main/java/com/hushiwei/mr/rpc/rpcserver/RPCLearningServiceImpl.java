package com.hushiwei.mr.rpc.rpcserver;

/**
 * Created by hushiwei on 2018/3/28.
 * desc :Hadoop RPC协议通常是一个Java接口，定义了server端对外提供的服务接口，需要在server端进行实现
 *
 * server端的协议实现中不需要关注Socket通信
 *
 */
public class RPCLearningServiceImpl implements RPCLearningServiceInterface {
    @Override
    public String loging(String name) {
        System.out.println("Exec Login function ...");
        return "Hello, " + name + ". Welcome you!";
    }

    @Override
    public String multip(int a, int b) {
        System.out.println("Prepare to exec multip function in server ....");
        return a + " * " + b + " = " + a * b;
    }
}
