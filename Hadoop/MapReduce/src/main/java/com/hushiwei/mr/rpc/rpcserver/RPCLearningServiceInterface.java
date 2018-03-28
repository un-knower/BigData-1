package com.hushiwei.mr.rpc.rpcserver;

/**
 * Created by hushiwei on 2018/3/28.
 * desc : RPC协议是client端和server端之间的通信接口，它定义了server端对外提供的服务接口。
 */
public interface RPCLearningServiceInterface {

    // 协议版本号,不同版本号的client和sever之间不能相互通信
    static final long versionID = 1L;

    // 登录方法
    String loging(String name);

    // 乘法
    String multip(int a, int b);
}
