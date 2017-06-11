package com.hushiwei.thrift.demo;

import org.apache.thrift.TException;

/**
 * Created by HuShiwei on 2016/8/20 0020.
 */

/**
 * 实现接口Iface
 */
public class HelloWorldImpl implements HelloWorldService.Iface {
    public HelloWorldImpl() {

    }
    @Override
    public String sayHello(String username) throws TException {
        return "Hi,"+username+" welcome to my world";
    }
}
