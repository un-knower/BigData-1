package com.hsw.Pattern.ObserverPattern;

/**
 * Created by HuShiwei on 2016/10/12 0012.
 * Observer接口的 update 方法.主题数据改变的时候,通过调用它,发送数据到观察者
 */
public interface Observer {
    public void update(float temp, float humidity, float pressure);
}
