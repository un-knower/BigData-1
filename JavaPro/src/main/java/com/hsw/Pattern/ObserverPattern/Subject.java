package com.hsw.Pattern.ObserverPattern;

/**
 * Created by HuShiwei on 2016/10/12 0012.
 * 主题接口:
 * 注册成为这个主题的观察者
 * 从这个主题的观察者中退出
 * 主题改变的时候,通着所有的观察者
 */
public interface Subject {
    public void registerObserver(Observer o);
    public void removeObservers(Observer o);
//    当主题状态改变时,这个方法会被调用,以通知所有的观察者
    public void notifyObservers();

}
