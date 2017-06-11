package com.hsw.Pattern.ObserverPattern;

/**
 * Created by HuShiwei on 2016/10/12 0012.
 * 实现Observer接口,成为一个观察者
 * CurrentCoditionsDisplay在构造方法中注册成为一个主题的观察者
 */
public class JustDisplay implements Observer ,DisplayElement {
    private float tempetature;
    private float humidity;
    private Subject weatherData;

    public JustDisplay(Subject weatherData) {
        this.weatherData = weatherData;
        weatherData.registerObserver(this);

    }
    public void display() {
        System.out.println("温度: " + tempetature + " 湿度: " + humidity + "% humidity");
    }

    public void update(float temp, float humidity, float pressure) {
        this.tempetature = temp;
        this.humidity = humidity;
        display();
    }
}
