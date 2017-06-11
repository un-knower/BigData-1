package com.hsw.Pattern.StrategyPattern;

/**
 * Created by HuShiwei on 2016/10/11 0011.
 */
public class MallardDuck extends Duck {
    public MallardDuck() {
        quackBehavior = new Quack();
        flyBehavior = new FlyWithWings();
    }
    public void display() {
        System.out.println("I`m a real Mallard duck");
    }
}
