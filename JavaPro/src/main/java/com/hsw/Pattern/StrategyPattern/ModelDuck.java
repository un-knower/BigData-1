package com.hsw.Pattern.StrategyPattern;

/**
 * Created by HuShiwei on 2016/10/11 0011.
 */
public class ModelDuck extends Duck {
    public ModelDuck() {
        flyBehavior = new FlyNoWay();
        quackBehavior = new Quack();
    }
    public void display() {
        System.out.println("I`m a model duck");
    }
}
