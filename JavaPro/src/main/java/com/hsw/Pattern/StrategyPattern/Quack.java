package com.hsw.Pattern.StrategyPattern;

/**
 * Created by HuShiwei on 2016/10/11 0011.
 */
//呱呱叫
public class Quack implements QuackBehavior {
    public void quack() {
        System.out.println("Quack");
    }
}
