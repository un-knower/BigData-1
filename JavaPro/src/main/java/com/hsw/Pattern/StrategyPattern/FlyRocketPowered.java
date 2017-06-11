package com.hsw.Pattern.StrategyPattern;

/**
 * Created by HuShiwei on 2016/10/11 0011.
 */
public class FlyRocketPowered implements FlyBehavior {
    public void fly() {
        System.out.println("I`m flying with a rocket!");
    }
}
