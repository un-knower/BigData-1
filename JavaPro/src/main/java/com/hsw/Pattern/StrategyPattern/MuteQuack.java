package com.hsw.Pattern.StrategyPattern;

/**
 * Created by HuShiwei on 2016/10/11 0011.
 */
// 不会叫
public class MuteQuack implements QuackBehavior {
    public void quack() {
        System.out.println("<< Slience >>");
    }
}
