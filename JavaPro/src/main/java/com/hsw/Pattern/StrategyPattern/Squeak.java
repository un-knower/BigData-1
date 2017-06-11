package com.hsw.Pattern.StrategyPattern;

/**
 * Created by HuShiwei on 2016/10/11 0011.
 */

//吱吱的叫声
public class Squeak implements QuackBehavior {
    public void quack() {
        System.out.println("Squeak");
    }
}
