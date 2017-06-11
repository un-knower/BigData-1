package com.hsw.LearnThread.ERROR;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class Bank {
    private double money;

    public Bank(double money) {
        this.money = money;
    }

    public double drawMoney(double drawMoney) {
        money = money - drawMoney;
        return money;
    }

    public double getMoney() {
        return money;
    }
}
