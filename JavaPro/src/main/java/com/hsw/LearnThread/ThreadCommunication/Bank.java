package com.hsw.LearnThread.ThreadCommunication;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class Bank {
    private double money;
    boolean flag = false;

    public Bank(double money) {
        this.money = money;
    }

    /**
     * 取钱方法
     * @param drawMoney
     * @return
     */
    public double drawMoney(double drawMoney) {
        money = money - drawMoney;
        return money;
    }

    /**
     * 存钱
     * @param saveMoney
     * @return
     */
    public double saveMoney(double saveMoney) {
        money = money + saveMoney;
        return money;
    }

    /**
     * 查询余额
     * @return
     */
    public double getMoney() {
        return money;
    }
}
