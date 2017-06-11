package com.hsw.LearnThread.Bank;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class Account {
    private double balance;//账户余额

    public Account(double balance) {
        this.balance = balance;
    }

    /**
     * 取钱
     * @param drawBalance
     * @return
     */
    public double drawMoney(double drawBalance) {
        balance = balance - drawBalance;
        return balance;
    }

    /**
     * 查询余额
     * @return
     */
    public double getBalance() {
        return balance;
    }
}
