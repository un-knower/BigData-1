package com.hsw.LearnThread.ERROR;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class BankTest {
    public static void main(String[] args) {
        Bank bank = new Bank(1000);
        new DrawMoney("取钱线程1", bank, 200).start();
        new DrawMoney("取钱线程2", bank, 200).start();
        new DrawMoney("取钱线程3", bank, 200).start();
    }
}
