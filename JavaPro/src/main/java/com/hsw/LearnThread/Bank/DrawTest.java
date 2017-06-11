package com.hsw.LearnThread.Bank;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class DrawTest {
    public static void main(String[] args) {
        Account account = new Account(1000);
        new DrawThread("A账户: ",account,500).start();
        new DrawThread("B账户: ",account,300).start();
        new DrawThread("C账户: ",account,100).start();
    }
}
