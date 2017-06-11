package com.hsw.LearnThread.ThreadCommunication;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */

/**
 * 从单消费者到多消费者,只需要把notify修改成notifyAll即可
 */
public class BankTest {
    public static void main(String[] args) {
        Bank bank = new Bank(5000);
        new SaveThread("存钱线程1",bank,400).start();
//        new SaveThread("存钱线程2",bank,400).start();
        new DrawThread("取钱线程1",bank,500).start();
//        new DrawThread("取钱线程2",bank,500).start();
//        new DrawThread("取钱线程2",bank,500).start();
    }
}
