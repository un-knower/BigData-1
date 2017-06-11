package com.hsw.LearnThread.Bank;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class DrawThread extends Thread {
    private Account account;
    private double drawBalance;

    public DrawThread(String name, Account account, double drawBalance) {
        super(name);
        this.account=account;
        this.drawBalance=drawBalance;
    }
    @Override
    public void run() {
        while (true) {
//            同步代码块,将共享访问的Account对象作为锁
            synchronized (account) {
                if (account.getBalance()<drawBalance) {
                    System.out.println(Thread.currentThread()+" : 余额不足,当前余额: "+account.getBalance());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                account.drawMoney(drawBalance);
                System.out.println(Thread.currentThread()+"取钱成功,取走金额: "+drawBalance+" ,当前余额: "+account.getBalance());
            }
        }
    }
}
