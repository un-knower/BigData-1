package com.hsw.LearnThread.ThreadCommunication;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class SaveThread extends Thread {
    private Bank bank;
    private double saveMoney;

    public SaveThread(String threadName, Bank bank, double saveMoney) {
        super(threadName);
        this.bank=bank;
        this.saveMoney = saveMoney;
    }

    @Override
    public void run() {
        while (true) {
            synchronized (bank) {
                if (bank.flag) {
                    try {
                        bank.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                bank.saveMoney(saveMoney);
                System.out.println(Thread.currentThread().getName() + " 存款成功,存款金额: " + saveMoney + ",账户余额:" + bank.getMoney());
                bank.flag = true;
                bank.notify();
//                bank.notifyAll();
            }
        }
    }
}
