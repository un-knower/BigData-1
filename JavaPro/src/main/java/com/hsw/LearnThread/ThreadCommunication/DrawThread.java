package com.hsw.LearnThread.ThreadCommunication;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class DrawThread extends Thread {
    private Bank bank;
    private double drawMoney;

    public DrawThread(String threadName, Bank bank, double drawMoney) {
        super(threadName);
        this.bank = bank;
        this.drawMoney = drawMoney;
    }
    @Override
    public void run() {
        while (true) {
            synchronized (bank) {
                if (!bank.flag) {
                    try {
                        bank.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (bank.getMoney() < drawMoney) {
                    System.out.println("余额不足,当前余额是: " + bank.getMoney());
                    break;
                }
                bank.drawMoney(drawMoney);
                System.out.println(Thread.currentThread().getName()+" 取款成功,取款金额是: "+drawMoney+",账户余额: "+bank.getMoney());
                bank.flag = false;
//                bank.notifyAll();
                bank.notify();
            }
        }
    }
}
