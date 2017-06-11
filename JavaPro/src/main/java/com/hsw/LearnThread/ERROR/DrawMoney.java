package com.hsw.LearnThread.ERROR;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class DrawMoney extends Thread {
    private Bank bank;
    private double drawMoney;

    public DrawMoney(String name, Bank bank, double drawMoney) {
        super(name);
        this.bank = bank;
        this.drawMoney = drawMoney;
    }

    @Override
    public void run() {
        while (true) {
                if (bank.getMoney() < drawMoney) {
                    System.out.println("余额不足...");
                    break;
                }
                bank.drawMoney(drawMoney);

                System.out.println(Thread.currentThread().getName() + " ---> 取钱成功: " + drawMoney + ",账户余额: " + bank.getMoney());

        }

    }
}
