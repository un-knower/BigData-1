package com.hsw.LearnThread.DeadLock;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class Thread2 extends Thread {
    @Override
    public void run() {
        synchronized (String.class) {
            System.out.println(Thread.currentThread().getName()+"...外层锁");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            synchronized (Object.class) {
                System.out.println("haha,我也锁主了");

            }
        }
    }
}
