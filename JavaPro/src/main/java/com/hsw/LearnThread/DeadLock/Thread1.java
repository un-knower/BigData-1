package com.hsw.LearnThread.DeadLock;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class Thread1 extends Thread {
    @Override
    public void run() {
        synchronized (Object.class) {
            System.out.println(Thread.currentThread().getName()+"...外层锁");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            synchronized (String.class) {
                System.out.println("haha,锁主了");

            }
        }
    }
}
