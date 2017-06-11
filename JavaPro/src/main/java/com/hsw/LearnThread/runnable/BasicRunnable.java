package com.hsw.LearnThread.runnable;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class BasicRunnable implements Runnable {
    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            System.out.println(Thread.currentThread().getName()+" ---> "+i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
