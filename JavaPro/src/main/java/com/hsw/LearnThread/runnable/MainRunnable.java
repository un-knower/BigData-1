package com.hsw.LearnThread.runnable;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class MainRunnable {
    public static void main(String[] args) {
        Thread thread = new Thread(new BasicRunnable());
        thread.setName("Child Thread");
        thread.start();
        for (int i = 0; i < 10; i++) {
            System.out.println(Thread.currentThread()+" ---> "+i);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
