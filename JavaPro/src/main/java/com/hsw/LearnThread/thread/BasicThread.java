package com.hsw.LearnThread.thread;

/**
 * Created by HuShiwei on 2016/12/6 0006.
 */
public class BasicThread extends Thread {
    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            System.out.println("Thread ID: "+i+" --- "+Thread.currentThread().getName());
        }
    }
}
