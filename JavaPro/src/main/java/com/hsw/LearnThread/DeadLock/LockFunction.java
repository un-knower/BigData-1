package com.hsw.LearnThread.DeadLock;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class LockFunction {
    public static void main(String[] args) {
        Thread1 thread1 = new Thread1();
        Thread2 thread2 = new Thread2();
        thread1.start();
        thread2.start();
    }
}
