package com.hsw.LearnThread.thread;

import com.hsw.LearnThread.runnable.LiftOff;

/**
 * Created by HuShiwei on 2016/12/6 0006.
 */
public class BasicThreads {
    public static void main(String[] args) {
        runThread(5);
    }

    public static void runThread(int num) {
        for (int i = 0; i < num; i++) {
            new Thread(new LiftOff()).start();
//            new LiftOff().run();
        }
        System.out.println("Waiting for LiftOff");
    }
}
