package com.hsw.LearnThread.thread;

import com.hsw.LearnThread.runnable.BasicRunnable;

/**
 * Created by HuShiwei on 2016/12/6 0006.
 */
public class MainFunction {
    public static void main(String[] args) {
        Thread thread = new Thread(new BasicRunnable());
        thread.start();
        System.out.println(thread.getState());
    }
}
