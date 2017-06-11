package com.hsw.LearnThread.show;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class SynFunction {
    public static void main(String[] args) {
        Working working = new Working();
        TomThread tomThread = new TomThread(working);
        JerryThread jerryThread = new JerryThread(working);
        tomThread.start();
        jerryThread.start();
    }
}
