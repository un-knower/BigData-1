package com.hsw.LearnThread.show;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class JerryThread extends Thread {
    private Working working;

    public JerryThread(Working working) {
        this.working = working;
    }
    @Override
    public void run() {
        working.jerry();
    }
}
