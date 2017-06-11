package com.hsw.LearnThread.show;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class TomThread extends Thread {
    private Working working;

    public TomThread(Working working) {
        this.working = working;
    }

    @Override
    public void run() {
        working.tom();
    }
}
