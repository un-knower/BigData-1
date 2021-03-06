package com.hsw.LearnThread.One2One;

/**
 * Created by HuShiwei on 2016/12/8 0008.
 */
public class Consumer implements Runnable {
    private Resource resource;

    public Consumer(Resource resource) {
        this.resource = resource;
    }
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            resource.destory();
        }
    }
}
