package com.hsw.LearnThread.One2One;

/**
 * Created by HuShiwei on 2016/12/8 0008.
 */
public class Test {
    public static void main(String[] args) {
        Resource resource = new Resource(0);
        new Thread(new Producer(resource)).start();
        new Thread(new Producer(resource)).start();
        new Thread(new Consumer(resource)).start();
        new Thread(new Consumer(resource)).start();
    }
}
