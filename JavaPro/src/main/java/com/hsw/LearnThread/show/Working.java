package com.hsw.LearnThread.show;

/**
 * Created by HuShiwei on 2016/12/7 0007.
 */
public class Working {
    public synchronized void tom() {
        System.out.println("tom in..."+"\n"+"tom say: Hello Jerry ");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("tom out...");
    }

    public synchronized void jerry() {
        System.out.println("jerry in..."+"\n"+"jerry say: Hello Tom");
        System.out.println("jerry out...");

    }
}
