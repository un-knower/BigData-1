package com.hsw.LearnThread.One2One;

/**
 * Created by HuShiwei on 2016/12/8 0008.
 */
public class Resource {
    private int num = 0;
    boolean flag = false;

    public Resource(int num) {
        this.num = num;
    }

//    生产资源
    public synchronized void createNum() {
//        flag为true表示有资源。那么生产者就不能生产了，生产者线程就开始wait等待
        while (flag) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        如果到了这里，表示：生产者已经被唤醒了，那么意味着资源已经被消费了，生产者要开始生产了
        num++;
        System.out.println(Thread.currentThread().getName()+" 生产者线程: ---> "+num);
//        生产者生产完资源后，把flag标记为true.表示有资源了
        flag = true;
//        有资源后，生产者去唤醒消费者去消费资源
//        notify();
        notifyAll();
    }

    //    消费资源
    public synchronized void destory() {
//        因为flag为true时候有资源，那么！flag表示没用资源了，因此消费者线程就需要等待了，等待生产者线程去生产资源
        while (!flag) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        到了这里，说明消费者线程被生产者线程唤醒了，那么也意味着有资源了
        System.out.println(Thread.currentThread().getName()+" 消费者线程: -----------> "+num);
//        消费了数据，那么消费者就得把flag标记为false，表示没用数据了，提醒生产者得去生产数据了
        flag = false;
//        接下来就得去唤醒生产者去生产数据了
//        notify();
        notifyAll();
    }
}
