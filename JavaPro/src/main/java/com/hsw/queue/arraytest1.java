package com.hsw.queue;

/**
 * Created by HuShiwei on 2016/12/8 0008.
 */
public class arraytest1 {
    public static void main(String[] args) {
        queuearray1 queuearray1 = new queuearray1(10);
        queuearray1.enqueue(2);
        queuearray1.enqueue("world");
        queuearray1.enqueue(4);
        queuearray1.enqueue(5);
        queuearray1.enqueue("hello");

        System.out.println(queuearray1.getSize());
        while (!queuearray1.isEmpty()) {
            System.out.println(queuearray1.dequeue());
        }

        System.out.println(queuearray1.isEmpty());
    }
}
