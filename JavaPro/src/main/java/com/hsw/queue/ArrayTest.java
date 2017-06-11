package com.hsw.queue;

/**
 * Created by HuShiwei on 2016/12/8 0008.
 */
public class ArrayTest {
    public static void main(String[] args) {
        QueueArray queueArray = new QueueArray(20);
        queueArray.enqueue(1);
        queueArray.enqueue(2);
        queueArray.enqueue(3);
        queueArray.enqueue(4);
        queueArray.enqueue(5);


        System.out.println(queueArray.getSize());
//        Object dequeue = queueArray.dequeue();
//        System.out.println(dequeue);
//        System.out.println(dequeue);
        for (int i = 0; i < 5; i++) {
            Object dequeue = queueArray.dequeue();
            System.out.println(dequeue);
        }
        System.out.println(queueArray.isEmpty());
    }
}
