package com.hsw.queue;

/**
 * Created by HuShiwei on 2016/12/8 0008.
 */
public interface Queue {
    public int getSize();

    public boolean isEmpty();

    public void enqueue(Object e);

    public Object dequeue();

    public Object peek();
}
