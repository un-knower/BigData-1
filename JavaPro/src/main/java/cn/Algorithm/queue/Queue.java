package cn.Algorithm.queue;


import cn.Algorithm.exception.QueueEmptyException;

/**
 * Created by HuShiwei on 2016/10/18 0018.
 * FIFO先进先出
 */
public interface Queue {
    public int getSize();//返回队列的大小

    public boolean isEmpty();//判断队列是否为空

    public void enqueue(Object e);//数据元素e入队

    public Object dequeue() throws QueueEmptyException;//队首元素出队

    public Object peek() throws QueueEmptyException;//取队首元素
}
