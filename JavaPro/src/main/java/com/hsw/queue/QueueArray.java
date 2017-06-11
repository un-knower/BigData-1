package com.hsw.queue;

/**
 * Created by HuShiwei on 2016/12/8 0008.
 */

/**
 * 队列的顺序存储实现
 */
public class QueueArray implements Queue {
    private static final int CAP = 7;//队列默认大小
    private Object[] elements;//数据元素数组
    private int capacity;//数组的大小
    private int front;//队首指针
    private int rear;//队尾指针

    public QueueArray() {
        this(CAP);
    }
//    初始化队列
    public QueueArray(int cap){
        capacity = cap + 1;
        elements = new Object[capacity];
        front = rear = 0;
    }
//    返回队列的大小
    @Override
    public int getSize() {
        return (rear-front+capacity)%capacity;
    }
//    判断队列是否为空
    @Override
    public boolean isEmpty() {
        return front==rear;
    }

//    数据元素e入队
    @Override
    public void enqueue(Object e) {
        if (getSize()==capacity-1) {
            expandSpace();
        }
        elements[rear] = e;
        rear = (rear + 1) % capacity;
    }

//    扩充队列大小
    private void expandSpace() {
        Object[] objects = new Object[elements.length * 2];
        int i=front;
        int j = 0;
        while (i != rear) {
            objects[j++] = elements[i];
            i = (i + 1) % capacity;
        }
        elements = objects;
        capacity = elements.length;
//        设置新的队首队尾指针
        front = 0;
        rear = j;

    }

//    队首元素出队
    @Override
    public Object dequeue() {
        if (isEmpty()) {
            try {
                throw new Exception("错误: 队列空了...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Object element = elements[front];
        elements[front] = null;
        front = (front + 1) % capacity;
        return element;
    }

//    取队首元素
    @Override
    public Object peek() {
        if (isEmpty()) {
            try {
                throw new Exception("错误: 队列空了...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return elements[front];
    }
}
