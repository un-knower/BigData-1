package com.hsw.queue;

/**
 * Created by HuShiwei on 2016/12/8 0008.
 */
public class queuearray1 implements Queue{
    private static final int CAP = 7;
    private int capacity;
    private Object[] elements;
    private int front;
    private int rear;

    public queuearray1(){
        this(7);
    }
    public queuearray1(int cap) {
        capacity = cap + 1;
        elements = new Object[capacity];
        front = 0;
        rear = 0;
    }

    @Override
    public int getSize() {
        return (rear-front+capacity)%capacity;
    }

    @Override
    public boolean isEmpty() {
        return rear==front;
    }

    @Override
    public void enqueue(Object e) {
        if (getSize()==(capacity-1)) {
            expanseSpace();
        }
        elements[rear] = e;
        rear = (rear + 1) % capacity;
    }

    private void expanseSpace() {
        Object[] objects = new Object[capacity * 2];
        int i = front;
        int j = 0;
        while (i!=rear) {
            objects[j++] = elements[i];
            i = (i + 1) % capacity;
        }
        elements = objects;
        front = 0;
        rear = elements.length;

    }
    @Override
    public Object dequeue() {
        if (isEmpty()) {
            try {
                throw new Exception("error,the queue is enough");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Object element = elements[front];
        front = (front + 1) % capacity;
        return element;
    }

    @Override
    public Object peek() {
        if (isEmpty()) {
            try {
                throw new Exception("error,the queue is enough");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return elements[front];
    }
}
