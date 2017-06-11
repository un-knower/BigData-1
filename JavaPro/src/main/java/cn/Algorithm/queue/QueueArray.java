package cn.Algorithm.queue;


import cn.Algorithm.exception.QueueEmptyException;

/**
 * Created by HuShiwei on 2016/10/18 0018.
 * 队列的顺序存储实现
 * 底层实现使用:循环数组
 */
public class QueueArray implements Queue {
    private static final int CAP = 7;//队列默认大小
    private Object[] elements;//数据元素数组
    private int capacity;//数组的大小elements.length
    private int front;//队首指针,指向队首
    private int rear;//队尾指针,指向队尾后一个位置

    public QueueArray() {
        this(CAP);
    }

    public QueueArray(int cap) {
        capacity = cap + 1;
        elements = new Object[capacity];
        front = rear = 0;
    }

    //    返回队列的大小
    public int getSize() {
        return (rear - front + capacity) % capacity;
    }

    //    判断队列是否为空
    public boolean isEmpty() {
        return front == rear;
    }

    public void enqueue(Object e) {
        if (getSize() == capacity - 1) {
            expandSpace();
        }
        elements[rear] = e;
        rear = (rear + 1) % capacity;

    }

    private void expandSpace() {
        Object[] objects = new Object[capacity * 2];
        int i = front;
        int j = 0;
//        将front开始到rear前一个存储单元的元素复制到新数组
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

    public Object dequeue() throws QueueEmptyException {
        if (isEmpty()) {
            throw new QueueEmptyException("错误,队列为空");
        }
        Object data = elements[front];
        elements[front] = null;
        front = (front + 1) % capacity;
        return data;
    }

    public Object peek() throws QueueEmptyException {
        if (isEmpty()) {
            throw new QueueEmptyException("错误,队列为空");
        }
        return elements[front];
    }
}
