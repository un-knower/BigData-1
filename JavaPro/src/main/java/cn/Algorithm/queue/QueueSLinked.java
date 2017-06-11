package cn.Algorithm.queue;


import cn.Algorithm.exception.QueueEmptyException;
import cn.Algorithm.list.SLNode;

/**
 * Created by HuShiwei on 2016/10/18 0018.
 * 队列的链式存储实现
 * 队首指针:指向队首元素的前一个节点,即始终指向链表空的头节点.
 * 队尾指针:指向队列当前队尾元素所在的节点.
 * 当队列为空时,队首指针与队尾指针均指向空的头节点.
 */
public class QueueSLinked implements Queue {
    private SLNode front;
    private SLNode rear;
    private int size;

    public QueueSLinked() {
        front = new SLNode();
        rear = front;
        size = 0;
    }

//    返回队列的大小
    public int getSize() {
        return size;
    }

//    判断队列是否为空
    public boolean isEmpty() {
        return size==0;
    }

//    数据元素e入队
    public void enqueue(Object e) {
        SLNode slNode = new SLNode(e, null);
        rear.setNext(slNode);
        rear = slNode;
        size++;
    }

//    队首元素出队
    public Object dequeue() throws QueueEmptyException {
        if (size < 1) {
            throw new QueueEmptyException("错误,队列为空");
        }
        SLNode p = front.getNext();
        front.setNext(p.getNext());
        size--;
        if (size<1) {
            rear = front;
        }
        return p.getData();
    }

//    取队首元素
    public Object peek() throws QueueEmptyException {
        if (size < 1) {
            throw new QueueEmptyException("错误,队列为空");
        }
        return front.getNext().getData();
    }
}
