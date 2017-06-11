package cn.Algorithm.stack;


import cn.Algorithm.list.SLNode;
import cn.Algorithm.exception.StackEmptyException;

/**
 * Created by HuShiwei on 2016/10/18 0018.
 * Stack的链式存储结构
 */
public class StackSLinked implements Stack {
    private SLNode top;//栈表首结点引用
    private int size;//栈的大小

    public StackSLinked() {
        top = null;
        size = 0;
    }

    //    返回堆栈的大小
    public int getSize() {
        return size;
    }

    //    判断堆栈是否为空
    public boolean isEmpty() {
        return size == 0;
    }

    //    数据元素e入栈
    public void push(Object e) {
        SLNode slNode = new SLNode(e, top);
        top = slNode;
        size++;

    }

    //    栈顶元素出栈
    public Object pop() throws StackEmptyException {
        if (size < 1) {
            throw new StackEmptyException("错误,堆栈为空. ");
        }
        Object data = top.getData();
        top = top.getNext();
        size--;
        return data;
    }

    public Object peek() throws StackEmptyException {
        if (size < 1) {
            throw new StackEmptyException("错误,堆栈为空. ");
        }
        return top.getData();
    }
}
