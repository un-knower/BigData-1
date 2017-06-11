package cn.Algorithm.stack;


import cn.Algorithm.exception.StackEmptyException;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 * FILO先进后出
 */
public interface Stack {
//    返回堆栈的大小
    public int getSize();
//    判断堆栈是否为空
    public boolean isEmpty();
//    数据元素e入栈
    public void push(Object e);
//    栈顶元素出栈
    public Object pop() throws StackEmptyException;
//    取栈顶元素,但不出栈
    public Object peek() throws StackEmptyException;
}
