package cn.Algorithm.list;


import cn.Algorithm.stragegy.Strategy;
import cn.Algorithm.exception.OutOfBoundaryException;
import cn.Algorithm.stragegy.DefaultStrategy;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 * 线性表的单链表实现
 */
public class ListSLinked implements List{

    private Strategy strategy;//数据元素比较策略
    private SLNode head;// 单链表首结点引用
    private int size;// 线性表中数据元素的个数

    public ListSLinked() {
        this(new DefaultStrategy());
    }

    public ListSLinked(Strategy strategy) {
        this.strategy = strategy;
        head = new SLNode();
        size = 0;
    }

    private SLNode getPreNode(Object e) {
        SLNode p = head;
        while (p.getNext() != null) {
            // TODO: 2016/10/17 0017
        }
        return null;
    }
    public int getSize() {
        return 0;
    }

    public boolean isEmpty() {
        return false;
    }

    public boolean contains(Object e) {
        return false;
    }

    public int indexOf(Object e) {
        return 0;
    }

    public void insert(int i, Object e) throws OutOfBoundaryException {

    }

    public boolean insertBefore(Object obj, Object e) {
        return false;
    }

    public boolean insertAfter(Object obj, Object e) {
        return false;
    }

    public Object remove(int i) throws OutOfBoundaryException {
        return null;
    }

    public boolean remove(Object e) {
        return false;
    }

    public Object replace(int i, Object e) throws OutOfBoundaryException {
        return null;
    }

    public Object get(int i) throws OutOfBoundaryException {
        return null;
    }
}
