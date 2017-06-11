package cn.Algorithm.list;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 * 双向链表的简单实现
 * java中对象的引用也就是指针的指向
 */
public class DLNode implements Node {
    private Object element;
    private DLNode pre;
    private DLNode next;

    public DLNode() {
        this(null,null,null);
    }
    public DLNode(Object ele, DLNode pre, DLNode next){
        this.element = ele;
        this.pre = pre;
        this.next = next;
    }

    public DLNode getNext(){
        return next;
    }
    public void setNext(DLNode next){
        this.next = next;
    }
    public DLNode getPre(){
        return pre;
    }
    public void setPre(DLNode pre){
        this.pre = pre;
    }
    /****************Node Interface Method**************/
    public Object getData() {
        return element;
    }

    public void setData(Object obj) {
        element = obj;
    }
}
