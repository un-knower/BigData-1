package cn.Algorithm.tree;


import cn.Algorithm.list.Iterator;
import cn.Algorithm.list.LinkedList;
import cn.Algorithm.list.LinkedListDLNode;
import cn.Algorithm.list.Node;

/**
 * Created by HuShiwei on 2016/10/19 0019.
 * 二叉树存储结构节点定义
 */
public class BinTreeNode implements Node {
    private Object data;//数据域
    private BinTreeNode parent;//父节点
    private BinTreeNode lChild;//左孩子
    private BinTreeNode rChild;//右孩子
    private int height;//以该节点为根的子数的高度
    private int size;//该节点子孙数(包括节点本身)

    public BinTreeNode() {
        this(null);
    }

    public BinTreeNode(Object e) {
        data = e;
        height = 0;
        size = 1;
        parent = lChild = rChild = null;
    }
    public Object getData() {
        return data;
    }

    public void setData(Object obj) {
        data = obj;
    }





    /******
     * 辅助方法,判断当前节点位置情况
     ******/

//    判断是否有父亲
    public boolean hasParent() {
        return parent != null;
    }

    //    判断是否有左孩子
    public boolean hasLChild() {
        return lChild != null;
    }

    //    判断是否有右孩子
    public boolean hasRChild() {
        return rChild != null;
    }

    //    判断是否为叶子节点
    public boolean isLeaf() {
        return !hasLChild() && !hasRChild();
    }

    //    判断是否为某节点的左孩子
    public boolean isLChild() {
        return (hasParent() && this == parent.lChild);
    }

    //    判断是否为某节点的右孩子
    public boolean isRChild() {
        return (hasParent() && this == parent.rChild);
    }

    /**
     * 与高度相关的方法
     */

//    取节点的高度,即以该节点为根的树的高度
    public int getHeight() {
        return height;
    }

    public void updateHeight() {
        int newH = 0;//新高度初始化为0,高度等于左右子树高度加1中的大者
        if (hasLChild()) {
            newH = Math.max(newH, getLChild().getHeight() + 1);
        }
        if (hasRChild()) {
            newH = Math.max(newH, getRChild().getHeight() + 1);
        }
//        高度没有发生变化则直接返回
        if (newH == height) {
            return;
        }
//        否则更新高度
        height = newH;
//        递归更新祖先的高度
        if (hasParent()) {
            getParent().updateHeight();
        }
    }

    /**
     * 与size相关的方法
     */
//    取以该节点为根的树的节点数
    public int getSize() {
        return size;
    }

    public void updateSize() {
        size = 1;//初始化为1,节点本身
        if (hasLChild()) {
            size += getLChild().getSize();//加上左子树规模
        }
        if (hasRChild()) {
            size += getRChild().getSize();//加上右子树规模
        }
        if (hasParent()) {
            getParent().updateSize();//递归更新祖先的规模
        }
    }

    /**
     * 与父亲相关的方法
     */
//    取父节点
    public BinTreeNode getParent() {
        return parent;
    }

    public void setParent(BinTreeNode parent) {
        this.parent = parent;
    }

    //    断开与父亲的关系
    public void server() {
        if (!hasParent()) {
            return;
        }
        if (isLChild()) {
            parent.lChild = null;
        } else {
            parent.rChild = null;
        }
        parent.updateSize();//更新父节点及其祖先高度
        parent.updateHeight();//更新父节点及其祖先规模
        parent = null;
    }

    /**
     * 与LChild相关的方法
     */
//    取左孩子
    public BinTreeNode getLChild() {
        return lChild;
    }

//    设置当前节点的左孩子,返回原左孩子
    public BinTreeNode setLChild(BinTreeNode lc) {
        BinTreeNode oldLC = this.lChild;
        if (hasLChild()) {
            lChild.server();//断开当前左孩子与节点的关系
        }
        if (lc != null) {
            lc.server();//断开lc与其父节点的关系
            this.lChild = lc;//确定父子关系
            lc.parent = this;
            this.updateHeight();//更新父节点及其祖先高度
            this.updateSize();//更新父节点及其祖先规模
        }
        return oldLC;//返回原右孩子
    }

    /**
     * 与RChild相关的方法
     */
//    取右孩子
    public BinTreeNode getRChild() {
        return rChild;
    }

//    设置当前节点的右孩子,返回原右孩子
    public BinTreeNode setRChild(BinTreeNode rc) {
        BinTreeNode oldRC = this.rChild;
        if (hasParent()) {
            rChild.server();//断开当前右孩子与节点的关系
        }
        if (rc != null) {
            rc.server();//断开rc与其父节点的关系
            this.rChild = rc;//确定父子关系
            rc.parent = this;
            this.updateHeight();//更新父节点及其祖先高度
            this.updateSize();//更新父节点及其祖先规模
        }
        return oldRC;//返回原右孩子
    }

    //    先序遍历二叉树
    public Iterator preOrder() {
        LinkedListDLNode list = new LinkedListDLNode();
//        preOrderRecursion(this.root,list);
        return list.elements();

    }

    //    先序遍历的递归算法
    private void preOrderRecursion(BinTreeNode rt, LinkedList list) {
        if(rt==null) return;          //递归基,空树直接返回
        list.insertLast(rt);          //访问根结点
        preOrderRecursion(rt.getLChild(),list);  //遍历左子树
        preOrderRecursion(rt.getRChild(),list);  //遍历右子树
    }
}
