package cn.Algorithm.stragegy;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 */
public interface Strategy {
//    判断两个元素是否相等
    public boolean equal(Object obj1, Object obj2);

    /**
     * 比较两个数据元素的大小
     * 如果 obj1 < obj1 返回-1
     * 如果 obj1 = obj1 返回 0
     * 如果 obj1 > obj1 返回 1
     *
     */
    public int compare(Object obj1, Object obj2);
}
