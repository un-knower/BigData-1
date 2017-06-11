package cn.Algorithm.stragegy;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 */
public class DefaultStrategy implements Strategy {
    public boolean equal(Object obj1, Object obj2) {
        return obj1.toString().equals(obj2.toString());
    }

    public int compare(Object obj1, Object obj2) {
        int compareTo = obj1.toString().compareTo(obj2.toString());
        if (compareTo == 0) {
            return 0;
        } else if (compareTo > 0) {
            return 1;
        } else {
            return -1;
        }
    }
}
