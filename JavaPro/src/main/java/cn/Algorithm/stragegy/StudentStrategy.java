package cn.Algorithm.stragegy;


import cn.Algorithm.element.Student;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 */
public class StudentStrategy implements Strategy {
    public boolean equal(Object obj1, Object obj2) {
        if (obj1 instanceof Student && obj2 instanceof Student) {
            Student s1 = (Student) obj1;
            Student s2 = (Student) obj2;
            return s1.getSid().equals(s2.getSid());
        } else {
            return false;
        }
    }

    public int compare(Object obj1, Object obj2) {
        if (obj1 instanceof Student && obj2 instanceof Student) {
            Student s1 = (Student) obj1;
            Student s2 = (Student) obj2;
            return s1.getSid().compareTo(s2.getSid());
        } else {
            return obj1.toString().compareTo(obj2.toString());
        }
    }
}
