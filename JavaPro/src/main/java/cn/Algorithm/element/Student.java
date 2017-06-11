package cn.Algorithm.element;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 */
public class Student extends People {
    private String sid;

    public Student() {
        this("","","");
    }
    public Student(String name, String id, String sid) {
        super(name,id);
        this.sid = sid;
    }

    public void sayHello() {
        super.sayHello();
        System.out.println("I am a student of department of computer science.");
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }
}
