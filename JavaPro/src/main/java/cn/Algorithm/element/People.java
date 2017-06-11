package cn.Algorithm.element;

/**
 * Created by HuShiwei on 2016/10/17 0017.
 */
public class People {
    private String name;
    private String id;

    public People() {
        this("", "");
    }
    public People(String name, String id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void sayHello() {
        System.out.println("Hello world");
    }

    public void sayHi() {
        System.out.println("Hi EveryOne");
    }
}
