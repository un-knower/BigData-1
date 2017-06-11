package cn.scala.chapter11;
class Test{
}
public class JavaClassTypeDemo {
    public static void main(String[] args) {
        //获取String的类
        System.out.println("String.class="+String.class);
        //获取String对象的类型
        System.out.println("\"123\".getClass()="+"123".getClass());
        System.out.println("............................................");
        //获取自定义Test的类
        System.out.println("Test.class="+Test.class);
        //获取自定义Test对象的类型
        System.out.println("new Test().getClass()="+new Test().getClass());
    }
}
