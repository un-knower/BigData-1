package cn.scala.chapter06;

/**
 * Created by 周志湖 on 2016/3/6.
 */
class Utils{
    public static Double PI=3.141529;
    public static String getName(){
        return "圆周率";
    }
}
public class JavaExample6_01 {
    public static void main(String[] args) {
        System.out.println(Utils.PI);
        System.out.println(Utils.getName());
    }
}
