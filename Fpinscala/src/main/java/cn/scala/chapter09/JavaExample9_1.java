package cn.scala.chapter09;

/**
 * Java Switch语句使用演示
 */
public class JavaExample9_1 {
    public static void main(String[] args) {
        for (int i = 1; i < 5 ; i++) {
            switch (i){
                //带break语句
                case 1: System.out.println("1");
                    break;
                //不带break语句，会意外陷入其它分支，同时输出2、3
                case 2: System.out.println("2");
                case 3: System.out.println("3");break;
                default:System.out.println("default");
                //case 语句后不能接表达式
                //case i%5==0: System.out.println("10");
            }
        }
    }
}
