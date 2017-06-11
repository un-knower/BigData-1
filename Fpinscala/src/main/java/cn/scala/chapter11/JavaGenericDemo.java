package cn.scala.chapter11;

import java.util.ArrayList;
import java.util.List;

public class JavaGenericDemo {
    public static void main(String[] args) {
        List<String> listStr = new ArrayList<>();
        List<Integer> listInteger = new ArrayList<>();
        //下面的代码会报错
        printAll(listInteger);
        printAll(listStr);

    }

    public static void  printAll(List<? super Integer> list){
       //具体代码逻辑
    }
}
