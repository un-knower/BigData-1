package cn.scala.chapter12;
import cn.scala.chapter13.Student;
/**
 * Java中使用Scala泛型代码示例
 */
public class ScalaGenericInJava {
    public static void main(String[] args){
        Student<String,Integer> student=new Student<String,Integer>("小李",18);
        //Scala版本的getter方法
        System.out.println(student.name());
        //JavaBean版本的getter方法
        System.out.println(student.getName());
    }
}
