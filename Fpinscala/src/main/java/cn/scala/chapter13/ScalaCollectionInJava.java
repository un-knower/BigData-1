package cn.scala.chapter12;
import scala.collection.JavaConversions;
import scala.collection.mutable.HashMap;
import scala.collection.mutable.Map;
/**
 * 在Java代码中调用Scala集合
 */
public class ScalaCollectionInJava {
    public static void main(String[] args) {
       Map<String,String> scalaBigDataTools=new HashMap<>();
        scalaBigDataTools.put("Hadoop","the most popular big data processing tools");
        scalaBigDataTools.put("Hive","the most popular interactive query tools");

        //scala.collection.mutable.Map不能使用Java语法中的for each语句对集合中的元素进行遍历
       /* for (String key: scalaBigDataTools.keySet()) {
            System.out.println(scalaBigDataTools.get(key));
        }*/

        //使用scala集合中提供的foreach方法，但需要自己实现相应的函数，处理方式较为复杂
       /* scalaBigDataTools.foreach(new Function1<Tuple2<String, String>, Object>(){

        });*/

        //将scala.collection.mutable.Map转换成java.util.Map
        java.util.Map<String,String> javaBigDataTools =JavaConversions.asJavaMap(scalaBigDataTools);
        //这里便可以使用for each语句对集合进行遍历
        for (String key: javaBigDataTools.keySet()) {
            System.out.println(javaBigDataTools.get(key));
        }
    }

}
